"""
Provider "gdrive", exports GDriveProvider
"""
# pylint: disable=missing-docstring, consider-using-f-string

import io
import time
import logging
import threading
import hashlib
from ssl import SSLError
import json
from typing import Generator, Optional, List, Dict, Any, Tuple
from unittest.mock import patch

import arrow
import google.oauth2.credentials
import google.auth.exceptions
from googleapiclient.discovery import build  # pylint: disable=import-error
from googleapiclient.errors import HttpError  # pylint: disable=import-error
from httplib2 import HttpLib2Error
from googleapiclient.http import _should_retry_response  # This is necessary because google masks errors
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload  # pylint: disable=import-error

from cloudsync.utils import debug_args, memoize, debug_sig
from cloudsync import Provider, OInfo, DIRECTORY, FILE, NOTKNOWN, Event, DirInfo, OType
from cloudsync.exceptions import CloudTokenError, CloudDisconnectedError, CloudFileNotFoundError, CloudTemporaryError, \
    CloudFileExistsError, CloudCursorError, CloudOutOfSpaceError
from cloudsync.oauth import OAuthConfig, OAuthError, OAuthProviderInfo

CACHE_QUOTA_TIME = 120


__version__ = "2.0.1"


class GDriveFileDoneError(Exception):
    pass


log = logging.getLogger(__name__)
logging.getLogger('googleapiclient').setLevel(logging.INFO)
logging.getLogger('googleapiclient.discovery').setLevel(logging.ERROR)


class GDriveInfo(DirInfo):  # pylint: disable=too-few-public-methods
    pids: List[str] = []
    # oid, hash, otype and path are included here to satisfy a bug in mypy,
    # which does not recognize that they are already inherited from the grandparent class
    oid: str
    hash: Any
    otype: OType
    path: str

    def __init__(self, *a, pids=None, **kws):
        super().__init__(*a, **kws)
        if pids is None:
            pids = []
        self.pids = pids


class GDriveProvider(Provider):  # pylint: disable=too-many-public-methods, too-many-instance-attributes
    case_sensitive = False
    default_sleep = 15
    large_file_size = 4 * 1024 * 1024
    upload_block_size = 4 * 1024 * 1024

    name = 'gdrive'
    _oauth_info = OAuthProviderInfo(
        auth_url='https://accounts.google.com/o/oauth2/v2/auth',
        token_url='https://accounts.google.com/o/oauth2/token',
        scopes=['https://www.googleapis.com/auth/drive',
                'https://www.googleapis.com/auth/drive.activity.readonly'
                ],
    )
    _redir = 'urn:ietf:wg:oauth:2.0:oob'
    _folder_mime_type = 'application/vnd.google-apps.folder'
    _io_mime_type = 'application/octet-stream'

    def __init__(self, oauth_config: Optional[OAuthConfig] = None):
        super().__init__()
        self.__root_id = None
        self.__cursor: Optional[str] = None
        self._creds = None
        self._client = None
        self._mutex = threading.Lock()
        self._ids: Dict[str, str] = {}
        self._trashed_ids: Dict[str, str] = {}
        self._oauth_config = oauth_config

    @property
    def connected(self):
        return self._client is not None

    @memoize(expire_secs=CACHE_QUOTA_TIME)
    def get_quota(self):
        res = self._api('about', 'get', fields='storageQuota, user')

        quota = res['storageQuota']
        user = res['user']
        permission_id = user['permissionId']
        login = user['emailAddress']

        used = int(quota['usage'])
        if 'limit' in quota and quota['limit']:
            limit = int(quota['limit'])
        else:
            # It is possible for an account to have unlimited space - pretend it's 1TB
            limit = 1024 * 1024 * 1024 * 1024
        maxup = int(quota.get('maxUploadSize', 0))

        return {
            "permissionId": permission_id,
            "used": used,
            "limit": limit,
            "login": login,
            "max_upload": maxup
        }

    def reconnect(self):
        self.connect(self._creds)

    def connect_impl(self, creds):
        log.debug('Connecting to googledrive')
        if not self._client or creds != self._creds:
            if creds:
                self._creds = creds
            else:
                raise CloudTokenError("no creds")

            refresh_token = creds and creds.get('refresh_token')

            if not refresh_token:
                raise CloudTokenError("acquire a token using authenticate() first")

            try:
                new = self._oauth_config.refresh(self._oauth_info.token_url, refresh_token,
                                                 scope=self._oauth_info.scopes)
                google_creds = google.oauth2.credentials.Credentials(new.access_token, new.refresh_token,
                                                                     scopes=self._oauth_info.scopes)
                self._client = build(
                    'drive', 'v3', credentials=google_creds, cache_discovery=False)
                try:
                    self.get_quota.clear()  # pylint: disable=no-member
                    quota = self.get_quota()
                except SSLError:  # pragma: no cover
                    # Seeing some intermittent SSL failures that resolve on retry
                    log.warning('Retrying intermittent SSLError')
                    quota = self.get_quota()
                self._creds = creds
                return quota['permissionId']
            except OAuthError as e:
                self.disconnect()
                raise CloudTokenError(repr(e))
            except CloudTokenError:
                self.disconnect()
                raise
            except Exception as e:
                raise CloudDisconnectedError(repr(e))
        return self.connection_id

    @staticmethod
    def _get_reason_from_http_error(e):
        # gets a default something (actually the message, not the reason) using their secret interface
        # noinspection PyProtectedMember
        reason = e._get_reason()  # pylint: disable=protected-access

        # parses the JSON of the content to get the reason from where it really lives in the content
        try:  # this code was copied from googleapiclient/http.py:_should_retry_response()
            data = json.loads(e.content.decode('utf-8'))
            if isinstance(data, dict):
                reason = data['error']['errors'][0]['reason']
            else:
                reason = data[0]['error']['errors']['reason']
        except (UnicodeDecodeError, ValueError, KeyError):
            log.warning('Invalid JSON content from response: %s', e.content)

        return reason

    @staticmethod
    def __escape(filename: str):
        ret = filename
        ret = ret.replace("\\", "\\\\")
        ret = ret.replace("'", "\\'")
        return ret

    def _api(self, resource, method, *args,
             **kwargs):  # pylint: disable=arguments-differ, too-many-branches, too-many-statements
        if not self._client:
            raise CloudDisconnectedError("currently disconnected")

        with self._mutex:
            try:
                if resource == 'media':
                    res = args[0]
                    args = args[1:]
                else:
                    res = getattr(self._client, resource)()

                meth = getattr(res, method)(*args, **kwargs)

                if resource == 'media' or (resource == 'files' and method == 'get_media'):
                    ret = meth
                else:
                    ret = meth.execute()
                log.debug("api: %s (%s) -> %s", method, debug_args(args, kwargs), ret)

                return ret
            except SSLError as e:
                if "WRONG_VERSION" in str(e):
                    # httplib2 used by google's api gives this weird error for no discernable reason
                    raise CloudTemporaryError(str(e))
                raise
            except google.auth.exceptions.RefreshError:
                self.disconnect()
                raise CloudTokenError("refresh error")
            except HttpError as e:
                log.debug("api: %s (%s) -> %s", method, debug_args(args, kwargs), e.resp.status)
                if str(e.resp.status) == '416':
                    raise GDriveFileDoneError()

                if str(e.resp.status) == '413':
                    raise CloudOutOfSpaceError('Payload too large')

                if str(e.resp.status) == '409':
                    raise CloudFileExistsError('Another user is modifying')

                if str(e.resp.status) == '404':
                    raise CloudFileNotFoundError('File not found when executing %s.%s(%s)' % debug_args(
                        resource, method, kwargs
                    ))

                reason = self._get_reason_from_http_error(e)

                if str(e.resp.status) == '403' and str(reason) == 'storageQuotaExceeded':
                    raise CloudOutOfSpaceError("Storage storageQuotaExceeded")

                if str(e.resp.status) == '401':
                    self.disconnect()
                    raise CloudTokenError("Unauthorized %s" % reason)

                if str(e.resp.status) == '403' and str(reason) == 'parentNotAFolder':
                    raise CloudFileExistsError("Parent Not A Folder")

                if str(e.resp.status) == '403' and str(reason) == 'insufficientFilePermissions':
                    raise PermissionError("PermissionError")

                if (str(e.resp.status) == '403' and reason in (
                        'userRateLimitExceeded', 'rateLimitExceeded', 'dailyLimitExceeded')) \
                        or str(e.resp.status) == '429':
                    raise CloudTemporaryError("rate limit hit")

                # At this point, _should_retry_response() returns true for error codes >=500, 429, and 403 with
                #  the reason 'userRateLimitExceeded' or 'rateLimitExceeded'. 403 without content, or any other
                #  response is not retried. We have already taken care of some of those cases above, but we call this
                #  below to catch the rest, and in case they improve their library with more conditions. If we called
                #  meth.execute() above with a num_retries argument, all this retrying would happen in the google api
                #  library, and we wouldn't have to think about retries.
                should_retry = _should_retry_response(e.resp.status, e.content)
                if should_retry:
                    raise CloudTemporaryError("unknown error %s" % e)
                log.error("Unhandled %s error %s", e.resp.status, reason)
                raise
            except (TimeoutError, HttpLib2Error):
                self.disconnect()
                raise CloudDisconnectedError("disconnected on timeout")
            except ConnectionResetError:
                raise CloudTemporaryError("An existing connection was forcibly closed by the remote host")

    @property
    def _root_id(self):
        if not self.__root_id:
            res = self._api('files', 'get',
                            fileId='root',
                            fields='id',
                            )
            self.__root_id = res['id']
            self._ids[self.sep] = self.__root_id
        return self.__root_id

    def disconnect(self):
        self._client = None
        # clear cached session info!
        self.get_quota.clear()  # pylint: disable=no-member

    @property
    def latest_cursor(self):
        res = self._api('changes', 'getStartPageToken')
        if res:
            return res.get('startPageToken')
        else:
            return None

    @property
    def current_cursor(self):
        if not self.__cursor:
            self.__cursor = self.latest_cursor
        return self.__cursor

    @current_cursor.setter
    def current_cursor(self, val):
        if val is None:
            val = self.latest_cursor
        if not isinstance(val, str) and val is not None:
            raise CloudCursorError(val)
        self.__cursor = val

    def events(self) -> Generator[Event, None, None]:  # pylint: disable=too-many-locals, too-many-branches
        page_token = self.current_cursor
        while page_token is not None:
            # log.debug("looking for events, timeout: %s", timeout)
            response = self._api('changes', 'list', pageToken=page_token, spaces='drive',
                                 includeRemoved=True, includeItemsFromAllDrives=True, supportsAllDrives=True)
            new_cursor = response.get('newStartPageToken', None)
            for change in response.get('changes'):
                log.debug("got event %s", change)
                event = self._convert_to_event(change, new_cursor)
                log.debug("converted event %s as %s", change, event)
                yield event

            if new_cursor and page_token and new_cursor != page_token:
                self.__cursor = new_cursor
            page_token = response.get('nextPageToken')

    def _convert_to_event(self, change, new_cursor):
        ts = arrow.get(change.get('time')).float_timestamp
        oid = change.get('fileId')
        # File is removed: exists is False. Else: the file may be trashed, mark exists as None
        exists = None
        if change.get('removed') is True:
            exists = False

        fil = change.get('file')
        if fil:
            if fil.get('mimeType') == self._folder_mime_type:
                otype = DIRECTORY
            else:
                otype = FILE
        else:
            otype = NOTKNOWN

        ohash = None
        path = None

        event = Event(otype, oid, path, ohash, exists, ts, new_cursor=new_cursor)

        remove = []
        for cpath, coid in self._ids.items():
            if coid == oid:
                if cpath != path:
                    remove.append(cpath)  # remove the event's item if it's path changed

            if path and otype == DIRECTORY and self.is_subpath(path, cpath):
                remove.append(cpath)  # if the event's item is a folder, uncache its children

        for r in remove:
            self._ids.pop(r, None)

        if path:
            self._ids[path] = oid

        return event

    def _prep_upload(self, path, metadata):
        # modification time
        mtime = metadata.get("modifiedTime", time.time())
        mtime = arrow.get(mtime).isoformat()
        gdrive_info = {
            'modifiedTime': mtime
        }

        # mime type, if provided
        mime_type = metadata.get("mimeType", None)
        if mime_type:
            gdrive_info['mimeType'] = mime_type

        # path, if provided
        if path:
            _, name = self.split(path)
            gdrive_info['name'] = name

        # misc properties, if provided
        app_props = metadata.get("appProperties", None)
        if app_props:
            # caller can specify google-specific stuff, if desired
            gdrive_info['appProperties'] = app_props

        # misc properties, if provided
        app_props = metadata.get("properties", None)
        if app_props:
            # caller can specify google-specific stuff, if desired
            gdrive_info['properties'] = app_props

        log.debug("info %s", gdrive_info)

        return gdrive_info

    def _media_io(self, file_like) -> Tuple[MediaIoBaseUpload, int]:
        file_like.seek(0, io.SEEK_END)
        file_size = file_like.tell()
        file_like.seek(0, io.SEEK_SET)

        chunksize = self.upload_block_size
        resumable = file_size > chunksize
        return MediaIoBaseUpload(file_like, mimetype=self._io_mime_type, chunksize=chunksize,
                                 resumable=resumable), file_size

    def upload(self, oid, file_like, metadata=None) -> 'OInfo':
        if not metadata:
            metadata = {}
        gdrive_info = self._prep_upload(None, metadata)
        ul, size = self._media_io(file_like)

        fields = 'id, md5Checksum, modifiedTime'

        try:
            def api_call():
                return self._api('files', 'update',
                                 body=gdrive_info,
                                 fileId=oid,
                                 media_body=ul,
                                 fields=fields)
            if self._client:
                with patch.object(self._client._http.http, "follow_redirects", False):  # pylint: disable=protected-access
                    res = api_call()
            else:
                res = api_call()
        except OSError as e:
            self.disconnect()
            raise CloudDisconnectedError("OSError in file upload: %s" % repr(e))

        log.debug("response from upload %s", res)

        if not res:
            raise CloudTemporaryError("unknown response from drive on upload")

        mtime = res.get('modifiedTime')
        mtime = mtime and self._parse_time(mtime)

        md5 = res.get('md5Checksum', None)  # can be none if the user tries to upload to a folder
        if md5 is None:
            possible_conflict = self._info_oid(oid)
            if possible_conflict and possible_conflict.otype == DIRECTORY:
                raise CloudFileExistsError("Can only upload to a file: %s" % possible_conflict.path)

        return OInfo(otype=FILE, oid=res['id'], hash=md5, path=None, size=size, mtime=mtime)

    def create(self, path, file_like, metadata=None) -> 'OInfo':
        if not metadata:
            metadata = {}
        
        if self.exists_path(path):
            raise CloudFileExistsError()

        ul, size = self._media_io(file_like)

        fields = 'id, md5Checksum, size, modifiedTime'

        # Cache is accurate, just refreshed from exists_path() call
        parent_oid = self._get_parent_id(path, use_cache=True)
        metadata['appProperties'] = self._prep_app_properties(parent_oid)
        gdrive_info = self._prep_upload(path, metadata)
        gdrive_info['parents'] = [parent_oid]

        try:
            def api_call():
                return self._api('files', 'create',
                                 body=gdrive_info,
                                 media_body=ul,
                                 fields=fields)
            if self._client:
                with patch.object(self._client._http.http, "follow_redirects", False):  # pylint: disable=protected-access
                    res = api_call()
            else:
                res = api_call()
        except OSError as e:
            self.disconnect()
            raise CloudDisconnectedError("OSError in file create: %s" % repr(e))

        log.debug("response from create %s : %s", path, res)

        if not res:
            raise CloudTemporaryError("unknown response from drive on upload")

        self._ids[path] = res['id']

        log.debug("path cache %s", self._ids)

        size = int(res.get("size", 0))
        mtime = res.get('modifiedTime')
        mtime = mtime and self._parse_time(mtime)

        cache_ent = self.get_quota.get()  # pylint: disable=no-member
        if cache_ent:
            cache_ent["used"] += size

        return OInfo(otype=FILE, oid=res['id'], hash=res['md5Checksum'], path=path, size=size, mtime=mtime)

    def download(self, oid, file_like):
        req = self._api('files', 'get_media', fileId=oid)
        dl = MediaIoBaseDownload(file_like, req, chunksize=4 * 1024 * 1024)
        done = False
        while not done:
            try:
                _, done = self._api('media', 'next_chunk', dl)
            except GDriveFileDoneError:
                done = True

    def rename(self, oid, path):  # pylint: disable=too-many-locals, too-many-branches
        # Use cache to get parent id, no need to hit info_path twice
        possible_conflict = self.info_path(path)
        pid = self._get_parent_id(path, use_cache=True)

        add_pids = [pid]
        if pid == 'root':  # pragma: no cover
            # cant ever get hit from the tests due to test root
            add_pids = [self._root_id]

        info = self._info_oid(oid)
        if info is None:
            log.debug("can't rename, oid doesn't exist %s", debug_sig(oid))
            raise CloudFileNotFoundError(oid)
        remove_pids = info.pids
        old_path = info.path

        _, name = self.split(path)
        appProperties = self._prep_app_properties(pid)
        body = {'name': name, 'appProperties': appProperties}

        if possible_conflict:
            if FILE in (info.otype, possible_conflict.otype):
                if possible_conflict.oid != oid:  # it's OK to rename a file over itself, frex, to change case
                    raise CloudFileExistsError(path)
            else:
                if possible_conflict.oid != oid:
                    try:
                        next(self.listdir(possible_conflict.oid))
                        raise CloudFileExistsError("Cannot rename over non-empty folder %s" % path)
                    except StopIteration:
                        # Folder is empty, rename over it no problem
                        if possible_conflict.oid != oid:  # delete the target if we're not just changing case
                            self.delete(possible_conflict.oid)

        if not old_path:
            for cpath, coid in list(self._ids.items()):
                if coid == oid:
                    old_path = cpath

        if add_pids == remove_pids:
            add_pids_str = ""
            remove_pids_str = ""
        else:
            add_pids_str = ",".join(add_pids)
            remove_pids_str = ",".join(remove_pids)

        self._api('files', 'update', body=body, fileId=oid, addParents=add_pids_str, removeParents=remove_pids_str,
                  fields='id')

        if old_path:
            # TODO: this will break if the kids are cached but not the parent folder, I'm not convinced that can
            #   actually be the case at this point in the code, so, no need to fix until that can be established
            for cpath, coid in list(self._ids.items()):
                relative = self.is_subpath(old_path, cpath)
                if relative:
                    new_cpath = self.join(path, relative)
                    self._ids.pop(cpath)
                    self._ids[new_cpath] = coid

        log.debug("renamed %s -> %s", debug_sig(oid), body)

        return oid

    def listdir(self, oid) -> Generator[GDriveInfo, None, None]:  # pylint: disable=too-many-branches, too-many-locals
        if oid == self._root_id:
            query = f"'{oid}' in parents or sharedWithMe"
        else:
            query = f"'{oid}' in parents"
        page_token = None
        done = False
        while not done:
            try:
                res = self._api('files', 'list',
                                q=query,
                                spaces='drive',
                                fields='files(id, md5Checksum, parents, name, mimeType, trashed, shared, \
                                headRevisionId, capabilities, appProperties, modifiedTime, size), nextPageToken',
                                pageToken=page_token,
                                includeItemsFromAllDrives=True,
                                supportsAllDrives=True
                                )
                page_token = res.get('nextPageToken', None)
                if not page_token:
                    done = True
            except CloudFileNotFoundError:
                if self._info_oid(oid):
                    return
                log.debug("listdir oid gone %s", oid)
                raise

            if not res or not res['files']:
                if self.exists_oid(oid):
                    return
                raise CloudFileNotFoundError(oid)

            log.debug("listdir got res %s", res)

            for ent in res['files']:
                fid = ent['id']
                if fid == oid:
                    continue
                pids = ent.get('parents', [])
                if not pids and ent.get('shared'):
                    pids = self._resolve_missing_parent(ent) 
                fhash = ent.get('md5Checksum')
                name = ent['name']
                shared = ent['shared']
                readonly = not ent['capabilities']['canEdit']
                trashed = ent.get('trashed', False)
                mtime = ent.get('modifiedTime')
                mtime = mtime and self._parse_time(mtime)
                size = int(ent.get('size', 0))
                if ent.get('mimeType') == self._folder_mime_type:
                    otype = DIRECTORY
                else:
                    otype = FILE
                if not trashed:
                    yield GDriveInfo(otype, fid, fhash, None, shared=shared, readonly=readonly, pids=pids, name=name,
                                     mtime=mtime, size=size)

    def mkdir(self, path, metadata=None) -> str:  # pylint: disable=arguments-differ
        info = self.info_path(path)
        if info:
            if info.otype == FILE:
                raise CloudFileExistsError(path)
            log.debug("Skipped creating already existing folder: %s", path)
            return info.oid

        # Cache is accurate, just refreshed from info_path call
        pid = self._get_parent_id(path, use_cache=True)
        _, name = self.split(path)

        appProperties = self._prep_app_properties(pid)
        file_metadata = {
            'name': name,
            'parents': [pid],
            'mimeType': self._folder_mime_type,
            'appProperties': appProperties
        }
        if metadata:
            file_metadata.update(metadata)
        res = self._api('files', 'create',
                        body=file_metadata, fields='id')
        fileid = res.get('id')
        self._ids[path] = fileid
        return fileid

    # noinspection DuplicatedCode
    def delete(self, oid):
        info = self._info_oid(oid)
        if not info:
            log.debug("deleted non-existing oid %s", debug_sig(oid))
            return  # file doesn't exist already...
        if info.otype == DIRECTORY:
            try:
                next(self.listdir(oid))
                raise CloudFileExistsError("Cannot delete non-empty folder %s:%s" % (oid, info.name))
            except StopIteration:
                pass  # Folder is empty, delete it no problem
        if oid == self._root_id:
            raise CloudFileExistsError("Cannot delete root folder")
        try:
            self._api('files', 'delete', fileId=oid)
        except CloudFileNotFoundError:
            log.debug("deleted non-existing oid %s", debug_sig(oid))
        except PermissionError:
            try:
                log.debug("permission denied deleting %s:%s, unfile instead", debug_sig(oid), info.name)
                remove_str = ",".join(info.pids)
                self._api('files', 'update', fileId=oid, removeParents=remove_str, fields='id')
            except PermissionError:
                log.warning("Unable to delete oid %s.", debug_sig(oid))

        path = self._path_oid(oid, info=info)
        self._uncache(path)

    def _uncache(self, path):
        oid = self._cached_id(path)
        if oid is None:
            return
        for currpath, curroid in list(self._ids.items()):
            if curroid == oid:
                self._trashed_ids[currpath] = self._ids[currpath]
                del self._ids[currpath]
            elif self.is_subpath(path, currpath):
                self._ids.pop(currpath)

    def exists_oid(self, oid):
        return self._info_oid(oid) is not None

    def info_path(self, path: str, use_cache=True) -> Optional[OInfo]:  # pylint: disable=too-many-locals, too-many-branches
        if path == self.sep:
            self._ids[self.sep] = self._root_id
            return self.info_oid(self._root_id)

        try:
            parent_id = self._get_parent_id(path)
            _, name = self.split(path)

            escaped_name = self.__escape(name)
            if parent_id == self._root_id:
                query = f"(sharedWithMe or '{parent_id}' in parents) and name='{escaped_name}'"
            else:
                query = f"'{parent_id}' in parents and name='{escaped_name}'"

            res = self._api('files', 'list',
                            q=query,
                            spaces='drive',
                            fields='files(id, md5Checksum, parents, mimeType, trashed, name, shared, \
                            headRevisionId, capabilities, appProperties, modifiedTime, size)',
                            pageToken=None,
                            includeItemsFromAllDrives=True,
                            supportsAllDrives=True)
        except CloudFileNotFoundError:
            return None

        if not res['files']:
            if use_cache:  # double check against the cache -- google sometimes lies
                alt_oid = self._cached_id(path)
                if alt_oid is not None:
                    alt_info = self.info_oid(alt_oid)
                    if alt_info is not None and alt_info.path == path:
                        log.error("gdrive misreported NotFound for %s, it actually does exist")
                        return alt_info
                else:  # Turns out the cache was wrong, according to info_oid. Clear the cache entry for path
                    self._uncache(path)
            return None

        ent = res['files'][0]

        if ent.get('trashed'):
            # TODO:
            # need to write a tests that moves files to the trash, as if a user moved the file to the trash
            # then assert it shows up "file not found" in all queries
            return None

        oid = ent['id']
        pids = ent.get('parents')
        if not pids and res.get('shared'):
            pids = self._resolve_missing_parent(ent)

        fhash = ent.get('md5Checksum')
        name = ent.get('name')
        size = int(ent.get('size', 0))
        mtime = ent.get('modifiedTime')
        mtime = mtime and self._parse_time(mtime)

        # query is insensitive to certain features of the name
        # ....cache correct basename
        path = self.join(self.dirname(path), name)

        shared = ent['shared']
        readonly = not ent['capabilities']['canEdit']
        if ent.get('mimeType') == self._folder_mime_type:
            otype = DIRECTORY
        else:
            otype = FILE

        self._ids[path] = oid

        return GDriveInfo(otype, oid, fhash, path, shared=shared, readonly=readonly, name=name, pids=pids, size=size, mtime=mtime)

    def exists_path(self, path) -> bool:
        if self._cached_id(path):
            return True
        return self.info_path(path) is not None

    def _cached_id(self, path):
        if path == self.sep:
            return self._root_id
        else:
            return self._ids.get(path)

    def _get_parent_id(self, path, use_cache=False):
        if not path:
            return None

        parent, _ = self.split(path)

        if parent == path:
            return self._cached_id(parent)

        cached_id = self._cached_id(parent)
        if use_cache and cached_id:
            return cached_id

        # get the latest version of the parent path
        # it may have changed, or case may be different, etc.
        info = self.info_path(parent)
        if not info:
            raise CloudFileNotFoundError("parent %s must exist" % parent)

        # cache the latest version
        return self._cached_id(info.path)

    def _path_oid(self, oid, info=None, use_cache=True) -> Optional[str]:
        """convert oid to path"""
        if use_cache:
            for p, pid in self._ids.items():
                if pid == oid:
                    return p

            for p, pid in self._trashed_ids.items():
                if pid == oid:
                    return p

        if oid == self._root_id:
            return "/"

        # todo, better cache, keep up to date, etc.

        if not info:
            info = self._info_oid(oid)

        if info and info.pids and info.name:
            ppath = self._path_oid(info.pids[0])
            if ppath:
                path = self.join(ppath, info.name)
                self._ids[path] = oid
                return path
        return None

    def info_oid(self, oid: str, use_cache=True) -> Optional[GDriveInfo]:
        info = self._info_oid(oid)
        if info is None:
            return None
        # expensive
        info.path = self._path_oid(oid, info, use_cache=use_cache)
        return info

    # noinspection PyMethodOverriding
    @staticmethod
    def hash_data(file_like) -> str:
        # get a hash from a filelike that's the same as the hash i natively use
        md5 = hashlib.md5()
        for c in iter(lambda: file_like.read(32768), b''):
            md5.update(c)
        return md5.hexdigest()

    def _info_oid(self, oid) -> Optional[GDriveInfo]:
        try:
            res = self._api('files', 'get', fileId=oid, supportsAllDrives=True,
                            fields='name, md5Checksum, parents, mimeType, trashed, shared, \
                            headRevisionId, capabilities, appProperties, size, modifiedTime')
        except CloudFileNotFoundError:
            log.debug("info oid %s : not found", oid)
            if oid == self.__root_id:
                # Root id is stale, refresh
                self.__root_id = None
                new_root_id = self._root_id
                # prevent infinite recursion
                if new_root_id != oid:
                    return self._info_oid(new_root_id)

            return None

        log.debug("info oid %s", res)
        if res.get('trashed'):  # TODO: cache this result
            return None

        pids = res.get('parents')
        if not pids and res.get('shared'):
            pids = self._resolve_missing_parent(res)
        fhash = res.get('md5Checksum')
        name = res.get('name')
        shared = res['shared']
        size = int(res.get("size", 0))

        mtime = res.get('modifiedTime')
        mtime = mtime and self._parse_time(mtime)
        readonly = not res['capabilities']['canEdit']
        if res.get('mimeType') == self._folder_mime_type:
            otype = DIRECTORY
        else:
            otype = FILE

        return GDriveInfo(otype, oid, fhash, None, shared=shared, readonly=readonly, pids=pids, name=name, size=size, mtime=mtime)

    @classmethod
    def test_instance(cls):
        return cls.oauth_test_instance(prefix="GDRIVE", token_sep=",")

    def _clear_cache(self, *, oid=None, path=None):
        if oid is None and (path is None or path == '/'):
            self._ids = {}
            self._trashed_ids = {}
        else:
            for coid, cpath in list(self._ids.items()):
                if coid == oid or self.is_subpath(path, cpath):
                    self._ids.pop(cpath)
            for coid, cpath in list(self._trashed_ids.items()):
                if coid == oid or self.is_subpath(path, cpath):
                    self._ids.pop(cpath)
        return True

    def _prep_app_properties(self, parent_oid):
        if not parent_oid:
            return {}
        
        # Don't propagate pids for top level folders
        return {} if parent_oid == self._root_id else {'pid': parent_oid}

    def _resolve_missing_parent(self, res):
        # Check if there is a pid in app metadata, res is a gdrive Files resource
        appProperties = res.get('appProperties', {})
        meta_pid = appProperties.get('pid', None)
        if meta_pid:
            return [meta_pid]
        else:
            # shared folders without a parent should be interpreted as children of root
            return [self._root_id]

    @staticmethod
    def _parse_time(rfc3339_time_str):
        try:
            ret_val = arrow.get(rfc3339_time_str).timestamp
        except Exception as e:
            log.error("could not convert rfc3339 formatted time string '%s' to timestamp: %s", rfc3339_time_str, e)
            ret_val = 0
        return ret_val

__cloudsync__ = GDriveProvider
