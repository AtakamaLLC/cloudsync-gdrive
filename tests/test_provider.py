"""Imported test suite"""
import io
import datetime

from cloudsync.sync.state import FILE, DIRECTORY
from cloudsync.tests import *
from cloudsync_gdrive import EventFilter as GDriveEventFilter

SHARED_FOLDER_RESP = {'id': 'fake_oid', 'name': 'shared-fold', 'mimeType': 'application/vnd.google-apps.folder', 'trashed': False,
        'shared': True, 'capabilities': {'canEdit': True}}

# Trash a given oid
# Note, the provider will not receive an event for this action
def trash_oid(provider, oid):
    gdrive_info = provider._prep_upload(None, {})
    gdrive_info['trashed'] = True

    def api_call():
        return provider._api('files', 'update',
                         body=gdrive_info,
                         fileId=oid)

    if provider._client:
        with patch.object(provider._client._http.http, "follow_redirects", False):  # pylint: disable=protected-access
            res = api_call()
    else:
        res = api_call()

    return res

def test_trashed_files_folders(provider):
    base = provider.temp_name("base_fold")
    file_name1 = provider.join(base, provider.temp_name("file1.txt"))
    file_name2 = provider.join(base, provider.temp_name("file2.txt"))
    fold_name1 = provider.join(base, provider.temp_name("fold1"))
    fold_name2 = provider.join(base, provider.temp_name("fold2"))

    base_oid = provider.mkdir(base)
    file1_oid = provider.create(file_name1, io.BytesIO(b"hello")).oid
    file2_oid = provider.create(file_name2, io.BytesIO(b"world")).oid
    fold1_oid = provider.mkdir(fold_name1)
    fold2_oid = provider.mkdir(fold_name2)

    trash_oid(provider, file1_oid)
    trash_oid(provider, fold1_oid)

    assert not provider.info_oid(file1_oid)
    assert provider.info_oid(file2_oid)
    assert not provider.info_oid(fold1_oid)
    assert provider.info_oid(fold2_oid)

    # Listdir should ignore trashed files
    listdir_oids = [e.oid for e in list(provider.listdir(base_oid))]
    assert not file1_oid in listdir_oids
    assert file2_oid in listdir_oids
    assert not fold1_oid in listdir_oids
    assert fold2_oid in listdir_oids

    # This is necessary to mimic the event received from the gdrive api if
    # this folder were to be trashed from the web gui or from another user
    api = provider._api
    def patched_api(resource, method, *args, **kwargs):
        if resource == 'changes' and method == 'list':
            return { 'changes' : [{ 'fileId': fold1_oid, 'time': datetime.datetime.now(),
                'file': { 'mimeType': provider._folder_mime_type }, 'removed': False}]}
        else:
            return api(resource, method, *args, **kwargs)

    with patch.object(provider, "_api", side_effect=patched_api):
        events = list(provider.events())

    assert len(events) == 1
    # Mark trashed folder non existant
    assert not events[0].exists

def test_shared_folder_pids(provider):
    _api = provider._api
    def _mock_api(resource, method, *args, **kwargs):
        if resource == 'files' and method == 'list':
            return {'files': [SHARED_FOLDER_RESP]}
        elif resource == 'files' and method == 'get':
            return SHARED_FOLDER_RESP
        else:
            return _api(resource, method, *args, **kwargs)

    with patch.object(provider, "_api", side_effect=_mock_api):
        # Avoid __filter_root in mixin, test_root can't be in path
        listdir_res = list(provider.prov.listdir(provider._root_id))
        info_oid_res = provider.info_oid('fake_oid')

    assert len(listdir_res) == 1
    assert provider._root_id in listdir_res[0].pids
    assert provider._root_id in info_oid_res.pids

def test_event_filter(provider):
    # root not set
    assert not provider.root_path
    event = Event(FILE, "", "", "", True)
    assert provider._filter_event(event) == GDriveEventFilter.PROCESS
    event = Event(DIRECTORY, "", "", "", False)
    assert provider._filter_event(event) == GDriveEventFilter.PROCESS
    assert provider._filter_event(None) == GDriveEventFilter.IGNORE

    class MockSyncState:
        def get_path(self, oid):
            if oid == "in-root":
                return "/root/path"
            elif oid == "out-root":
                return "/path"
            else:
                return None

    # root set
    with patch.multiple(provider.prov, _root_oid="root_oid", _root_path="/root", sync_state=MockSyncState()):
        e = Event(FILE, "", "", "", False)
        assert provider._filter_event(e) == GDriveEventFilter.IGNORE
        e = Event(FILE, "in-root", "/root/path2", "hash", True)
        assert provider._filter_event(e) == GDriveEventFilter.PROCESS
        e = Event(FILE, "in-root", None, None, False)
        assert provider._filter_event(e) == GDriveEventFilter.PROCESS
        e = Event(FILE, "out-root", "/path2", "hash", True)
        assert provider._filter_event(e) == GDriveEventFilter.IGNORE
        e = Event(FILE, "out-root", None, None, False)
        assert provider._filter_event(e) == GDriveEventFilter.IGNORE
        e = Event(FILE, "in-root", "/path2", "hash", True)
        assert provider._filter_event(e) == GDriveEventFilter.PROCESS
        e = Event(FILE, "out-root", "/root/path2", "hash", True)
        assert provider._filter_event(e) == GDriveEventFilter.PROCESS
        e = Event(DIRECTORY, "out-root", "/root/path2", "hash", True)
        assert provider._filter_event(e) == GDriveEventFilter.WALK

        with pytest.raises(ValueError):
            if provider._filter_event(e):
                log.info("this should throw")
