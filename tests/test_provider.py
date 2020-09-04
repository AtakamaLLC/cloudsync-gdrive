"""Imported test suite"""
import io

from cloudsync.tests import *


def trash_oid(provider, oid):
    gdrive_info = provider._prep_upload(None, {})
    gdrive_info['trashed'] = True
    fields = 'id, md5Checksum, trashed'

    def api_call():
        return provider._api('files', 'update',
                         body=gdrive_info,
                         fileId=oid,
                         fields=fields)

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
