"""Imported test suite"""

from cloudsync.tests import *

SHARED_FOLDER_RESP = {'id': 'fake_oid', 'name': 'shared-fold', 'mimeType': 'application/vnd.google-apps.folder', 'trashed': False,
        'shared': True, 'capabilities': {'canEdit': True}}

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
