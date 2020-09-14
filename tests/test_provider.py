"""Imported test suite"""

from cloudsync.sync.state import FILE, DIRECTORY
from cloudsync.tests import *
from cloudsync_gdrive import EventFilter as GDriveEventFilter

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
