"""Imported test suite"""

from cloudsync.sync.state import FILE, DIRECTORY

from cloudsync.tests import *

from cloudsync_gdrive import EventFilter

def test_event_filter(provider):
    # root not set
    assert not provider.root_path
    event = Event(FILE, "", "", "", True)
    assert provider._filter_event(event) == EventFilter.PROCESS
    event = Event(DIRECTORY, "", "", "", False)
    assert provider._filter_event(event) == EventFilter.PROCESS
    assert provider._filter_event(None) == EventFilter.IGNORE

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
        assert provider._filter_event(e) == EventFilter.IGNORE
        e = Event(FILE, "in-root", "/root/path2", "hash", True)
        assert provider._filter_event(e) == EventFilter.PROCESS
        e = Event(FILE, "in-root", None, None, False)
        assert provider._filter_event(e) == EventFilter.PROCESS
        e = Event(FILE, "out-root", "/path2", "hash", True)
        assert provider._filter_event(e) == EventFilter.IGNORE
        e = Event(FILE, "out-root", None, None, False)
        assert provider._filter_event(e) == EventFilter.IGNORE
        e = Event(FILE, "in-root", "/path2", "hash", True)
        assert provider._filter_event(e) == EventFilter.PROCESS
        e = Event(FILE, "out-root", "/root/path2", "hash", True)
        assert provider._filter_event(e) == EventFilter.PROCESS
        e = Event(DIRECTORY, "out-root", "/root/path2", "hash", True)
        assert provider._filter_event(e) == EventFilter.WALK

        with pytest.raises(ValueError):
            if provider._filter_event(e):
                log.info("this should throw")
