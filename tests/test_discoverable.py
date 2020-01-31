import cloudsync


def test_discover():
    odp = cloudsync.get_provider("gdrive")

    import cloudsync_gdrive
    assert odp == cloudsync_gdrive.GDriveProvider
