
def test_uncurl_progress_reporting_to_annex(existing_dataset, monkeypatch):
    """Set up a repo that is used to download a key,
    check that we see progress reports
    """
    repo = existing_dataset.repo
    # enable uncurl to get a datalad code piece generate progress info
    repo.call_annex([
        'initremote',
        'uncurl',
        'type=external',
        'externaltype=uncurl',
        'encryption=none',
    ])
    # 1.7MB download, should never change
    testfilekey = 'MD5E-s1725572--3f9f0f5c05517686c008115a611586b1.zip'
    testfileurl = \
        'https://github.com/datalad/datalad/archive/refs/tags/0.18.3.zip'
    testfilename = 'datalad.zip'

    # register the key in the dataset with the source URL
    repo.call_annex(['registerurl', testfilekey, testfileurl])

    # record the key to be available from uncurl
    uncurl_uuid = repo.call_annex_records(['info', 'uncurl'])[0]['uuid']
    repo.call_annex(['setpresentkey', testfilekey, uncurl_uuid, '1'])

    # place key in worktree (not strictly required, but a more common setup)
    repo.call_annex(['fromkey', '--force', testfilekey, testfilename])

    # intercept progress logs in this process.  in order for progress reports
    # to appear here, uncurl needs to report them to git-annex, and our runner
    # setup needs to catch them and call `log_progress`. So this really is an
    # end-to-end test.

    logs = []

    # patch the log_progress() used in annexrepo.py
    def catch_progress(*args, **kwargs):
        logs.append(kwargs)

    import datalad.support.annexrepo
    monkeypatch.setattr(
        datalad.support.annexrepo,
        "log_progress",
        catch_progress,
    )

    # trigger a download. use git-annex directly such that there is
    # little chance that the uncurl remote process is talking to a
    # datalad parent process directly
    repo._call_annex_records(
        args=['get'],
        files=[testfilename],
        progress=True,
        total_nbytes=1725572,
    )
    # check that we got the progress init report
    assert any('total' in log for log in logs)
    # and at least one progress update -- do not check for more, because
    # on fast systems this may take very little time
    assert any('update' in log for log in logs)
