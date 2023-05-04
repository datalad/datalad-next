from pathlib import (
    Path,
    PurePosixPath,
)

from datalad.api import clone

from datalad_next.datasets import Dataset
from datalad_next.url_operations.file import FileUrlOperations

from datalad_next.utils import on_windows

from datalad_next.tests.utils import assert_result_count


nonoise = dict(result_renderer='disabled')


def make_archive_dataset(wpath):
    """Returns a path to generated dataset

    This dataset references an annex archive with no other annex files.
    The datalad special remote 'archivist' is enabled in the dataset and
    also set to autoenable.

    Returns
    -------
    Dataset, str, str, tuple(tuples)
      1. generated dataset instance
      2. the annex key for the included archive
      3. the leading directory of all files in the archive
      4. iterable with POSIX-path:content pairs for archive members.
         The path is relative to the leading archive directory, and
         can also be interpreted relative to the dataset root.
    """
    dscontent = (
        ('azip/file1.txt', 'zipfile1'),
        ('azip/file2.csv', 'zipfile2_muchcontent'),
        ('atar/file1.txt', 'tarfile1'),
        ('atar/file2.csv', 'tarfile2_muchcontent'),
    )
    srcds = Dataset(wpath / 'srcds').create(**nonoise)
    for fpath, fcontent in dscontent:
        fpath = srcds.pathobj / (PurePosixPath(fpath))
        fpath.parent.mkdir(parents=True, exist_ok=True)
        fpath.write_text(fcontent)
    srcds.save(**nonoise)

    # now create a second dataset that can pull all its content from
    # archives
    ads = Dataset(wpath / 'archiveds').create(**nonoise)
    # configure the archivist special remote for all dl_archive URL
    # handling
    ads.repo.call_annex([
        'initremote', 'archivist',
        'type=external', 'externaltype=archivist', 'encryption=none',
        'autoenable=true',
    ])

    archive_root = 'myarchive'
    archivetype = 'zip'

    akeys = {}

    for archivetype, ext in (('zip', ''), ('tar', '.gz')):
        archive_path = wpath / f"{archive_root}.{archivetype}{ext}"

        archive_path_inds = ads.pathobj / '.archives' / archive_path.name
        # create an archive, the easy way, by simply exporting the
        # entire dataset worktree
        srcds.export_archive(wpath / archive_root, archivetype=archivetype,
                             **nonoise)
        assert archive_path.exists()

        # add the archive (in a hidden dir) to be able to reference
        # it via a key
        aurl = archive_path.as_uri()
        if on_windows:
            # get annex does not like pathlib's file URLs
            # and requires a specific flavor (on windows)
            aurl = aurl.replace('file:///', 'file://')
        ads.repo.call_annex([
            'addurl', '--file', str(archive_path_inds), aurl])
        ads.save(**nonoise)
        # get the key of the archive
        akeys[archivetype] = ads.status(
            archive_path_inds, annex='basic', return_type='item-or-list',
            **nonoise)['key']
    return ads, akeys, archive_root, dscontent


def _check_archivist_addurl(atypes, ads, akeys, archive_root, dscontent):
    # run addurl on dl+archive URLs: this exercises CLAIMURL, CHECKURL,
    # TRANSFER_RETRIEVE
    for archivetype in atypes:
        for fpath, fcontent in dscontent:
            # take individual files from archives of different types
            if not fpath.startswith(f'a{archivetype}'):
                continue
            ads.repo.call_annex([
                'addurl',
                '--file',
                str(PurePosixPath(fpath)),
                f'dl+archive:{akeys[archivetype]}'
                f'#path={archive_root}/{fpath}&size={len(fcontent)}',
            ])
    # check that we reached the desired state.
    # the `str(Path())` construct is needed, because the dreaded
    # @normalize_paths
    # https://github.com/datalad/datalad/issues/4595#issuecomment-1406201397
    whereis = ads.repo.whereis(str(Path('azip', 'file1.txt')), output='full')
    # the file is known to exactly one remote (besides "here")
    assert len(whereis) == 2
    # and one remote is the archivist remote, and importantly not the
    # 'web' remote -- which would indicate a failure of claimurl/checkurl
    assert any(wi['description'] == '[archivist]' for wi in whereis.values())


def test_archivist_retrieval_and_checkpresent(tmp_path):
    ads, akeys, archive_root, dscontent = make_archive_dataset(
        tmp_path / 'src')

    # step 1: addurl
    _check_archivist_addurl(
        # check all archive types supported by FSSPEC
        ('zip', 'tar'),
        ads, akeys, archive_root, dscontent,
    )

    # make a clean dataset
    ads.save(**nonoise)

    # step 2: drop keys with dl+archive: URLs
    # now drop all archive member content. this should work,
    # because for each file there is a URL on record
    # -- hence always another copy
    # this requires archivist's CHECKPRESENT to function properly
    res = ads.drop(['azip', 'atar'], **nonoise)
    assert_result_count(
        res,
        # all files, plus the two directories we gave as arguments
        len(dscontent) + 2,
        action='drop',
        status='ok',
    )

    # step 3: retrieve keys with dl+archive: URLs from locally present archives
    res = ads.get(['azip', 'atar'], **nonoise)
    assert_result_count(
        res,
        len(dscontent),
        action='get',
        status='ok',
        type='file',
    )
    for fpath, fcontent in dscontent:
        assert (ads.pathobj / fpath).read_text() == fcontent

    # step 3: now drop ALL keys (incl. archives)
    # this will present a challenge for CHECKPRESENT:
    # without the archives no longer being around, it would requires remote
    # access or download to actually verify continued presence.
    # force this condition by dropping the archive keys first
    res = ads.drop('.archives', **nonoise)
    assert_result_count(
        res,
        # a tar and a zip
        2,
        action='drop',
        type='file',
        status='ok',
    )
    # and now keys that have their content from archives
    # this makes sure that CHECKPRESENT can handle the case of source
    # archives that are not present locally right now
    res = ads.drop('azip', **nonoise)
    assert_result_count(
        res,
        len(dscontent) / 2,
        action='drop',
        status='ok',
        type='file',
    )
    # but now we are removing the tar archive from its only known
    # remote source.
    wi = list(ads.repo.whereis(akeys['tar'], output='full', key=True).values())
    assert len(wi) == 1
    wi_urls = wi[0]['urls']
    assert len(wi_urls) == 1
    uh = FileUrlOperations()
    uh.delete(wi_urls[0])
    # and we should no longer be able to drop the files that have their
    # only source be this TAR archive
    assert_result_count(
        ads.drop('atar', on_failure='ignore', **nonoise),
        2,
        action='drop',
        status='error',
        type='file',
    )
    # and we can override that safety
    assert_result_count(
        ads.drop('atar', reckless='availability', **nonoise),
        2,
        action='drop',
        status='ok',
        type='file',
    )


def test_archivist_urlkey(tmp_path):
    """This tests adding a file key pointing inside an archive key without
    ever performing a full archive download (when fsspec) is around).

    The containing archive is only registered via a URL key.

    NOTE: This test relies on zenodo.org to be up and cooperative. However, it
    seems to monitor access frequency to individual records and may play dead
    from time to time.
    """
    ds = Dataset(tmp_path).create(**nonoise)
    res = ds.repo.call_annex_records([
        'addurl',
        # do not download the archive, and do not size checks
        '--relaxed',
        '--file', str(ds.pathobj / '.archive' / 'zenodo.zip'),
        'https://zenodo.org/record/6833100/files/datalad/datalad-next-0.4.1.zip',
    ])
    assert len(res) == 1
    res = res[0]
    assert res['success'] is True
    # we have a URL key (nothing was downloaded, and we even have no size info)
    assert res['key'].startswith('URL--')

    # and now we pick out one file in this archive and addurl it via archivist
    ds.repo.call_annex([
        'initremote', 'archivist',
        'type=external', 'externaltype=archivist', 'encryption=none',
        'autoenable=true',
    ])
    ds.repo.call_annex([
        'addurl',
        '--file', 'testfile',
        f'dl+archive:{res["key"]}#path=datalad-datalad-next-9b9c70a/.gitattributes&atype=zip',
    ])
    target_content = 'datalad_next/_version.py export-subst\n'
    assert (ds.pathobj / 'testfile').read_text() == target_content
    res = ds.status('testfile', annex='basic', return_type='item-or-list',
                    **nonoise)
    # we got a download, hence also a non-URL key
    assert not res['key'].startswith('URL')
