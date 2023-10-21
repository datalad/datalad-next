from pathlib import (
    Path,
    PurePosixPath,
)
import pytest

from datalad_next.annexremotes import UnsupportedRequest
from datalad_next.annexremotes.archivist import ArchivistRemote
from datalad_next.datasets import Dataset
from datalad_next.runners import CommandError

from datalad_next.tests.utils import assert_result_count


nonoise = dict(result_renderer='disabled')


@pytest.fixture(autouse=False, scope="function")
def archivist_dataset(tmp_path_factory):
    wpath = tmp_path_factory.mktemp("archivistds")
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
    return ads


@pytest.fixture(autouse=False, scope="function")
def populated_archivist_dataset(archivist_dataset, tmp_path_factory):
    """Returns a path to generated dataset

    This dataset references an annex archive with no other annexed files.
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
    wpath = tmp_path_factory.mktemp("archivistds")

    ads = archivist_dataset

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

    archive_root = wpath / 'myarchive'
    #archivetype = 'zip'

    akeys = {}

    # no ZIP just yet
    # for archivetype, ext in (('zip', ''), ('tar', '.gz')):
    for archivetype, ext in (('tar', '.gz'), ):
        archive_path = Path(f"{archive_root}.{archivetype}{ext}")

        archive_path_inds = ads.pathobj / '.archives' / archive_path.name
        # create an archive, the easy way, by simply exporting the
        # entire dataset worktree
        srcds.export_archive(archive_root, archivetype=archivetype,
                             **nonoise)
        assert archive_path.exists()

        # add the archive (in a hidden dir) to be able to reference
        # it via a key
        aurl = archive_path.as_uri()
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
                f'#path={archive_root.name}/{fpath}&size={len(fcontent)}',
            ])
    # check that we reached the desired state
    whereis = ads.repo.whereis(
        # this dance is needed, because the normalize_paths decorator
        # of whereis() required platform paths
        str(Path('atar', 'file1.txt')),
        output='full',
    )
    # the file is known to exactly one remote (besides "here")
    assert len(whereis) == 2
    # and one remote is the archivist remote, and importantly not the
    # 'web' remote -- which would indicate a failure of claimurl/checkurl
    assert any(wi['description'] == '[archivist]' for wi in whereis.values())


def _check_archivist_retrieval(archivist_dataset):
    nonoise = dict(result_renderer='disabled')

    ads, akeys, archive_root, dscontent = archivist_dataset

    # step 1: addurl
    _check_archivist_addurl(
        # check all archive types supported
        # no ZIP just yet
        #('zip', 'tar'),
        ('tar',),
        ads, akeys, archive_root, dscontent,
    )

    # make a clean dataset
    ads.save(**nonoise)

    # step 2: drop keys with dl+archive: URLs
    # now drop all archive member content. this should work,
    # because for each file there is a URL on record
    # -- hence always another copy
    # this requires archivist's CHECKPRESENT to function properly
    # no ZIP just yet
    #res = ads.drop(['azip', 'atar'], **nonoise)
    res = ads.drop(['atar'], **nonoise)
    assert_result_count(
        res,
        # all files, plus the two directories we gave as arguments
        # no ZIP just yet
        #len(dscontent) + 2,
        3,
        action='drop',
        status='ok',
    )

    # step 3: retrieve keys with dl+archive: URLs from locally present archives
    # no ZIP just yet
    #res = ads.get(['azip', 'atar'], **nonoise)
    res = ads.get(['atar'], **nonoise)
    assert_result_count(
        res,
        # no ZIP just yet
        # len(dscontent),
        2,
        action='get',
        status='ok',
        type='file',
    )
    for fpath, fcontent in dscontent:
        # no ZIP just yet
        if 'zip' in fpath:
            continue
        assert (ads.pathobj / fpath).read_text() == fcontent

    # step 4: now drop ALL keys (incl. archives)
    # this will present a challenge for CHECKPRESENT:
    # without the archives no longer being around, it would requires remote
    # access or download to actually verify continued presence.
    # force this condition by dropping the archive keys first
    res = ads.drop('.archives', **nonoise)
    assert_result_count(
        res,
        # a tar and a zip
        # no ZIP just yet
        #2,
        1,
        action='drop',
        type='file',
        status='ok',
    )
    # and 4a now drop the keys that have their content from archives
    # no ZIP just yet
    #res = ads.drop(['azip', 'atar'], **nonoise)
    res = ads.drop(['atar'], **nonoise)
    assert_result_count(
        res,
        # no ZIP just yet
        #len(dscontent),
        2,
        action='drop',
        status='ok',
        type='file',
    )
    # and now get again, this time with no archives around locally
    # no ZIP just yet
    #res = ads.get(['azip', 'atar'], **nonoise)
    res = ads.get(['atar'], **nonoise)
    assert_result_count(
        res,
        # no ZIP just yet
        # len(dscontent),
        2,
        action='get',
        status='ok',
        type='file',
    )
    for fpath, fcontent in dscontent:
        # no ZIP just yet
        if 'zip' in fpath:
            continue
        assert (ads.pathobj / fpath).read_text() == fcontent
    # and drop everything again to leave the dataset empty
    res = ads.drop(['.'], **nonoise)


def test_archivist_retrieval(populated_archivist_dataset):
    _check_archivist_retrieval(populated_archivist_dataset)

    # the following is either not possible or not identical between archivist
    # and the old datalad-archives special remotes
    ads, akeys, archive_root, dscontent = populated_archivist_dataset

    # step 5: remove the only remaining source of the archives, and check
    # how it get/fsck fails
    for archive in archive_root.parent.glob('*.*z*'):
        archive.unlink()
    with pytest.raises(CommandError) as e:
        # no ZIP just yet
        #ads.repo.call_annex(['get', 'atar', 'azip'])
        ads.repo.call_annex(['get', 'atar'])
    # make sure the "reason" is communicated outwards
    assert 'does not exist' in e.value.stderr
    with pytest.raises(CommandError):
        # no ZIP just yet
        #ads.repo.call_annex(['fsck', '-f', 'archivist', 'atar', 'azip'])
        ads.repo.call_annex(['fsck', '-f', 'archivist', 'atar'])


def test_archivist_retrieval_legacy(populated_archivist_dataset, monkeypatch):
    """Same as test_archivist_retrieval(), but performs everything via the
    datalad-core provided datalad-archives special remote code
    """
    with monkeypatch.context() as m:
        m.setenv("DATALAD_ARCHIVIST_LEGACY__MODE", "yes")
        _check_archivist_retrieval(populated_archivist_dataset)


def test_claimcheck_url():
    class DummyAnnex:
        def debug(*args, **kwargs):
            pass

        def info(*args, **kwargs):
            pass

        def error(*args, **kwargs):
            pass

    ar = ArchivistRemote(DummyAnnex())

    valid_url = \
        'dl+archive:MD5E-s1--e9f624eb778e6f945771c543b6e9c7b2.zip#path=f.txt'
    invalid_url = \
        'dl+BROKENarchive:MD5E-s1--e9f624eb778e6f945771c543b6e9c7b2.zip#path=f.txt'

    assert ar.claimurl(valid_url) is True
    assert ar.claimurl(invalid_url) is False

    assert ar.checkurl(valid_url) is True
    assert ar.checkurl(invalid_url) is False


def test_archivist_unsupported():
    ar = ArchivistRemote(None)

    with pytest.raises(UnsupportedRequest):
        ar.transfer_store('mykey', 'myfile')
    with pytest.raises(UnsupportedRequest):
        ar.remove('mykey')
