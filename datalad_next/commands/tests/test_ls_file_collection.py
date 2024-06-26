from pathlib import (
    Path,
    PurePath,
)
import pytest

from datalad.api import ls_file_collection

from datalad_next.constraints import CommandParametrizationError
# we need this fixture
from datalad_next.iter_collections.tests.test_iterzip import sample_zip
from datalad_next.tests import skipif_no_network

from ..ls_file_collection import LsFileCollectionParamValidator


def test_ls_file_collection_insufficient_args():
    with pytest.raises(CommandParametrizationError):
        ls_file_collection()

    # any collection needs some kind of identifier, just the type
    # parameter is not enough
    with pytest.raises(CommandParametrizationError):
        ls_file_collection('tarfile')

    # individual collection types have particular requirements re
    # the identifiers -- tarfile wants an existing path
    with pytest.raises(CommandParametrizationError):
        ls_file_collection('tarfile', 'http://example.com')

    # not a known collection type
    with pytest.raises(CommandParametrizationError):
        ls_file_collection('bogus', 'http://example.com')


def _check_archive_member_result(r, collection):
    # basics of a result
    assert r['action'] == 'ls_file_collection'
    assert r['status'] == 'ok'
    # a collection identifier, here the tar location
    assert 'collection' in r
    assert r['collection'] == collection
    # an item identifier, here a str-path of an archive member
    assert 'item' in r
    assert isinstance(r['item'], str)
    # item type info, here some filesystem-related category
    assert 'type' in r
    assert r['type'] in ('file', 'directory', 'symlink', 'hardlink')


def test_ls_file_collection_zipfile(sample_zip, no_result_rendering):
    for res in (
        ls_file_collection('zipfile', sample_zip),
        ls_file_collection('zipfile', sample_zip, hash='md5'),
    ):
        assert len(res) == 4
        # test a few basic properties that should be true for any result
        for r in res:
            _check_archive_member_result(r, sample_zip)


@skipif_no_network
def test_ls_file_collection_tarfile(sample_tar_xz, no_result_rendering):
    for res in (
        ls_file_collection('tarfile', sample_tar_xz),
        ls_file_collection('tarfile', sample_tar_xz, hash='md5'),
    ):
        assert len(res) == 6
        # test a few basic properties that should be true for any result
        for r in res:
            _check_archive_member_result(r, sample_tar_xz)


def test_ls_file_collection_directory(tmp_path, no_result_rendering):
    # smoke test on an empty dir
    res = ls_file_collection('directory', tmp_path)
    assert len(res) == 0


def test_ls_file_collection_gitworktree(existing_dataset, no_result_rendering):
    # smoke test on a plain dataset
    res = ls_file_collection('gitworktree', existing_dataset.pathobj)
    assert len(res) > 1
    assert all('gitsha' in r for r in res)

    # and with hashing
    res_hash = ls_file_collection('gitworktree', existing_dataset.pathobj,
                                  hash='md5')
    assert len(res) == len(res_hash)
    assert all('hash-md5' in r for r in res_hash)


def test_ls_file_collection_validator():
    val = LsFileCollectionParamValidator()

    with pytest.raises(RuntimeError):
        val.get_collection_iter(type='bogus', collection='any', hash=None)


@skipif_no_network
def test_replace_add_archive_content(sample_tar_xz, existing_dataset,
                                     no_result_rendering):
    ds = existing_dataset
    archive_path = ds.pathobj / '.datalad' / 'myarchive.tar.xz'
    # get archive copy in dataset (not strictly needed, but
    # add-archive-content worked like this
    ds.download({sample_tar_xz.as_uri(): archive_path})
    # properly safe to dataset (download is ignorant of datasets)
    res = ds.save(message='add archive')
    # the first result has the archive addition, snatch the archive key from it
    assert res[0]['path'] == str(archive_path)
    archive_key = res[0]['key']

    # now we can scan the archive and register keys for its content.
    # the order and specific composition of the following steps is flexible.
    # we could simply extract the local archive, save the content to the
    # dataset, and then register `dl+archive` URLs.
    # however, we will use an approach that does not require any data
    # to be present locally (actually not even the archive that we have locally
    # already for this test), but is instead based on some metadata
    # that is provided by `ls-file-collection` (but could come from elsewhere,
    # including `ls-file-collection` executed on a different host).
    file_recs = [
        r for r in ls_file_collection(
            'tarfile', sample_tar_xz, hash=['md5'],
        )
        # ignore any non-file, would not have an annex key.
        # Also ignores hardlinks (they consume no space (size=0), but could be
        # represented as regular copies of a shared key. however, this
        # requires further processing of the metadata records, in order to find
        # the size of the item that has the same checksum as this one)
        if r.get('type') == 'file'
    ]
    # we enable the `datalad-archives` special remote using a particular
    # configuration that `add-archive-content` would use.
    # this special remote can act on the particular URLs that we will add next
    ds.repo.call_annex([
        'initremote', 'datalad-archives', 'type=external',
        'externaltype=datalad-archives', 'encryption=none', 'autoenable=true'])
    # assign special `dl+archive` URLs to all file keys
    # the `datalad-archives` special remote will see them and perform the
    # extraction of file content from the archive on demand.
    # the entire operation is not doing any extraction or data retrieval,
    # because we have all information necessary to generate keys
    ds.addurls(
        # takes an iterable of dicts
        file_recs,
        # urlformat: handcrafted archive key, as expected by datalad-archive
        # (double braces to keep item and size as placeholders for addurls)
        f'dl+archive:{archive_key}#path={{item}}&size={{size}}',
        # filenameformat
        '{item}',
        key='et:MD5-s{size}--{hash-md5}',
    )
    # because we have  been adding the above URLs using a pure metadata-driven
    # approach, git-annex does not yet know that the archives remote actually
    # has the keys. we could use `annex setpresentkey` for that (fast local
    # operation), but here we use `fsck` to achieve a comprehensive smoke test
    # of compatibility with our hand-crafted and the special remote
    # implementation
    # (actually: without --fast the special remote crashes with a protocol
    #  error -- a bug in the special remote probably)
    ds.repo.call_annex(
        ['fsck', '--fast', '-f', 'datalad-archives'],
        files=['test-archive'],
    )
    # at this point we are done
    # check retrieval for a test file, which is not yet around
    testfile = ds.pathobj / 'test-archive' / '123_hard.txt'
    assert ds.status(
        testfile, annex='availability')[0]['has_content'] is False
    ds.get(testfile)
    assert testfile.read_text() == '123\n'


def test_ls_renderer():
    # nothing more than a smoke test
    ls_file_collection(
        'directory',
        Path(__file__).parent,
        result_renderer='tailored',
    )


def test_ls_annexworktree_empty_dataset(existing_dataset):
    res = ls_file_collection(
        'annexworktree',
        existing_dataset.pathobj,
        result_renderer='disabled'
    )
    assert len(res) == 3
    annexed_files = [annex_info for annex_info in res if 'annexkey' in annex_info]
    assert len(annexed_files) == 0


def test_ls_annexworktree_simple_dataset(existing_dataset):

    (existing_dataset.pathobj / 'sample.bin').write_bytes(b'\x00' * 1024)
    existing_dataset.save(message='add sample file')

    res = ls_file_collection(
        'annexworktree',
        existing_dataset.pathobj,
        result_renderer='disabled'
    )
    assert len(res) == 4
    annexed_files = [annex_info for annex_info in res if 'annexkey' in annex_info]
    assert len(annexed_files) == 1
    assert annexed_files[0]['type'] == 'annexed file'
    assert {
        'annexkey',
        'annexsize',
        'annexobjpath'
    }.issubset(set(annexed_files[0].keys()))
