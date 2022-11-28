from pathlib import Path
import pytest

from datalad_next.tests.utils import (
    with_tempfile,
    with_tree,
)
from datalad_next.constraints.dataset import EnsureDataset
from datalad_next.exceptions import IncompleteResultsError


# for some tests below it is important that this base config contains no
# url= or match= declaration (or any other tailoring to a specific use case)
std_initargs = [
    'type=external',
    'externaltype=uncurl',
    'encryption=none',
]

res_kwargs = dict(
    result_renderer='disabled',
)


@with_tempfile
@with_tree(tree={
    'lvlA1': {'lvlB2_flavor1.tar': 'data_A1B2F1'},
})
def test_uncurl(wdir=None, archive_path=None):
    archive_path = Path(archive_path)
    ds = EnsureDataset()(wdir).ds.create(**res_kwargs)
    dsca = ds.repo.call_annex
    dsca(['initremote', 'myuncurl'] + std_initargs + [
        'match=bingofile://(?P<basepath>.*)/(?P<lvlA>[^/]+)/(?P<lvlB>[^/]+)_(?P<flavor>.*)$ someothermatch',
        'url=file://{basepath}/{lvlA}/{lvlB}_{flavor}',
    ])
    data_url = (archive_path / 'lvlA1' / 'lvlB2_flavor1.tar').as_uri()
    # prefix the URL so git-annex has no idea how to handle it
    # (same as migrating from an obsolete system with no support anymore)
    data_url = f'bingo{data_url}'
    dsca(['addurl', '--file', 'data_A1B2F1.dat', data_url])
    assert (ds.pathobj / 'data_A1B2F1.dat').read_text() == 'data_A1B2F1'
    # file is known to be here and a (uncurl) remote
    assert len(ds.repo.whereis('data_A1B2F1.dat')) == 2
    # must survive an fsck (CHECKPRESENT)
    dsca(['fsck', '-q', '-f', 'myuncurl'])


def test_uncurl_ria_access(tmp_path):
    """
    - create dataset with test file and push into RIA store
    - create INDEPENDENT dataset and 'addurl' test file directly from RIA
    - test that addurls work without any config, just initremote with no
      custom settings
    - now move RIA and hence break URL
    - fix1: only using a URL template, point to dataset dir in RIA store
      plus some always available key-properties
    - alternative fix2: simpler template, plus match expression to
      "understand" some structural aspects of RIA and reuse them
    """
    # we create a dataset to bootstrap the test setup, with on file
    # of known content
    srcds = EnsureDataset()(tmp_path / 'srcds').ds.create(**res_kwargs)
    testfile_content = 'mikewashere!'
    (srcds.pathobj / 'testfile.txt').write_text(testfile_content)
    srcds.save(**res_kwargs)
    # pull out some essential properties for the underlying key for later
    # use in this test
    testkey_props = srcds.status(
        'testfile.txt', annex='basic', return_type='item-or-list', **res_kwargs)
    testkey_props = {
        k: v for k, v in testkey_props.items()
        if k in ('key', 'hashdirmixed', 'hashdirlower')
    }

    # establish a RIA sibling and push
    baseurl = (tmp_path / "ria").as_uri()
    srcds.create_sibling_ria(
        # use a ria+file:// URL for simplicity
        f'ria+{baseurl}',
        name='ria',
        new_store_ok=True,
        **res_kwargs
    )
    srcds.push(to='ria', **res_kwargs)
    # setup is done

    # start of the actual test
    # create a fresh dataset
    ds = EnsureDataset()(tmp_path / 'testds').ds.create(**res_kwargs)
    dsca = ds.repo.call_annex
    # we add uncurl WITH NO config whatsoever.
    # this must be enough to be able to use the built-in downloaders
    target_fname = 'mydownload.txt'
    dsca(['initremote', 'myuncurl'] + std_initargs)
    dsca(['addurl', '--file', target_fname,
          # we download from the verbatim, hand-crafted URL
          f'{baseurl}/{srcds.id[:3]}/{srcds.id[3:]}/annex/objects/'
          f'{testkey_props["hashdirmixed"]}'
          f'{testkey_props["key"]}/{testkey_props["key"]}'
    ])
    assert (ds.pathobj / target_fname).read_text() == testfile_content
    # make sure the re-downloaded key ends up having the same keyname in
    # the new dataset
    assert ds.status(
        target_fname, annex='basic', return_type='item-or-list',
        **res_kwargs)['key'] == testkey_props['key']

    # now we drop the key...
    ds.drop(target_fname, **res_kwargs)
    assert not (ds.pathobj / target_fname).exists()
    # ...and we move the RIA store to break the recorded
    # URL (simulating an infrastructure change)
    (tmp_path / 'ria').rename(tmp_path / 'ria_moved')

    # verify that no residual magic makes data access possible
    with pytest.raises(IncompleteResultsError):
        ds.get(target_fname, **res_kwargs)

    # fix it via an access URL config,
    # point directly via a hard-coded dataset ID
    # NOTE: last line is no f-string!
    url_tmpl = f'{(tmp_path / "ria_moved").as_uri()}/{srcds.id[:3]}/{srcds.id[3:]}' \
        + '/annex/objects/{annex_dirhash}/{annex_key}/{annex_key}'
    ds.configuration(
        'set', f'remote.myuncurl.uncurl-url={url_tmpl}', **res_kwargs)
    ds.get(target_fname, **res_kwargs)

    # but we can also do without hard-coding anything, so let's drop again
    ds.drop(target_fname, **res_kwargs)
    assert not (ds.pathobj / target_fname).exists()

    # for that we need to add a match expression that can "understand"
    # the original URL. All we need is to distinguish the old base path
    # from the structured components in the RIA store (for the
    # latter we can simple account for 4 levels of sudirs
    ds.configuration(
        'set',
        'remote.myuncurl.uncurl-match='
        'file://(?P<basepath>.*)/(?P<dsdir>[^/]+/[^/]+)/annex/objects/.*$',
        **res_kwargs)
    # NOTE: last line is no f-string!
    url_tmpl = f'{(tmp_path / "ria_moved").as_uri()}' \
        + '/{dsdir}/annex/objects/{annex_dirhash}/{annex_key}/{annex_key}'
    ds.configuration(
        'set', f'remote.myuncurl.uncurl-url={url_tmpl}', **res_kwargs)
    ds.get(target_fname, **res_kwargs)
    assert (ds.pathobj / target_fname).read_text() == testfile_content
