from pathlib import Path
import pytest
import re

from datalad_next.tests.utils import (
    with_tempfile,
    with_tree,
)
from datalad_next.constraints.dataset import EnsureDataset
from datalad_next.exceptions import IncompleteResultsError
from datalad_next.url_operations.any import AnyUrlOperations

from ..uncurl import (
    UncurlRemote,
    UnsupportedRequest,
)


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

class NoOpAnnex:
    def error(*args, **kwargs):
        pass

    def info(*args, **kwargs):
        pass

    def debug(*args, **kwargs):
        pass


def test_uncurl_store(tmp_path):
    # not yet
    r = UncurlRemote(NoOpAnnex())
    with pytest.raises(UnsupportedRequest):
        r.transfer_store(None, None)


def test_uncurl_remove(tmp_path):
    # not yet
    r = UncurlRemote(NoOpAnnex())
    with pytest.raises(UnsupportedRequest):
        r.remove(None)


def test_uncurl_claimurl(tmp_path):
    r = UncurlRemote(NoOpAnnex())
    # if we have a match expression defined, this determines claim or noclaim
    r.match = [re.compile('bongo.*')]
    assert r.claimurl('bongo://joe')
    assert not r.claimurl('http://example.com')
    r.match = None
    # without a match expression, the url handler decides
    r.url_handler = AnyUrlOperations()
    for url in ('http://example.com',
                'https://example.com',
                'ssh://example.com',
                'file:///home/me'):
        assert r.claimurl(url)
    assert not r.claimurl('bongo://joe')


def test_uncurl_checkurl(tmp_path):
    exists_path = tmp_path / 'testfile'
    exists_path.write_text('123')
    exists_url = exists_path.as_uri()
    no_exists_url = (tmp_path / 'notestfile').as_uri()
    # checkurl is basically an 'exists?' test against a URL.
    # the catch is that this test is not performed against the
    # incoming URL, but against the mangled URL that is the result
    # of instantiating the URL template based on all properties
    # extractable from the URL alone (via any configured match
    # expressions)
    r = UncurlRemote(NoOpAnnex())
    r.url_handler = AnyUrlOperations()
    # no match and no template defined
    assert not r.checkurl(no_exists_url)
    assert r.checkurl(exists_url)
    #
    # now with rewriting
    #
    # MIH cannot think of a usecase where declaring a URL template
    # would serve any purpose without also declaring a match expression
    # here.
    # outside the context of checkurl() this is different: It would
    # make sense to declare a template that only uses standard key properties
    # in order to define/declare upload targets for existing keys.
    # consequently, checkurl() is ignoring a template-based rewriting
    # when no match is defined, or when the matchers cannot extract all
    # necessary identifiers from the incoming (single) URL in order to
    # instantiate the URL template. In such cases, the original URL is
    # used for checking
    r.url_tmpl = '{absurd}'
    assert not r.checkurl(no_exists_url)
    assert r.checkurl(exists_url)

    # now add a matcher to make use of URL rewriting even for 'addurl'-type
    # use case, such as: we want to pass a "fixed" URL verbatim to some kind
    # of external redirector service
    r.url_tmpl = 'http://httpbin.org/redirect-to?url={origurl}'
    r.match = [
        re.compile('.*(?P<origurl>http://.*)$'),
    ]
    assert not r.checkurl('garbledhttp://httpbin.org/status/404')
    assert r.checkurl('garbledhttp://httpbin.org/bytes/24')


# sibling of `test_uncurl_checkurl()`, but more high-level
def test_uncurl_addurl_unredirected(tmp_path):
    ds = EnsureDataset()(tmp_path / 'ds').ds.create(**res_kwargs)
    dsca = ds.repo.call_annex
    # same set as in `test_uncurl_checkurl()`
    dsca(['initremote', 'myuncurl'] + std_initargs + [
        'match=.*(?P<origurl>http://.*)$',
        'url=http://httpbin.org/redirect-to?url={origurl}',
    ])
    # feed it a broken URL, which must be getting fixed by the rewritting
    # (pulls 24 bytes)
    testurl = 'garbledhttp://httpbin.org/bytes/24'
    dsca(['addurl', '--file=dummy', testurl])
    # we got what we expected
    assert ds.status(
        'dummy', annex='basic', return_type='item-or-list', **res_kwargs
        )['bytesize'] == 24
    # make sure git-annex recorded an unmodified URL
    assert any(testurl in r.get('urls', [])
               for r in ds.repo.whereis('dummy', output='full').values())


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
    # confirm checkpresent acknowledges this
    dsca(['fsck', '-q', '-f', 'myuncurl'])
    # confirm transfer_retrieve acknowledges this
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
    # confirm checkpresent acknowledges this
    dsca(['fsck', '-q', '-f', 'myuncurl'])
    # confirm transfer_retrieve acknowledges this
    ds.get(target_fname, **res_kwargs)
    assert (ds.pathobj / target_fname).read_text() == testfile_content