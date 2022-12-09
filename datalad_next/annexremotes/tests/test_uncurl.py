from pathlib import Path
import pytest
import re

from datalad_next.utils import on_windows
from datalad_next.tests.utils import (
    skip_ssh,
    skip_if_on_windows,
    with_tempfile,
    with_tree,
)
from datalad_next.constraints.dataset import EnsureDataset
from datalad_next.exceptions import (
    CommandError,
    UrlOperationsRemoteError,
    IncompleteResultsError,
)
from datalad_next.url_operations.any import AnyUrlOperations

from ..uncurl import (
    RemoteError,
    UncurlRemote,
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


def test_uncurl_remove_no_tmpl(tmp_path):
    # without a template configured we refuse to remove anything
    # for the simple reason that it may not be clear what is being
    # removed at all. We could only iterate over all recorded URLs
    # and wipe out the key from "the internet". This is, however,
    # a rather unexpected thing to do from a user perspective --
    # who would expect a single key instance "at the uncurl remote"
    # to be removed. The best proxy we have for this expectation
    # is a URL tmpl being configured, pointing to such a single
    # location
    r = UncurlRemote(NoOpAnnex())
    with pytest.raises(RemoteError):
        r.remove(None)


def test_uncurl_transfer_store_no_tmpl():
    r = UncurlRemote(NoOpAnnex())
    r.url_handler = AnyUrlOperations()
    # whenever there is not template configured
    with pytest.raises(RemoteError):
        r.transfer_store(None, '')


def test_uncurl_checktretrieve():

    def handler(url):
        raise UrlOperationsRemoteError(url)

    def get_urls(key):
        return 'some'

    r = UncurlRemote(NoOpAnnex())
    r.get_key_urls = get_urls

    # we raise the correct RemoteError and not the underlying
    # UrlOperationsRemoteError
    with pytest.raises(RemoteError):
        r._check_retrieve('somekey', handler, ('blow', 'here'))


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


# RIA tooling is not working for this test on windows
# https://github.com/datalad/datalad/issues/7212
@skip_if_on_windows
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
    assert not ds.status(
        target_fname, annex='availability', return_type='item-or-list',
        **res_kwargs)['has_content']
    # ...and we move the RIA store to break the recorded
    # URL (simulating an infrastructure change)
    (tmp_path / 'ria').rename(tmp_path / 'ria_moved')

    # verify that no residual magic makes data access possible
    with pytest.raises(IncompleteResultsError):
        ds.get(target_fname, **res_kwargs)

    # fix it via an access URL config,
    # point directly via a hard-coded dataset ID
    # NOTE: last line is no f-string!
    url_tmpl = (
        tmp_path / "ria_moved" / srcds.id[:3] / srcds.id[3:]
        ).as_uri() + '/annex/objects/{annex_dirhash}/{annex_key}/{annex_key}'
    ds.configuration(
        'set', f'remote.myuncurl.uncurl-url={url_tmpl}', **res_kwargs)
    # confirm checkpresent acknowledges this
    dsca(['fsck', '-q', '-f', 'myuncurl'])
    # confirm transfer_retrieve acknowledges this
    ds.get(target_fname, **res_kwargs)

    # but we can also do without hard-coding anything, so let's drop again
    ds.drop(target_fname, **res_kwargs)
    assert not ds.status(
        target_fname, annex='availability', return_type='item-or-list',
        **res_kwargs)['has_content']

    # for that we need to add a match expression that can "understand"
    # the original URL. All we need is to distinguish the old base path
    # from the structured components in the RIA store (for the
    # latter we can simple account for 4 levels of sudirs
    ds.configuration(
        'set',
        'remote.myuncurl.uncurl-match='
        'file://(?P<basepath>.*)/(?P<dsdir>[^/]+/[^/]+)/annex/objects/.*$',
        scope='local', **res_kwargs)
    # NOTE: last line is no f-string!
    url_tmpl = (tmp_path / "ria_moved").as_uri() \
        + '/{dsdir}/annex/objects/{annex_dirhash}/{annex_key}/{annex_key}'
    ds.configuration(
        'set', f'remote.myuncurl.uncurl-url={url_tmpl}',
        scope='local', **res_kwargs)
    # confirm checkpresent acknowledges this
    dsca(['fsck', '-q', '-f', 'myuncurl'])
    # confirm transfer_retrieve acknowledges this
    ds.get(target_fname, **res_kwargs)
    assert (ds.pathobj / target_fname).read_text() == testfile_content


def test_uncurl_store(tmp_path):
    ds = EnsureDataset()(tmp_path / 'ds').ds.create(**res_kwargs)
    testfile = ds.pathobj / 'testfile1.txt'
    testfile_content = 'uppytyup!'
    testfile.write_text(testfile_content)
    ds.save(**res_kwargs)
    dsca = ds.repo.call_annex
    # init the remote with a template that places keys in the same structure
    # as annex/objects within a bare remote repo
    dsca(['initremote', 'myuncurl'] + std_initargs + [
        # intentional double-braces at the end to get templates into the template
        f'url={(tmp_path / "upload").as_uri()}/{{annex_dirhash_lower}}{{annex_key}}/{{annex_key}}',
    ])
    # store file at remote
    dsca(['copy', '-t', 'myuncurl', str(testfile)])
    # let remote verify presence
    dsca(['fsck', '-q', '-f', 'myuncurl'])
    # doublecheck
    testfile_props = ds.status(testfile, annex='basic',
                               return_type='item-or-list', **res_kwargs)
    assert (tmp_path / "upload" / testfile_props['hashdirlower'] /
            testfile_props['key'] / testfile_props['key']
        ).read_text() == testfile_content
    # we have no URLs recorded
    assert all(not v['urls']
               for v in ds.repo.whereis(str(testfile), output='full').values())
    # yet we can retrieve via uncurl, because local key properties are enough
    # to fill the template
    ds.drop(testfile, **res_kwargs)
    assert not ds.status(
        testfile, annex='availability', return_type='item-or-list',
        **res_kwargs)['has_content']
    dsca(['copy', '-f', 'myuncurl', str(testfile)])
    assert testfile.read_text() == testfile_content

    if on_windows:
        # remaining bits assume POSIX FS
        return

    ds.config.set(
        'remote.myuncurl.uncurl-url',
        # same as above, but root with no write-permissions
        'file:///youshallnotpass/{annex_dirhash_lower}{annex_key}/{annex_key}',
        scope='local',
    )
    with pytest.raises(CommandError):
        dsca(['fsck', '-q', '-f', 'myuncurl'])
    with pytest.raises(CommandError) as exc:
        dsca(['copy', '-t', 'myuncurl', str(testfile)])


@skip_ssh
def test_uncurl_store_via_ssh(tmp_path):
    ds = EnsureDataset()(tmp_path / 'ds').ds.create(**res_kwargs)
    testfile = ds.pathobj / 'testfile1.txt'
    testfile_content = 'uppytyup!'
    testfile.write_text(testfile_content)
    ds.save(**res_kwargs)
    dsca = ds.repo.call_annex
    # init the remote with a template that places keys in the same structure
    # as annex/objects within a bare remote repo
    dsca(['initremote', 'myuncurl'] + std_initargs + [
        # intentional double-braces at the end to get templates into the template
        f'url={(tmp_path / "upload").as_uri().replace("file://", "ssh://localhost")}/{{annex_key}}',
    ])
    # store file at remote
    dsca(['copy', '-t', 'myuncurl', str(testfile)])
    # let remote verify presence
    dsca(['fsck', '-q', '-f', 'myuncurl'])


def test_uncurl_remove(tmp_path):
    testfile = tmp_path / 'testdeposit' / 'testfile1.txt'
    testfile_content = 'uppytyup!'
    testfile.parent.mkdir()
    testfile.write_text(testfile_content)
    ds = EnsureDataset()(tmp_path / 'ds').ds.create(**res_kwargs)
    dsca = ds.repo.call_annex
    # init without URL template
    dsca(['initremote', 'myuncurl'] + std_initargs)
    # add the testdeposit by URL
    target_fname = ds.pathobj / 'target1.txt'
    dsca(['addurl', '--file', str(target_fname), testfile.as_uri()])
    # it will not drop without a URL tmpl
    # see test_uncurl_remove_no_tmpl() for rational
    with pytest.raises(CommandError):
        dsca(['drop', '-f', 'myuncurl', str(target_fname)])
    assert testfile.read_text() == testfile_content

    # now make it possible
    # use the simplest possible match expression
    ds.configuration(
        'set',
        'remote.myuncurl.uncurl-match=file://(?P<allofit>.*)$',
        scope='local', **res_kwargs)
    # and the presence of a tmpl enables deletion
    ds.configuration(
        'set', 'remote.myuncurl.uncurl-url=file://{allofit}',
        scope='local', **res_kwargs)
    dsca(['drop', '-f', 'myuncurl', str(target_fname)])
    assert not testfile.exists()


# >30s
def test_uncurl_testremote(tmp_path):
    "Point git-annex's testremote at uncurl"
    ds = EnsureDataset()(tmp_path / 'ds').ds.create(**res_kwargs)
    dsca = ds.repo.call_annex
    dsca(['initremote', 'myuncurl'] + std_initargs
         # file://<basepath>/key
         + [f'url=file://{tmp_path / "remotepath"}/{{annex_key}}'])
    # Temporarily disable this until
    # https://github.com/datalad/datalad-dataverse/issues/127
    # is sorted out. Possibly via
    # https://git-annex.branchable.com/bugs/testremote_is_not_honoring_--backend
    if not on_windows:
        # not running with --fast to also cover key chunking
        dsca(['testremote', '--quiet', 'myuncurl'])
