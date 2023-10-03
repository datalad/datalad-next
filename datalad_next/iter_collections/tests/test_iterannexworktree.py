from pathlib import (
    PurePath,
)

from datalad import cfg as dlcfg

from datalad_next.datasets import Dataset

from ..annexworktree import (
    iter_annexworktree,
)


def _mkds(tmp_path_factory, monkeypatch, cfg_overrides):
    with monkeypatch.context() as m:
        for k, v in cfg_overrides.items():
            m.setitem(dlcfg.overrides, k, v)
        dlcfg.reload()
        ds = Dataset(tmp_path_factory.mktemp('ds')).create(
            result_renderer='disabled')
    dlcfg.reload()
    return ds


def _dotests(ds):
    test_file_content = 'test_file'
    test_file = ds.pathobj / 'annexed' / 'subdir' / 'file1.txt'
    test_file.parent.mkdir(parents=True)
    test_file.write_text(test_file_content)
    # we create an additional file where the content will be dropped
    # to test behavior on unavailable annex key
    droptest_content = 'somethingdropped'
    droptest_file = ds.pathobj / 'annexed' / 'dropped.txt'
    droptest_file.write_text(droptest_content)
    ds.save(result_renderer='disabled')
    ds.drop(droptest_file, reckless='availability',
            result_renderer='disabled')

    # get results for the annexed files
    query_path = ds.pathobj / 'annexed'
    res = list(iter_annexworktree(
        query_path, untracked=None, link_target=True,
    ))
    assert len(res) == 2
    #
    # pick the present annex file to start
    r = [r for r in res if r.name.name == 'file1.txt'][0]
    assert r.name == PurePath('subdir', 'file1.txt')
    # we cannot check gitsha and symlink content for identity, it will change
    # depending on the tuning
    # we cannot check the item type, because it will vary across repository
    # modes (e.g., adjusted unlocked)
    assert r.annexsize == len(test_file_content)
    assert r.annexkey == 'MD5E-s9--37b87ee8c563af911dcc0f949826b1c9.txt'
    # with `link_target=True` we get an objpath that is relative to the
    # query path, and we find the actual key file there
    assert (query_path / r.annexobjpath).read_text() == test_file_content
    #
    # now pick the dropped annex file
    r = [r for r in res if r.name.name == 'dropped.txt'][0]
    assert r.name == PurePath('dropped.txt')
    # we get basic info regardless of availability
    assert r.annexsize == len(droptest_content)
    assert r.annexkey == 'MD5E-s16--770a06889bc88f8743d1ed9a1977ff7b.txt'
    # even with an absent key file, we get its would-be location,
    # and it is relative to the query path
    assert r.annexobjpath.parts[:2] == ('..', '.git')


def test_iter_annexworktree(tmp_path_factory, monkeypatch):
    ds = _mkds(tmp_path_factory, monkeypatch, {})
    _dotests(ds)


def test_iter_annexworktree_tuned(tmp_path_factory, monkeypatch):
    # same as test_file_content(), but with a "tuned" annexed that
    # no longer matches the traditional setup.
    # we need to be able to cope with that too
    ds = _mkds(tmp_path_factory, monkeypatch, {
        'annex.tune.objecthash1': 'true',
        'annex.tune.branchhash1': 'true',
        'annex.tune.objecthashlower': 'true',
    })
    _dotests(ds)
