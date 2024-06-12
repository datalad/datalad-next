import datalad_next.patches.enabled

# run all -core tests, because with _push() we patched a central piece
from datalad.core.distributed.tests.test_push import *

# import those directly to pass mypy tests, although they are already imported
# by the above import
from datalad.tests.utils_pytest import (
    DEFAULT_REMOTE,
    SkipTest,
    assert_in_results,
    assert_not_in_results,
    assert_result_count,
    eq_,
    known_failure_githubci_win,
    slow,
    with_tempfile,
)

from datalad_next.datasets import Dataset


# we override this specific test, because the original behavior is no longer
# value, because our implementation behaves "better"
def test_gh1811(tmp_path, no_result_rendering):
    srcpath = tmp_path / 'src'
    clonepath = tmp_path / 'clone'
    # `annex=false` is the only change from the -core implementation
    # of the test. For normal datasets with an annex, the problem underlying
    # gh1811 is no longer valid, because of more comprehensive analysis of
    # what needs pushing in this case
    orig = Dataset(srcpath).create(annex=False)
    (orig.pathobj / 'some').write_text('some')
    orig.save()
    clone = Clone.__call__(source=orig.path, path=clonepath)
    (clone.pathobj / 'somemore').write_text('somemore')
    clone.save()
    clone.repo.call_git(['checkout', 'HEAD~1'])
    res = clone.push(to=DEFAULT_REMOTE, on_failure='ignore')
    assert_result_count(res, 1)
    assert_result_count(
        res, 1,
        path=clone.path, type='dataset', action='publish',
        status='impossible',
        message='There is no active branch, cannot determine remote '
                'branch',
    )


# taken from datalad-core@250386f1fd83af7a3df72347c9b26a4afd66baa7
@slow  # can run over 30 sec when running in parallel with n=2. Cannot force serial yet, see https://github.com/pytest-dev/pytest-xdist/issues/385
# Removed @known_failure_githubci_win-decorator
@with_tempfile(mkdir=True)
@with_tempfile(mkdir=True)
@with_tempfile(mkdir=True)
def test_nested_pushclone_cycle_allplatforms(origpath=None, storepath=None, clonepath=None):
    if 'DATALAD_SEED' in os.environ:
        # we are using create-sibling-ria via the cmdline in here
        # this will create random UUIDs for datasets
        # however, given a fixed seed each call to this command will start
        # with the same RNG seed, hence yield the same UUID on the same
        # machine -- leading to a collision
        raise SkipTest(
            'Test incompatible with fixed random number generator seed'
        )
    # the aim here is this high-level test a std create-push-clone cycle for a
    # dataset with a subdataset, with the goal to ensure that correct branches
    # and commits are tracked, regardless of platform behavior and condition
    # of individual clones. Nothing fancy, just that the defaults behave in
    # sensible ways
    from datalad.cmd import WitlessRunner as Runner
    run = Runner().run

    os.environ['DATALAD_EXTENSIONS_LOAD'] = 'next'

    # create original nested dataset
    with chpwd(origpath):
        run(['datalad', 'create', 'super'], env=os.environ)
        run(
            [
                'datalad', 'create', '-d', 'super',
                str(Path('super', 'sub'))
            ],
            env=os.environ
        )

    # verify essential linkage properties
    orig_super = Dataset(Path(origpath, 'super'))
    orig_sub = Dataset(orig_super.pathobj / 'sub')

    (orig_super.pathobj / 'file1.txt').write_text('some1')
    (orig_sub.pathobj / 'file2.txt').write_text('some1')
    with chpwd(orig_super.path):
        run(['datalad', 'save', '--recursive'], env=os.environ)

    # TODO not yet reported clean with adjusted branches
    #assert_repo_status(orig_super.path)

    # the "true" branch that sub is on, and the gitsha of the HEAD commit of it
    orig_sub_corr_branch = \
        orig_sub.repo.get_corresponding_branch() or orig_sub.repo.get_active_branch()
    orig_sub_corr_commit = orig_sub.repo.get_hexsha(orig_sub_corr_branch)

    # make sure the super trackes this commit
    assert_in_results(
        orig_super.subdatasets(),
        path=orig_sub.path,
        gitshasum=orig_sub_corr_commit,
        # TODO it should also track the branch name
        # Attempted: https://github.com/datalad/datalad/pull/3817
        # But reverted: https://github.com/datalad/datalad/pull/4375
    )

    # publish to a store, to get into a platform-agnostic state
    # (i.e. no impact of an annex-init of any kind)
    store_url = 'ria+' + get_local_file_url(storepath)
    with chpwd(orig_super.path):
        run(
            [
                'datalad', 'create-sibling-ria', '--recursive',
                '-s', 'store', store_url, '--new-store-ok'
            ],
            env=os.environ
        )
        run(
            ['datalad', 'push', '--recursive', '--to', 'store'],
            env=os.environ
        )

    # we are using the 'store' sibling's URL, which should be a plain path
    store_super = AnnexRepo(orig_super.siblings(name='store')[0]['url'], init=False)
    store_sub = AnnexRepo(orig_sub.siblings(name='store')[0]['url'], init=False)

    # both datasets in the store only carry the real branches, and nothing
    # adjusted
    for r in (store_super, store_sub):
        eq_(set(r.get_branches()), set([orig_sub_corr_branch, 'git-annex']))

    # and reobtain from a store
    cloneurl = 'ria+' + get_local_file_url(str(storepath), compatibility='git')
    with chpwd(clonepath):
        run(
            ['datalad', 'clone', cloneurl + '#' + orig_super.id, 'super'],
            env=os.environ
        )
        run(
            ['datalad', '-C', 'super', 'get', '--recursive', '.'],
            env=os.environ
        )

    # verify that nothing has changed as a result of a push/clone cycle
    clone_super = Dataset(Path(clonepath, 'super'))
    clone_sub = Dataset(clone_super.pathobj / 'sub')
    assert_in_results(
        clone_super.subdatasets(),
        path=clone_sub.path,
        gitshasum=orig_sub_corr_commit,
    )

    for ds1, ds2, f in ((orig_super, clone_super, 'file1.txt'),
                        (orig_sub, clone_sub, 'file2.txt')):
        eq_((ds1.pathobj / f).read_text(), (ds2.pathobj / f).read_text())

    # get status info that does not recursive into subdatasets, i.e. not
    # looking for uncommitted changes
    # we should see no modification reported
    assert_not_in_results(
        clone_super.status(eval_subdataset_state='commit'),
        state='modified')
    # and now the same for a more expensive full status
    assert_not_in_results(
        clone_super.status(recursive=True),
        state='modified')
