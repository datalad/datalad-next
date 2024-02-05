"""Tooling for test implementations

.. currentmodule:: datalad_next.tests
.. autosummary::
   :toctree: generated

   BasicGitTestRepo
   DEFAULT_BRANCH
   DEFAULT_REMOTE
   assert_in
   assert_in_results
   assert_result_count
   assert_status
   create_tree
   eq_
   get_deeply_nested_structure
   ok_
   ok_good_symlink
   ok_broken_symlink
   run_main
   skip_if_on_windows
   skip_if_root
   skip_wo_symlink_capability
   swallow_logs
   skipif_no_network
"""
# TODO `assert_raises` is excluded above to avoid syntax errors in the docstring
# rather than fixing those, we should replace it with `pytest.raises` entirely

from .utils import (
    BasicGitTestRepo,
    DEFAULT_BRANCH,
    DEFAULT_REMOTE,
    assert_in,
    assert_in_results,
    assert_raises,
    assert_result_count,
    assert_status,
    create_tree,
    eq_,
    get_deeply_nested_structure,
    ok_,
    ok_good_symlink,
    ok_broken_symlink,
    run_main,
    skip_if_on_windows,
    skip_if_root,
    skip_wo_symlink_capability,
    swallow_logs,
)


from .marker import (
    skipif_no_network,
)
