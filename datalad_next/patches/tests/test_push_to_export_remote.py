from unittest.mock import patch

from datalad.tests.utils import (
    assert_false,
    assert_in,
    assert_in_results,
    assert_raises,
    assert_status,
    assert_true,
    eq_,
    ok_,
)


from datalad_next.patches.push_to_export_remote import _is_export_remote


class MockRepo:
    def get_special_remotes(self):
        return {
            0: {
                "name": "no-target",
                "exporttree": "no"
            },
            1: {
                "name": "yes-target",
                "exporttree": "yes"
            }
        }


def test_is_export_remote():
    # Ensure that None is handled properly
    assert_false(_is_export_remote(None))

    # Ensure that dicts without "exporttree" keyword are handled correctly
    assert_false(_is_export_remote({}))

    # Ensure that "exporttree" is interpreted correctly
    assert_false(_is_export_remote({"exporttree": "no"}))
    assert_true(_is_export_remote({"exporttree": "yes"}))


def test_patch_pass_through():
    # Ensure that the original _transfer_data is called if the target remote
    # has exporttree # not set to "yes"
    from datalad.core.distributed.push import _transfer_data as _original_transfer_data
    from datalad_next.patches.push_to_export_remote import _transfer_data

    print(_original_transfer_data, _transfer_data)
    _transfer_data(*([None]*9))
    with patch("datalad_next.patches.push_to_export_remote.push._push_data") as pd_mock:
        _original_transfer_data(
            repo=MockRepo(),
            ds=None,
            target="no-target",
            content=[],
            data="",
            force=None,
            jobs=None,
            res_kwargs=dict(),
            got_path_arg=False)
        eq_(pd_mock.call_count, 1)


def test_patch_execute_export():
    # Ensure that export is called if the target remote has exporttree not set
    # to "yes"
    from datalad_next.patches.push_to_export_remote import _transfer_data
    with patch("datalad_next.patches.push_to_export_remote.push._push_data") as pd_mock:
        _transfer_data(
            repo=MockRepo(),
            ds=None,
            target="yes-target",
            content=[],
            data="",
            force=None,
            jobs=None,
            res_kwargs=dict(),
            got_path_arg=False)
        eq_(pd_mock.call_count, 1)
