from unittest.mock import (
    MagicMock,
    patch,
)

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


from datalad_next.patches.push_to_export_remote import (
    _is_export_remote,
    _transfer_data,
)


class MockRepo:
    def __init__(self, return_special_remotes=True):
        self.return_special_remotes = return_special_remotes

    def get_special_remotes(self):
        if self.return_special_remotes:
            return {
                0: {
                    "name": "no-target",
                    "exporttree": "no"
                },
                1: {
                    "name": "yes-target",
                    "exporttree": "yes"
                },
                2: {
                    "name": "some-target",
                    "exporttree": "no"
                }
            }
        else:
            return {}

    def call_git(self, *args, **kwargs):
        return


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
    with patch("datalad_next.patches.push_to_export_remote.push._push_data") as pd_mock:
        results = tuple(_transfer_data(
            repo=MockRepo(),
            ds=None,
            target="no-target",
            content=[],
            data="",
            force=None,
            jobs=None,
            res_kwargs=dict(),
            got_path_arg=False))
        eq_(pd_mock.call_count, 1)


def test_patch_execute_export():
    # Ensure that export is called if the target remote has exporttree set to
    # "yes"
    ds_mock = MagicMock()
    ds_mock.config.getbool.return_value = False
    module_name = "datalad_next.patches.push_to_export_remote"
    with patch(f"{module_name}.push._push_data") as pd_mock, \
         patch(f"{module_name}._get_export_log_entry") as gele_mock:

        gele_mock.return_value = None
        results = tuple(_transfer_data(
            repo=MockRepo(),
            ds=ds_mock,
            target="yes-target",
            content=[],
            data="",
            force=None,
            jobs=None,
            res_kwargs={"some": "arg"},
            got_path_arg=False))
        eq_(pd_mock.call_count, 0)
        assert_in(
            {"target": "yes-target", "status": "ok", "some": "arg"},
            results)


def test_patch_skip_ignore_targets_export():
    ds_mock = MagicMock()
    ds_mock.config.getbool.return_value = True
    with patch("datalad_next.patches.push_to_export_remote.lgr") as lgr_mock:
        results = tuple(_transfer_data(
            repo=MockRepo(),
            ds=ds_mock,
            target="yes-target",
            content=[],
            data="",
            force=None,
            jobs=None,
            res_kwargs={"some": "arg"},
            got_path_arg=False))
        eq_(lgr_mock.debug.call_count, 2)
        assert_true(lgr_mock.mock_calls[1].args[0].startswith("Target"))


def test_patch_check_envpatch():
    # Ensure that export is called if the target remote has exporttree not set
    # to "yes"
    ds_mock = MagicMock()
    ds_mock.config.getbool.return_value = False
    module_name = "datalad_next.patches.push_to_export_remote"
    with patch(f"{module_name}.push._push_data") as pd_mock, \
         patch(f"{module_name}.needs_specialremote_credential_envpatch") as nsce_mock, \
         patch(f"{module_name}.get_specialremote_credential_envpatch") as gsce_mock, \
         patch(f"{module_name}._get_export_log_entry") as gele_mock, \
         patch(f"{module_name}._get_credentials") as gc_mock:

        nsce_mock.return_value = True
        gsce_mock.return_value = {"WEBDAVU": "hans", "WEBDAVP": "abc"}
        gele_mock.return_value = None
        gc_mock.return_value = {"secret": "abc", "user": "hans"}
        results = tuple(_transfer_data(
            repo=MockRepo(),
            ds=ds_mock,
            target="yes-target",
            content=[],
            data="",
            force=None,
            jobs=None,
            res_kwargs={"some": "arg"},
            got_path_arg=False))
        eq_(pd_mock.call_count, 0)
        assert_in(
            {"target": "yes-target", "status": "ok", "some": "arg"},
            results)


def test_no_special_remotes():
    # Ensure that the code works if no special remotes exist
    with patch("datalad_next.patches.push_to_export_remote.push._push_data") as pd_mock:
        results = tuple(_transfer_data(
            repo=MockRepo(return_special_remotes=False),
            ds=None,
            target="no-target",
            content=[],
            data="",
            force=None,
            jobs=None,
            res_kwargs=dict(),
            got_path_arg=False))
        eq_(pd_mock.call_count, 1)
