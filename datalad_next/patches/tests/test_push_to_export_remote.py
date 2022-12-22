from pathlib import Path
from typing import Generator
from unittest.mock import (
    MagicMock,
    call,
    patch,
)

from datalad.runner.exception import CommandError
from datalad_next.tests.utils import (
    SkipTest,
    assert_false,
    assert_in,
    assert_in_results,
    assert_true,
    eq_,
)

from datalad_next.patches.push_to_export_remote import (
    _get_export_log_entry,
    _is_export_remote,
    _is_valid_treeish,
    _transfer_data,
    get_export_records,
)


module_name = "datalad_next.patches.push_to_export_remote"


class MockRepo:
    def __init__(self, return_special_remotes: bool = True):
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

    def _call_annex_records_items_(self, *args, **kwargs):
        yield {
            "command": f"export {args[0][3]}",
            "file": "file.txt",
            "success": True,
            "input": [],
            "error-messages": []
        }
        yield {
            "command": f"export {args[0][3]}",
            "success": False,
            "input": [],
            "error-messages":
                ["external special remote error: WHATEVER WENT WRONG"],
            "file": "somefile"}


def _call_transfer(target: str,
                   config_result: bool,
                   return_special_remotes: bool = True) -> Generator:
    ds_mock = MagicMock()
    ds_mock.config.getbool.return_value = config_result
    ds_mock.pathobj = Path("/root")
    return _transfer_data(
        repo=MockRepo(return_special_remotes),
        ds=ds_mock,
        target=target,
        content=[],
        data="",
        force=None,
        jobs=None,
        res_kwargs={"path": str(Path("/root"))},
        got_path_arg=False)


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
    with patch("datalad_next.patches.push_to_export_remote.mod_push._push_data") as pd_mock:
        tuple(_call_transfer("no-target", False))
        eq_(pd_mock.call_count, 1)


def test_patch_execute_export():
    # Ensure that export is called if the target remote has exporttree set to
    # "yes"
    with patch(f"{module_name}.mod_push._push_data") as pd_mock, \
         patch(f"{module_name}._get_export_log_entry") as gele_mock:
        gele_mock.return_value = None
        results = tuple(_call_transfer("yes-target", False))
        eq_(pd_mock.call_count, 0)
        assert_in_results(results,
                          path=str(Path("/root/file.txt")),
                          target="yes-target",
                          action="copy",
                          status="ok")
        assert_in_results(results,
                          path=str(Path("/root/somefile")),
                          target="yes-target",
                          action="copy",
                          status="error")


def test_patch_skip_ignore_targets_export():
    with patch(f"{module_name}.lgr") as lgr_mock:
        tuple(_call_transfer("yes-target", True))
        assert_in(
            call.debug(
                "Target '%s' is set to annex-ignore, exclude from data-export.",
                'yes-target'
            ),
            lgr_mock.mock_calls
        )


def test_patch_check_envpatch():
    # Ensure that export is called if the target remote has exporttree not set
    # to "yes"
    with patch(f"{module_name}.mod_push._push_data") as pd_mock, \
         patch(f"{module_name}.needs_specialremote_credential_envpatch") as nsce_mock, \
         patch(f"{module_name}.get_specialremote_credential_envpatch") as gsce_mock, \
         patch(f"{module_name}._get_export_log_entry") as gele_mock, \
         patch(f"{module_name}._get_credentials") as gc_mock:

        nsce_mock.return_value = True
        gsce_mock.return_value = {"WEBDAVU": "hans", "WEBDAVP": "abc"}
        gele_mock.return_value = None
        gc_mock.return_value = {"secret": "abc", "user": "hans"}
        results = tuple(_call_transfer("yes-target", False))
        eq_(pd_mock.call_count, 0)
        assert_in_results(results,
                          path=str(Path("/root/file.txt")),
                          target="yes-target",
                          action="copy",
                          status="ok")
        assert_in_results(results,
                          path=str(Path("/root/somefile")),
                          target="yes-target",
                          action="copy",
                          status="error")


def test_no_special_remotes():
    # Ensure that the code works if no special remotes exist
    with patch(f"{module_name}.mod_push._push_data") as pd_mock:
        tuple(_call_transfer("no-target", False, False))
        eq_(pd_mock.call_count, 1)


def test_get_export_records_no_exports():
    class NoExportRepo:
        def call_git_items_(self, *args, **kwargs):
            raise CommandError(
                stderr="fatal: Not a valid object name git-annex:export.log")

    results = tuple(get_export_records(NoExportRepo()))
    eq_(results, ())


def test_get_export_records():
    class SomeExportsRepo:
        def call_git_items_(self, *args, **kwargs):
            return [
                f"{i}.3s from{i}:to 0000{i}"
                for i in (3, 1, 4, 5, 2)
            ]

    result = tuple(get_export_records(SomeExportsRepo()))
    expected = tuple(
        {
            "timestamp": float(i + .3),
            "source-annex-uuid": f"from{i}",
            "destination-annex-uuid": f"to",
            "treeish": f"0000{i}"
        }
        for i in range(1, 6)
    )
    for remote_info in expected:
        assert_in(remote_info, result)


def test_get_export_log_entry():
    # Expect the youngest entry to be returned.
    class ManyExportsRepo:
        def call_git_items_(self, *args, **kwargs):
            return [
                f"{i}.3s from{i}:to 0000{i}"
                for i in (3, 4, 1, 5, 2)
            ]

        def get_export_records(self):
            yield from get_export_records(self)

    result = _get_export_log_entry(ManyExportsRepo(), "to")
    eq_(
        result,
        {
            "timestamp": 5.3,
            "source-annex-uuid": "from5",
            "destination-annex-uuid": "to",
            "treeish": f"00005"
        }
    )


def test_is_valid_treeish():
    raise SkipTest(
        "this test is skipped until issue "
        "https://github.com/datalad/datalad-next/issues/39 is solved")

    class LogRepo:
        def call_git_items_(self, *args, **kwargs):
            return [
                f"commit{i} 0000{i}"
                for i in range(4)
            ]

    # Check successful validation
    export_entry = {"treeish": "00002"}
    assert_true(_is_valid_treeish(LogRepo(), export_entry))

    # Check unsuccessful validation
    export_entry = {"treeish": "10000"}
    assert_false(_is_valid_treeish(LogRepo(), export_entry))
