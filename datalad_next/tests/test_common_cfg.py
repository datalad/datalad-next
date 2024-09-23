import datalad_next
from datalad_next.commands.legacy_result_handler import LegacyResultHandler


def test_annexretry():
    from datalad.interface.common_cfg import definitions
    assert definitions['datalad.annex.retry']['default'] == 1


def test_getset_result_handler_():
    orig_handler = datalad_next.get_command_result_handler_class()
    datalad_next.set_command_result_handler_class(LegacyResultHandler)
    assert datalad_next.get_command_result_handler_class() \
        == LegacyResultHandler
    datalad_next.set_command_result_handler_class(orig_handler)
