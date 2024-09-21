from datalad.tests.test_config import *


# this datalad-core test is causing a persistent git config modification
# this is not legal on datalad-next, we must wrap and protect
_test_cross_cfgman_update = test_cross_cfgman_update


def test_cross_cfgman_update(datalad_cfg, tmp_path):
    _test_cross_cfgman_update(tmp_path)
