import os
import pytest

skipif_no_network = pytest.mark.skipif(
    'DATALAD_TESTS_NONETWORK' in os.environ,
    reason='DATALAD_TESTS_NONETWORK is set'
)
