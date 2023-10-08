import pytest

from ..protocols import StdOutCaptureGeneratorProtocol

from ..run import run


def test_run_timeout():
    with pytest.raises(TimeoutError):
        with run(['sleep', '3'],
                 StdOutCaptureGeneratorProtocol,
                 timeout=1) as sp:
            # must poll, or timeouts are not checked
            list(sp)

