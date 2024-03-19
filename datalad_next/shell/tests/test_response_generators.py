from __future__ import annotations

from typing import cast

import pytest

from datalad_next.runners.iter_subproc import OutputFrom
from ..response_generators import (
    FixedLengthResponseGeneratorPosix,
    VariableLengthResponseGeneratorPosix,
    lgr as response_generator_lgr
)
from ..operations.posix import DownloadResponseGeneratorPosix
from ..operations.common import lgr as posix_common_lgr


class DummyOutputFrom(OutputFrom):
    def __init__(self,
                 iterable: list[bytes] | None = None,
                 ) -> None:
        super().__init__(None, None)
        self.iterable = iterable

    def send(self, _):
        if self.iterable:
            return self.iterable.pop(0)
        raise StopIteration


def test_unknown_state_detection_in_variable():
    # Check that the response generator detects an unknown internal state.
    # Since different
    response_generator = VariableLengthResponseGeneratorPosix(
        cast(OutputFrom, DummyOutputFrom())
    )
    response_generator.state = '<-no-such-state->'
    with pytest.raises(RuntimeError):
        response_generator.send(b'')


def test_unknown_state_detection():
    # Check that the response generator detects an unknown internal state.
    # Since different
    response_generators = [
        VariableLengthResponseGeneratorPosix(
            cast(OutputFrom, DummyOutputFrom())
        ),
        FixedLengthResponseGeneratorPosix(
            cast(OutputFrom, DummyOutputFrom()),
            100,
        ),
        DownloadResponseGeneratorPosix(
            cast(OutputFrom, DummyOutputFrom())
        ),
    ]
    for response_generator in response_generators:
        response_generator.state = '<-no-such-state->'
        with pytest.raises(RuntimeError):
            response_generator.send(b'')


def test_trailing_content_detection_in_variable(monkeypatch):
    # Check that the response generator detects a trailing newline.
    input_list = []
    warning_list = []
    response_generator = VariableLengthResponseGeneratorPosix(
        cast(OutputFrom, DummyOutputFrom(input_list))
    )
    input_list.extend([
        b'123\n',
        response_generator.stream_marker,
        b'0\nEXTRA-CONTENT\n',
    ])
    monkeypatch.setattr(
        response_generator_lgr,
        'warning',
        lambda *args, **kwargs: warning_list.append((args, kwargs))
    )
    assert tuple(response_generator) == (b'123\n',)
    assert warning_list == [(
        ('unexpected output after return code: %s', "b'EXTRA-CONTENT\\n'"),
        {}
    )]


def test_trailing_content_detection_in_fixed(monkeypatch):
    # Check that the response generator detects a trailing newline.
    input_list = [b'1230\nEXTRA-CONTENT\n']
    warning_list = []
    response_generator = FixedLengthResponseGeneratorPosix(
        cast(OutputFrom, DummyOutputFrom(input_list)),
        3,
    )
    monkeypatch.setattr(
        response_generator_lgr,
        'warning',
        lambda *args, **kwargs: warning_list.append((args, kwargs))
    )
    assert tuple(response_generator) == (b'123',)
    assert warning_list == [(
        ('unexpected output after return code: %s', "b'EXTRA-CONTENT\\n'"),
        {}
    )]


def test_trailing_content_detection_in_download(monkeypatch):
    # Check that the response generator detects a trailing newline.
    input_list = [b'3\n1230\nEXTRA-CONTENT\n']
    warning_list = []
    response_generator = DownloadResponseGeneratorPosix(
        cast(OutputFrom, DummyOutputFrom(input_list)),
    )
    monkeypatch.setattr(
        posix_common_lgr,
        'warning',
        lambda *args, **kwargs: warning_list.append((args, kwargs))
    )
    assert tuple(response_generator) == (b'123',)
    assert warning_list == [(
        ('unexpected output after return code: %s', "b'EXTRA-CONTENT\\n'"),
        {}
    )]
