import logging
import io

from datalad.tests.utils_pytest import (
    assert_raises,
    eq_,
)
from datalad.utils import swallow_outputs

from ..base import (
    BackendError,
    Master,
    NotLinkedError,
    Protocol,
    ProtocolError,
    UnsupportedRequest,
)


class FakeBackend(object):
    def error(self, message):
        raise ValueError(message)

    def gen_key(self, val):
        raise BackendError('not worky-worky')

    def verify_content(self, f, key):
        raise BackendError('not worky-worky')

    def can_verify(self):
        raise RuntimeError('intentional blow')


def test_protocol():
    """Test essential protocol (error) behavior"""
    p = Protocol(FakeBackend())
    # no empty lines
    assert_raises(ProtocolError, p.command, '')
    # version check
    eq_(p.command('GETVERSION'), 'VERSION 1')
    # unknown command
    assert_raises(UnsupportedRequest, p.command, 'GOTNOCLUE')
    # pass arg to command method
    with assert_raises(ValueError) as ve:
        p.command('ERROR my message')
        eq_(str(ve.exception), 'my message')
    # backend failure is communicated for key generation
    eq_(p.command('GENKEY for-some-file'), 'GENKEY-FAILURE not worky-worky')
    # and for key verification
    eq_(p.command('VERIFYKEYCONTENT for-some-file mykey'), 'VERIFYKEYCONTENT-FAILURE')


def test_master():
    master = Master()
    assert_raises(NotLinkedError, master.Listen)
    master.LinkBackend(FakeBackend())
    with swallow_outputs() as cmo:
        master.Listen(io.StringIO('GETVERSION'))
        eq_(cmo.out, 'VERSION 1\n')
    with swallow_outputs() as cmo:
        master.Listen(io.StringIO('FUNKY'))
        # comes with a DEBUG message showing exactly what went wrong
        eq_(cmo.out, "DEBUG Unknown request 'FUNKY'\nUNSUPPORTED-REQUEST\n")
    assert_raises(SystemExit, master.Listen, io.StringIO('CANVERIFY'))
    with swallow_outputs() as cmo:
        master.progress(15)
        eq_(cmo.out, 'PROGRESS 15\n')


