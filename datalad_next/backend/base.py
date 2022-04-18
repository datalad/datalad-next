# Helper module to develop git-annex backends
#
# https://git-annex.branchable.com/design/external_backend_protocol/
#
# Derived from AnnexRemote Copyright (C) 2017  Silvio Ankermann (GPL-3)
"""Interface and essential utilities to implement external git-annex backends
"""

import logging

from abc import (
    ABCMeta,
    abstractmethod,
)

import sys
import traceback


class Backend(metaclass=ABCMeta):
    """Metaclass for backends.

    It implements the communication with git-annex via the external backend
    protocol. More information on the protocol is available at
    https://git-annex.branchable.com/design/external_backend_protocol/

    External backends can be built by implementing the abstract methods defined
    in this class.

    Attributes
    ----------
    annex : Master
        The Master object to which this backend is linked. Master acts as an
        abstraction layer for git-annex.
    """

    def __init__(self, annex):
        self.annex = annex

    @abstractmethod
    def can_verify(self):
        """Returns whether the backend can verify the content of files match a
        key it generated. The verification does not need to be
        cryptographically secure, but should catch data corruption.

        Returns
        -------
        bool
        """

    @abstractmethod
    def is_stable(self):
        """Returns whether a key it has generated will always have the same
        content. The answer to this is almost always yes; URL keys are an
        example of a type of key that may have different content at different
        times.

        Returns
        -------
        bool
        """

    @abstractmethod
    def is_cryptographically_secure(self):
        """ Returns whether keys it generates are verified using a
        cryptographically secure hash.

        Note that sha1 is not a cryptographically secure hash any longer.
        A program can change its answer to this question as the state of the
        art advances, and should aim to stay ahead of the state of the art by
        a reasonable amount of time.

        Returns
        -------
        bool
        """

    @abstractmethod
    def gen_key(self, local_file):
        """Examine the content of `local_file` and from it generate a key.

        While it is doing this, it can send any number of PROGRESS messages
        indication the position in the file that it's gotten to.

        Parameters
        ----------
        local_file: str
            Path for which to generate a key.
            Note that in some cases, local_file may contain whitespace.

        Returns
        -------
        str
          The generated key.

        Raises
        ------
        BackendError
            If the file could not be received from the backend.
        """

    @abstractmethod
    def verify_content(self, key, content_file):
        """Examine a file and verify it has the content expected given a key

        While it is doing this, it can send any number of PROGRESS messages
        indicating the position in the file that it's gotten to.

        If `can_verify() == False`, git-annex not ask to do this.


        Returns
        -------
        bool
        """

    def error(self, error_msg):
        """Communicate a generic error.

        Can be sent at any time if things get too messed up to
        continue.  If the program receives an error() from git-annex, it can
        exit with its own error().  Eg.: self.annex.error("Error received.
        Exiting.") raise SystemExit

        Parameters
        ----------
        error_msg : str
            The error message received from git-annex
        """
        self.annex.error("Error received. Exiting.")
        raise SystemExit


# Exceptions
class AnnexError(Exception):
    """
    Common base class for all annexbackend exceptions.
    """


class ProtocolError(AnnexError):
    """
    Base class for protocol errors
    """


class UnsupportedRequest(ProtocolError):
    """
    Must be raised when an optional request is not supported by the backend.
    """


class UnexpectedMessage(ProtocolError):
    """
    Raised when git-annex sends a message which is not expected at the moment
    """


class BackendError(AnnexError):
    """
    Must be raised by the backend when a request did not succeed.
    """


class NotLinkedError(AnnexError):
    """
    Will be raised when a Master instance is accessed without being
    linked to a Backend instance
    """


class AnnexLoggingHandler(logging.StreamHandler):
    """
    Stream Handler that sends log records to git annex via the backend protocol
    """
    def __init__(self, annex):
        super().__init__()
        self.annex = annex
        self.setFormatter(
            logging.Formatter('%(name)s - %(levelname)s - %(message)s'))

    def emit(self, record: logging.LogRecord):
        log_entry = self.format(record)
        for line in log_entry.splitlines():
            self.annex.debug(line)


class Protocol(object):
    """
    Helper class handling the receiving part of the protocol (git-annex to
    backend) It parses the requests coming from git-annex and calls the
    respective method of the backend object.
    """

    def __init__(self, backend):
        self.backend = backend
        self.version = "VERSION 1"

    def command(self, line):
        line = line.strip()
        parts = line.split(" ", 1)
        if not parts:
            raise ProtocolError("Got empty line")

        method = self.lookupMethod(parts[0]) or self.do_UNKNOWN

        try:
            if len(parts) == 1:
                reply = method()
            else:
                reply = method(parts[1])
        except TypeError as e:
            raise SyntaxError(e)
        else:
            return reply

    def lookupMethod(self, command):
        return getattr(self, 'do_' + command.upper(), None)

    def check_key(self, key):
        if len(key.split()) != 1:
            raise ValueError("Invalid key. Key contains whitespace character")

    def do_UNKNOWN(self, *arg):
        raise UnsupportedRequest(f'Unknown request {arg}')

    def do_GETVERSION(self):
        return self.version

    def do_CANVERIFY(self):
        return 'CANVERIFY-YES' if self.backend.can_verify() else 'CANVERIFY-NO'

    def do_ISSTABLE(self):
        return 'ISSTABLE-YES' if self.backend.is_stable() else 'ISSTABLE-NO'

    def do_ISCRYPTOGRAPHICALLYSECURE(self):
        return 'ISCRYPTOGRAPHICALLYSECURE-YES' \
            if self.backend.is_cryptographically_secure() \
            else 'ISCRYPTOGRAPHICALLYSECURE-NO'

    def do_GENKEY(self, *arg):
        try:
            key = self.backend.gen_key(arg[0])
            return f'GENKEY-SUCCESS {key}'
        except BackendError as e:
            return f'GENKEY-FAILURE {str(e)}'

    def do_VERIFYKEYCONTENT(self, *arg):
        try:
            success = self.backend.verify_content(*arg[0].split(" ", 1))
        except BackendError:
            success = False
        return 'VERIFYKEYCONTENT-SUCCESS' if success \
            else 'VERIFYKEYCONTENT-FAILURE'

    def do_ERROR(self, message):
        self.backend.error(message)


class Master(object):
    """
    Metaclass for backends.

    Attributes
    ----------
    input : io.TextIOBase
        Where to listen for git-annex request messages.
        Default: sys.stdin
    output : io.TextIOBase
        Where to send replies and backend messages
        Default: sys.stdout
    backend : Backend
        A class implementing the Backend interface to which this master
        is linked.
    """

    def __init__(self, output=sys.stdout):
        """
        Initialize the Master with an ouput.

        Parameters
        ----------
        output : io.TextIOBase
            Where to send replies and backend messages
            Default: sys.stdout
        """
        self.output = output

    def LinkBackend(self, backend):
        """
        Link the Master to a backend. This must be done before calling Listen()

        Parameters
        ----------
        backend : Backend
            A class implementing Backend interface to which this master
            will be linked.
        """
        self.backend = backend
        self.protocol = Protocol(backend)

    def LoggingHandler(self):
        """
        Gets an instance of AnnexLoggingHandler

        Returns
        -------
        AnnexLoggingHandler
        """
        return AnnexLoggingHandler(self)

    def Listen(self, input=sys.stdin):
        """
        Listen on `input` for messages from git annex.

        Parameters
        ----------
        input : io.TextIOBase
            Where to listen for git-annex request messages.
            Default: sys.stdin

        Raises
        ----------
        NotLinkedError
            If there is no backend linked to this master.
        """
        if not (hasattr(self, 'backend') and hasattr(self, 'protocol')):
            raise NotLinkedError("Please execute LinkBackend(backend) first.")

        self.input = input
        while True:
            # due to a bug in python 2 we can't use an iterator here: https://bugs.python.org/issue1633941
            line = self.input.readline()
            if not line:
                break
            line = line.rstrip()
            try:
                reply = self.protocol.command(line)
                if reply:
                    self._send(reply)
            except UnsupportedRequest as e:
                self.debug(str(e))
                self._send ("UNSUPPORTED-REQUEST")
            except Exception as e:
                for line in traceback.format_exc().splitlines():
                    self.debug(line)
                self.error(e)
                raise SystemExit

    def _ask(self, request, reply_keyword, reply_count):
        self._send(request)
        line = self.input.readline().rstrip().split(" ", reply_count)
        if line and line[0] == reply_keyword:
            line.extend([""] * (reply_count+1-len(line)))
            return line[1:]
        else:
            raise UnexpectedMessage(
                f"Expected {reply_keyword} and {reply_count} "
                f"values. Got {line}")

    def debug(self, *args):
        """
        Tells git-annex to display the message if --debug is enabled.

        Parameters
        ----------
        message : str
            The message to be displayed to the user
        """

        self._send("DEBUG", *args)

    def error(self, *args):
        """
        Generic error. Can be sent at any time if things get too messed up to continue.
        When possible, raise a BackendError inside the respective functions.
        The backend program should exit after sending this, as git-annex will
        not talk to it any further.

        Parameters
        ----------
        error_msg : str
            The error message to be sent to git-annex
        """
        self._send("ERROR", *args)

    def progress(self, progress):
        """
        Indicates the current progress of the transfer (in bytes). May be repeated 
        any number of times during the transfer process, but it's wasteful to update
        the progress until at least another 1% of the file has been sent.
        This is highly recommended for ``*_store()``. (It is optional but good for
        ``*_retrieve()``.)

        Parameters
        ----------
        progress : int
            The current progress of the transfer in bytes.
        """
        self._send("PROGRESS {progress}".format(progress=int(progress)))

    def _send(self, *args, **kwargs):
        print(*args, file=self.output, **kwargs)
        self.output.flush()
