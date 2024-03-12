from __future__ import annotations

import sys
from pathlib import PurePosixPath

import pytest
from more_itertools import consume

import datalad
from datalad.tests.utils_pytest import (
    on_osx,
    on_windows,
    skip_if,
)
from datalad_next.runners import (
    CommandError,
    iter_subproc,
)
from datalad_next.url_operations.ssh import ssh_url2openargs
from ..response_generators import (
    FixedLengthResponseGeneratorPosix,
    VariableLengthResponseGeneratorPosix,
    VariableLengthResponseGeneratorPowerShell,
    lgr as response_generator_lgr
)
from ..shell import (
    ShellCommandExecutor,
    shell,
)
from .. import posix


# Some files that are usually found on POSIX systems, i.e. Linux, OSX
common_files = [b'/etc/passwd', b'/etc/shells']


def _get_cmdline(ssh_url: str):
    args, parsed = ssh_url2openargs(ssh_url, datalad.cfg)
    return ['ssh'] + args, parsed.path


@pytest.mark.parametrize('file_name', common_files)
def test_basic_functionality(sshserver, file_name):
    ssh_url = sshserver[0]
    with shell(_get_cmdline(ssh_url)[0]) as ssh:
        _check_ls_result(ssh, file_name)


def test_basic_functionality_multi(sshserver):
    # Similar to `test_basic_functionality`, but executes all commands on the
    # same connection.
    ssh_url = sshserver[0]
    with shell(_get_cmdline(ssh_url)[0]) as ssh_executor:
        for file_name in common_files:
            _check_ls_result(ssh_executor, file_name)


def _check_ls_result(ssh_executor, file_name: bytes):
    results = ssh_executor(b'ls ' + file_name)
    assert b''.join(results) == file_name + b'\n'
    assert results.returncode == 0


def test_return_code_functionality(sshserver):
    ssh_url = sshserver[0]
    with shell(_get_cmdline(ssh_url)[0]) as ssh:
        results = ssh(b'bash -c "exit 123"')
        with pytest.raises(CommandError) as e:
            consume(results)
        assert results.returncode == 123


@pytest.mark.parametrize('cmd,expected', [
    (b'echo 0123456789', b'0123456789\n'),
    (b'echo -n 0123456789', b'0123456789')
])
def test_stdout_forwarding(sshserver, cmd, expected):
    ssh_url = sshserver[0]
    with shell(_get_cmdline(ssh_url)[0]) as ssh:
        _check_echo_result(ssh, cmd, expected)


def test_stdout_forwarding_multi(sshserver):
    # Similar to `test_stdout_forwarding`, but executes all commands on the
    # same connection.
    ssh_url = sshserver[0]
    with shell(_get_cmdline(ssh_url)[0]) as ssh:
        for cmd, expected in [(b'echo 0123456789', b'0123456789\n'),
                              (b'echo -n 0123456789', b'0123456789')]:
            _check_echo_result(ssh, cmd, expected)


def _check_echo_result(ssh: ShellCommandExecutor, cmd: bytes, expected: bytes):
    results = ssh(cmd)
    assert b''.join(results) == expected
    assert results.returncode == 0


def test_exit_if_unlimited_stdin_is_closed(sshserver):
    # Check that the test terminates if stdin is closed

    ssh_url, local_path = sshserver
    ssh_args, ssh_path = _get_cmdline(ssh_url)
    test_file_name = 'cat-123'
    # We know the ssh-server is on a POSIX system
    ssh_path = (ssh_path + '/' + test_file_name).encode()
    with \
            shell(ssh_args) as ssh_executor, \
            iter_subproc([sys.executable, '-c', 'print("0123456789")']) as cat_feed:

        results = ssh_executor(b'cat >' + ssh_path, stdin=cat_feed)
        ssh_executor.close()
        consume(results)
        assert results.returncode == 0
        assert (local_path / test_file_name).read_text() == '0123456789\n'


def test_continuation_after_stdin_reading(sshserver):
    # check that the connection continues to work, after stdin was fed into the
    # remote command.

    ssh_url, local_path = sshserver
    ssh_args, ssh_path = _get_cmdline(ssh_url)
    feed_command = [sys.executable, '-c', 'print("0123456789", end="")']
    with \
            shell(ssh_args) as ssh_executor, \
            iter_subproc(feed_command) as dd_feed_1, \
            iter_subproc(feed_command) as dd_feed_2:

        for file_name, feed in (('dd-123', dd_feed_1), ('dd-456', dd_feed_2)):
            server_path = (ssh_path + '/' + file_name).encode()
            results = ssh_executor(
                b'dd bs=1 count=10 of=' + server_path,
                stdin=feed
            )
            consume(results)
            assert results.returncode == 0
            assert (local_path / file_name).read_text() == '0123456789'

        _check_ls_result(ssh_executor, common_files[0])


def test_upload(sshserver, tmp_path):
    ssh_url, local_path = sshserver
    ssh_args, ssh_path = _get_cmdline(ssh_url)
    content = '0123456789'
    test_file_name = 'upload_123'
    upload_file = tmp_path / test_file_name
    upload_file.write_text(content)
    with shell(ssh_args) as ssh_executor:
        # perform an operation on the remote shell
        _check_ls_result(ssh_executor, common_files[0])

        # upload file to server and verify its content
        posix.upload(
            ssh_executor,
            upload_file,
            PurePosixPath(ssh_path + '/' + test_file_name)
        )
        assert (local_path / test_file_name).read_text() == content

        # perform another operation on the remote shell to ensure functionality
        _check_ls_result(ssh_executor, common_files[0])


def test_download_ssh(sshserver, tmp_path):
    ssh_url, local_path = sshserver
    ssh_args, ssh_path = _get_cmdline(ssh_url)
    content = '0123456789'
    test_file_name = 'download_123'
    server_file = local_path / test_file_name
    server_file.write_text(content)
    download_file = tmp_path / test_file_name
    with shell(ssh_args) as ssh_executor:
        # perform an operation on the remote shell
        _check_ls_result(ssh_executor, common_files[0])

        if on_osx:
            # The OSX CI image uses localhost as ssh-server, on OSX that
            # provides a `stat`-command that does not work with
            # `DownloadResponseGeneratorPosix`, therefore we added a new
            # response generator, that works with the ``stat``-command on OSX.
            rp_class = posix.DownloadResponseGeneratorOSX
        else:
            rp_class = posix.DownloadResponseGeneratorPosix

        # download file from server and verify its content
        posix.download(
            ssh_executor,
            PurePosixPath(ssh_path + '/' + test_file_name),
            download_file,
            rp_class,
        )
        assert download_file.read_text() == content

        # perform another operation on the remote shell to ensure functionality
        _check_ls_result(ssh_executor, common_files[0])


# This test only works on Posix-like systems because it executes a local
# bash command. It does not work on OSX, because the `stat` command has a
# different interface.
@skip_if(on_windows or on_osx)
def test_download_local_bash(tmp_path):
    content = '0123456789'
    download_file = tmp_path / 'download_123'
    download_file.write_text(content)
    result_file = tmp_path / 'result_123'
    with shell(['bash']) as bash:
        _check_ls_result(bash, common_files[0])

        # download file from server and verify its content
        posix.download(
            bash,
            PurePosixPath(download_file),
            result_file
        )
        assert result_file.read_text() == content

        # perform another operation on the remote shell to ensure functionality
        _check_ls_result(bash, common_files[0])


# This test only works on Posix-like systems because it executes a local bash
@skip_if(on_windows)
def test_upload_local_bash(tmp_path):
    content = '0123456789'
    upload_file = tmp_path / 'upload_123'
    upload_file.write_text(content)
    result_file = tmp_path / 'result_123'
    with shell(['bash']) as bash:
        _check_ls_result(bash, common_files[0])

        # upload file to server and verify its content
        posix.upload(bash, upload_file, PurePosixPath(result_file))
        assert result_file.read_text() == content

        # perform another operation on the remote shell to ensure functionality
        _check_ls_result(bash, common_files[0])


def test_delete(sshserver):
    ssh_url, local_path = sshserver
    ssh_args, ssh_path = _get_cmdline(ssh_url)

    files_to_delete = ('f1', 'f2', 'f3')
    with shell(ssh_args) as ssh_executor:
        for file in files_to_delete:
            (local_path / file).write_text(f'content_{file}')
            # verify that the remote files exist on the server
            _check_ls_result(ssh_executor, (ssh_path + '/' + file).encode())

        # delete files on server
        posix.delete(
            ssh_executor,
            [PurePosixPath(ssh_path) / file for file in files_to_delete],
            force=False,
        )

        # verify that the remote files were deleted
        for file in files_to_delete:
            assert not (local_path / file).exists()


def test_returncode():
    with pytest.raises(RuntimeError):
        with shell(['ssh', 'xyz@localhost:22']):
            pass


@skip_if(not on_windows)
def test_powershell_basic():
    with shell(
            ['powershell', '-Command', '-'],
            zero_command_rg_class=VariableLengthResponseGeneratorPowerShell,
    ) as pwsh:
        r = pwsh(b'Get-ChildItem')
        consume(r)
        if r.returncode != 0:
            print(
                'powershell_basic stderr output:',
                repr(b''.join(r.stderr_deque)),
                file=sys.stderr)
        assert r.returncode == 0
        r = pwsh(b'Get-ChildItem -Path C:\\')
        consume(r)
        if r.returncode != 0:
            print(
                'powershell_basic stderr output:',
                repr(b''.join(r.stderr_deque)),
                file=sys.stderr)
        assert r.returncode == 0
        pwsh.close()


@skip_if(not on_windows)
def test_powershell_repr():
    with shell(
            ['powershell', '-Command', '-'],
            zero_command_rg_class=VariableLengthResponseGeneratorPowerShell,
    ) as pwsh:
        assert "ShellCommandExecutor(['powershell', '-Command', '-'])" == repr(pwsh)
        pwsh.close()


@skip_if(on_windows)
def test_posix_repr():
    with shell(['bash']) as ssh:
        assert "ShellCommandExecutor(['bash'])" == repr(ssh)
        ssh.close()


# This test only works on Posix-like systems because it executes a local
# bash command
@skip_if(on_windows)
def test_variable_length_reuse(monkeypatch):
    # This test ensures that the `VariableLengthResponseGenerator` can be
    # reused, e.g. after it was used for command zero, even if there is
    # unexpected output after the return code.
    def mocked_get_command_list(command: bytes) -> list[bytes]:
        return [
            command + b' ; x=$?; echo -e -n "'
            + response_generator.end_marker
            + b'\\n"; echo -e "$x\\nsome stuff"\n'
        ]

    log_messages = []

    def mocked_log(*args):
        log_messages.append(args[0])

    with shell(['bash']) as bash:
        response_generator = VariableLengthResponseGeneratorPosix(bash.stdout)
        monkeypatch.setattr(
            response_generator,
            'get_command_list',
            mocked_get_command_list
        )
        monkeypatch.setattr(response_generator_lgr, 'warning', mocked_log)
        result = bash(
            b'echo hello',
            response_generator=response_generator
        )
        assert tuple(result) == (b'hello\n',)
        assert result.returncode == 0
        assert all(
            msg.startswith('unexpected output after return code: ')
            for msg in log_messages
        )
        bash.close()


# This test only works on Posix-like systems because it executes a local bash
@skip_if(on_windows)
def test_bad_zero_command(monkeypatch):
    monkeypatch.setattr(
        VariableLengthResponseGeneratorPosix,
        'zero_command',
        b'tessdsdsdt 0 -eq 1'
    )
    with pytest.raises(RuntimeError):
        with shell(['bash']):
            pass


def test_fixed_length_response_generator():
    with shell(['bash']) as bash:
        response_generator = FixedLengthResponseGeneratorPosix(
            bash.stdout,
            length=10
        )
        result = bash(
            b'echo -n 0123456789',
            response_generator=response_generator
        )
        assert tuple(result) == (b'0123456789',)
        assert result.returncode == 0

        # Check that only 10 bytes are consumed and any excess bytes show up
        # in the return code.
        with pytest.raises(ValueError):
            result = bash(
                b'echo -n 0123456789abc',
                response_generator=response_generator
            )
            tuple(result)


# This test only works on Unix-like systems because it executes a local bash.
@skip_if(on_windows)
def test_download_length_error():
    with shell(['bash']) as bash:
        if on_osx:
            # The OSX CI image uses localhost as ssh-server, on OSX that
            # provides a `stat`-command that does not work with
            # `DownloadResponseGeneratorPosix`, therefore we added a new
            # response generator, that works with the ``stat``-command on OSX.
            response_generator = posix.DownloadResponseGeneratorOSX(bash.stdout)
        else:
            response_generator = posix.DownloadResponseGeneratorPosix(bash.stdout)
        result = bash(b'unknown_file', response_generator=response_generator)
        with pytest.raises(CommandError):
            assert tuple(result) == ()
        assert result.returncode == 23

        # perform another operation on the remote shell to ensure functionality
        _check_ls_result(bash, common_files[0])


# This test only works on Linux systems because it executes a local bash and
# uses a version of `stat` that is not available on OSX.
@skip_if(on_windows or on_osx)
def test_download_error(tmp_path):
    with shell(['bash']) as bash:
        with pytest.raises(CommandError):
            posix.download(
                bash,
                PurePosixPath('/thisdoesnotexist'),
                tmp_path / 'downloaded_file',
            )

        # perform another operation on the remote shell to ensure functionality
        _check_ls_result(bash, common_files[0])
