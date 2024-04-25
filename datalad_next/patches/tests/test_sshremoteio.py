from pathlib import PurePosixPath
import pytest
import subprocess

from datalad.distributed.ora_remote import (
    RIARemoteError,
    SSHRemoteIO,
)


def test_sshremoteio(sshserver, tmp_path):
    sshurl, sshlocalpath = sshserver
    io = SSHRemoteIO(sshurl)
    # relative path, must be interpreted relative to given base url
    testfpath = 'dummy.txt'
    # we run in a tmp dir, test file must not exit
    assert not io.exists(testfpath)

    # TODO this content has a trailing newline, because "write_file()" requires
    # that. Madness. Remove when fixed, must work without.
    testcontent = 'two\nlines'
    io.write_file(testfpath, testcontent)
    # now we have a file
    assert io.exists(testfpath)
    # read content matches
    assert io.read_file(testfpath) == testcontent

    # create directory, make it interesting and have a space in the name
    testdirpath = 'something spacy'
    assert not io.exists(testdirpath)
    io.mkdir(testdirpath)
    assert io.exists(testdirpath)

    # download the testfile to local storage
    local_testfpath = tmp_path / testfpath
    # no progress callback
    io.get(testfpath, local_testfpath, lambda x: x)
    assert local_testfpath.read_text() == testcontent

    # upload to subdir
    testfpath_subdir = f'{testdirpath}/{testfpath}'
    assert not io.exists(testfpath_subdir)
    # TODO make absolutification unnecessary
    from urllib.parse import urlparse
    io.put(
        local_testfpath,
        f'{urlparse(sshurl).path}/{testfpath_subdir}',
        # no progress callback
        lambda x: x)
    assert io.exists(testfpath_subdir)

    # symlinks
    testfpath_link = 'dummy_link.txt'
    assert not io.exists(testfpath_link)
    io.symlink(testfpath, testfpath_link)
    assert io.exists(testfpath_link)
    assert io.read_file(testfpath_link) == testcontent

    # rename and delete
    # requires a Pure(Posix)Path object here
    io.rename(testfpath_subdir, PurePosixPath('deleteme'))
    assert not io.exists(testfpath_subdir)
    io.remove(PurePosixPath('deleteme'))
    assert not io.exists('deleteme')
    io.remove_dir(PurePosixPath(testdirpath))
    assert not io.exists(testdirpath)


def test_sshremoteio_7z(sshserver, tmp_path):
    sshurl, sshlocalpath = sshserver
    io = SSHRemoteIO(sshurl)
    # ensure we have a remote 7z
    if not io.get_7z():
        raise pytest.skip("No 7z available on SSH server target")

    testarchivefpath = 'my.7z'
    testfpath = 'dummy space.txt'
    testcontent = 'two\nlines\n'
    io.write_file(testfpath, testcontent)

    io.servershell(
        f'7z a "{testarchivefpath}" "{testfpath}"',
        check=True,
    )
    # we have an archive
    assert io.exists(testarchivefpath)
    # we have the test file in it
    assert io.in_archive(testarchivefpath, testfpath)
    # the "in" test means something
    assert not io.in_archive(testarchivefpath, "random_name")

    # we can pull from the archive
    extractfpath = tmp_path / 'extracted.txt'
    io.get_from_archive(testarchivefpath, testfpath, extractfpath, lambda x: x)
    assert extractfpath.read_text() == testcontent

    with pytest.raises(RIARemoteError):
        io.get_from_archive(
            'invalid_archive', testfpath, extractfpath, lambda x: x)
    with pytest.raises(RIARemoteError):
        io.get_from_archive(
            testarchivefpath, 'invalid_member', extractfpath, lambda x: x)
