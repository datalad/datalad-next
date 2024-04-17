from pathlib import PurePosixPath
from datalad.distributed.ora_remote import SSHRemoteIO


def test_sshremoteio(sshserver, tmp_path):
    sshurl, sshlocalpath = sshserver
    io = SSHRemoteIO(sshurl)
    # relative path, must be interpreted relative to given base url
    testfpath = 'dummy.txt'
    # we run in a tmp dir, test file must not exit
    assert not io.exists(testfpath)

    # TODO this content has a trailing newline, because "write_file()" requires
    # that. Madness. Remove when fixed, must work without.
    testcontent = 'two\nlines\n'
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

