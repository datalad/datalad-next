from datalad.distributed.ora_remote import SSHRemoteIO


def test_sshremoteio(sshserver):
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
