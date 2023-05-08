import os

from datalad_next.tests.utils import SkipTest

from ..fsspec import (
    FsspecUrlOperations,
)

target_reqfile_md5sum = '7b5a906014a9e6f6b24dfc0b4aa5a2bd'
target_reqfile_content = """\
# Theoretically we don't want -e here but ATM pip would puke if just .[full] is provided
# Since we use requirements.txt ATM only for development IMHO it is ok but
# we need to figure out/complaint to pip folks
-e .[devel]
"""


def test_fsspec_download(tmp_path):
    # test a bunch of different (chained) URLs that point to the same content
    # on different persistent storage locations
    ops = FsspecUrlOperations()
    for url in (
        # included in a ZIP archive
        'zip://datalad-datalad-cddbe22/requirements-devel.txt::https://zenodo.org/record/7497306/files/datalad/datalad-0.18.0.zip?download=1',
        # included in a TAR archive
        'tar://datalad-0.18.0/requirements-devel.txt::https://files.pythonhosted.org/packages/dd/5e/9be11886ef4c3c64e78a8cdc3f9ac3f27d2dac403a6337d5685cd5686770/datalad-0.18.0.tar.gz',
        # pushed to github
        'github://datalad:datalad@0.18.0/requirements-devel.txt',
    ):
        props = ops.download(url, tmp_path / 'dummy', hash=['md5'])
        assert props['md5'] == target_reqfile_md5sum
        assert (tmp_path / 'dummy').read_text() == target_reqfile_content


def test_fsspec_download_authenticated(tmp_path, tmp_keyring):
    if 'DATALAD_CREDENTIAL_awshcp_SECRET' not in os.environ:
        # only attempt if we have a dedicated test credential
        # full set of requirements is:
        # DATALAD_CREDENTIAL_awshcp_KEY=...
        # DATALAD_CREDENTIAL_awshcp_SECRET=...
        # DATALAD_CREDENTIAL_awshcp_REALM=s3://hcp-openaccess.s3.amazonaws.com
        # the actual credential name does not matter (it only binds the
        # properties, so we pick 'awshcp'), but the realm needs to fit
        raise SkipTest

    ops = FsspecUrlOperations()
    target_path = tmp_path / 'dummy'
    res = ops.download(
        's3://hcp-openaccess/HCP_1200/835657/MNINonLinear/Results/tfMRI_WM_RL/EVs/0bk_faces.txt',
        tmp_path / 'dummy',
        hash=['md5'],
    )
    # actually read the file (it is tiny) to make sure that the entire
    # download workflow worked
    assert target_path.read_text() == '36.279\t27.5\t1\n'
    # we get the corresponding checksum report in the result too
    assert res['md5'] == '977c35302f83e4da2fb63b782a249812'


def test_fsspec_sniff(tmp_path):
    ops = FsspecUrlOperations()
    fpath = tmp_path / 'probe.txt'
    fpath.write_text('6bytes')
    res = ops.sniff(fpath.as_uri())
    # res will have all kinds of info (with a `stat_` prefix in the key),
    # but the one thing we need is `content-length` (normalized key from
    # `stat_size`)
    assert res['content-length'] == 6


def test_fsspec_upload(tmp_path):
    ops = FsspecUrlOperations()
    spath = tmp_path / 'src.txt'
    spath.write_text('6bytes')
    dpath = tmp_path / 'newsubdir' / 'dst.txt'
    res = ops.upload(spath, dpath.as_uri(), hash=['md5'])
    assert dpath.read_text() == '6bytes'
    assert res['md5'] == 'd3c9ca3ddd1347a43a856f47efcece79'


def test_fsspec_delete(tmp_path):
    ops = FsspecUrlOperations()
    fpath = tmp_path / 'target.txt'
    fpath.write_text('6bytes')
    assert fpath.exists()
    res = ops.delete(fpath.as_uri())
    assert not fpath.exists()
    # we get a standard stat report on what the deleted content
    # used to be
    assert res['content-length'] == 6
