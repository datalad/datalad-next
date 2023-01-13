
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
