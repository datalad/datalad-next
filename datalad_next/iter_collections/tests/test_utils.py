from datalad_next.tests import skip_wo_symlink_capability

from ..utils import FileSystemItem


def test_FileSystemItem(tmp_path):
    testfile = tmp_path / 'file1.txt'
    testfile_content = 'content'
    testfile.write_text(testfile_content)

    item = FileSystemItem.from_path(testfile)
    assert item.size == len(testfile_content)
    assert item.link_target is None


@skip_wo_symlink_capability
def test_FileSystemItem_linktarget(tmp_path):
    testfile = tmp_path / 'file1.txt'
    testfile_content = 'short'
    testfile.write_text(testfile_content)
    testlink = tmp_path / 'link'
    testlink.symlink_to(testfile)

    item = FileSystemItem.from_path(testlink)
    assert testfile.samefile(item.link_target)
    # size of the link file does not anyhow propagate the size of the
    # link target
    assert item.size != len(testfile_content)

    # we can disable link resolution
    item = FileSystemItem.from_path(testlink, link_target=False)
    assert item.link_target is None
