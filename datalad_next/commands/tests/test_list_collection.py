from pathlib import Path
from shutil import rmtree

from ..list_collection import detect_collection_type


def test_local_file(tmp_path: Path):

    expected_types = [
        ('', 'file'),
        ('.x', 'file'),
        ('.tar', 'tar'),
        ('.tar.gz', 'tar'),
        ('.tgz', 'tar'),
        ('.zip', 'zip'),
        ('.7z', '7z')
    ]

    # Test plain file detection
    for suffix, expected_type in expected_types:
        test_file_path = tmp_path / ('test_file' + suffix)
        test_file_path.write_text('abc')
        assert detect_collection_type(str(test_file_path)) == expected_type


def test_local_dir_types(tmp_path: Path):

    bare_subdirs = [
        'branches', 'config', 'description', 'HEAD',
        'hooks', 'info', 'objects', 'refs',
    ]

    expected_types = [
        ([], 'directory'),
        (['.git'], 'git'),
        (['.git', '.datalad'], 'dataset'),
        (bare_subdirs, 'git-bare'),
    ]

    test_dir_path = tmp_path / 'test_dir'
    for subdir_names, expected_type in expected_types:
        test_dir_path.mkdir()
        for subdir_name in subdir_names:
            (test_dir_path / subdir_name).mkdir()
        assert detect_collection_type(str(test_dir_path)) == expected_type
        rmtree(str(test_dir_path))
