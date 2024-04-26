from ..annex import has_initialized_annex


def test_has_initialized_annex(existing_dataset):
    # for the root
    assert has_initialized_annex(existing_dataset.pathobj)
    # for a subdir
    assert has_initialized_annex(existing_dataset.pathobj / '.datalad')


def test_no_initialized_annex(existing_noannex_dataset, tmp_path):
    # for the root
    assert not has_initialized_annex(existing_noannex_dataset.pathobj)
    # for a subdir
    assert not has_initialized_annex(
        existing_noannex_dataset.pathobj / '.datalad')
    # for a random directory
    assert not has_initialized_annex(tmp_path)
