from datalad.api import clone
from datalad_next.tests import skip_if_on_windows


# we cannot yet run on windows. see
# https://github.com/datalad/datalad-next/issues/654
def test_ria_ssh_roundtrip(
        sshserver, existing_dataset, no_result_rendering,
        tmp_path):
    ds = existing_dataset
    sshurl, sshlocalpath = sshserver
    testfile = ds.pathobj / 'testfile1.txt'
    testfile_content = 'uppytyup!'
    testfile.write_text(testfile_content)
    ds.save()
    # create store
    ds.create_sibling_ria(
        f'ria+{sshurl}',
        name='datastore',
        new_store_ok=True,
    )
    # push to store
    ds.push(to='datastore')
    # clone from store into a new location
    dsclone = clone(
        source=f'ria+{sshurl}#{ds.id}',
        path=tmp_path,
    )
    dsclone.get('.')
    assert ds.id == dsclone.id
    assert (ds.pathobj / 'testfile1.txt').read_text() \
        == (dsclone.pathobj / 'testfile1.txt').read_text() \
        == 'uppytyup!'
