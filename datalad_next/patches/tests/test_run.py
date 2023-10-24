from pathlib import Path
import pytest

from datalad.local.rerun import get_run_info

from datalad_next.exceptions import IncompleteResultsError
from datalad_next.tests.utils import (
    SkipTest,
    assert_result_count,
)


def test_substitution_config_default(existing_dataset):
    ds = existing_dataset

    if ds.config.get('datalad.run.substitutions.python') is not None:
        # we want to test default handling when no config is set
        raise SkipTest(
            'Test assumptions conflict with effective configuration')

    # the {python} placeholder is not explicitly defined, but it has
    # a default, which run() should discover and use
    res = ds.run('{python} -c "True"', result_renderer='disabled')
    assert_result_count(res, 1, action='run', status='ok')

    # make sure we could actually detect breakage with the check above
    with pytest.raises(IncompleteResultsError):
        ds.run('{python} -c "breakage"', result_renderer='disabled')


def test_runrecord_portable_paths(existing_dataset):
    ds = existing_dataset
    dsrepo = ds.repo
    infile = ds.pathobj / 'inputs' / 'testfile.txt'
    outfile = ds.pathobj / 'outputs' / 'testfile.txt'
    infile.parent.mkdir()
    outfile.parent.mkdir()
    infile.touch()
    ds.save()
    assert not outfile.exists()
    # script copies any 'inputs' to the outputs dir
    res = ds.run(
        '{python} -c "'
        'from shutil import copyfile;'
        'from pathlib import Path;'
        r"""[copyfile(f.strip('\"'), Path.cwd() / \"outputs\" / Path(f.strip('\"')).name)"""
        # we need to use a raw string to contain the inputs expansion,
        # on windows they would contain backslashes that are unescaped
        r""" for f in r'{inputs}'.split()]"""
        '"',
        result_renderer='disabled',
        # we need to pass relative paths ourselves
        # https://github.com/datalad/datalad/issues/7516
        inputs=[str(infile.relative_to(ds.pathobj))],
        outputs=[str(outfile.relative_to(ds.pathobj))],
    )
    # verify basic outcome
    assert_result_count(res, 1, action='run', status='ok')
    assert outfile.exists()

    # branch we expect the runrecord on
    branch = dsrepo.get_corresponding_branch() or dsrepo.get_active_branch()
    cmsg = dsrepo.format_commit('%B', branch)

    # the IOspecs are stored in POSIX conventions
    assert r'"inputs/testfile.txt"' in cmsg
    assert r'"outputs/testfile.txt"' in cmsg

    # get_run_info() reports in platform conventions
    msg, run_info = get_run_info(ds, cmsg)
    assert run_info
    for k in ('inputs', 'outputs'):
        specs = run_info.get(k)
        assert len(specs) > 0
        for p in specs:
            assert (ds.pathobj / p).exists()


def test_runrecord_oldnative_paths(existing_dataset):
    ds = existing_dataset
    # this test is imitating a rerun situation, so we create the
    # inputs and outputs
    infile = ds.pathobj / 'inputs' / 'testfile.txt'
    outfile = ds.pathobj / 'outputs' / 'testfile.txt'
    for fp in (infile, outfile):
        fp.parent.mkdir()
        fp.touch()

    cmsg = (
        '[DATALAD RUNCMD] /home/mih/env/datalad-dev/bin/python -c ...\n\n'
        '=== Do not change lines below ===\n'
        '{\n'
        ' "chain": [],\n'
        ' "cmd": "{python} -c True",\n'
        # use the ID of the test dataset to ensure proper association
        f' "dsid": "{ds.id}",\n'
        ' "exit": 0,\n'
        ' "extra_inputs": [],\n'
        ' "inputs": [\n'
        # make windows path, used to be stored in escaped form
        r'   "inputs\\testfile.txt"' '\n'
        ' ],\n'
        ' "outputs": [\n'
        # make windows path, used to be stored in escaped form
        r'   "outputs\\testfile.txt"' '\n'
        ' ],\n'
        ' "pwd": "."\n'
        '}\n'
        '^^^ Do not change lines above ^^^\n'
    )
    msg, run_info = get_run_info(ds, cmsg)
    assert run_info['inputs'][0] == str(Path('inputs', 'testfile.txt'))
    assert run_info['outputs'][0] == str(Path('outputs', 'testfile.txt'))
