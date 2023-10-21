from datalad_next.utils import chpwd
from datalad_next.tests.utils import run_main


def test_cli_configoverrides(existing_dataset):
    # test whether a `datalad -c ...` is effective within the
    # execution environment of a subprocess (for a non-datalad
    # configuration item
    with chpwd(existing_dataset.path):
        out, err = run_main(
            [
                '-c', 'bogusdataladtestsec.subsec=unique',
                'run',
                'git config bogusdataladtestsec.subsec',
            ],
            # git-config would fail, if the config item is unknown
            exit_code=0,
        )
