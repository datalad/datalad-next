from pathlib import Path

from datalad.dataset.gitrepo import GitRepo as LeanGitRepo
from datalad.support.annexrepo import AnnexRepo as LegacyAnnexRepo


class LeanAnnexRepo(LegacyAnnexRepo):
    """git-annex repository representation with a minimized API

    This is a companion of :class:`LeanGitRepo`. In the same spirit, it
    restricts its API to a limited set of method that extend
    :class:`LeanGitRepo`.

    """
    #CA .. autosummary::

    #CA    call_annex
    #CA    call_annex_oneline
    #CA    call_annex_success
    # list of attributes permitted in the "lean" API. This list extends
    # the API of LeanGitRepo
    # TODO extend whitelist of attributes as necessary
    _lean_attrs = [
        #CA # these are the ones we intend to provide
        #CA 'call_annex',
        #CA 'call_annex_oneline',
        #CA 'call_annex_success',
        # and here are the ones that we need to permit in order to get them
        # to run
        '_check_git_version',
        #CA '_check_git_annex_version',
        # used by AnnexRepo.__init__() -- should be using `is_valid()`
        'is_valid_git',
        'is_valid_annex',
        '_is_direct_mode_from_config',
        #CA '_call_annex',
        #CA 'call_annex_items_',
    ]

    # intentionally limiting to just `path` as the only constructor argument
    def __new__(cls, path: Path):
        for attr in dir(cls):
            if not hasattr(LeanGitRepo, attr) \
                    and callable(getattr(cls, attr)) \
                    and attr not in LeanAnnexRepo._lean_attrs:
                setattr(cls, attr, _unsupported_method)

        obj = super(LegacyAnnexRepo, cls).__new__(cls)

        return obj


def _unsupported_method(self, *args, **kwargs):
    raise NotImplementedError('method unsupported by LeanAnnexRepo')
