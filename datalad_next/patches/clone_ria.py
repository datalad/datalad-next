""""""

__docformat__ = 'restructuredtext'

import logging
from typing import Dict

from datalad.core.distributed.clone import (
    postclone_preannex_cfg_ria,
    postclonecfg_ria,
)

from datalad.distribution.dataset import Dataset

from datalad_next.patches import clone as mod_clone

lgr = logging.getLogger('datalad.core.distributed.clone')


def _post_git_init_processing_(
    *,
    destds: Dataset,
    gitclonerec: Dict,
    remote: str,
    **kwargs
):
    yield from orig_post_git_init_processing_(
        destds=destds, gitclonerec=gitclonerec, remote=remote,
        **kwargs)

    # In case of RIA stores we need to prepare *before* annex is called at all
    if gitclonerec['type'] == 'ria':
        postclone_preannex_cfg_ria(destds, remote=remote)


def _pre_final_processing_(
        *,
        destds: Dataset,
        gitclonerec: Dict,
        remote: str,
        **kwargs
):
    if gitclonerec['type'] == 'ria':
        yield from postclonecfg_ria(destds, gitclonerec,
                                    remote=remote)

    yield from orig_pre_final_processing_(
        destds=destds, gitclonerec=gitclonerec, remote=remote,
        **kwargs)


# apply patch
lgr.debug(
    'Apply datalad-next RIA patch to clone.py:_post_git_init_processing_')
# we need to preserve the original function to be able to call it in the patch
orig_post_git_init_processing_ = mod_clone._post_git_init_processing_
mod_clone._post_git_init_processing_ = _post_git_init_processing_
lgr.debug(
    'Apply datalad-next RIA patch to clone.py:_pre_final_processing_')
orig_pre_final_processing_ = mod_clone._pre_final_processing_
mod_clone._pre_final_processing_ = _pre_final_processing_
