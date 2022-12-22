"""Add RIA-store support to ``clone()``\

This feature was previously spaghetti-coded directly into ``clone_dataset()``,
and is now factored out into a patch set.
"""

__docformat__ = 'restructuredtext'

import logging
from typing import Dict

from datalad.core.distributed.clone import (
    postclone_preannex_cfg_ria,
    postclonecfg_ria,
)

from datalad_next.datasets import Dataset
from datalad_next.utils.patch import apply_patch

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
orig_post_git_init_processing_ = apply_patch(
    'datalad_next.patches.clone', None, '_post_git_init_processing_',
    _post_git_init_processing_,
    msg='Apply datalad-next RIA patch to clone.py:_post_git_init_processing_')
orig_pre_final_processing_ = apply_patch(
    'datalad_next.patches.clone', None, '_pre_final_processing_',
    _pre_final_processing_,
    msg='Apply datalad-next RIA patch to clone.py:_pre_final_processing_')
