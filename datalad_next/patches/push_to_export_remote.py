import logging
from typing import (
    Dict,
    Generator,
    Iterable,
    Optional,
    Union,
)

import datalad.core.distributed.push as push
from datalad.distribution.dataset import Dataset
from datalad.support.annexrepo import AnnexRepo


__docformat__ = "restructuredtext"


lgr = logging.getLogger('datalad_next.push_to_export_remote')


def _is_export_remote(repo: AnnexRepo,
                      target: str,
                      ) -> bool:
    """Check if remote "target" exists and has exporttree set to "yes"

    :param repo: the repository that contains the remote
    :param target: name of the remote that should be checked
    :return: True if git annex reports exporttree is yes, else False
    """
    remote_info = [
        record
        for record in repo.get_special_remotes().values()
        if record.get("name") == target]
    if len(remote_info) == 1:
        return remote_info[0].get("exporttree") == "yes"
    return False


def _is_webdav_remote(repo: AnnexRepo,
                      target: str,
                      ) -> bool:
    """Check if remote "target" exists and has type set to "webdav"

    :param repo: the repository that contains the remote
    :param target: name of the remote that should be checked
    :return: True if git annex reports exporttree is yes, else False
    """
    remote_info = [
        record
        for record in repo.get_special_remotes().values()
        if record.get("name") == target]
    if len(remote_info) == 1:
        return remote_info[0].get("exporttree") == "yes"
    return False


def _transfer_data(repo: AnnexRepo,
                   ds: Dataset,
                   target: str,
                   content: Iterable,
                   data: str,
                   force: Optional[str],
                   jobs: Optional[Union[str, int]],
                   res_kwargs: Dict,
                   got_path_arg: bool
                   ) -> Generator:

    if _is_export_remote(repo, target):
        # TODO:
        #  - check for configuration entries, e.g. what to export
        #  - check for all kind of things that are checked in push._push_data
        #  - proper error handling
        #  - connect with credential handling

        lgr.debug("Exporting HEAD to a remote with exporttree == yes")

        if ds.config.getbool('remote.{}'.format(target), 'annex-ignore', False):
            lgr.debug(
                "Target '%s' is set to annex-ignore, exclude from data-export.",
                target,
            )
            return

        res_kwargs['target'] = target

        repo.call_git(["annex", "export", "HEAD", "--to", target])
        yield dict(
            res_kwargs,
            action='export',
            status='ok',
        )
    else:
        yield from push._push_data(
            ds,
            target,
            content,
            data,
            force,
            jobs,
            res_kwargs.copy(),
            got_path_arg=got_path_arg,
        )


lgr.debug("Patching datalad.core.distributed.push._transfer_data")
push._transfer_data = _transfer_data
