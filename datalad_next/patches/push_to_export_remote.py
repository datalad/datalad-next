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
from datalad_mihextras.export_to_webdav import ExportToWEBDAV


__docformat__ = "restructuredtext"


lgr = logging.getLogger('datalad_next.push_to_export_remote')


def _is_export_remote_webdav(repo: AnnexRepo,
                             target: str,
                             ) -> bool:
    """Check whether a remote has exporttree set to "yes"
    :param repo: the repository that contains the remote
    :param target: name of the remote that should be checked
    :param res_kwargs: preset kwargs for the result
    :return: True if git annex reports exporttree is yes, else False
    """
    remote_info = [
        record
        for record in repo.get_special_remotes().values()
        if record.get("name") == target and record.get("type") == "webdav"]
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

    if _is_export_remote_webdav(repo, target):
        yield from ExportToWEBDAV()(
            dataset=ds,
            to=target,
            url=None,
            mode="auto"
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
