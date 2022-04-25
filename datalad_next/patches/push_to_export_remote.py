import logging
from typing import (
    Dict,
    Generator,
    Iterable,
    Optional,
    Union,
)
from unittest.mock import patch

import datalad.core.distributed.push as push
from datalad.distribution.dataset import Dataset
from datalad.runner.exception import CommandError
from datalad.support.annexrepo import AnnexRepo
from datalad.support.exceptions import CapturedException
from datalad_next.credman import CredentialManager
from datalad_next.utils import (
    get_specialremote_credential_envpatch,
    get_specialremote_credential_properties,
    needs_specialremote_credential_envpatch,
)


lgr = logging.getLogger('datalad.core.distributed.push')


def _is_export_remote(remote_info: Optional[Dict]) -> bool:
    """Check if remote_info is valid and has exporttree set to "yes"

    Parameters
    ----------
    remote_info: Optional[Dict]
        Optional dictionary the contains git annex special.

    Returns
    -------
    bool
        True if exporttree key is contained in remote_info and is set to yes,
        else False.
    """
    if remote_info is not None:
        return remote_info.get("exporttree") == "yes"
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

    remote_info = ([
        record
        for record in repo.get_special_remotes().values()
        if record.get("name") == target] or [None])[0]

    if _is_export_remote(remote_info):
        # TODO:
        #  - check for configuration entries, e.g. what to export
        #  - check for all kind of things that are checked in push._push_data

        lgr.debug("Exporting HEAD to a remote with exporttree == yes")

        if ds.config.getbool('remote.{}'.format(target), 'annex-ignore', False):
            lgr.debug(
                "Target '%s' is set to annex-ignore, exclude from data-export.",
                target)
            return

        # Check for credentials
        sr_params = {
            "type": remote_info.get("type"),
            "url": remote_info.get("url")
        }
        credentials = None
        credential_properties = get_specialremote_credential_properties(sr_params)
        if credential_properties:
            # TODO: lower prio: factor this if clause out, also used in
            #  create_sibling_webdav.py
            credential_manager = CredentialManager(ds.config)
            credentials = (credential_manager.query(
                _sortby='last-used',
                **credential_properties) or [(None, None)])[0][1]

        # If we have credentials, check whether we require an environment patch
        env_patch = {}
        if credentials and needs_specialremote_credential_envpatch(sr_params["type"]):
            env_patch = get_specialremote_credential_envpatch(
                sr_params["type"],
                credentials)

        res_kwargs['target'] = target

        with patch.dict('os.environ', env_patch):
            try:
                repo.call_git(["annex", "export", "HEAD", "--to", target])
            except CommandError as cmd_error:
                ce = CapturedException(cmd_error)
                yield dict(
                    **res_kwargs,
                    status="error",
                    message=str(ce),
                    exception=ce)
                return

        yield dict(
            **res_kwargs,
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
