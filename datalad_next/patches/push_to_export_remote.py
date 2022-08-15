import logging
from pathlib import Path
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
from datalad.support.constraints import EnsureChoice
from datalad.support.exceptions import CapturedException
from datalad.support.param import Parameter
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


def _get_credentials(ds: Dataset,
                     remote_info: Dict
                     ) -> Optional[Dict]:

    # Check for credentials
    params = {
        "type": remote_info.get("type"),
        "url": remote_info.get("url")
    }
    credentials = None
    credential_properties = get_specialremote_credential_properties(params)
    if credential_properties:
        # TODO: lower prio: factor this if clause out, also used in
        #  create_sibling_webdav.py
        credential_manager = CredentialManager(ds.config)
        credentials = (credential_manager.query(
            _sortby='last-used',
            **credential_properties) or [(None, None)])[0][1]
    return credentials


def get_export_records(repo: AnnexRepo) -> Generator:
    """Read exports that git-annex recorded in its 'export.log'-file

    Interpret the lines in export.log. Each line has the following structure:

        time-stamp " " source-annex-uuid ":" destination-annex-uuid " " treeish

    Parameters
    ----------
    repo: AnnexRepo
        The annex repo from which exports should be determined

    Returns
    -------
    Generator
        Generator yielding one dictionary for each export entry in git-annex.
        Each dictionary contains the keys: "timestamp", "source-annex-uuid",
        "destination-annex-uuid", "treeish". The timestamp-value is a float,
        all other values are strings.
    """
    try:
        for line in repo.call_git_items_(["cat-file", "blob", "git-annex:export.log"]):
            result_dict = dict(zip(
                [
                    "timestamp",
                    "source-annex-uuid",
                    "destination-annex-uuid",
                    "treeish"
                ],
                line.replace(":", " ").split()
            ))
            result_dict["timestamp"] = float(result_dict["timestamp"][:-1])
            yield result_dict
    except CommandError as command_error:
        # Some errors indicate that there was no export yet.
        # May depend on Git version
        expected_errors = (
            "fatal: Not a valid object name git-annex:export.log",
            "fatal: path 'export.log' does not exist in 'git-annex'", # v2.36
        )
        if command_error.stderr.strip() in expected_errors:
            return
        raise


def _get_export_log_entry(repo: AnnexRepo,
                          target_uuid: str
                          ) -> Optional[Dict]:
    target_entries = [
        entry
        for entry in repo.get_export_records()
        if entry["destination-annex-uuid"] == target_uuid]

    if not target_entries:
        return None
    return sorted(target_entries, key=lambda e: e["timestamp"])[-1]


def _is_valid_treeish(repo: AnnexRepo,
                      export_entry: Dict,
                      ) -> bool:

    # Due to issue https://github.com/datalad/datalad-next/issues/39
    # fast-forward validation has to be re-designed.
    return True
    #for line in repo.call_git_items_(["log", "--pretty=%H %T"]):
    #    commit_hash, treeish = line.split()
    #    if treeish == export_entry["treeish"]:
    #        return True
    #return False


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

    target_uuid, remote_info = ([
        (uuid, info) for uuid, info in repo.get_special_remotes().items()
        if info.get("name") == target] or [(None, None)])[0]

    if not _is_export_remote(remote_info):
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
        return

    from datalad.interface.results import annexjson2result

    # TODO:
    #  - check for configuration entries, e.g. what to export

    lgr.debug(f"Exporting HEAD of {ds} to remote {remote_info}")

    if ds.config.getbool('remote.{}'.format(target), 'annex-ignore', False):
        lgr.debug(
            "Target '%s' is set to annex-ignore, exclude from data-export.",
            target)
        return

    if force not in ("all", "export"):
        export_entry = _get_export_log_entry(repo, target_uuid)
        if export_entry:
            if export_entry["source-annex-uuid"] != repo.uuid:
                yield dict(
                    **res_kwargs,
                    status="error",
                    message=f"refuse to export to {target}, because the "
                            f"last known export came from another repo "
                            f"({export_entry['source-annex-uuid']}). Use "
                            f"--force=export to enforce the export anyway.")
                return
            if not _is_valid_treeish(repo, export_entry):
                yield dict(
                    **res_kwargs,
                    status="error",
                    message=f"refuse to export to {target}, because the "
                            f"current state is not a fast-forward of the "
                            f"last known exported state. Use "
                            f"--force=export to enforce the export anyway.")
                return

    credentials = _get_credentials(ds, remote_info)

    # If we have credentials, check whether we require an environment patch
    env_patch = {}
    remote_type = remote_info.get("type")
    if credentials and needs_specialremote_credential_envpatch(remote_type):
        env_patch = get_specialremote_credential_envpatch(
            remote_type,
            credentials)

    res_kwargs['target'] = target

    with patch.dict('os.environ', env_patch):
        try:
            for result in repo._call_annex_records_items_(
                [
                    "export", "HEAD",
                    "--to", target
                ],
                progress=True
            ):
                result_adjusted = \
                    annexjson2result(result, ds, **res_kwargs)
                # annexjson2result overwrites 'action' with annex' 'command',
                # even if we provided our 'action' within res_kwargs. Therefore,
                # change afterwards instead:
                result_adjusted['action'] = "copy"
                yield result_adjusted

        except CommandError as cmd_error:
            ce = CapturedException(cmd_error)
            yield {
                **res_kwargs,
                "action": "copy",
                "status": "error",
                "message": str(ce),
                "exception": ce
            }


lgr.debug("Patching datalad.core.distributed.push._transfer_data")
push._transfer_data = _transfer_data


lgr.debug("Patching datalad.core.distributed.push.Push docstring and parameters")
push.Push.__doc__ += """\


    The following feature is added by the datalad-next extension:

    If a target is a git-annex special remote that has "exporttree" set to
    "yes", push will call 'git-annex export' to export the current HEAD to the
    remote target. This will usually result in a copy of the file tree, to which
    HEAD refers, on the remote target. A git-annex special remote with
    "exporttree" set to "yes" can, for example, be created with the datalad
    command "create-sibling-webdav" with the option "--mode=filetree" or 
    "--mode=filetree-only".
"""
push.Push._params_["force"] = Parameter(
    args=("-f", "--force",),
    doc="""force particular operations, possibly overruling safety
    protections or optimizations: use --force with git-push ('gitpush');
    do not use --fast with git-annex copy ('checkdatapresent'); force an
    annex export (to git annex remotes with "exporttree" set to "yes");
    combine all force modes ('all').""",
    constraints=EnsureChoice(
        'all', 'gitpush', 'checkdatapresent', 'export', None))


from datalad.interface.base import build_doc
push.Push.__call__.__doc__ = None
push.Push = build_doc(push.Push)

lgr.debug("Patching datalad.support.AnnexRepo.get_export_records (new method)")
AnnexRepo.get_export_records = get_export_records
