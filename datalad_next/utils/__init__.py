"""Assorted utility functions

.. currentmodule:: datalad_next.utils
.. autosummary::
   :toctree: generated

   DataladAuth
   MultiHash
   check_symlink_capability
   chpwd
   ensure_list
   external_versions
   log_progress
   parse_www_authenticate
   patched_env
   rmtree
   get_specialremote_param_dict
   get_specialremote_credential_properties
   update_specialremote_credential
   needs_specialremote_credential_envpatch
   get_specialremote_credential_envpatch
"""

from datalad.utils import (
    # TODO REMOVE FOR V2.0
    Path,
    check_symlink_capability,
    chpwd,
    # TODO REMOVE FOR V2.0
    ensure_bool,
    ensure_list,
    # TODO https://github.com/datalad/datalad-next/issues/626
    get_dataset_root,
    # TODO REMOVE FOR V2.0
    # only needed for `interface_utils` patch and better be imported
    # in there
    getargspec,
    # TODO REMOVE FOR V2.0
    get_wrapped_class,
    # TODO REMOVE FOR V2.0
    knows_annex,
    # TODO REMOVE FOR V2.0
    # in datalad_next.consts
    on_linux,
    # TODO REMOVE FOR V2.0
    # in datalad_next.consts
    on_windows,
    # TODO REMOVE FOR V2.0
    rmtemp,
    rmtree,
    # TODO REMOVE FOR V2.0
    # only a test utility and should move there
    swallow_outputs,
)
# TODO REMOVE FOR V2.0
# internal helper of create_sibling_webdav
from datalad.distribution.utils import _yield_ds_w_matching_siblings
from datalad.support.external_versions import external_versions

# TODO REMOVE FOR V2.0
from datalad_next.credman import CredentialManager
from .log import log_progress
from .multihash import MultiHash
from .requests_auth import (
    DataladAuth,
    parse_www_authenticate,
)
from .specialremote import (
    get_specialremote_param_dict,
    get_specialremote_credential_properties,
    update_specialremote_credential,
    specialremote_credential_envmap,
    needs_specialremote_credential_envpatch,
    get_specialremote_credential_envpatch,
)
from .patch import patched_env

# TODO REMOVE EVERYTHING BELOW FOR V2.0
# https://github.com/datalad/datalad-next/issues/611
from typing import (
    Any,
    Dict,
)


class ParamDictator:
    """Parameter dict access helper

    This class can be used to wrap a dict containing function parameter
    name-value mapping, and get/set values by parameter name attribute
    rather than via the ``__getitem__`` dict API.
    """
    def __init__(self, params: Dict):
        self.__params = params

    def __getattr__(self, attr: str):
        if attr.startswith('__'):
            return super().__getattribute__(attr)
        return self.__params[attr]

    def __setattr__(self, attr: str, value: Any):
        if attr.startswith('_ParamDictator__'):
            super().__setattr__(attr, value)
        else:
            self.__params[attr] = value
