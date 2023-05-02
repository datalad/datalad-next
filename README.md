# DataLad NEXT extension

[![Build status](https://ci.appveyor.com/api/projects/status/dxomp8wysjb7x2os/branch/main?svg=true)](https://ci.appveyor.com/project/mih/datalad-next/branch/main)
[![codecov](https://codecov.io/gh/datalad/datalad-next/branch/main/graph/badge.svg?token=2P8rak7lSX)](https://codecov.io/gh/datalad/datalad-next)
[![crippled-filesystems](https://github.com/datalad/datalad-next/workflows/crippled-filesystems/badge.svg)](https://github.com/datalad/datalad-next/actions?query=workflow%3Acrippled-filesystems)
[![docs](https://github.com/datalad/datalad-next/workflows/docs/badge.svg)](https://github.com/datalad/datalad-next/actions?query=workflow%3Adocs)
[![Documentation Status](https://readthedocs.org/projects/datalad-next/badge/?version=latest)](http://docs.datalad.org/projects/next/en/latest/?badge=latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![GitHub release](https://img.shields.io/github/release/datalad/datalad-next.svg)](https://GitHub.com/datalad/datalad-next/releases/)
[![PyPI version fury.io](https://badge.fury.io/py/datalad-next.svg)](https://pypi.python.org/pypi/datalad-next/)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.6833099.svg)](https://doi.org/10.5281/zenodo.6833099)

This DataLad extension can be thought of as a staging area for additional
functionality, or for improved performance and user experience. Unlike other
topical or more experimental extensions, the focus here is on functionality
with broad applicability. This extension is a suitable dependency for other
software packages that intend to build on this improved set of functionality.

## Installation

```
# create and enter a new virtual environment (optional)
$ virtualenv --python=python3 ~/env/dl-next
$ . ~/env/dl-next/bin/activate
# install from PyPi
$ python -m pip install datalad-next
```

## How to use

Additional commands provided by this extension are immediately available
after installation. However, in order to fully benefit from all improvements,
the extension has to be enabled for auto-loading by executing:

    git config --global --add datalad.extensions.load next

Doing so will enable the extension to also alter the behavior the core DataLad
package and its commands.

## Summary of functionality provided by this extension

- A replacement sub-system for credential handling that is able to handle arbitrary
  properties for annotating a secret, and facilitates determining suitable
  credentials while minimizing avoidable user interaction, without compromising
  configurability. A convenience method is provided that implements a standard
  workflow for obtaining a credential.
- A user-facing `credentials` command to set, remove, and query credentials.
- The `create-sibling-...` commands for the platforms GitHub, GIN, GOGS, Gitea
  are equipped with improved credential handling that, for example, only stores
  entered credentials after they were confirmed to work, or auto-selects the
  most recently used, matching credentials, when none are specified.
- A `create-sibling-webdav` command for hosting datasets on a WebDAV server via
  a sibling tandem for Git history and file storage. Datasets hosted on WebDAV
  in this fashion are cloneable with `datalad-clone`. A full annex setup
  for storing complete datasets with historical file content version, and an
  additional mode for depositing single-version dataset snapshot are supported.
  The latter enables convenient collaboration with audiences that are not using
  DataLad, because all files are browsable via a WebDAV server's point-and-click
  user interface.
- Enhance `datalad-push` to automatically export files to git-annex special
  remotes configured with `exporttree=yes`.
- Speed-up `datalad-push` when processing non-git special remotes. This particularly
  benefits less efficient hosting scenarios like WebDAV.
- Enhance `datalad-siblings enable` (`AnnexRepo.enable_remote()`) to automatically
  deploy credentials for git-annex special remotes that require them.
- `git-remote-datalad-annex` is a Git remote helper to push/fetch to any
  location accessible by any git-annex special remote.
- `git-annex-backend-XDLRA` (originally available from the `mihextras` extension)
  is a custom external git-annex backend used by `git-remote-datalad-annex`. A base
  class to facilitate development of external backends in Python is also provided.
- Enhance `datalad-configuration` to support getting configuration from "global"
  scope without a dataset being present.
- New modular framework for URL operations. This framework directly supports operation
  on `http(s)`, `ssh`, and `file` URLs, and can be extended with custom functionality
  for additional protocols or even interaction with specific individual servers.
  The basic operations `download`, `upload`, `delete`, and `stat` are recognized,
  and can be implemented. The framework offers uniform progress reporting and
  simultaneous content has computation. This framework is meant to replace and
  extend the downloader/provide framework in the DataLad core package. In contrast
  to its predecessor it is integrated with the new credential framework, and
  operations beyond downloading.
- `git-annex-remote-uncurl` is a special remote that exposes the new URL
  operations framework via git-annex. It provides flexible means to compose
  and rewrite URLs (e.g., to compensate for storage infrastructure changes)
  without having to modify individual URLs recorded in datasets. It enables
  seamless transitions between any services and protocols supported by the
  framework. This special remote can replace the `datalad` special remote
  provided by the DataLad core package.
- A `download` command is provided as a front-end for the new modular URL
  operations framework.
- A `python-requests` compatible authentication handler (`DataladAuth`) that
  interfaces DataLad's credential system.
- Boosted throughput of DataLad's `runner` component for command execution.
- Substantially more comprehensive replacement for DataLad's `constraints` system
  for type conversion and parameter validation.

## Summary of additional features for DataLad extension development

- Framework for uniform command parameter validation. Regardless of the used
  API (Python, CLI, or GUI), command parameters are uniformly validated. This
  facilitates a stricter separation of parameter specification (and validation)
  from the actual implementation of a command. The latter can now focus on a
  command's logic only, while the former enables more uniform and more
  comprehensive validation and error reporting. Beyond per-parameter validation
  and type-conversion also inter-parameter dependency validation and value
  transformations are supported.
- Improved composition of importable functionality. Key components for `commands`,
  `annexremotes`, `datasets` (etc) are collected in topical top-level modules that
  provide "all" necessary pieces in a single place.
- `webdav_server` fixture that automatically deploys a local WebDAV
  server.
- Utilities for HTTP handling
  - `probe_url()` discovers redirects and authentication requirements for an HTTP
    URL
  - `get_auth_realm()` returns a label for an authentication realm that can be used
    to query for matching credentials
- Utilities for special remote credential management:
  - `get_specialremote_credential_properties()` inspects a special remote and returns
    properties for querying a credential store for matching credentials
  - `update_specialremote_credential()` updates a credential in a store after
    successful use
  - `get_specialremote_credential_envpatch()` returns a suitable environment "patch"
    from a credential for a particular special remote type
- Helper for runtime-patching other datalad code (`datalad_next.utils.patch`)
- Base class for implementing custom `git-annex` backends.
- A set of `pytest` fixtures to:
  - check that no global configuration side-effects are left behind by a test
  - check that no secrets are left behind by a test
  - provide a temporary configuration that is isolated from a user environment
    and from other tests
  - provide a temporary secret store that is isolated from a user environment
    and from other tests
  - provide a temporary credential manager to perform credential deployment
    and manipulation isolated from a user environment and from other tests

## Patching the DataLad core package.

Some of the features described above rely on a modification of the DataLad core
package itself, rather than coming in the form of additional commands. Loading
this extension causes a range of patches to be applied to the `datalad` package
to enable them. A comprehensive description of the current set of patch is
available at http://docs.datalad.org/projects/next/en/latest/#datalad-patches

## Acknowledgements

This DataLad extension was developed with funding from the Deutsche
Forschungsgemeinschaft (DFG, German Research Foundation) under grant SFB 1451
([431549029](https://gepris.dfg.de/gepris/projekt/431549029), INF project).
