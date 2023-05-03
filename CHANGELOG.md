# 1.0.0b2 (2023-03-17)

## 💫 Enhancements and new features

- `CredentialManager`
  - The Credential Manager gained a new helper, ``obtain()``, that supports a credential selection by name/ID, falls back to querying with a set of properties, and would finally resort to an interactive credential query from the user. ([#216](https://github.com/datalad/datalad-next/pull/216) by @mih)

  - All optional arguments of the CredentialManager are now keyword-argument-only ([#230](https://github.com/datalad/datalad-next/pull/230) by @mih)

  - Users no longer need to provide type hints for legacy credentials in "provider" configurations ([#247](https://github.com/datalad/datalad-next/pull/247) by @mih)

  - Credential reporting supports a ``cred_type`` annotation ([#257](https://github.com/datalad/datalad-next/pull/257) by @mih)

  - Credential errors for Github-like remotes were improved to hint users how to update or set new credentials ([#235](https://github.com/datalad/datalad-next/pull/235) by @mih)

- `UrlOperations`
  - The URL handler can now load configurations from config files ([#222](https://github.com/datalad/datalad-next/pull/222) by @mih)

  - Improved messaging within `URLOperationsRemoteError` ([#308](https://github.com/datalad/datalad-next/pull/308) by @mih)

- `Parameter validation`
  - A new `validate_defaults` parameter of ``EnsureCommandParameterization`` allows opt-in parameter validation, which causes processing of any specified parameter's default. ([#227](https://github.com/datalad/datalad-next/pull/227) by @mih)

  - A new base class ``ConstraintError`` can communicate parameter validation errors and can associate constraint violations with a particular context.  ``CommandParametrizationError`` uses it to communicate violations for a full command parameterization at once and is used in an improved `EnsureCommandParametrization` constraint. Callers can now also decide whether to perform an exhaustive parameter validation, or fail on first error. ([#234](https://github.com/datalad/datalad-next/pull/234) by @mih)

  - A new ``ConstraintWithPassthrough`` constraint exposes `EnsureParameterConstraint`'s pass-through feature ([#244](https://github.com/datalad/datalad-next/pull/244) by @mih)

  - `EnsureCommandParameterization` learned a `tailor_for_dataset()` parameter that can be used to identify which parameters' constraints should be tailored for which dataset. This allows tailoring constraints for particular datasets ([#260](https://github.com/datalad/datalad-next/pull/260) by @mih)

  - ``EnsurePath`` can be tailored to dataset instances to resolve paths against a given Dataset ([#271](https://github.com/datalad/datalad-next/pull/271) by @mih)

  - The ``EnsureDataset`` constraint learned an optional check for a valid dataset ID ([#279](https://github.com/datalad/datalad-next/pull/279) by @adswa)

  - A ``WithDescription`` meta constraints paves the way for custom docs for parameters: If given, it replaces the original parameter documentation, and can be used to tailor descriptions for specific use cases. ([#294](https://github.com/datalad/datalad-next/pull/294) by @mih)

  - Parameter violations gained structured error reporting and customized rendering of parameter violations ([#306](https://github.com/datalad/datalad-next/pull/306) by @mih)

  - ``EnsureGeneratorFromFileLike`` became more suitable for batch mode use by learning to yield instead of raise internal exceptions, if configured by the caller ([#278](https://github.com/datalad/datalad-next/pull/278) by @mih)



## 🐛 Bug Fixes

- Previously, the last used credential matching a ``realm`` was used unconditionally. Now, credentials without secrets are excluded. ([#248](https://github.com/datalad/datalad-next/pull/248) by @mih)

- ``AND`` and ``OR`` compounds for Constraints do not modify Constraints in place anymore, but return a new instance. ([#292](https://github.com/datalad/datalad-next/pull/292) by @mih)

- Even though the ``EnsureDataset`` constraint returns ``DatasetParameter`` objects, ``_execute_command`` that would patch up DataLad commands wasn't able to work with them ([#269](https://github.com/datalad/datalad-next/pull/269) by @adswa)

## 🪓 Deprecations and removals

- The URL operation ``sniff`` was renamed to ``stat``. ([#231](https://github.com/datalad/datalad-next/pull/231) by @adswa)

- `serve_path_via_webdav()` that came with 0.2 was deprecated in favor of the `webdav_server` fixture ([#301](https://github.com/datalad/datalad-next/pull/301) by @mih)

## 📝 Documentation

- A dedicated Developer Guide section of the docs was introduced ([#304](https://github.com/datalad/datalad-next/pull/304) by @adswa)

- The README mentions the `uncurl` special remote, and the documentation now provide installation information

- ``CONTRIBUTING.md`` was updated on patching ([#262](https://github.com/datalad/datalad-next/pull/262/) by @mih)
## 🏠 Internal

- Package dependencies were made explicit ([#212](https://github.com/datalad/datalad-next/pull/212) by @mih)
229
- Misc. code reorganization:

  - The CredentialManager was elevated to a toplevel module ([#229](https://github.com/datalad/datalad-next/pull/220) by @mih)

  - Dataset-lookup behavior of the ``credentials`` command became identical to ``downlad`` ([#256](https://github.com/datalad/datalad-next/pull/256) by @mih)

- The DataLad runner performance patch and all patches to clone
  functionality were removed as they are included in datalad-0.18.1; The dependency was adjusted accordingly. ([#218](https://github.com/datalad/datalad-next/pull/218) by @mih)

- Compound constraints got a comprehensive ``__repr__`` to improve debugging ([#276](https://github.com/datalad/datalad-next/pull/276) by @mih)

- discontinue legacy code ([#300](https://github.com/datalad/datalad-next/pull/300/) by @mih)

## 🛡 Tests

- Automatic CI builds were disabled for changes constrained to the following files and directories: `.github/`, `CHANGELOG.md`, `CITATION.cff`, `CONTRIBUTORS`, `LICENSE`, `Makefile`, `README.md`, `readthedocs.yml`

- Coverage reports for the uncurl special remote ([#220](https://github.com/datalad/datalad-next/pull/220) by @mih)

- Tests will not fail if coverage uploads fail ([#241](https://github.com/datalad/datalad-next/pull/241/files) by @mih)

- GitHub actions use the `datalad-installer` to install git-annex ([#239](https://github.com/datalad/datalad-next/pull/239/files) by @mih)

- A bug in DataLad's test setup causes configuration managers to leak across datasets (https://github.com/datalad/datalad/issues/7297). Next implemented test isolation for keyring and config as a fix ([#263](https://github.com/datalad/datalad-next/pull/263) by @mih)

- A number of new pytest fixtures were introduced:
  - `memory_keyring` ([#254](https://github.com/datalad/datalad-next/pull/254) by @mih), which was then replaced by ``tmp_keywing`` ([#264](https://github.com/datalad/datalad-next/pull/264))
  - `dataset` and `existing_dataset` ([#296](https://github.com/datalad/datalad-next/pull/296) by @mih)
  - `webdav_server` ([#297](https://github.com/datalad/datalad-next/pull/297/) by @mih)
  - `httpbin` ([#313](https://github.com/datalad/datalad-next/pull/313) by @jwodder)

- 100% coverage for constraints ([#259](https://github.com/datalad/datalad-next/pull/259/))


# 1.0.0b1 (2022-12-23)

## 💫 Enhancements and new features

- Improved composition of importable functionality. Key components for `commands`,
  `annexremotes`, `datasets` (etc) are collected in topical top-level modules that
  provide "all" necessary pieces in a single place.

- Add patch to `ThreadedRunner` to use a more optimal buffer size for its
  read thread. This was previously fixed to 1024 bytes, and now uses the
  value of `shutil.COPY_BUFSIZE` as a platform-tailored default. This can
  boost the throughput from several tens to several hundreds MB/s.

- A new `download` command aims to replace any download-related functionality
  in DataLad. It supports single-pass checksumming, progress reporting for
  any supported URL scheme. Currently support schemes are `http(s)://`,
  `file://`, and `ssh://`. The new command integrates with the `datalad-next`
  credential system, and supports auto-discovery, interactive-prompt-on-demand,
  and (optional) save-on-success of credentials.
  Additional URL scheme handlers can be provided by extension packages. Unlike
  the datalad-core downloaders framework, they can be fully standalone, as long
  as they implement a lean adaptor class (see `datalad_next.url_operations`).

  The `AnyUrlOperations` is provided to enable generic usage in client code
  where an underlying handler is auto-selected based on the URL scheme.
  `datalad_next.url_operations.any._urlscheme_handler` contains a (patch-able)
  mapping of scheme identifiers to handler classes.

  The `uncurl` special remote makes this URL operations framework accessible
  via `git-annex`. It provides flexible means to compose and rewrite URLs (e.g.,
  to compensate for storage infrastructure changes) without having to modify
  individual URLs recorded in datasets. It enables seamless transitions between
  any services and protocols supported by the framework.

- A `python-requests` compatible authentication handler (`DataladAuth`) that
  interfaces DataLad's credential system has been added.

- A substantially more comprehensive replacement for DataLad's `constraints`
  system for type conversion and parameter validation has been developed and
  is included in this release. This includes all types of the predecessor
  in the DataLad core package, and a large number of additions, including

  - `EnsureMapping` (aids handling of key-value specification)
  - `EnsureGeneratorFromFileLike` (aids reading inputs from, e.g. STDIN; see
    the `download` command for how reading JSON-lines input can be supported
    in addition with virtually no changes to the actual command implementation)
  - `EnsurePath` (existing or not, particular formats, etc.)
  - `EnsureJSON` (automatic validation and loading)
  - `Ensure(Parsed)URL` (pattern matching, requiring/forbidding components)
  - `EnsureGitRefName` (check for compliance with Git's naming rules)

- Commands can now opt-in to receive fully validated parameters. This can
  substantially simplify the implementation complexity of a command at
  the expense of a more elaborate specification of the structural and
  semantic properties of the parameters. This specification is achieved
  by declaring an `EnsureCommandParameterization`, in a `_validator_` member
  of a command's `ValidatedInterface` class.

  This feature is introduced as a patch to the command execution in
  datalad-core. With this patch, commands are now exclusively called
  with keyword-style parameters only.

  This feature is in an early stage of development (although all incldued
  commands have already been ported to use it) that will likely undergo
  substantial changes in the coming releases.

- A new `EnsureDataset` constraint is provided that returns a
  `DatasetParameter` on successful validation. This return value contains
  the original input specification, and the `Dataset` class instance.
  The `resolve_path()` utility is adjust to support this parameter-type,
  thereby making the use of the `require_dataset()` utility obsolete.

- As a companion for the `http(s)://` URL handling for the new `download`
  command, a `requests`-compatible authentication handler has been implemented
  that integrates with the `datalad-next` credential system.

## 📝 Documentation

- All runtime patches are now documented and included in the readthedocs-hosted
  documentation.

## 🏠 Internal

- No code uses `Constraint` implementations from the DataLad core package
  anymore.

- Further expand type annotations of the code base.

# 0.6.3 (2022-10-26) -- Tests only

## 🐛 Bug Fixes

- Small change in the tests of the `tree` command for more robust behavior
  across Python and pytest versions.
  https://github.com/datalad/datalad-next/pull/117 (by @bpoldrack)

# 0.6.2 (2022-10-14) -- Hidden secrets

## 🐛 Bug Fixes

- `CredentialManager` no longer splits a credential input prompt into a
  prompt message (`ui.message()`) and the actual input (`ui.question()`)
  this enables DataLad Gooey to properly render this jointly as an
  input dialog with a description.
  https://github.com/datalad/datalad-next/pull/113 (by @bpoldrack)

## 💫 Enhancements and new features

- `CredentialManager.get()` and the `credentials` command now also report
  credential fragments for which there is no secret on record. This enables
  the discovery of DataLad's legacy credentials, and for setting a secret
  for them for use with the next credential system. Moreover, it reports
  half-configured credentials, and facilitates their clean-up or completion,
  for example with DataLad Gooey's credential management GUI.

# 0.6.1 (2022-09-27)

## 💫 Enhancements and new features

- A new patch set break up the implementation of `clone_dataset()`
  into its procedural components, and makes it more accessible for
  extension patches. There are no behavior changes associated with
  this internal reorganization.

# 0.6.0 (2022-08-25)

## 🐛 Bug Fixes

- Fixed datalad-push always reporting success when pushing to
  an export remote.
  Fixes https://github.com/datalad/datalad-next/issues/88 via
  https://github.com/datalad/datalad-next/pull/93 (by @bpoldrack)

- Token secrets entered for GitHub-like sibling creation are now stored by
  default under a name matching the API endpoint hostname (e.g.
  'api.github.com'), rather than a confusing and conflict-prone 'None'.
  Using the `--credential` option, an alternative name can be given, as before.
  Fixes https://github.com/datalad/datalad-next/issues/97 via
  https://github.com/datalad/datalad-next/pull/98 (by @mih)

## 💫 Enhancements and new features

- The `configuration` command now indicates the absence of a particular
  configuration setting queried via `get` with a `status='impossible'`
  result. This change enables the distinction of an unset configuration
  item from an item set to an empty string with the default
  CLI result renderer.
  Fixes https://github.com/datalad/datalad/issues/6851 via
  https://github.com/datalad/datalad-next/pull/87 by @mih

- The default of the configuration item `datalad.annex.retry`
  (in effect when not explicitly configured otherwise) is changed
  from `3` to `1`. This prevents a set of performance and user experience
  issues resulting from, e.g., repeated download attempts, even
  when no change in outcome can be expected (e.g., a wrong or
  no credential supplied). This change can cause a loss of robustness
  in download behavior for services that indeed experience spurious
  failures. Its is recommended to specifically parametrize such command
  calls (e.g., downloads in CI runs) with an appropriate configuration
  override.
  Fixes https://github.com/datalad/datalad/issues/6969 and
  https://github.com/datalad/datalad/issues/6509 (by @mih)

- New `tree` command for traversing a directory hierarchy.
  Like the UNIX equivalent, it can visualize a directory tree.
  Additionally, it annotates the output with DataLad-related
  information, like the location of dataset, and their nesting
  depth. Besides visualization, `tree` also reports structured
  data in the form of result records that enable other applications
  to use `tree` for gathering data from the file system.
  Fixes https://github.com/datalad/datalad-next/issues/78 via
  https://github.com/datalad/datalad-next/pull/92 (by @catetrai)

## 📝 Documentation

- Add an example of adding a `user_password`-type credentials, with a
  given `user` property, to the examples in the `credentials`
  command. https://github.com/datalad/datalad-next/pull/103 (by @mslw)

# 0.5.0 (2022-07-19)

## 💫 Enhancements and new features

- The `configuration` command no longer requires a datasets to be present
  for a `get` operation to retrieve a configuration item from scope `global`.
  Fixes [#6864](https://github.com/datalad/datalad/issues/6854) via
  [#86](https://github.com/datalad/datalad-next/pull/86) (by @mih)

# 0.4.1 (2022-07-14)

## 🐛 Bug Fixes

- Fix a missing import in the credential retrieval for GitHub-like sibling
  creation, which made it impossible to discover credentials without
  providing an explicit credential name.

# 0.4.0 (2022-07-08) --  datalad-annex:: for all

#### 💫 Enhancements and new features

- `datalad-annex::` Git remote helper now uses `git annex transferkey` instead
  of `fsck` to "probe" for `XDLRA` repository keys. This avoid problems due to
  a behavior change in git-annex 10.20220525, and can also speed-up operation for
  slow special remotes, by avoiding a dedicated probe-request.
  [#76](https://github.com/datalad/datalad-next/pull/76) (by @mih)
- `datalad-annex::` Git remote helper now fully compatible with the Windows
  platform, by working around [a git-annex
  issue](https://git-annex.branchable.com/bugs/Fails_to_drop_key_on_windows___40__Access_denied__41__)
  [#77](https://github.com/datalad/datalad-next/pull/77) (by @mih)

#### 🐛 Bug Fixes

- Prevent docstring duplication in patched `push` command
  [#71](https://github.com/datalad/datalad-next/pull/71) (by @mih)

#### 📝 Documentation

- Bibliographic information on authorship was added
  [#80](https://github.com/datalad/datalad-next/pull/80) (by @mslw)

#### 🛡 Tests

- The test battery is now using `pytest`. This change required bumping the
  dependency on DataLad to version 0.17.0.
  [#73](https://github.com/datalad/datalad-next/pull/73) (by @mih)

#### 🏠 Internal

- Reduced code duplication by consolidating on a common helper for sibling
  identification, now available from DataLad 0.17.0
  [#82](https://github.com/datalad/datalad-next/pull/82) (by @adswa)

#### Authors: 3

- Michael Hanke (@mih)
- Michał Szczepanik (@mslw)
- Adina Wagner (@adswa)


# 0.3.0 (2022-05-25) --  Optimized push

#### 💫 Enhancements and new features

- Make push avoid refspec handling for special remote push targets. See PR
  https://github.com/datalad/datalad-next/pull/64 for details on the associated
  behavior changes that are introduced with this new patch.

# 0.2.2 (2022-04-29) --  More docs!

#### 📝 Documentation

- Adjusted documentation of patched datalad-core commands now also shows
  properly in Python sessions.
- Extended the documentation on collaborative workflows with
  ``datalad-annex::``Git remotes and WebDAV siblings.

# 0.2.1 (2022-04-28) --  User experience

#### 💫 Enhancements and new features

- Disable auto-enabling of webdav storage remotes on clone. Datalad does not
  yet support the needed inspection to determine the necessary credentials
  automatically. Instead an explicit `datalad sibling enable` call is required.
  This is now also added to the documentation.
- Make sure that `create-sibling-webdav` does not ask users to input the
  internal `realm` property, when prompting for credentials.
- `CredentialManager` now displays more appropriate labels when prompting for a
  secret, e.g. `password` instead of `user_password`.

# 0.2.0 (2022-04-28) --  WebDAV

This release primarly brings the ability to store DataLad datasets on a WebDAV
server. This is done in a way that allows for cloning such dataset with
`datalad clone` from such a WebDAV server too. This feature enables
full-featured DataLad-based collaborative workflows on widely available cloud
storage systems, such as owncloud/next/cloud -- which are also the basis for
several institutional services like the European Open Science Cloud's (EOSC)
B2DROP service.

#### 💫 Enhancements and new features

- A `create-sibling-webdav` command for hosting datasets on a WebDAV server via
  a sibling tandem for Git history and file storage. Datasets hosted on WebDAV
  in this fashion are cloneable with `datalad-clone`. A full annex setup  for
  storing complete datasets with historical file content version, and an
  additional mode for depositing single-version dataset snapshot are supported.
  The latter enables convenient collaboration with audiences that are not using
  DataLad, because all files are browsable via a WebDAV server's point-and-click
  user interface.
- Enhance `datalad-push` to automatically export files to git-annex special
  remotes configured with `exporttree=yes`.
- Enhance `datalad-siblings enable` (`AnnexRepo.enable_remote()` to
  automatically  deploy credentials for git-annex special remotes that require
  them.
- `git-remote-datalad-annex` is a Git remote helper to push/fetch to any
  location accessible by any git-annex special remote.
- `git-annex-backend-XDLRA` (originally available from the `mihextras`
  extension) is a custom external git-annex backend used by
  git-remote-datalad-annex. A base class to facilitate development of external
  backends in Python is also provided.
- `serve_path_via_webdav` test decorator that automatically deploys a local
  WebDAV server.
- `with_credential` test decorator that temporarily deploy a credential to the
  local credential system.
- Utilities for HTTP handling
  - `probe_url()` to discover redirects and authentication requirements for an
    HTTP  URL
  - `get_auth_realm()` return a label for an authentication realm that can be
    used  to query for matching credentials


- Utilities for special remote credential management:
  - `get_specialremote_credential_properties()` inspects a special remote and
    return  properties for querying a credential store for matching credentials
  - `update_specialremote_credential()` updates a credential in a store after
    successful use
  - `get_specialremote_credential_envpatch()` returns a suitable environment
    "patch" from a credential for a particular special remote type

# 0.1.0 (2022-03-31) --  Credentials, please!

#### 💫 Enhancements and new features

- A new credential management system is introduced that enables storage and
  query of credentials with any number of properties associated with a secret.
  These properties are stored as regular configuration items, following the
  scheme `datalad.credential.<name>.<property>`. The special property `secret`
  lives in a keystore, but can be overriden using the normal configuration
  mechanisms. The new system continues to support the previous credential storage
  setup. Fixes [#6519](https://github.com/datalad/datalad/issues/6519)
  ([@mih](https://github.com/mih))
- A new `credentials` command enables query, modification and storage of
  credentials. Legacy credentials are also supported, but may require the
  specification of a `type`, such as (`token`, or `user_password`) to be
  discoverable. Fixes [#396](https://github.com/datalad/datalad/issues/396)
  ([@mih](https://github.com/mih))
- Two new configuration settings enable controlling how the interactive entry
  of credential secrets is conducted for the new credential manager:
  `datalad.credentials.repeat-secret-entry` can be used to turn off the default
  double-entry of secrets, and `datalad.credentials.hidden-secret-entry` can turn
  off the default hidden entry of secrets. Fixes
  [#2721](https://github.com/datalad/datalad/issues/2721)
  ([@mih](https://github.com/mih))


#### Authors: 1

- Michael Hanke ([@mih](https://github.com/mih))

---
