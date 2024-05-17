# 1.4.0 (2024-05-17)

## 🐛 Bug Fixes

- RIA over SSH access from Mac clients to Linux server was broken
  due to an inappropriate platform check that assumed that local and
  remote platform are identical.
  Fixes https://github.com/datalad/datalad/issues/7536 via
  https://github.com/datalad/datalad-next/pull/653 (by @mih)

- `next-status` has received a number of fixes:

  - It no longer issues undesirable modification reports
    that are based on `mtime` changes alone (i.e., no content change).
    Fixes https://github.com/datalad/datalad-next/issues/639 via
    https://github.com/datalad/datalad-next/pull/650 (by @mih)
  - It now detects staged changes in repositories with no
    commit.
    Fixes https://github.com/datalad/datalad-next/issues/680 via
    https://github.com/datalad/datalad-next/pull/681 (by @mih)
  - `next-status -r mono` now reports on new commits in submodules.
    Previously this was ignored, leading to the impression of
    clean datasets despite unsaved changes.
    Fixes https://github.com/datalad/datalad-next/issues/645 via
    https://github.com/datalad/datalad-next/pull/679 (by @mih)

- `iter_annexworktree()` can now also be used on plain Git repos,
  and would behave exactly as if reporting on non-annexed files
  in a git-annex repo. Previously, a cryptic `iterable did not yield
  matching item for route-in item, cardinality mismatch?` error was
  issued in this case.
  Fixes https://github.com/datalad/datalad-next/issues/670 via
  https://github.com/datalad/datalad-next/pull/673 (by @mih)

## 💫 Enhancements and new features

- `datalad_next.shell` provides a context manager for (long-running)
  shell or interpreter subprocesses. Within the context any number of
  commands can be executed in such a shell, and each command can
  process input (iterables), and yield output (iterables). This feature
  is suitable for running and controlling "remote shells" like a login
  shell on a server via SSH. A range of utilities is provided to
  employ this functionality for special purpose implementations
  (e.g., accept fixed-length or variable-length process output).
  A suite of operations like download/upload file to a remote shell is
  provided for POSIX-compliant shells `datalad_next.shell.operations.posix`.
  https://github.com/datalad/datalad-next/pull/596 (by @christian-monch)

- A rewrite of `SSHRemoteIO`, the RIA SSH-operations implementation from
  datalad-core is provided as a patch. It is based on the new `shell`
  feature, and provides more robust operations. It's IO performance is
  at the same level as `scp`-based down/uploads. In contrast to the
  original implementation, it support fine-grained progress reporting
  for uploads and downloads.
  Via https://github.com/datalad/datalad-next/pull/655 (by @mih)

- The `SpecialRemote` base class in datalad-core is patched to support
  a standard `close()` method for implementing resource release and cleanup
  operations. The main special remote entry point has been altered to
  run implementations within a `closing()` context manager to guarantee
  execution of such handlers.
  Via https://github.com/datalad/datalad-next/pull/655 (by @mih)

- A new `has_initialized_annex()` helper function is provided to
  test for a locally initialized annex in a repo.
  Via https://github.com/datalad/datalad-next/pull/673 (by @mih)

- `iter_annexworktree()` can now also be used on plain Git repositories,
  and it yields the same output and behavior as running on a git-annex
  repository with no annex'ed content (just tracked with Git).
  Fixes https://github.com/datalad/datalad-next/issues/670 via
  https://github.com/datalad/datalad-next/pull/673 (by @mih)

- `next-status` and `iter_gitstatus()` have been improved to
  report on further modifications after a file addition has been
  originally staged.
  Fixes https://github.com/datalad/datalad-next/issues/637 via
  https://github.com/datalad/datalad-next/pull/679 (by @mih)

- `next-status` result rendering has been updated to be more markedly
  different than git-status's. Coloring is now exclusively
  determined by the nature of a change, rather than being partially
  similar to git-status's index-updated annotation. This reduces
  the chance for misinterpretations, and does not create an undesirable
  focus on the Git index (which is largely ignored by DataLad).
  Fixes https://github.com/datalad/datalad-next/issues/640 via
  https://github.com/datalad/datalad-next/pull/679 (by @mih)

- A large 3k-line patch set replaces almost the entire RIA implementation,
  including the ORA special remote, and the `create-sibling-ria` command.
  The new implementation brings uniform support for Windows clients, progress
  reporting for uploads and downloads via SSH, and a faster and more
  robust behavior for SSH-based operations (based on the new remote
  shell feature).
  Fixes https://github.com/datalad/datalad-next/issues/654 via
  https://github.com/datalad/datalad-next/pull/669 (by @christian-monch)

## 📝 Documentation

- Git-related subprocess execution helpers are now accessible in the
  rendered documentation, and all supported file collections are now
  mentioned in the `ls-file-collection` command help.
  Fixes https://github.com/datalad/datalad-next/issues/668 via
  https://github.com/datalad/datalad-next/pull/671 (by @mih)

## 🛡 Tests

- Test setup has been improved to support a uniform, datalad-next
  enabled environment for subprocesses too. This extends the scope
  of testing to special remote implementations and other code that
  is executed in subprocesses, and relies on runtime patches.
  See https://github.com/datalad/datalad-next/pull/i665 (by @mih)

# 1.3.0 (2024-03-19)

## 💫 Enhancements and new features

- Code organization is adjusted to clearly indicate what is part of the
  package's public Python API. Anything that can be imported directly from
  the top-level of any sub-package is part of the public API.
  As an example: `from datalad_next.runners import iter_git_subproc`
  imports a part of the public API, but
  `from datalad_next.runners.git import iter_git_subproc` does not.
  See `README.md` for more information.
  Fixes https://github.com/datalad/datalad-next/issues/613 via
  https://github.com/datalad/datalad-next/pull/615 (by @mih)
  https://github.com/datalad/datalad-next/pull/617 (by @mih)
  https://github.com/datalad/datalad-next/pull/618 (by @mih)
  https://github.com/datalad/datalad-next/pull/619 (by @mih)
  https://github.com/datalad/datalad-next/pull/620 (by @mih)
  https://github.com/datalad/datalad-next/pull/621 (by @mih)
  https://github.com/datalad/datalad-next/pull/622 (by @mih)
  https://github.com/datalad/datalad-next/pull/623 (by @mih)

- New `patched_env` context manager for patching a process'
  environment. This avoids the for importing `unittest` outside
  test implementations.
  Via https://github.com/datalad/datalad-next/pull/633 (by @mih)

- `call_git...()` functions received a new `force_c_locale`
  parameter. This can be set whenever Git output needs to be parsed
  to force running the command with `LC_ALL=C`. Such an environment
  manipulation is off by default and not done unconditionally to
  let localized messaging through in a user's normal locale.

## 🐛 Bug Fixes

- `datalad-annex::` Git remote helper now tests for a repository
  deposit, and distinguishes an absent remote repository deposit
  vs cloning from an empty repository deposit. This rectifies
  confusing behavior (successful clones of empty repositories
  from broken URLs), but also fixes handling of subdataset clone
  candidate handling in `get` (which failed to skip inaccessible
  `datalad-annex::` URLs for the same reason).
  Fixes https://github.com/datalad/datalad-next/issues/636 via
  https://github.com/datalad/datalad-next/pull/638 (by @mih)

## 📝 Documentation

- API docs have been updated to include all top-level symbols
  of any sub-package, or in other words: the public API.
  See https://github.com/datalad/datalad-next/pull/627 (by @mih)

## 🏠 Internal

- The `tree` command no longer uses the `subdatasets` command
  for queries, but employs the recently introduced `iter_submodules()`
  for leaner operations.
  See https://github.com/datalad/datalad-next/pull/628 (by @mih)

- `call_git...()` functions are established as the only used abstraction
  to interface with Git and git-annex commands outside the use in
  DataLad's `Repo` classes. Any usage of DataLad's traditional
  `Runner` functionality is discontinued.
  Fixes https://github.com/datalad/datalad-next/issues/541 via
  https://github.com/datalad/datalad-next/pull/632 (by @mih)

- Type annotations have been added to the implementation of the
  `uncurl` git-annex remote. A number of unhandled conditions have
  been discovered and were rectified.


# 1.2.0 (2024-02-02)

## 🐛 Bug Fixes

- Fix an invalid escape sequence in a regex that caused a syntax warning.
  Fixes https://github.com/datalad/datalad-next/issues/602 via
  https://github.com/datalad/datalad-next/pull/603 (by @mih)

## 💫 Enhancements and new features

- Speed up of status reports for repositories with many submodules.
  An early presence check for submodules skips unnecessary evaluation
  steps. Fixes https://github.com/datalad/datalad-next/issues/606 via
  https://github.com/datalad/datalad-next/pull/607 (by @mih)

## 🏠 Internal

- Fix implementation error in `ParamDictator` class that caused a test
  failure. The class itself is unused and has been scheduled for removal.
  See https://github.com/datalad/datalad-next/issues/611 and
  https://github.com/datalad/datalad-next/pull/610 (by @christian-monch)

## 🛡 Tests

- Promote a previously internal fixture to provide a standard
  `modified_dataset` fixture. This fixture is sessions-scope, and
  yields a dataset with many facets of modification, suitable for
  testing change reporting. The fixture verifies that no
  modifications have been applied to the testbed. (by @mih)

- `iterable_subprocess` tests have been robustified to better handle the
  observed diversity of execution environments. This addresseses, for example,
  https://bugs.debian.org/1061739.
  https://github.com/datalad/datalad-next/pull/614 (by @christian-monch)

# 1.1.0 (2024-01-21) -- Iterate!

## 💫 Enhancements and new features

- A new paradigm for subprocess execution is introduced. The main
  workhorse is `datalad_next.runners.iter_subproc`. This is a
  context manager that feeds input to subprocesses via iterables,
  and also exposes their output as an iterable. The implementation
  is based on https://github.com/uktrade/iterable-subprocess, and
  a copy of it is now included in the sources. It has been modified
  to work homogeneously on the Windows platform too.
  This new implementation is leaner and more performant. Benchmarks
  suggest that the execution of multi-step pipe connections of Git
  and git-annex commands is within 5% of the runtime of their direct
  shell-execution equivalent (outside Python).
  See https://github.com/datalad/datalad-next/pull/538 (by @mih),
  https://github.com/datalad/datalad-next/pull/547 (by @mih).

  With this change a number of additional features have been added,
  and internal improvements have been made. For example, any
  use of `ThreadedRunner` has been discontinued. See
  https://github.com/datalad/datalad-next/pull/539 (by @christian-monch),
  https://github.com/datalad/datalad-next/pull/545 (by @christian-monch),
  https://github.com/datalad/datalad-next/pull/550 (by @christian-monch),
  https://github.com/datalad/datalad-next/pull/573 (by @christian-monch)

  - A new `itertools` module was added. It provides implementations
    of iterators that can be used in conjunction with `iter_subproc`
    for standard tasks. This includes the itemization of output
    (e.g., line-by-line) across chunks of bytes read from a process
    (`itemize`), output decoding (`decode_bytes`), JSON-loading
    (`json_load`), and helpers to construct more complex data flows
    (`route_out`, `route_in`).

  - The `more_itertools` package has been added as a new dependency.
    It is used for `datalad-next` iterator implementations, but is also
    ideal for client code that employed this new functionality.

  - A new `iter_annexworktree()` provides the analog of `iter_gitworktree()`
    for git-annex repositories.

  - `iter_gitworktree()` has been reimplemented around `iter_subproc`. The
    performance is substantially improved.

  - `iter_gitworktree()` now also provides file pointers to
    symlinked content. Fixes https://github.com/datalad/datalad-next/issues/553
    via https://github.com/datalad/datalad-next/pull/555 (by @mih)

  - `iter_gitworktree()` and `iter_annexworktree()` now support single
    directory (i.e., non-recursive) reporting too.
    See https://github.com/datalad/datalad-next/pull/552

  - A new `iter_gittree()` that wraps `git ls-tree` for iterating over
    the content of a Git tree-ish.
    https://github.com/datalad/datalad-next/pull/580 (by @mih).

  - A new `iter_gitdiff()` wraps `git diff-tree|files` and provides a flexible
    basis for iteration over changesets.

- `PathBasedItem`, a dataclass that is the bases for many item types yielded
  by iterators now more strictly separates `name` property from path semantics.
  The name is a plain string, and an additional, explicit `path` property
  provides it in the form of a `Path`. This simplifies code (the
  `_ZipFileDirPath` utility class became obsolete and was removed), and
  improve performance.
  Fixes https://github.com/datalad/datalad-next/issues/554 and
  https://github.com/datalad/datalad-next/issues/581 via
  https://github.com/datalad/datalad-next/pull/583 (by @mih)

- A collection of helpers for running Git command has been added at
  `datalad_next.runners.git`. Direct uses of datalad-core runners,
  or `subprocess.run()` for this purpose have been replaced with call
  to these utilities.
  https://github.com/datalad/datalad-next/pull/585 (by @mih)

- The performance of `iter_gitworktree()` has been improved by about
  10%. Fixes https://github.com/datalad/datalad-next/issues/540
  via https://github.com/datalad/datalad-next/pull/544 (by @mih).

- New `EnsureHashAlgorithm` constraint to automatically expose
  and verify algorithm labels from `hashlib.algorithms_guaranteed`
  Fixes https://github.com/datalad/datalad-next/issues/346 via
  https://github.com/datalad/datalad-next/pull/492 (by @mslw @adswa)

- The `archivist` remote now supports archive type detection
  from `*E`-type annex keys for `.tgz` archives too.
  Fixes https://github.com/datalad/datalad-next/issues/517 via
  https://github.com/datalad/datalad-next/pull/518 (by @mih)

- `iter_zip()` uses a dedicated, internal `PurePath` variant to report on
  directories (`_ZipFileDirPath`). This enables more straightforward
  `item.name in zip_archive` tests, which require a trailing `/` for
  directory-type archive members.
  https://github.com/datalad/datalad-next/pull/430 (by @christian-monch)

- A new `ZipArchiveOperations` class added support for ZIP files, and enables
  their use together with the `archivist` git-annex special remote.
  https://github.com/datalad/datalad-next/pull/578 (by @christian-monch)

- `datalad ls-file-collection` has learned additional collections types:

  - The new `zipfile` collection type that enables uniform reporting on
    the additional archive type.

  - The new `annexworktree` collection that enhances the `gitworktree`
    collection by also reporting on annexed content, using the new
    `iter_annexworktree()` implementation. It is about 15% faster than a
    `datalad --annex basic --untracked no -e no -t eval`.

  - The new `gittree` collection for listing any Git tree-ish.

  - A new `iter_gitstatus()` can replace the functionality of
    `GitRepo.diffstatus()` with a substantially faster implementation.
    It also provides a novel `mono` recursion mode that completely
    hides the notion of submodules and presents deeply nested
    hierarchies of datasets as a single "monorepo".
    https://github.com/datalad/datalad-next/pull/592 (by @mih)

- A new `next-status` command provides a substantially faster
  alternative to the datalad-core `status` command. It is closely
  aligned to `git status` semantics, only reports changes (not repository
  listings), and supports type change detection. Moreover, it exposes
  the "monorepo" recursion mode, and single-directory reporting options
  of `iter_gitstatus()`. It is the first command to use `dataclass`
  instances as result types, rather than the traditional dictionaries.

- `SshUrlOperations` now supports non-standard SSH ports, non-default
  user names, and custom identity file specifications.
  Fixed https://github.com/datalad/datalad-next/issues/571 via
  https://github.com/datalad/datalad-next/pull/570 (by @mih)

- A new `EnsureRemoteName` constraint improves the parameter validation
  of `create-sibling-webdav`. Moreover, the command has been uplifted
  to support uniform parameter validation also for the Python API.
  Missing required remotes, or naming conflicts are now detected and
  reported immediately before the actual command implementation runs.
  Fixes https://github.com/datalad/datalad-next/issues/193 via
  https://github.com/datalad/datalad-next/pull/577 (by @mih)

- `datalad_next.repo_utils` provide a collection of implementations
  for common operations on Git repositories. Unlike the datalad-core
  `Repo` classes, these implementations do no require a specific
  data structure or object type beyond a `Path`.

## 🐛 Bug Fixes

- Add patch to fix `update`'s target detection for adjusted mode datasets
  that can crash under some circumstances.
  See https://github.com/datalad/datalad/issues/7507, fixed via
  https://github.com/datalad/datalad-next/pull/509 (by @mih)

- Comparison with `is` and a literal was replaced with a proper construct.
  While having no functional impact, it removes an ugly `SyntaxWarning`.
  Fixed https://github.com/datalad/datalad-next/issues/526 via
  https://github.com/datalad/datalad-next/pull/527 (by @mih)

## 📝 Documentation

- The API documentation has been substantially extended. More already
  documented API components are now actually renderer, and more documentation
  has been written.

## 🏠 Internal

- Type annotations have been extended. The development workflows now inform
  about type annotation issues for each proposed change.

- Constants have been migrated to `datalad_next.consts`.
  https://github.com/datalad/datalad-next/pull/575 (by @mih)

## 🛡 Tests

- A new test verifies compatibility with HTTP serves that do not report
  download progress.
  https://github.com/datalad/datalad-next/pull/369 (by @christian-monch)

- The overall noise-level in the test battery output has been reduced
  substantially. INFO log messages are no longer shown, and command result
  rendering is largely suppressed. New test fixtures make it easier
  to maintain tidier output: `reduce_logging`, `no_result_rendering`.
  The contribution guide has been adjusted encourage their use.

- Tests that require an unprivileged system account to run are now skipped
  when executed as root. This fixes an issue of the Debian package.
  https://github.com/datalad/datalad-next/pull/593 (by @adswa)

# 1.0.2 (2023-10-23) -- Debianize!

## 🏠 Internal

- The `www-authenticate` dependencies is dropped. The functionality is
  replaced by a `requests`-based implementation of an alternative parser.
  This trims the dependency footprint and facilitates Debian-packaging.
  The previous test cases are kept and further extended.
  Fixes https://github.com/datalad/datalad-next/issues/493 via
  https://github.com/datalad/datalad-next/pull/495 (by @mih)

## 🛡 Tests

- The test battery now honors the `DATALAD_TESTS_NONETWORK` environment
  variable and downgrades by skipping any tests that require external
  network access. (by @mih)

# 1.0.1 (2023-10-18)

## 🐛 Bug Fixes

- Fix f-string syntax in error message of the `uncurl` remote.
  https://github.com/datalad/datalad-next/pull/455 (by @christian-monch)

- `FileSystemItem.from_path()` now honors its `link_target` parameter, and
  resolves a target for any symlink item conditional on this setting.
  Previously, a symlink target was always resolved.
  Fixes https://github.com/datalad/datalad-next/issues/462 via
  https://github.com/datalad/datalad-next/pull/464 (by @mih)

- Update the vendor installation of versioneer to v0.29. This
  resolves an installation failure with Python 3.12 due to
  the removal of an ancient class.
  Fixes https://github.com/datalad/datalad-next/issues/475 via
  https://github.com/datalad/datalad-next/pull/483 (by @mih)

- Bump dependency on Python to 3.8. This is presently the oldest version
  still supported upstream. However, some functionality already used
  3.8 features, so this is also a bug fix.
  Fixes https://github.com/datalad/datalad-next/issues/481 via
  https://github.com/datalad/datalad-next/pull/486 (by @mih)

## 💫 Enhancements and new features

- Patch datalad-core's `run` command to honor configuration defaults
  for substitutions. This enables placeholders like `{python}` that
  point to `sys.executable` by default, and need not be explicitly
  defined in system/user/dataset configuration.
  Fixes https://github.com/datalad/datalad-next/issues/478 via
  https://github.com/datalad/datalad-next/pull/485 (by @mih)

## 📝 Documentation

- Include `gitworktree` among the available file collection types
  listed in `ls-file-collection`'s docstring.  Fixes
  https://github.com/datalad/datalad-next/issues/470 via
  https://github.com/datalad/datalad-next/pull/471 (by @mslw)

- The renderer API documentation now includes an entrypoint for the
  runner-related functionality and documentation at
  https://docs.datalad.org/projects/next/en/latest/generated/datalad_next.runners.html
  Fixes https://github.com/datalad/datalad-next/issues/466 via
  https://github.com/datalad/datalad-next/pull/467 (by @mih)

## 🛡 Tests

- Simplified setup for subprocess test-coverage reporting. Standard
  pytest-cov features are not employed, rather than the previous
  approach that was adopted from datalad-core, which originated
  in a time when testing was performed via nose.
  Fixes https://github.com/datalad/datalad-next/issues/453 via
  https://github.com/datalad/datalad-next/pull/457 (by @mih)


# 1.0.0 (2023-09-25)

This release represents a milestone in the development of the extension.
The package is reorganized to be a collection of more self-contained
mini-packages, each with its own set of tests.

Developer documentation and guidelines have been added to aid further
development. One particular goal is to establish datalad-next as a proxy
for importing datalad-core functionality for other extensions. Direct imports
from datalad-core can be minimized in favor of imports from datalad-next.
This helps identifying functionality needed outside the core package,
and guides efforts for future improvements.

The 1.0 release marks the switch to a more standard approach to semantic
versioning. However, although a substantial improvements have been made,
the 1.0 version nohow indicates a slowdown of development or a change in the
likelihood of (breaking) changes. They will merely become more easily
discoverable from the version label alone.

Notable high-level features introduced by this major release are:

- The new `UrlOperations` framework to provide a set of basic operations like
  `download`, `upload`, `stat` for different protocols. This framework can be
  thought of as a replacement for the "downloaders" functionality in
  datalad-core -- although the feature list is not 100% overlapping. This new
  framework is more easily extensible by 3rd-party code.

- The `Constraints` framework elevates parameter/input validation to the next
  level. In contrast to datalad-core, declarative input validation is no longer
  limited to the CLI. Instead, command parameters can now be validated regardless
  of the entrypoint through which a command is used. They can be validated
  individually, but also sets of parameters can be validated jointly to implement
  particular interaction checks. All parameter validations can now be performed
  exhaustive, to present a user with a complete list of validation errors, rather
  then the fail-on-first-error method implemented exclusively in datalad-core.
  Validation errors are now reported using dedicated structured data type to aid
  their communication via non-console interfaces.

- The `Credentials` system has been further refined with more homogenized
  workflows and deeper integration into other subsystems. This release merely
  represents a snapshot of continued development towards a standardization of
  credential handling workflows.

- The annex remotes `uncurl` and `archivist` are replacements for the
  datalad-core implementations `datalad` and `datalad-archive`. The offer
  substantially improved configurability and leaner operation -- built on the
  `UrlOperations` framework.

- A growing collection of iterator (see `iter_collections`) aims to provide
  fast (and more Pythonic) operations on common data structures (Git worktrees,
  directories, archives). The can be used as an alternative to the traditional
  `Repo` classes (`GitRepo`, `AnnexRepo`) from datalad-core.

- Analog to `UrlOperations` the `ArchiveOperations` framework aims to provide
  an abstraction for operations on different archive types (e.g., TAR). The
  represent an alternative to the traditional implementations of
  `ExtractedArchive` and `ArchivesCache` from datalad-core, and aim at leaner
  resource footprints.

- The collection of runtime patches for datalad-core has been further expanded.
  All patches are now individually documented, and applied using a set of standard
  helpers (see http://docs.datalad.org/projects/next/en/latest/patches.html).

For details, please see the changelogs of the 1.0.0 beta releases below.

## 💫 Enhancements and new features

- `TarArchiveOperations` is the first implementation of the `ArchiveOperations`
  abstraction, providing archive handlers with a set of standard operations:
  - `open` to get a file object for a particular archive member
  - `__contains__` to check for the presence of a particular archive member
  - `__iter__` to get an iterator for processing all archive members
  https://github.com/datalad/datalad-next/pull/415 (by @mih)

## 🐛 Bug Fixes

- Make `TarfileItem.name` be of type `PurePosixPath` to reflect the fact
  that a TAR archive can contain members with names that cannot be represent
  unmodified on a non-POSIX file system.
  https://github.com/datalad/datalad-next/pull/422 (by @mih)
  An analog change is done for `ZipfileItem.name`.
  https://github.com/datalad/datalad-next/pull/409 (by @christian-monch)

- Fix `git ls-file` parsing in `iter_gitworktree()` to be compatible with
  file names that start with a `tab` character.
  https://github.com/datalad/datalad-next/pull/421 (by @christian-monch)

## 📝 Documentation

- Expanded guidelines on test implementations.

- Add missing and fix wrong docstrings for HTTP/WebDAV server related fixtures.
  https://github.com/datalad/datalad-next/pull/445 (by @adswa)

## 🏠 Internal

- Deduplicate configuration handling code in annex remotes.
  https://github.com/datalad/datalad-next/pull/440 (by @adswa)

## 🛡 Tests

- New test fixtures have been introduced to replace traditional test helpers
  from datalad-core:

  - `datalad_interactive_ui` and `datalad_noninteractive_ui` for testing
    user interactions. They replace `with_testsui`.
    https://github.com/datalad/datalad-next/pull/427 (by @mih)

- Expand test coverage for `create_sibling_webdav` to include recursive
  operation.
  https://github.com/datalad/datalad-next/pull/434 (by @adswa)


# 1.0.0b3 (2023-06-09)

## 🐛 Bug Fixes

- Patch `CommandError`, the standard exception raised for any non-zero exit
  command execution to now reports which command failed with `repr()` too.
  Previously, only `str()` would produce an informative message about a failure,
  while `repr()` would report `CommandError('')`, unless a dedicated message was
  provided. (by @mih)

- Some error messages (in particular from within git-annex special remotes)
  exhibited uninformative error messages like `CommandError('')`. This
  is now fixed by letting `CommandError` produce the same error rendering
  in `__str__` and `__repr__`. Previously, `RuntimeError.__repr__` was used,
  which was unaware of command execution details also available in the exception.
  https://github.com/datalad/datalad-next/pull/386 (by @mih)

- The `datalad-annex` Git remote helper can now handle the case where
  a to-be-clone repository has a configured HEAD ref that does not
  match the local configured default (e.g., `master` vs `main`
  default branch).
  Fixes https://github.com/datalad/datalad-next/issues/412 via
  https://github.com/datalad/datalad-next/pull/411 (by @mih)

- Patch `create_sibling_gitlab` to work with present day GitLab deployments.
  This required adjusting the naming scheme for the `flat` and `collection`
  layouts. Moreover, the `hierarchy` layout is removed. it has never been
  fully implemented, and conceptually suffers from various corner-cases
  that cannot be (easily) addressed. Consequently, the `collection` layout
  is the new default. It's behavior matches that of `hierarchy` as far as this
  was functional, hence there should be no breakage for active users.
  https://github.com/datalad/datalad-next/pull/413

## 💫 Enhancements and new features

- Patch the process entrypoint of DataLad's git-annex special remote
  implementations to funnel internal progress reporting to git-annex
  via standard `PROGRESS` protocol messages. This makes it obsolete
  (in many cases) to implement custom progress reporting, and the
  use of the standard `log_progress()` helper (either directly or
  indirectly) is sufficient to let both a parent DataLad process
  or git-annex see progress reports from special remotes.
  Fixes https://github.com/datalad/datalad-next/issues/328 via
  https://github.com/datalad/datalad-next/pull/329 (by @mih)

- The `HttpUrlOperations` handler now supports custom HTTP headers.
  This makes it possible to define custom handlers in configuration
  that include such header customization, for example to send
  custom secret or session IDs.
  Fixes https://github.com/datalad/datalad-next/issues/336 (by @mih)

- `Constraint` implementations now raise `ConstraintError` consistently
  on a violation. This now makes it possible to distinguish properly
  handled violations from improper implementation of such checks.
  Moreover, `raise_for()` is now used consistently, providing
  uniform, structured information on such violations.
  `ConstraintError` is derived from `ValueError` (the exception
  that was previously (mostly) raised. Therefore, client-code should
  continue to work without modification, unless a specific wording
  of an exception message is relied upon. In few cases, an implicit
  `TypeError` (e.g., `EnsureIterableof`) has been replaced by an
  explicit `ConstraintError`, and client code needs to be adjusted.
  The underlying exception continues to be available via
  `ConstraintError.caused_by`. (by @mih)

- New `MultiHash` helper to compute multiple hashes in one go.
  Fixes https://github.com/datalad/datalad-next/issues/345 (by @mih)

- As a companion of `LeanGitRepo` a `LeanAnnexRepo` has been added.  This class
  is primarily used to signal that particular code does not require the full
  `AnnexRepo` API, but works with a much reduced API, as defined by that class.
  The API definition is not final and will grow in future releases to accommodate
  all standard use cases.  https://github.com/datalad/datalad-next/pull/387
  (by @mih)

- Dedicated dataclasses for common types, such as git-annex keys (`AnnexKey`)
  and `dl+archives:` URLs (`ArchivistLocator`) have been added. They support
  parsing and rendering their respective plain-text representations. These new
  types are now also available for more precise type annotation and argument
  validation. (by @mih)

- `datalad_next.archive_operations` has been added, and follows the pattern
  established by the `UrlOperations` framework, to provide uniform handling
  to different archive types. Two main (read) operations are supported:
  iteration over archive members, and access to individual member content
  via a file-like. (by @mih)

- New `archivist` git-annex special remote, as a replacement for the
  `datalad-archives` remote. It is implemented as a drop-in replacement
  with the ability to also fall-back on the previous implementation.
  In comparison to its predecessor, it reduces the storage overhead
  from 200% to 100% by doing partial extraction from fully downloaded
  archives. It is designed to be extended with support for partial
  access to remote archives (thereby reducing storage overhead to zero),
  but this is not yet implemented.

- New `datalad_next.iter_collections` module providing iterators for
  items in particular collections, such as TAR or ZIP archives members,
  the content of a file system directory, or the worktree of a Git repository.
  Iterators yield items of defined types that typically carry information on
  the properties of collections items, and (in the case of files) access to
  their content.

- New command `ls_file_collection()` is providing access to a select set
  of collection iterators via the DataLad command. In addition to the
  plain iterators, it provide uniform content hashing across all
  supported collection types.

- The `datalad-annex` Git remote helper can now recognize and handle
  legacy repository deposits made by its predecessor from `datalad-osf`.
  https://github.com/datalad/datalad-next/pull/411 (by @mih)

## 🏠 Internal

- Remove DataLad runner performance patch, and all patches to clone
  functionality. They are included in datalad-0.18.1, dependency adjusted.

- New `deprecated` decorator for standardized deprecation handling
  of commands, functions, and also individual keyword arguments of
  callables, and even particular values for such arguments.
  Inspired by https://github.com/datalad/datalad/issues/6998.
  Contributed by @adswa

- Use the correct type annotation for `cfg`-parameter of
  `datalad_next.utils.requests_auth.DataladAuth.__init__()`
  https://github.com/datalad/datalad-next/pull/385 (by @christian-monch)

- The patch registry has been moved to `datalad_next.patches.enabled`,
  and the `apply_patch()` helper is now located in `datalad_next.patches`
  directly to avoid issues with circular dependencies when patching
  core components like the `ConfigManager`. The documentation on patching
  has been adjusted accordingly.
  https://github.com/datalad/datalad-next/pull/391 (by @mih)

- The `main()` entrypoint of the `datalad-annex` Git remote helper has
  be generalized to be more reusable by other (derived) remote helper
  implementations.
  https://github.com/datalad/datalad-next/pull/411 (by @mih)


# 1.0.0b2 (2023-03-17)

## 💫 Enhancements and new features

- `CredentialManager`
  - The Credential Manager gained a new helper, ``obtain()``, that supports a
    credential selection by name/ID, falls back to querying with a set of
    properties, and would finally resort to an interactive credential query from
    the user. ([#216](https://github.com/datalad/datalad-next/pull/216) by @mih)
  - All optional arguments of the CredentialManager are now
    keyword-argument-only
    ([#230](https://github.com/datalad/datalad-next/pull/230) by @mih)
  - Users no longer need to provide type hints for legacy credentials in
    "provider" configurations
    ([#247](https://github.com/datalad/datalad-next/pull/247) by @mih)
  - Credential reporting supports a ``cred_type`` annotation
    ([#257](https://github.com/datalad/datalad-next/pull/257) by @mih)
  - Credential errors for GitHub-like remotes were improved to hint users how
    to update or set new credentials
    ([#235](https://github.com/datalad/datalad-next/pull/235) by @mih)

- `UrlOperations`
  - The URL handler can now load configurations from config files
    ([#222](https://github.com/datalad/datalad-next/pull/222) by @mih)
  - Improved messaging within `URLOperationsRemoteError`
    ([#308](https://github.com/datalad/datalad-next/pull/308) by @mih)

- `Parameter validation`
  - A new `validate_defaults` parameter of ``EnsureCommandParameterization``
    allows opt-in parameter validation, which causes processing of any
    specified parameter's default.
    ([#227](https://github.com/datalad/datalad-next/pull/227) by @mih)
  - A new base class ``ConstraintError`` can communicate parameter validation
    errors and can associate constraint violations with a particular context.
    ``CommandParametrizationError`` uses it to communicate violations for a full
    command parameterization at once and is used in an improved
    `EnsureCommandParametrization` constraint. Callers can now also decide whether
    to perform an exhaustive parameter validation, or fail on first error.
    ([#234](https://github.com/datalad/datalad-next/pull/234) by @mih)
  - A new ``ConstraintWithPassthrough`` constraint exposes
    `EnsureParameterConstraint`'s pass-through feature
    ([#244](https://github.com/datalad/datalad-next/pull/244) by @mih)
  - `EnsureCommandParameterization` learned a `tailor_for_dataset()` parameter
    that can be used to identify which parameters' constraints should be
    tailored for which dataset. This allows tailoring constraints for particular
    datasets ([#260](https://github.com/datalad/datalad-next/pull/260) by @mih)
  - ``EnsurePath`` can be tailored to dataset instances to resolve paths
    against a given Dataset
    ([#271](https://github.com/datalad/datalad-next/pull/271) by @mih)
  - The ``EnsureDataset`` constraint learned an optional check for a valid
    dataset ID ([#279](https://github.com/datalad/datalad-next/pull/279) by
    @adswa)
  - A ``WithDescription`` meta constraints paves the way for custom docs for
    parameters: If given, it replaces the original parameter documentation, and
    can be used to tailor descriptions for specific use cases.
    ([#294](https://github.com/datalad/datalad-next/pull/294) by @mih)
  - Parameter violations gained structured error reporting and customized
    rendering of parameter violations
    ([#306](https://github.com/datalad/datalad-next/pull/306) by @mih)
  - ``EnsureGeneratorFromFileLike`` became more suitable for batch mode use by
    learning to yield instead of raise internal exceptions, if configured by
    the caller ([#278](https://github.com/datalad/datalad-next/pull/278) by @mih)

## 🐛 Bug Fixes

- Previously, the last used credential matching a ``realm`` was used
  unconditionally. Now, credentials without secrets are excluded.
  ([#248](https://github.com/datalad/datalad-next/pull/248) by @mih)

- ``AND`` and ``OR`` compounds for Constraints do not modify Constraints in
  place anymore, but return a new instance.
  ([#292](https://github.com/datalad/datalad-next/pull/292) by @mih)

- Even though the ``EnsureDataset`` constraint returns ``DatasetParameter``
  objects, ``_execute_command`` that would patch up DataLad commands wasn't
  able to work with them
  ([#269](https://github.com/datalad/datalad-next/pull/269) by @adswa)

## 🪓 Deprecations and removals

- The URL operation ``sniff`` was renamed to ``stat``.
  ([#231](https://github.com/datalad/datalad-next/pull/231) by @adswa)

- `serve_path_via_webdav()` that came with 0.2 was deprecated in favor of the
  `webdav_server` fixture
  ([#301](https://github.com/datalad/datalad-next/pull/301) by @mih)

## 📝 Documentation

- A dedicated Developer Guide section of the docs was introduced
  ([#304](https://github.com/datalad/datalad-next/pull/304) by @adswa)

- The README mentions the `uncurl` special remote, and the documentation now
  provide installation information

- ``CONTRIBUTING.md`` was updated on patching
  ([#262](https://github.com/datalad/datalad-next/pull/262/) by @mih)

## 🏠 Internal

- Package dependencies were made explicit
  ([#212](https://github.com/datalad/datalad-next/pull/212) by @mih)

- Misc. code reorganization:
  - The CredentialManager was elevated to a top-level module
    ([#229](https://github.com/datalad/datalad-next/pull/220) by @mih)
  - Dataset-lookup behavior of the ``credentials`` command became identical to
    ``download`` ([#256](https://github.com/datalad/datalad-next/pull/256) by
    @mih)

- The DataLad runner performance patch and all patches to clone functionality
  were removed as they are included in datalad-0.18.1; The dependency was
  adjusted accordingly. ([#218](https://github.com/datalad/datalad-next/pull/218)
  by @mih)

- Compound constraints got a comprehensive ``__repr__`` to improve debugging
  ([#276](https://github.com/datalad/datalad-next/pull/276) by @mih)

- Discontinue legacy code
  ([#300](https://github.com/datalad/datalad-next/pull/300/) by @mih)

## 🛡 Tests

- Automatic CI builds were disabled for changes constrained to the following
  files and directories: `.github/`, `CHANGELOG.md`, `CITATION.cff`,
  `CONTRIBUTORS`, `LICENSE`, `Makefile`, `README.md`, `readthedocs.yml`

- Coverage reports for the uncurl special remote
  ([#220](https://github.com/datalad/datalad-next/pull/220) by @mih)

- Tests will not fail if coverage uploads fail
  ([#241](https://github.com/datalad/datalad-next/pull/241/files) by @mih)

- GitHub actions use the `datalad-installer` to install git-annex
  ([#239](https://github.com/datalad/datalad-next/pull/239/files) by @mih)

- A bug in DataLad's test setup causes configuration managers to leak across
  datasets (https://github.com/datalad/datalad/issues/7297). Next implemented
  test isolation for keyring and config as a fix
  ([#263](https://github.com/datalad/datalad-next/pull/263) by @mih)

- A number of new pytest fixtures were introduced:
  - `memory_keyring` ([#254](https://github.com/datalad/datalad-next/pull/254)
    by @mih), which was then replaced by ``tmp_keywing``
    ([#264](https://github.com/datalad/datalad-next/pull/264))
  - `dataset` and `existing_dataset`
    ([#296](https://github.com/datalad/datalad-next/pull/296) by @mih)
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

  This feature is in an early stage of development (although all included
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

This release primarily brings the ability to store DataLad datasets on a WebDAV
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
  lives in a keystore, but can be overridden using the normal configuration
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
