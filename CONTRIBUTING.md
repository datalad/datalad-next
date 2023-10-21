# Contributing to `datalad-next`

- [What contributions are most suitable for `datalad-next`](#when-should-i-consider-a-contribution-to-datalad-next)
- [Style guide](#contribution-style-guide)
- [Code organization](#code-organization)
- [How to implement runtime patches](#runtime-patches)
- [How to implement imports](#imports)
- [Prohibited DataLad functionality](#prohibited-datalad-core-features)


## When should I consider a contribution to `datalad-next`?

In short: whenever a contribution to the DataLad core package would make sense, it should also be suitable for `datalad-next`.


### What contributions should be directed elsewhere?

Special interest, highly domain-specific functionality is likely better suited for a topical DataLad extension package.

Functionality that requires complex additional dependencies, or is highly platform-specific might also be better kept in a dedicated extension package.

If in doubt, it is advisable to file an issue and ask for feedback before preparing a contribution.


### When is a contribution to `datalad-next` preferable over one to the DataLad core package?

New feature releases of `datalad-next` are happening more frequently. Typically, every 4-6 weeks.

New features depending on other `datalad-next` features are, by necessity, better directed at `datalad-next`.


## Contribution style guide

A contribution must be complete with code, tests, and documentation.

`datalad-next` is a staging area for features, hence any code is expected to move and morph. Therefore, tests are essential. A high test-coverage is desirable. Contributors should aim for 95% coverage or better. Tests must be dedicated for the code of a particular contribution. It is not sufficient, if other code happens to also exercise a new feature.

New code should be type-annotated. At minimum, a type annotation of the main API (e.g., function signatures) is needed.

Docstrings should be complete with information on parameters, return values, and exception behavior. Documentation should be added to and rendered with the sphinx-based documentation.


### Code organization

In `datalad-next`, all code is organized in shallow sub-packages. Each sub-package is located in a directory within the `datalad_next` package.

Consequently, there are no top-level source files other than a few exceptions for technical reasons (`__init__.py`, `conftest.py`, `_version.py`).

A sub-package contains any number of code files, and a `tests` directory with all test implementations for that particular sub-package, and only for that sub-package. Other, deeper directory hierarchies are not to be expected.

There is no limit to the number of files. Contributors should strive for files with less than 500 lines of code.

Within a sub-package, code should generally use relative imports. The corresponding tests should also import the tested code via relative imports.

Code users should be able to import the most relevant functionality from the sub-package's `__init__.py`.

If possible, sub-packages should have a "central" place for imports of functionality from outside `datalad-next` and the Python standard library. Other sub-package code should then import from this place via relative imports. This aims to make external dependencies more obvious, and import-error handling and mitigation for missing dependencies simpler and cleaner. Such a location could be the sub-package's `__init__.py`, or possibly a dedicated `dependencies.py`.

Sub-packages should be as self-contained as possible. Individual components in `datalad-next` should strive to be easily migratable to the DataLad core package. This means that any organization principles like *all-exceptions-go-into-a-single-location-in-datalad-next* do not apply. For example, each sub-package should define its exceptions separately from others. When functionality is shared between sub-packages, absolute imports should be made.

There is one special sub-package in `datalad-next`: `patches`. All runtime patches to be applied to the DataLad core package must be placed here.


### Runtime patches

The `patches` sub-package contains all runtime patches that are applied by `datalad-next`.  Patches are applied on-import of `datalad-next`, and may modify arbitrary aspects of the runtime environment. A patch is enabled by adding a corresponding `import` statement to `datalad_next/patches/enabled.py`. The order of imports in this file is significant. New patches should consider behavior changes caused by other patches, and should be considerate of changes imposed on other patches.

`datalad-next` is imported (and thereby its patches applied) whenever used
directly (e.g., when running commands provided by `datalad-next`, or by an
extension that uses `datalad-next`).  In addition, it is imported by the
DataLad core package itself when the configuration item
`datalad.extensions.load=next` is set.

Patches modify an external implementation that is itself subject to change. To improve the validity and longevity of patches, it is helpful to consider a few guidelines:

- Patches should use `datalad_next.patches.apply_patch()` to perform the patching, in order to yield uniform (logging) behavior

- Patches should be as self-contained as possible. The aim is for patches to be merged upstream (at the patched entity) as quickly as possible. Self-contained patches facilitate this process.

- Patches should maximally limit their imports from sources that are not the patch target. The helps to detect when changes to the patch target (or its environment) are made, and also helps to isolate the patch from changes in the general environment of the patches software package that are unrelated to the specific patched code.


### Imports

#### Import centralization per sub-package

If possible, sub-packages should have a "central" place for imports of functionality from outside `datalad-next` and the Python standard library. Other sub-package code should then import from this place via relative imports. This aims to make external dependencies more obvious, and import-error handling and mitigation for missing dependencies simpler and cleaner. Such a location could be the sub-package's `__init__.py`, or possibly a dedicated `dependencies.py`.

#### No "direct" imports from `datalad`

This is a specialization of the "Import centralization" rule above. All sub-package code should import from `datalad` into a *single* dedicated location inside the sub-package. All other sub-package code imports from this location.

The aim is to clearly see what of the huge DataLad API is actually relevant for a particular feature. For some generic helpers it may be best to import them to `datalad_next.utils` or `datalad_next.tests.utils`.


### Prohibited DataLad core features

The following components of the `datalad` package must not be used (directly) in contributions to `datalad-next`, because they have been replace by a different solution with the aim to phase them out.

#### `require_dataset()`

Commands must use `datalad_next.constraints.dataset.EnsureDataset` instead.

#### nose-style decorators in test implementations

The use of decorators like `with_tempfile` is not allowed.
`pytest` fixtures have to be used instead.
A *temporary* exception *may* be the helpers that are imported in `datalad_next.tests.utils`.
However, these will be reduced and removed over time, and additional usage only adds to the necessary refactoring effort.
Therefore new usage is highly discouraged.

#### nose-style assertion helpers in test implementations

The use of helpers like `assert_equal` is not allowed.
`pytest` constructs have to be used instead -- this typically means plain `assert` statements.
A *temporary* exception *may* be the helpers that are imported in `datalad_next.tests.utils`.
However, these will be reduced and removed over time, and additional usage only adds to the necessary refactoring effort.
Therefore new usage is highly discouraged.
