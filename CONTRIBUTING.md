# Contributing to `datalad-next`

## When should I consider a contribution to `datalad-next`?

In short: whenever a contribution to the DataLad core package would make sense, it should also be suitable for `datalad-next`.

### What contributions should be directed elsewhere?

Special interest, highly domain-specific functionality is likely better suited for a topical DataLad extension package.

Functionality that requires complex additional dependencies, or is highly platform-specific might also be better kept in a dedicated extension package.

If in doubt, it is advisable to file an issue and ask for feedback before preparing a contribution.

### When is a contribution to `datalad-next` preferable over one to the DataLad core package?

New feature releases of `datalad-next` are happening more frequently. Typically, every 4-6 weeks.

New features depending on other `datalad-next` features are, by necessity, better directed at `datalad-next`.

## What is important for a successful contribution to `datalad-next`?

A contribution must be complete with code, tests, and documentation.

`datalad-next` is a staging area for features, hence any code is expected to move and morph. Therefore, tests are essential. A high test-coverage is desirable. Contributors should aim for 95% coverage or better. Tests must be dedicated for the code of a particular contribution. It is not sufficient, if other code happens to also exercise a new feature.

New code should be type-annotated. At minimum, a type annotation of the main API (e.g., function signatures) is needed.

Docstrings should be complete with information on parameters, return values, and exception behavior. Documentation should be added to and rendered with the sphinx-based documentation.

Contributions should be organized to match the code organization implemented in `datalad-next`.

## Code organization

In `datalad-next`, all code is organized in shallow sub-packages. Each sub-package is located in a directory within the `datalad_next` package.

A sub-package contains any number of code files, and a `tests` directory with all test implementations for that particular sub-package, and only for that sub-package. Other, deeper directory hierarchies are not to be expected.

There is no limit to the number of files. Contributors should strive for files with less than 500 lines of code.

Within a sub-package, code should generally use relative imports. The corresponding tests should also import the tested code via relative imports.

Code users should be able to import the most relevant functionality from the sub-package's `__init__.py`.

If possible, sub-packages should have a "central" place for imports of functionality from outside `datalad-next` and the Python standard library. Other sub-package code should then import from this place via relative imports. This aims to make external dependencies more obvious, and import-error handling and mitigation for missing dependencies simpler and cleaner. Such a location could be the sub-package's `__init__.py`, or possibly a dedicated `dependencies.py`.

Sub-packages should be as self-contained as possible. Individual components in `datalad-next` should strive to be easily migratable to the DataLad core package. This means that any organization principles like *all-exceptions-go-into-a-single-location-in-datalad-next* do not apply. For example, each sub-package should define its exceptions separately from others. When functionality is shared between sub-packages, absolute imports should be made.

There is one special sub-package in `datalad-next`: `patches`. All runtime patches to be applied to the DataLad core package must be placed here.
