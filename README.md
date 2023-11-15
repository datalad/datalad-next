# iterable-subprocess

[![PyPI package](https://img.shields.io/pypi/v/iterable-subprocess?label=PyPI%20package&color=%234c1)](https://pypi.org/project/iterable-subprocess/) [![Test suite](https://img.shields.io/github/actions/workflow/status/uktrade/iterable-subprocess/test.yml?label=Test%20suite)](https://github.com/uktrade/iterable-subprocess/actions/workflows/test.yml) [![Code coverage](https://img.shields.io/codecov/c/github/uktrade/iterable-subprocess?label=Code%20coverage)](https://app.codecov.io/gh/uktrade/iterable-subprocess)

Python context manager to communicate with a subprocess using iterables. This offers a higher level interface to subprocesses than Python's built-in subprocess module, and is particularly helpful when data won't fit in memory and has to be streamed.

This also allows an external subprocess to be naturally placed in a chain of iterables as part of a data processing pipeline.


## Installation

```bash
pip install iterable-subprocess
```


## Usage

A single context manager `iterable_subprocess` is exposed. The first parameter is the `args` argument passed to the [Popen Constructor](https://docs.python.org/3/library/subprocess.html#popen-constructor), and the second is an iterable whose items must be `bytes` instances and are sent to the subprocess's standard input.

Returned from the function is an iterable whose items are `bytes` instances of the process's standard output.

```python
from iterable_subprocess import iterable_subprocess

# In a real case could be a generator function that reads from the filesystem or the network
iterable_of_bytes = (
    b'first\n',
    b'second\n',
    b'third\n',
)

with iterable_subprocess(['cat'], iterable_of_bytes) as output:
    for chunk in output:
        print(chunk)
```


## Exceptions

Python's `subprocess.Popen` is used to start the process, and any exceptions it raises are propagated without transformation. For example, if the subprocess can't be found, then a `FileNotFoundError` is raised.

If the process starts, but exits with a non-zero return code, then an `iterable_subprocess.IterableSubprocessError` exception will be raised with two members:

- `returncode` - the return code of the process
- `stderr` - the final 65536 bytes of the standard error of the process

However, if the process starts, but an exception is raised from inside the context or from the source iterable, then this exception is propagated, even if the process subsequently exits with a non-zero return code.


## Example: unzip the first file of a ZIP archive while downloading

It's possible to download the bytes of a ZIP file in Python, and unzip by passing the bytes to `funzip`, as in the following example.

```python
import httpx
from iterable_subprocess import iterable_subprocess

with \
        httpx.stream('GET', 'https://www.example.com/my.zip') as r, \
        iterable_subprocess(['funzip'], r.iter_bytes()) as unzipped_chunks:

    for chunk in unzipped_chunks:
        print(chunk)
```

Note that it's also possible to stream unzip files without resorting to another process using [stream-unzip](https://github.com/uktrade/stream-unzip).


## Example: download file using curl and process in Python

You would usually download directly from Python, but as an example, you can download using the curl executable and process its output in Python.

```python
from iterable_subprocess import iterable_subprocess

url = 'https://data.api.trade.gov.uk/v1/datasets/uk-tariff-2021-01-01/versions/v3.0.212/tables/measures-on-declarable-commodities/data?format=csv'
with iterable_subprocess(['curl', '--no-progress-meter', '--fail-with-body', url], ()) as output:
    for chunk in output:
        print(chunk)
```
