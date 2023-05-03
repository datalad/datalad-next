"""Recognize DATALAD_TESTS_TMP_KEYRING_PATH to set alternative secret storage

Within `pytest` DataLad uses the plaintext keyring backend. This backend has no
built-in way to configure a custom file location for secret storage from the
outside. This patch looks for a DATALAD_TESTS_TMP_KEYRING_PATH environment
variable, and uses its value as a file path for the storage.

This makes it possible to (temporarily) switch storage. This feature is used
by the ``tmp_keyring`` pytest fixture. This patch is needed in addition to the
test fixture in order to apply such changes also to child processes, such as
special remotes and git remotes.
"""

from os import environ

if 'DATALAD_TESTS_TMP_KEYRING_PATH' in environ:
    import keyring
    kr = keyring.get_keyring()
    kr.file_path = environ['DATALAD_TESTS_TMP_KEYRING_PATH']
