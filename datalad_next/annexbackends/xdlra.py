"""git-annex external backend XDLRA for git-remote-datalad-annex"""

from pathlib import Path
import zipfile

from .base import (
    Backend,
    BackendError,
    Master,
)


class DataladRepoAnnexBackend(Backend):
    """Implementation of an external git-annex backend

    This backend is tightly coupled to the `git-remote-datalad-annex`
    and hardly of any general utility. It is essentially aiming to be
    the leanest possible implementation to get git-annex to transport
    the content of two distinct files to and from a special remote.
    This backend is unlike most backends, because there is no fixed
    association of a particular file content to a particular key.
    In other words, the key content is expected to change without
    any change in the key name.

    Only two keys are supported:

    - ``XDLRA--refs``
    - ``XDLRA--repo-export``

    ``XDLRA--refs`` contains a "refs" list of a Git repository, similar
    to the output of ``git for-each-ref``. ``XDLRA--repo-export`` hold
    a ZIP archive of a bare Git repository.

    """
    def can_verify(self):
        # we can verify that a key matches the type of content
        # this is basically no more than a sanity check that a
        # download yielded something that we can work with for
        # downstream clone processing
        return True

    def is_stable(self):
        # the content behind a key is not always the same
        # in fact, it is typically different each time
        return False

    def is_cryptographically_secure(self):
        # we are not using any hashes
        return False

    def gen_key(self, local_file):
        localfile = Path(local_file)

        if _is_component_repoexport(localfile):
            return "XDLRA--repo-export"
        elif _is_component_refs(localfile):
            return "XDLRA--refs"
        else:
            # local_file is a TMP location, no use in reporting it
            raise BackendError('Unrecognized repository clone component')

    def verify_content(self, key, content_file):
        return self.gen_key(content_file) == key


def _is_component_refs(path):
    return path.read_text().endswith(' HEAD\n')


def _is_component_repoexport(path):
    return zipfile.is_zipfile(path)


def main():
    """Entry point for the backend utility"""
    master = Master()
    backend = DataladRepoAnnexBackend(master)
    master.LinkBackend(backend)
    master.Listen()
