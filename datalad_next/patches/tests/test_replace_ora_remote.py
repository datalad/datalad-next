from __future__ import annotations

import pytest

from ..replace_ora_remote import (
    canonify_url,
    de_canonify_url,
)
from datalad_next.utils import on_windows


@pytest.mark.parametrize("scheme", ['ria+file', 'file'])
def test_canonify(scheme):
    url_uncanonified = scheme + '://C:/a/b/c'
    url_canonified = scheme + ':///C:/a/b/c'

    if on_windows:
        assert canonify_url(url_canonified) == url_canonified
        assert canonify_url(url_uncanonified) == url_canonified
    else:
        assert canonify_url(url_canonified) == url_canonified
        assert canonify_url(url_uncanonified) == url_uncanonified


@pytest.mark.parametrize("scheme", ['ria+file', 'file'])
def test_de_canonify(scheme):
    url_uncanonified = scheme + '://C:/a/b/c'
    url_canonified = scheme + ':///C:/a/b/c'

    if on_windows:
        assert de_canonify_url(url_canonified) == url_uncanonified
        assert de_canonify_url(url_uncanonified) == url_uncanonified
    else:
        assert de_canonify_url(url_canonified) == url_canonified
        assert de_canonify_url(url_uncanonified) == url_uncanonified
