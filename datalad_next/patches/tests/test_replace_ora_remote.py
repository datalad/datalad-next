from __future__ import annotations

import pytest

from ..replace_ora_remote import (
    canonify_url,
    de_canonify_url,
)


@pytest.mark.parametrize("scheme", ['ria+file', 'file'])
def test_canonify(scheme, monkeypatch):
    url_uncanonified = scheme + '://C:/a/b/c'
    url_canonified = scheme + ':///C:/a/b/c'

    monkeypatch.setattr(
        'datalad_next.patches.replace_ora_remote.on_windows',
        True,
    )
    assert canonify_url(url_canonified) == url_canonified
    assert canonify_url(url_uncanonified) == url_canonified

    monkeypatch.setattr(
        'datalad_next.patches.replace_ora_remote.on_windows',
        False,
    )
    assert canonify_url(url_canonified) == url_canonified
    assert canonify_url(url_uncanonified) == url_uncanonified


@pytest.mark.parametrize("scheme", ['ria+file', 'file'])
def test_de_canonify(scheme, monkeypatch):
    url_uncanonified = scheme + '://C:/a/b/c'
    url_canonified = scheme + ':///C:/a/b/c'

    monkeypatch.setattr(
        'datalad_next.patches.replace_ora_remote.on_windows',
        True,
    )
    assert de_canonify_url(url_canonified) == url_uncanonified
    assert de_canonify_url(url_uncanonified) == url_uncanonified

    monkeypatch.setattr(
        'datalad_next.patches.replace_ora_remote.on_windows',
        False,
    )
    assert de_canonify_url(url_canonified) == url_canonified
    assert de_canonify_url(url_uncanonified) == url_uncanonified
