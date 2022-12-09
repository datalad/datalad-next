"""
*uncurl* git-annex external special remote
==========================================

This implementation is a git-annex accessible interface to datalad-next's URL
operations framework. It serves two main purposes:

1. Combine git-annex's capabilities of registering and accessing file content
   via URLs with DataLad's access credential management and (additional or
   alternative) transport protocol implementations.

2. Minimize the maintenance effort for datasets (primarily) composed from
   content that is remotely accessible via URLs from systems other than
   Datalad or git-annex in the event of an infrastructure transition
   (e.g. moving to a different technical system or a different data
   organization on a storage system).

Requirements
------------

This special remote implementation requires git-annex version 8.20210127 (or
later) to be available.

Download helper with credential management support
--------------------------------------------------

The simplest way to use this remote is to initialize it without any particular
configuration::

    $ git annex initremote uncurl type=external externaltype=uncurl encryption=none
    initremote uncurl ok
    (recording state in git...)

Once initialized, or later enabled in a clone, ``git-annex addurl`` will check
with the *uncurl* remote whether it can handle a particular URL, and will let
the remote perform the download in case of positive response. By default, the
remote will claim any URLs with a scheme that the local datalad-next
installation supports. This always includes ``file://``, ``http://``, and
``https://``, but is extensible, and a particular installation may also support
``ssh://`` (by default when openssh is installed), or other schemes.

With this setup, download requests now use DataLad's credential system for
authentication. DataLad will automatically lookup matching credentials, prompt
for manual entry if none are found, and offer to store them securely for later
use after having used them successfully::

    $ git annex addurl http://httpbin.org/basic-auth/myuser/mypassword
    Credential needed for access to http://httpbin.org/basic-auth/myuser/mypassword
    user: myuser
    password: 
    password (repeat): 
    Enter a name to save the credential
    (sniffing http://httpbin.org/basic-auth/myuser/mypassword) securely for future
    re-use, or 'skip' to not save the credential
    name: httpbin-dummy

    addurl http://httpbin.org/basic-auth/myuser/mypassword (from uncurl) (to ...) 
    ok
    (recording state in git...)

By adding files via downloads from URLs in this fashion, datasets can be built
that track information across a range of locations/services, using a possibly
heterogeneous set of access methods.

This feature is very similar to the ``datalad`` special remote implementation
included in the core DataLad package. The difference here is that alternative
implementations of downloaders are employed and the ``datalad-next`` credential
system is used instead of the "providers" mechanism from DataLad's core
package.


Transforming recorded URLs
--------------------------

The main benefit of using *uncurl* is, however, only revealed when the original
snapshot of where data used to be accessible becomes invalid, maybe because
data were moved to a different storage system, or simply a different host.

This would typically require an update of each, now broken, access URL. For
datasets with thousands or even millions of files this can be an expensive
operation. For data portal operators providing a large number of datasets it is
even more tedious.

*uncurl* enables programmatic, on-access URL rewriting. This is similar, in
spirit, to Git's ``url.<base>.insteadOf`` URL modification feature. However,
modification possibilities reach substantially beyond replacing a base URL.

This feature is based on two customizable settings: 1) a *URL template*; and
2) a *set of match expressions* that extract additional identifiers
from any recorded access URL for an annex key.

Here is an example: Let's say a file in a dataset has a recorded access URL
of::

    https://data.example.org/c542/s7612_figure1.pdf

We can let *uncurl* know that ``c542`` is actually an identifier for a
particular collection of items in this data store. Likewise ``s7612`` is an
identifier of a particular item in that collection, and ``figure1.pdf`` is the
name of a component in that collection item. The following Python regular
expression can be used to "decompose" the above URL into these semantic
components::

  (?P<site>https://[^/]+)/(?P<collection>c[^/]+)/(?P<item>s[^/]+)_(?P<component>.*)$

This expression is not the most readable, but it basically chunks the URL
into segments of ``(?P<name>...)``, so-called named groups (see a
`live demo of this expression <https://www.debuggex.com/r/Aa1yua-awXBuqZ39>`__).

This expression, and additional ones like it, can set as a configuration
parameter of an *uncurl* remote setup. Extending the configuration established
by the ``initremote`` call above::

    $ git annex enableremote uncurl \\
        'match=(?P<site>https://[^/]+)/(?P<collection>c[^/]+)/(?P<item>s[^/]+)_(?P<component>.*)$'

The last argument is quoted to prevent it from being processed by the shell.

With the match expression configured, URL rewriting can be enabled by declaring
a URL template as another configuration item. The URL template uses the `Python
Format String Syntax
<https://docs.python.org/3/library/string.html#format-string-syntax>`__. If the
new URL for the file above is now
``http://newsite.net/ex-archive/c542_s7612_figure1.pdf``, we can declare
the following URL template to have *uncurl* go to the new site::

    http://newsite.net/ex-archive/{collection}_{item}_{component}

This template references the identifiers of the named groups we defined in the
match expression. Again, the URL template can be set via ``git annex
enableremote``::

    $ git annex enableremote uncurl \\
        'url=http://newsite.net/ex-archive/{collection}_{item}_{component}'

There is no need to separate the ``enableremote`` calls. Both configuration can
be given at the same time. In fact, they can also be given to ``initremote``
immediately.

The three identifiers ``site``, ``collection``, ``item``, and ``component`` are
actually a custom addition to a standard set of identifiers that are available
for composing URLs via a template.

- ``datalad_dsid`` - the DataLad dataset ID (UUID)
- ``annex_dirhash`` - "mixed" variant of the two level hash for a particular key
  (uses POSIX directory separators, and included a trailing separator)
- ``annex_dirhash_lower`` - "lower case" variant of the two level hash for a
  particular key (uses POSIX directory separators, and included a trailing
  separator)
- ``annex_key`` - git-annex key name for a request
- ``annex_remoteuuid`` - UUID of the special remote (location) used by git-annex
- ``git_remotename`` - Name of the Git remote for the uncurl special remote

.. note::
   The URL template must "resolve" to a complete and valid URL. This cannot
   be verified at configuration time, because even the URL scheme could be a
   dynamic setting.

Uploading content
-----------------

The *uncurl* special remote can upload file content or store annex keys
via supported URL schemes whenever a URL template is defined. At minimum,
storing at ``file://`` and ``ssh://`` URLs are supported. But other URL
scheme handlers with upload support may be available at a local DataLad
installation.


Deleting content
----------------

As for uploading, deleting content is only permitted with a configured
URL template. Moreover, it also depends on the delete operation being
supported for a particular URL scheme.


Configuration overrides
-----------------------

Both match expressions and the URL template can also be configured in a dataset's
configuration (committed branch configuration, or any Git configuration scope
(local, global, system) using the following configuration item names:

- ``remote.<remotename>.uncurl-url``
- ``remote.<remotename>.uncurl-match``

where ``<remotename>`` is the name of the special remote in the dataset.

A URL template provided via configuration *overrides* one defined in the special
remote setup via ``init/enableremote``.

Match expressions defined as configuration items *extend* the set of match
expressions that may be included in the special remote setup via
``init/enableremote``. The ``remote.<remotename>.uncurl-match`` configuration
item can be set as often as necessary (which one match expression each).

Tips
----

When multiple match expressions are defined, it is recommended to use unique
names for each match-group to avoid collisions.
"""
from __future__ import annotations

from functools import partial
import json
from pathlib import Path
import re
from urllib.parse import urlparse

# we intentionally limit ourselves to the most basic interface
# and even that we only need to get a `ConfigManager` instance.
# If that class would support a plain path argument, we could
# avoid it entirely
from datalad_next.datasets import LeanGitRepo

from datalad_next.exceptions import (
    CapturedException,
    UrlOperationsRemoteError,
    UrlOperationsResourceUnknown,
)
from datalad_next.url_operations.any import AnyUrlOperations
from datalad_next.utils import ensure_list

from . import (
    RemoteError,
    SpecialRemote,
    UnsupportedRequest,
    super_main
)



class UncurlRemote(SpecialRemote):
    """ """
    def __init__(self, annex):
        super().__init__(annex)
        self.configs.update(
            url='Python format language template composing an access URL',
            match='(whitespace-separated list of) regular expression(s) to match particular components in supported URL via named groups',
        )
        self.repo = None
        self.url_tmpl = None
        self.match = None
        self.url_handler = None
        # cache of properties that do not vary within a session
        # or across annex keys
        self.persistent_tmpl_props = {}

    def initremote(self):
        # at present there is nothing that needs to be done on init/enable.
        # the remote is designed to work without any specific setup too
        pass

    def prepare(self):
        # we need the git remote name to be able to look up config about
        # that remote
        remotename = self.annex.getgitremotename()
        # get the repo to gain access to its config
        self.repo = LeanGitRepo(self.annex.getgitdir())
        # check the config for a URL template setting
        self.url_tmpl = self.repo.cfg.get(
            f'remote.{remotename}.uncurl-url', '')
        # only if we have no local, overriding, configuration ask git-annex
        # for the committed special remote config on the URL template
        if not self.url_tmpl:
            # ask for the commit config, could still be empty
            self.url_tmpl = self.annex.getconfig('url')
            # TODO test the case of a fully absent URL template
            # that would be fine and only verbatim recorded URLs could
            # be sent to a downloader

        # unconditionally ask git-annex for a match-url setting, any local
        # config ammends, and does not override
        self.match = self.annex.getconfig('match')
        if self.match:
            self.match = self.match.split()
            # TODO implement sanity checks, but running it through re.compile()
            # might just be enough
            self.match = [re.compile(m) for m in self.match]
        # extend with additonal matchers from local config
        self.match = (self.match or []) + [
            re.compile(m)
            for m in ensure_list(self.repo.cfg.get(
                f'remote.{remotename}.uncurl-match', [], get_all=True))
        ]

        self.message(
            f'URL rewriting template: {self.url_tmpl!r}', type='debug')
        self.message(
            f'Active URL match expressions: {[e.pattern for e in self.match]!r}',
            type='debug')

        # let the URL hander use the repo's config
        self.url_handler = AnyUrlOperations(cfg=self.repo.cfg)

        # cache template properties
        # using function arg name syntax, we need the identifiers to be valid
        # Python symbols to work in `format()`
        self.persistent_tmpl_props.update(
            datalad_dsid=self.repo.cfg.get('datalad.dataset.id', ''),
            git_remotename=remotename,
            annex_remoteuuid=self.annex.getuuid(),
        )


    def claimurl(self, url):
        """Needs to check if want to handle a given URL

        If match expressions are configured, matches the URL against all known
        URL expressions, and returns `True` if there is any match, or
        `False` otherwise.

        If no match expressions are configured, return `True` of the URL
        scheme is supported, or `False` otherwise.
        """
        if self.match:
            return self.is_recognized_url(url)
        else:
            return self.url_handler.is_supported_url(url)

    def checkurl(self, url):
        """
        When running `git-annex addurl`, this is called after CLAIMURL
        indicated that we could handle a URL. It can return information
        on the URL target (e.g., size of the download, a target filename,
        or a sequence thereof with additional URLs pointing to individual
        components that would jointly make up the full download from the
        given URL. However, all of that is optional, and a simple `True`
        returned is sufficient to make git-annex call `TRANSFER RETRIEVE`.
        """
        # try-except, because a URL template might need something
        # that cannot be extracted from this very URL.
        # we cannot consult a key that may already point to the same
        # content as the URL, and may have other information --
        # we simply dont have it at this point
        try:
            url = self.get_mangled_url(
                url,
                self.url_tmpl,
                self.extract_tmpl_props(
                    tmpl=self.url_tmpl,
                    urls=[url],
                ),
            )
        except KeyError as e:
            self.message(
                'URL rewriting template requires unavailable component '
                f'{e}, continuing with original URL',
                type='debug',
            )
            # otherwise go ahead with the orginal URL. the template might
            # just be here to aid structured uploads
        try:
            urlprops = self.url_handler.sniff(url)
            return True
        except UrlOperationsRemoteError as e:
            # leave a trace in the logs
            CapturedException(e)
            return False
        # we could return a URL/size/filename triple instead of a bool.
        # this would make git annex download from a URL different from the input,
        # and to the given filename.
        # it could be nice to report the (mangled) url, even if the handler reports
        # a potentially deviating URL (redirects, etc.). Keeping external
        # resolvers in the loop can be intentional, and users could provide
        # resolved URL if they consider that desirable.
        # however, going with the original URL kinda does that already, rewriting
        # is happening anyways. And not reporting triplets avoids the issue
        # of git-annex insisting to write into a dedicated directory for this
        # download.

    def transfer_retrieve(self, key, filename):
        self._check_retrieve(
            key,
            partial(self.url_handler.download, to_path=Path(filename)),
            ('download', 'from'),
        )

    def checkpresent(self, key):
        return self._check_retrieve(
            key,
            self.url_handler.sniff,
            ('find', 'at'),
        )

    def transfer_store(self, key, filename):
        return self._store_delete(
            key,
            partial(self.url_handler.upload, from_path=Path(filename)),
            'cannot store',
        )

    def remove(self, key):
        try:
            return self._store_delete(
                key,
                # we have to map parameter names to be able to use a common
                # helper with transfer_store(), because UrlOperations.upload()
                # needs to get the URL as a second argument, hence we need
                # to pass parameters as keyword-args
                lambda to_url: self.url_handler.delete(url=to_url),
                'refuses to delete',
            )
        except UrlOperationsResourceUnknown as e:
            self.message(
                'f{key} not found at the remote, skipping', type='debug')

    #
    # helpers
    #
    def is_recognized_url(self, url):
        return any(m.match(url) for m in self.match or [])

    def get_key_urls(self, key) -> list[str]:
        # ask git-annex for the URLs it has on record for the key.
        # this will also work within checkurl() for a temporary key
        # generated by git-annex after claimurl()
        urls = self.annex.geturls(key, prefix='')
        self.message(f"Known urls for {key!r}: {urls}", type='debug')
        if self.url_tmpl:
            # we have a rewriting template. extract all properties
            # from all known URLs and instantiate the template
            # to get the ONE desired URL
            props = self.extract_tmpl_props(
                tmpl=self.url_tmpl,
                urls=urls,
                key=key,
            )
            url = self.get_mangled_url(
                fallback_url=None,
                tmpl=self.url_tmpl,
                tmpl_props=props,
            )
            return [url]
        # we have no rewriting template, and must return all URLs we know
        # to let the caller sort it out
        return urls

    def get_mangled_url(self, fallback_url, tmpl, tmpl_props):
        if not tmpl:
            # the best thing we can do without a URL template is to
            # return the URL itself
            return fallback_url
        url = tmpl.format(**tmpl_props)
        return url

    def extract_tmpl_props(self, tmpl, *, urls=None, key=None):
        # look up all the standard
        allprops = dict(self.persistent_tmpl_props)
        if key:
            allprops['annex_key'] = key
            # if we are working on a specific key, check the template if it
            # needs more key-specific properties. The conditionals below
            # are intentionally unprecise to avoid false-negatives given the
            # flexibility of the format-string-syntax
            if 'annex_dirhash' in tmpl:
                allprops['annex_dirhash'] = self.annex.dirhash(key)
            if 'annex_dirhash_lower' in tmpl:
                allprops['annex_dirhash_lower'] = self.annex.dirhash_lower(key)
        # try all URLs against all matchers
        for url in ensure_list(urls):
            for matcher in (self.match or []):
                match = matcher.match(url)
                if not match:
                    # ignore any non-match
                    continue
                # we only support named groups in expressions so this is sufficient
                props = match.groupdict()
                if any(p in allprops and allprops[p] != props[p] for p in props):
                    self.message(
                        'Partial URL property shadowing detected. '
                        'Avoid by using unique expression match group names.',
                        type='debug'
                    )
                allprops.update(props)
        return allprops

    def _check_retrieve(self, key, handler, action: tuple):
        urls = self.get_key_urls(key)
        # depending on the configuration (rewriting template or not)
        # we could have one or more URLs to try
        for url in urls:
            try:
                handler(url)
                # we succeeded, no need to try again
                return True
            except UrlOperationsResourceUnknown:
                # general system access worked, but at the key location is nothing
                # to be found
                return False
            except UrlOperationsRemoteError as e:
                # return False only if we could be sure that the remote
                # system works properly and just the key is not around
                ce = CapturedException(e)
                self.message(
                    f'Failed to {action[0]} key {key!r} {action[1]} {url!r}',
                    type='debug')
        raise RemoteError(
            f'Failed to {action[0]} {key!r} {action[1]} any of {urls!r}')

    def _store_delete(self, key, handler, action: str):
        if not self.url_tmpl:
           raise RemoteError(
                f'Remote {action} content without a configured URL template')
        url = self.get_key_urls(key)
        # we have a rewriting template, so we expect exactly one URL
        assert len(url) == 1
        url = url[0]
        try:
            handler(to_url=url)
        except UrlOperationsResourceUnknown:
            # pass-through, would happen when removing a non-existing key,
            # which git-annex wants to be a OK thing to happen.
            # handler in callers
            raise
        except Exception as e:
            # we need to raise RemoteError whenever we could not perform
            raise RemoteError from e


_sniff2checkurl_map = {
    'content-length': 'size',
}
"""Translate property names returned by AnyUrlOperations.sniff()
to those expected from checkurl()"""


def main():
    """cmdline entry point"""
    super_main(
        cls=UncurlRemote,
        remote_name='uncurl',
        description=\
        "flexible access data (in archive systems) "
        "via a variety of identification schemes",
    )
