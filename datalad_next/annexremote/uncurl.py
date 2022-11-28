"""
Goals

- Keep deposited data accessible without having to update (also deposited)
  datalad datasets

Identifiers can only come from URLs.

Q: are the standard identifiers (dsid, annex uuid) any good? They would never work
in a template with checkurl(). But they would work, when uploading existing keys.

Setup

It is enough to simply enable the remote in a dataset. But this is a special case
similar to what the `datalad` special remote is doing. It can be used to
employ this special remote as a download helper with datalad protocol and
credential support.

However, typically one would want to define a `match` expression to limit the
handling of URL to a particular host, or data structures on one or more hosts.

When different access methods are required to read from vs write to the same
location, as second uncurl remote with sameas= must be configured.

Configuration

- `remote.<remotename>.uncurl-url`
- `remote.<remotename>.uncurl-match`

Built-in URL components

- `datalad_dsid` - the DataLad dataset ID (UUID)
- `annex_dirhash` - "mixed" variant of the two level hash for a particular key
  (uses POSIX directory separators, and included a trailing separator)
- `annex_dirhash_lower` - "lower case" variant of the two level hash for a
  particular key (uses POSIX directory separators, and included a trailing
  separator)
- `annex_key` - git-annex key name for a request
- `annex_remoteuuid` - UUID of the special remote (location) used by git-annex
- `git_remotename` - Name of the Git remote for the uncurl special remote

Additional components can be defined by configuring `match` expression
configurations that are evaluated on each recorded URL for any given git-annex
key. It is recommended that each `match` expression uses unique match group
identifiers to avoid value conflicts.

https://docs.python.org/3/library/string.html#format-string-syntax
"""

from functools import partial
import json
from pathlib import Path
import re
from urllib.parse import urlparse

# we intentionally limit ourselves to the most basic interface
# and even that we only need to get a `ConfigManager` instance.
# If that class would support a plain path argument, we could
# avoid it entirely
from datalad.dataset.gitrepo import GitRepo

from datalad_next.exceptions import (
    CapturedException,
    DownloadError,
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
        self.repo = GitRepo(self.annex.getgitdir())
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
        except DownloadError as e:
            # TODO untested and subject to change due to
            # https://github.com/datalad/datalad-next/issues/154
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

    #
    # unsupported (yet)
    #
    def transfer_store(self, key, filename):
        raise UnsupportedRequest('"uncurl" remote cannot store content')

    def remove(self, key):
        raise UnsupportedRequest('"uncurl" remote cannot remove content')

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
            except DownloadError as e:
                # TODO subject to change due to
                # https://github.com/datalad/datalad-next/issues/154
                # TODO return False if we can be sure that the remote
                # system works properly and just the key is not around
                ce = CapturedException(e)
                self.message(
                    f'Failed to {action[0]} key {key!r} {action[1]} {url!r}',
                    type='debug')
        raise RemoteError(
            f'Failed to {action[0]} {key!r} {action[1]} any of {urls!r}')


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
