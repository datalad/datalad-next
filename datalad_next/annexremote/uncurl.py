"""
Goals

- Keep deposited data accessible without having to update (also deposited)
  datalad datasets

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

import json
import re

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
        self.dl_url_template = None
        self.match = None
        self.url_handler = None
        # cache of properties that do not vary within a session
        # or across annex keys
        self.persistent_tmpl_props = {}

    def initremote(self):
        self.message("INIT", type='info')

    def prepare(self):
        self.message("PREPARE", type='info')
        # we need the git remote name to be able to look up config about
        # that remote
        remotename = self.annex.getgitremotename()
        # get the repo to gain access to its config
        self.repo = GitRepo(self.annex.getgitdir())
        # check the config for a URL template setting
        self.dl_url_template = self.repo.cfg.get(
            f'remote.{remotename}.uncurl-url', '')
        # only if we have no local, overriding, configuration ask git-annex
        # for the committed special remote config on the URL template
        if not self.dl_url_template:
            # ask for the commit config, could still be empty
            self.dl_url_template = self.annex.getconfig('url')
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

        self.message(f'TEMPLATE {self.dl_url_template!r}', type='info')
        self.message(f'MATCH {self.match!r}', type='info')

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

        If not match expressions are configured, return `True` of the URL
        scheme is supported, or `False` otherwise.
        """
        self.message(f"CLAIMURL {url}", type='info')
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
        self.message(f"CHECKURL {url}", type='info')
        # TODO test the case where the URL template needs something
        # that cannot be extracted from this very URL.
        # we cannot consult a key that may already point to the same
        # content as the URL, and may have other information --
        # we simply dont have it at this point
        url = self.get_mangled_url(
            url,
            self.dl_url_template,
            self.extract_tmpl_props(
                tmpl=self.dl_url_template,
                urls=[url],
            ),
        )
        try:
            urlprops = self.url_handler.sniff(url)
        except DownloadError as e:
            # leave a trace in the logs
            CapturedException(e)
            return False
        urlprops = {
            _sniff2checkurl_map.get(k, k): v
            for k, v in urlprops.items() if k in ('content-length', 'url')
        }
        self.message("DONECHECKURL", type='info')
        return [urlprops] if urlprops else True

    def transfer_retrieve(self, key, filename):
        self.message(f"RETRIEVE {key} {filename}", type='info')
        urls = self.get_key_urls(key)
        # depending on the configuration (rewriting template or not)
        # we could have one or more URLs to try
        for url in urls:
            # TODO handle errors
            try:
                self.url_handler.download(url, filename)
                # we succeeded, no need to try again
                return
            except DownloadError as e:
                ce = CapturedException(e)
                self.message(f'Failed to download key {key!r} from {url!r}',
                             type='debug')
        raise RemoteError(f'Failed to download {key!r} from any of {urls!r}')

    def checkpresent(self, key):
        self.message(f"CHECKPRESENT {key}", type='info')
        # TODO deduplicate with transfer_retrieve()
        urls = self.get_key_urls(key)
        # depending on the configuration (rewriting template or not)
        # we could have one or more URLs to try
        for url in urls:
            # TODO handle errors
            try:
                self.message(f"CHECKPRESENT SNIFF {url}", type='info')
                self.url_handler.sniff(url)
                # TODO compare with the key-size, if there as any
                # we succeeded, no need to try again
                return True
            except DownloadError as e:
                ce = CapturedException(e)
                self.message(f'Failed to find key {key!r} at {url!r}',
                             type='debug')
        raise RemoteError(f'Failed to find {key!r} at any of {urls!r}')

    #
    # unsupported
    #
    def transfer_store(self, key, filename):
        raise UnsupportedRequest('"uncurl" remote cannot store content')

    def remove(self, key):
        raise UnsupportedRequest('"uncurl" remote cannot remove content')

    #
    # helpers
    #
    def is_recognized_url(self, url):
        return any(m.match(url) for m in self.match)

    def get_key_urls(self, key) -> list[str]:
        # ask git-annex for the URLs it has on record for the key.
        # this will also work within checkurl() for a temporary key
        # generated by git-annex after claimurl()
        urls = self.annex.geturls(key, prefix='')
        self.message(f"KNOWN URLS {urls}", type='info')
        if self.dl_url_template:
            # we have a rewriting template. extract all properties
            # from all known URLs and instantiate the template
            # to get the ONE desired URL
            props = self.extract_tmpl_props(
                tmpl=self.dl_url_template,
                urls=urls,
                key=key,
            )
            self.message(f"PROPERTIES {props}", type='info')
            url = self.get_mangled_url(
                fallback_url=None,
                tmpl=self.dl_url_template,
                tmpl_props=props,
            )
            self.message(f"FINALURL {url}", type='info')
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
                        'Partial URL property shadowing detected. Avoid by using '
                        'unique expression match group names.', type='debug')
                allprops.update(props)
        return allprops


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
