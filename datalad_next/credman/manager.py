# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See LICENSE file distributed along with the datalad_osf package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Credential management and query"""

# allow for |-type UnionType declarations
from __future__ import annotations

__docformat__ = 'restructuredtext'

__all__ = ['CredentialManager']

from datetime import datetime
import logging
import re

from typing import (
    Dict,
    List,
    Tuple,
)

import datalad
from datalad_next.config import ConfigManager
from datalad_next.exceptions import (
    CapturedException,
    CommandError,
)
from datalad_next.uis import ui_switcher as ui

lgr = logging.getLogger('datalad.credman')


class CredentialManager(object):
    """Facility to get, set, remove and query credentials.

    A credential in this context is a set of properties (key-value pairs)
    associated with exactly one secret.

    At present, the only backend for secret storage is the Python keyring
    package, as interfaced via a custom DataLad wrapper. Store for credential
    properties is implemented using DataLad's (i.e. Git's) configuration
    system. All properties are stored in the `global` (i.e., user) scope
    under configuration items following the pattern::

      datalad.credential.<name>.<property>

    where ``<name>`` is a credential name/identifier, and ``<property>`` is an
    arbitrarily named credential property, whose name must follow the
    git-config syntax for variable names (case-insensitive, only alphanumeric
    characters and ``-``, and must start with an alphabetic character).

    Create a ``CredentialManager`` instance is fast, virtually no
    initialization needs to be performed. All internal properties are lazily
    evaluated.  This facilitates usage in code where it is difficult to
    incorporate a long-lived central instance.

    API

    With one exception, all parameter names of methods in the core API
    outside ``**kwargs`` must have a ``_`` prefix that distinguishes credential
    properties from method parameters. The one exception is the ``name``
    parameter, which is used as a primary identifier (albeit being optional
    for some operations).

    The ``obtain()`` method is provided as an additional convenience, and
    implements a standard workflow for obtaining a credential in a wide variety
    of scenarios (credential name, credential properties, secret either
    respectively already known or yet unknown).
    """
    valid_property_names_regex = re.compile(r'[a-z0-9]+[a-z0-9-]*$')

    secret_names = {
        'user_password': 'password',
    }

    def __init__(self, cfg: ConfigManager | None = None):
        """

        Parameters
        ----------
        cfg: ConfigManager, optional
          If given, all configuration queries are performed using this
          ``ConfigManager`` instance. Otherwise ``datalad.cfg`` is used.
        """
        self.__cfg = cfg
        self.__cred_types = None
        self.__keyring = None

    # main API
    #

    #
    # Design remark:
    # This is the keyhole through which any retrieval-related processing goes
    # (get, query, obtain). Any normalization performed in here will
    # automatically also effect these other operations.
    #
    def get(self, name=None, *, _prompt=None, _type_hint=None, **kwargs):
        """Get properties and secret of a credential.

        This is a read-only method that never modifies information stored
        on a credential in any backend.

        Credential property lookup is supported via a number approaches.  When
        providing ``name``, all existing corresponding configuration items are
        found and reported, and an existing secret is retrieved from name-based
        secret backends (presently ``keyring``). When providing a ``type``
        property or a ``_type_hint`` the lookup of additional properties in the
        keyring-backend is enabled, using predefined property name lists
        for a number of known credential types.

        For all given property keys that have no value assigned after the
        initial lookup, manual/interactive entry is attempted, whenever
        a custom ``_prompt`` was provided. This include requesting a secret.
        If manually entered information is contained in the return credential
        record, the record contains an additional ``_edited`` property with a
        value of ``True``.

        If no secret is known after lookup and a potential manual data entry,
        a plain ``None`` is returned instead of a full credential record.

        Parameters
        ----------
        name: str, optional
          Name of the credential to be retrieved
        _prompt: str or None
          Instructions for credential entry to be displayed when missing
          properties are encountered. If ``None``, manual entry is disabled.
        _type_hint: str or None
          In case no ``type`` property is included in ``kwargs``, this parameter
          is used to determine a credential type, to possibly enable further
          lookup/entry of additional properties for a known credential type
        **kwargs:
          Credential property name/value pairs to overwrite/amend potentially
          existing properties. For any property with a value
          of ``None``, manual data entry will be performed, unless a value
          could be retrieved on lookup, or prompting was not enabled.

        Returns
        -------
        dict or None
          Return ``None``, if no secret for the credential was found or entered.
          Otherwise returns the complete credential record, comprising all
          properties and the secret. An additional ``_edited`` key with a
          value of ``True`` is added whenever the returned record contains
          manually entered information.

        Raises
        ------
        ValueError
          When the method is called without any information that could be
          used to identify a credential
        """
        if name is None and _type_hint is None and not kwargs:
            # there is no chance that this could work
            raise ValueError(
                'CredentialManager.get() called without any identifying '
                'information')
        if name is None:
            # there is no chance we can retrieve any stored properties
            # but we can prompt for some below
            cred = {}
        else:
            # if we have a chance to query for stored legacy credentials
            # we do this first to have the more modern parts of the
            # system overwrite them reliably
            cred = self._get_legacy_credential_from_keyring(
                name,
                type_hint=kwargs.get('type', _type_hint),
            ) or {}

            # and now take whatever we got from the legacy store and update
            # it from what we have in the config
            cred_update = self._get_credential_from_cfg(name)
            if set(cred_update.keys()) >= set(
                    k for k in cred.keys() if k != '_from_backend'):
                # we are overwriting all info from a possible legacy credential
                # take marker off
                cred.pop('_from_backend', None)
            cred.update(cred_update)

        # we merge existing credential properties with overrides.
        # we are only adding 'enter-please' markers (i.e. None) for properties
        # that have no known value yet
        cred.update(**{k: v for k, v in kwargs.items()
                       if v is not None or k not in cred})

        # final word on the credential type, if there is any type info at all
        # `cred` will have a `type` property after this
        self._assign_credential_type(cred, _type_hint)

        # determine what is missing and possibly prompt for it
        self._complete_credential_props(name, cred, _prompt)

        if not cred.get('secret'):
            # no secret, no credential
            if any(not p.startswith('_') for p in cred):
                lgr.debug(
                    'Not reporting on credential fragment '
                    '(name=%r) with no secret: %r',
                    name, cred,
                )
            return

        return cred

    def set(self,
            name,
            *,
            _lastused=False,
            _suggested_name=None,
            _context=None,
            **kwargs):
        """Set credential properties and secret

        Presently, all supported backends require the specification of
        a credential ``name`` for storage. This may change in the future,
        when support for alternative backends is added, at which point
        the ``name`` parameter would become optional.

        All properties provided as `kwargs` with keys not starting with `_` and
        with values that are not ``None`` will be stored. If ``kwargs`` do not
        contain a ``secret`` specification, manual entry will be attempted. The
        associated prompt with be either the name of the ``secret`` field of a
        known credential (as identified via a ``type`` property), or the label
        ``'secret'``.

        All properties with an associated value of ``None`` will be removed
        (unset).

        Parameters
        ----------
        name: str or None
          Credential name. If None, the name will be prompted for and setting
          the credential is skipped if no name is provided.
        _lastused: bool, optional
          If set, automatically add an additional credential property
          ``'last-used'`` with the current timestamp in ISO 8601 format.
        _suggested_name: str, optional
          If `name` is None, this name (if given) is presented as a default
          suggestion that can be accepted without having to enter it manually.
          If this name suggestion conflicts with an existing credential, it
          is ignored and not presented as a suggestion.
        _context: str, optional
          If given, will be included in the prompt for a missing credential
          name to provide context for a user. It should be  written to fit
          into a parenthical statement after "Enter a name to save the
          credential (...)", e.g. "for download from <URL>".
        **kwargs:
          Any number of credential property key/value pairs to set (update),
          or remove. With one exception, values of ``None`` indicate removal
          of a property from a credential. However, ``secret=None`` does not
          lead to the removal of a credential's secret, because it would
          result in an incomplete credential. Instead, it will cause a
          credential's effective ``secret`` property to be written to the
          secret store. The effective secret might come from other sources,
          such as particular configuration scopes or environment variables
          (i.e., matching the ``datalad.credential.<name>.secret``
          configuration item.
          Properties whose names start with an underscore are automatically
          removed prior storage.

        Returns
        -------
        dict or None
          key/values of all modified credential properties with respect
          to their previously recorded values. None is returned in case
          a user did not enter a missing credential name. If a user entered
          a credential name, it is included in the returned dictionary
          under the 'name' key.

        Raises
        ------
        RuntimeError
          This exception is raised whenever a property cannot be removed
          successfully. Likely cause is that it is defined in a configuration
          scope or backend for which write-access is not supported.
        ValueError
          When property names in kwargs are not syntax-compliant.
        """
        updated = {}

        if not name:
            known_credentials = self._get_known_credential_names()
            if _suggested_name in known_credentials:
                # ignore name suggestion, conflicts with existing credential
                _suggested_name = None
            prompt = 'Enter a name to save the credential'
            if _context:
                prompt = f'{prompt} ({_context})'
            prompt = f"{prompt} securely for future reuse, " \
                     "or 'skip' to not save the credential"
            if _suggested_name:
                prompt = f'{prompt}, or leave empty to accept the name ' \
                         f'{_suggested_name!r}'
            while not name:
                entered = self._ask_property('name', prompt=prompt)
                if entered == 'skip':
                    return
                elif entered is None:
                    # the user was no able to enter a value (non-interactive
                    # session). we raise to not let this go unnoticed.
                    raise ValueError('no credential name provided for setting')
                elif not entered:
                    if not _suggested_name:
                        ui.message('Cannot proceed without a credential name')
                        continue
                    # otherwise take the default
                    entered = _suggested_name
                if entered in known_credentials:
                    ui.message(
                        f'A credential with the name {entered!r} already '
                        'exists, please provide a different name.')
                else:
                    name = entered
            updated['name'] = name

        # we strip internal properties, such as '_edited' automatically
        # forcing each caller to to this by hand is kinda pointless, if
        # they can never be stored anyway, and e.g. a previous `get()`
        # would include one for any credentials that was manually entered
        kwargs = self._strip_internal_properties(kwargs)
        # check syntax for the rest
        verify_property_names(kwargs)
        # retrieve the previous credential state, so we can merge with the
        # incremental changes, and report on effective updates
        prev_cred = self.get(
            name=name,
            # we never want any manual interaction at this point
            _prompt=None,
            # if we know the type, hence we can do a query for legacy secrets
            # and properties. This will migrate them to the new setup
            # over time
            _type_hint=kwargs.get('type'),
        )
        # merge incoming with existing properties to create an updated
        # credential
        if prev_cred:
            prev_cred = self._strip_internal_properties(prev_cred)
            cred = dict(prev_cred, **kwargs)
        else:
            cred = dict(kwargs)
        # update last-used, if requested
        if _lastused:
            cred['last-used'] = datetime.now().isoformat()

        # remove props
        #
        remove_props = [
            k for k, v in cred.items()
            # can we really know that no 'secret' field was deposited
            # in the config backend? MIH does not think so. However,
            # secret=None has special semantics (update secret store
            # from config), hence we cannot use it to perform removal
            # of secrets from config here.
            # MIH did not find a rational for this setup. It was already
            # part of the original implementation. At least this is
            # documented in the `kwargs` docstring now.
            if v is None and k != 'secret']
        self._unset_credprops_anyscope(name, remove_props)
        updated.update(**{k: None for k in remove_props})

        # set non-secret props
        #
        set_props = {
            k: v for k, v in cred.items()
            if v is not None and k != 'secret'
        }
        for k, v in set_props.items():
            var = _get_cred_cfg_var(name, k)
            if self._cfg.get(var) == v:
                # desired value already exists, we are not
                # storing again to preserve the scope it
                # was defined in
                continue
            # we always write to the global scope (ie. user config)
            # credentials are typically a personal, not a repository
            # specific entity -- likewise secrets go into a personal
            # not repository-specific store
            # for custom needs users can directly set the respective
            # config
            self._cfg.set(var, v, scope='global', force=True, reload=False)
            updated[k] = v
        if set_props:
            self._cfg.reload()

        # set secret
        #
        # we aim to update the secret in the store, hence we must
        # query for a previous setting in order to reliably report
        # updates
        prev_secret = self._get_secret(prev_cred) if prev_cred else None
        if 'secret' not in cred:
            # we have no removal directive, reuse previous secret
            cred['secret'] = prev_secret
        if cred.get('secret') is None:
            # we want to reset the secret, consider active config
            # (which was already queried when retrieving the previous
            # credential above)
            cred['secret'] = prev_secret

        if cred.get('secret') is None:
            # we have no secret specified or in the store already: ask
            # typically we would end up here with an explicit attempt
            # to set a credential in a context that is known to an
            # interactive user, hence the messaging here can be simple
            cred['secret'] = self._ask_secret(
                CredentialManager.secret_names.get(cred.get('type'), 'secret'))
        # at this point we will have a secret. it could be from ENV
        # or provided, or entered. we always want to put it in the
        # store
        # we never ever write a secret to any other field-name than
        # 'secret'
        if cred['secret'] != self._keyring.get(name, 'secret'):
            # only report updated if actually different from before.
            # and "before" is what was in the secret store, because
            # we will write to it next. A secret could have been
            # provided via an ENV var, hence even with no change from
            # `prev_cred` there could be a change in the secret store
            updated['secret'] = cred['secret']
        # TODO make sure that there actually is a secret that is written
        # and not None
        self._keyring.set(name, 'secret', cred['secret'])
        return updated

    def remove(self, name, *, type_hint=None):
        """Remove a credential, including all properties and secret

        Presently, all supported backends require the specification of
        a credential ``name`` for lookup. This may change in the future,
        when support for alternative backends is added, at which point
        the ``name`` parameter would become optional, and additional
        parameters would be added.

        Returns
        -------
        bool
          True if a credential was removed, and False if not (because
          no respective credential was found).

        Raises
        ------
        RuntimeError
          This exception is raised whenever a property cannot be removed
          successfully. Likely cause is that it is defined in a configuration
          scope or backend for which write-access is not supported.
        """
        # prefix for all config variables of this credential
        prefix = _get_cred_cfg_var(name, '')

        to_remove = [
            k[len(prefix):] for k in self._cfg.keys()
            if k.startswith(prefix)
        ]
        removed = False
        if to_remove:
            self._unset_credprops_anyscope(name, to_remove)
            removed = True

        # delete the secret from the keystore, if there is any
        def del_field(name, field):
            global removed
            try:
                self._keyring.delete(name, field)
                removed = True
            except Exception as e:
                if self._keyring.get(name, field) is None:
                    # whatever it was, the target is reached
                    CapturedException(e)
                else:
                    # we could not delete the field
                    raise  # pragma: nocover

        del_field(name, 'secret')
        if type_hint:
            # remove legacy records too
            for field in self._cred_types.get(
                    type_hint, {}).get('fields', []):
                del_field(name, field)
        return removed

    def query_(self, **kwargs):
        """Query for all (matching) credentials.

        Credentials are yielded in no particular order.

        This method cannot find credentials for which only a secret
        was deposited in the keyring.

        This method does support lookup of credentials defined in DataLad's
        "provider" configurations.

        Parameters
        ----------
        **kwargs
          If not given, any found credential is yielded. Otherwise,
          any credential must match all property name/value
          pairs

        Yields
        ------
        tuple(str, dict)
          The first element in the tuple is the credential name, the second
          element is the credential record as returned by ``get()`` for any
          matching credential.
        """
        done = set()
        known_credentials = set((n, None) for n in self._get_known_credential_names())
        from itertools import chain
        for name, legacy_type_hint in chain(
                _yield_legacy_credential_types(),
                known_credentials):
            done.add(name)
            cred = self.get(name, _prompt=None, _type_hint=legacy_type_hint)
            if not cred and legacy_type_hint:
                # this legacy-type credential is not set. We still want to
                # report on it, because it is the only way for users that
                # discover these predefined credential "slots"
                cred = dict(type=legacy_type_hint)
            if legacy_type_hint is not None:
                # leading underscore to distinguish this internal marker from
                # an actual credential property.
                # the credentials command will then also treat it as such
                cred['_from_backend'] = 'legacy'
            if not cred:
                # no info on such a credential, not even legacy info
                # ignore
                continue
            if not kwargs:
                yield (name, cred)
            else:
                if all(cred.get(k) == v for k, v in kwargs.items()):
                    yield (name, cred)
                else:
                    continue

    def query(self, *, _sortby=None, _reverse=True, **kwargs):
        """Query for all (matching) credentials, sorted by a property

        This method is a companion of ``query_()``, and the same limitations
        regarding credential discovery apply.

        In contrast to ``query_()``, this method return a list instead of
        yielding credentials one by one. This returned list is optionally
        sorted.

        Parameters
        ----------
        _sortby: str, optional
          Name of a credential property to provide a value to sort by.
          Credentials that do not carry the specified property always
          sort last, regardless of sort order.
        _reverse: bool, optional
          Flag whether to sort ascending or descending when sorting.
          By default credentials are return in descending property
          value order. This flag does not impact the fact that credentials
          without the property to sort by always sort last.
        **kwargs
          Pass on as-is to ``query_()``

        Returns
        -------
        list(str, dict)
          Each item is a 2-tuple. The first element in each tuple is the
          credential name, the second element is the credential record
          as returned by ``get()`` for any matching credential.
        """
        matches = self.query_(**kwargs)
        if _sortby is None:
            return list(matches)

        # this makes sure that any credential that does not have the
        # sort-by property name sorts to the very end of the list
        # regardless of whether the sorting is ascending or descending
        def get_sort_key(x):
            # x is a tuple as returned by query_()
            prop_indicator = _sortby in x[1]
            if not _reverse:
                prop_indicator = not prop_indicator
            return (prop_indicator, x[1].get(_sortby))

        return sorted(matches, key=get_sort_key, reverse=_reverse)

    def obtain(self,
               name: str | None = None,
               *,
               prompt: str | None = None,
               type_hint: str | None = None,
               query_props: Dict | None = None,
               expected_props: List | Tuple | None = None):
        """Obtain a credential by query or prompt (if needed)

        This convenience method implements a standard workflow to obtain a
        credential.  It supports credential selection by credential
        name/identifier, and falls back onto querying for a credential matching
        a set of specified properties (as key-value mappings). If no suitable
        credential is known, a user is prompted to enter one interactively (if
        possible in the current session).

        If a credential was entered manually, any given ``type_hint`` will
        be included as a ``type`` property of the returned credential, and
        the returned credential has an ``_edited=True`` property.
        Likewise, any ``realm`` property included in the ``query_props``
        is included in the returned credential in this case.

        If desired, a credential workflow can be completed, after a credential
        was found to be valid/working, by storing or updating it in the
        credential store::

            cm = CredentialManager()
            cname, cprops = cm.obtain(...)
            # verify credential is working
            ...
            # set/update
            cm.set(cname, _lastused=True, **cprops)

        In the code sketch above, if ``cname`` is ``None`` (as it will be for
        a newly entered credential, ``set()`` will prompt for a name to store
        the credential under, and will offer a user the choice to skip storing
        a credential. For any previously known credential, the ``last-used``
        property will be updated to enable preferred selection in future
        credential discovery attempts via ``obtain()``.

        Examples
        --------

        Minimal call to get a credential entered (manually)::

          credman.obtain(type_hint='token', prompt='Credential please!')

        Without a prompt text no interaction is attempted, and without a type
        hint it is unknown what (and how much) to enter.

        Minimal call to retrieve a credential by its identifier::

          credman.obtain('my-github-token')

        Minimal call to retrieve the last-used credential for a particular
        authentication "realm". In this case "realm" is a property that was
        previously set to match a particular service/location, and is now used
        to match credentials against::

          credman.obtain(query_props={'realm': 'mysecretlair'})

        Parameters
        ----------
        name: str, optional
          Name of the credential to be retrieved
        prompt: str, optional
          Passed to ``CredentialManager.get()`` if a credential name was
          provided, or no suitable credential could be found by querying.
        type_hint: str, optional
          In case no ``type`` property is included in ``query_props``,
          this parameter is passed to ``CredentialManager.get()``.
        query_props: dict, optional
          Credential property to be used for querying for a suitable
          credential. When multiple credentials match a query, the last-used
          credential is selected.
        expected_props: list or tuple, optional
          When specified, a credential will be inspected to contain properties
          matching all listed property names, or a ``ValueError`` will be
          raised.

        Returns
        -------
        (str, dict)
          Credential name (possibly different from the input, when a credential
          was discovered based on properties), and credential properties.

        Raises
        ------
        ValueError
          Raised when no matching credential could be found and none was
          entered. Also raised, when a credential selected from a query result
          or a manually entered one is missing any of the properties with a
          name given in ``expected_props``.
        """
        cred = None
        if not name:
            if query_props:
                creds = self.query(_sortby='last-used', **(query_props or {}))
                if creds:
                    name, cred = creds[0]

        if not cred:
            kwargs = dict(
                # name could be none
                name=name,
                _prompt=prompt,
                _type_hint=type_hint,
            )
            try:
                cred = self.get(**kwargs)
                # check if we know the realm, if so include in the credential,
                realm = (query_props or {}).get('realm')
                if realm:
                    cred['realm'] = realm
                if name is None and type_hint:
                    # we are not expecting to retrieve a particular credential.
                    # make the type hint the actual type of the credential
                    cred['type'] = type_hint
            except Exception as e:
                lgr.debug('Obtaining credential failed: %s', e)

        if not cred:
            raise ValueError('No suitable credential found or specified')
        missing_props = [
            ep for ep in (expected_props or []) if ep not in cred
        ]
        if any(missing_props):
            raise ValueError(
                'No suitable credential or specified '
                f'(missing properties: {missing_props})')
        return name, cred

    # internal helpers
    #
    def _strip_internal_properties(self, cred: Dict) -> Dict:
        return {k: v for k, v in cred.items() if not k.startswith('_')}

    def _assign_credential_type(self, cred, _type_hint=None):
        """Set 'type' property (in-place)"""
        _type_hint = cred.get('type', _type_hint)
        if _type_hint:
            cred['type'] = _type_hint
            return

        # if we get here, we don't know what type this is
        # let's derive one for a few clear-cut cases where we can be
        # reasonable sure what type a credential is
        if set(cred) == set(('token',)):
            # all we have is a token property -- very likely a token-type
            # credential. Move the token to the secret property and
            # assign the type
            cred['type'] = 'token'
            cred['secret'] = cred.pop('token')

    def _complete_credential_props(
            self, name: str,
            cred: Dict,
            prompt: str | None,
    ) -> None:
        """Determine missing credential properties, and fill them in

        What properties are missing is determined based on credential
        type info, and their values will be prompted for (if a prompt
        was provided).

        The given credential is modified in place.
        """
        cred_type = cred.get('type')
        # import the definition of expected fields from the known
        # credential types
        cred_type_def = self._cred_types.get(
            cred_type,
            dict(fields=[], secret=None))
        required_fields = cred_type_def['fields'] or []
        secret_field = cred_type_def['secret']
        # mark required fields for this credential type
        for k in required_fields:
            if k == secret_field:
                # do nothing, if this is the secret key
                continue
            if k in cred:
                # do nothing if we have an incoming value for this key already
                continue
            # otherwise make sure we prompt for the essential
            # fields
            cred[k] = None

        # - prompt for required but missing prompts
        # - retrieve a secret
        prompted = False
        entered = {}
        for k, v in cred.items():
            if k in ('secret', secret_field):
                # a secret is either held in a 'secret' field, or in a dedicated field
                # defined by the cred_type_def. Both are handled below
                # handled below
                continue
            if prompt and v is None:
                # prevent double-prompting for entering a series of properties
                # of the same credential
                v = self._ask_property(k, None if prompted else prompt)
                if v is not None:
                    prompted = True
            if v:
                entered[k] = v

        # bulk merged, cannot do in-loop above, because we iterate over items()
        cred.update(entered)

        # extract the secret, from the assembled properties or a secret store
        secret = self._get_secret(cred, name=name, secret_field=secret_field)
        if prompt and secret is None:
            secret = self._ask_secret(
                type_hint=secret_field,
                prompt=None if prompted else prompt,
            )
            if secret:
                prompted = True

        if secret:
            cred['secret'] = secret

        # report whether there were any edits to the credential record
        # (incl. being entirely new), such that consumers can decide
        # to save a credentials, once battle-tested
        if prompted:
            cred['_edited'] = True

    def _get_credential_from_cfg(self, name: str) -> Dict:
        var_prefix = _get_cred_cfg_var(name, '')
        return {
            k[len(var_prefix):]: v
            for k, v in self._cfg.items()
            if k.startswith(var_prefix)
        }

    def _get_known_credential_names(self) -> set:
        known_credentials = set(
            '.'.join(k.split('.')[2:-1]) for k in self._cfg.keys()
            if k.startswith('datalad.credential.')
        )
        return known_credentials

    def _ask_property(self, name, prompt=None):
        if not ui.is_interactive:
            lgr.debug('Cannot ask for credential property %r in non-interactive session', name)
            return

        return ui.question(name, title=prompt)

    def _ask_secret(self, type_hint=None, prompt=None):
        if not ui.is_interactive:
            lgr.debug('Cannot ask for credential secret in non-interactive session')
            return

        return ui.question(
            type_hint or 'secret',
            title=prompt,
            repeat=self._cfg.obtain(
                'datalad.credentials.repeat-secret-entry'),
            hidden=self._cfg.obtain(
                'datalad.credentials.hidden-secret-entry'),
        )

    def _unset_credprops_anyscope(self, name, keys):
        """Reloads the config after unsetting all relevant variables

        This method does not modify the keystore.
        """
        nonremoved_vars = []
        for k in keys:
            var = _get_cred_cfg_var(name, k)
            if var not in self._cfg:
                continue
            try:
                self._cfg.unset(var, scope='global', reload=False)
            except CommandError as e:
                CapturedException(e)
                try:
                    self._cfg.unset(var, scope='local', reload=False)
                except CommandError as e:
                    CapturedException(e)
                    nonremoved_vars.append(var)
        if nonremoved_vars:
            raise RuntimeError(
                f"Cannot remove configuration items {nonremoved_vars} "
                f"for credential, defined outside global or local "
                "configuration scope. Remove manually")
        self._cfg.reload()

    def _get_legacy_credential_from_keyring(
            self,
            name: str,
            type_hint: str | None,
    ) -> Dict | None:
        """With a ``type_hint`` given or determined from a known legacy
        credential, attempts to retrieve a credential
        comprised of all fields defined in ``self._cred_types``. Otherwise
        ``None`` is returned.
        """
        if not type_hint:
            # no type hint given in any form. Last chance is that
            # this is a known legacy credential.
            # doing this query is a bit expensive, but getting a
            # credential is not a high-performance procedure, and
            # the gain in convenience is substantial -- otherwise
            # users would need to somehow know what they should be
            # looking for
            type_hint = dict(_yield_legacy_credential_types()).get(name)

        if not type_hint or type_hint not in self._cred_types:
            return

        cred = {}
        lc = self._cred_types[type_hint]
        for field in (lc['fields'] or []):
            if field == lc['secret']:
                continue
            val = self._keyring.get(name, field)
            if val:
                # legacy credentials used property names with underscores,
                # but this is no longer syntax-compliant -- fix on read
                cred[field.replace('_', '-')] = val

        if not cred:
            # there is nothing on a legacy credential with this name
            return None
        else:
            # at least some info came from the legacy backend, record that
            cred['_from_backend'] = 'legacy'
            cred['type'] = type_hint
            return cred

    def _get_secret(
            self,
            cred: Dict,
            name: str | None = None,
            secret_field: str | None = None,
    ) -> str | None:
        """Report a secret

        Either directly from the set of credential properties, or from a
        secret store.
        """
        # from literal 'secret' property
        secret = cred.get('secret')
        if secret:
            return secret
        # from secret store under 'secret' label
        if name:
            secret = self._keyring.get(name, 'secret')
            if secret:
                return secret
        # `secret_field` property
        if secret_field:
            secret = cred.get(secret_field)
            if secret:
                return secret
        # from secret store under `secret_field` label
        if name and secret_field:
            secret = self._keyring.get(name, secret_field)
            if secret:
                return secret

        # no secret found anywhere
        return

    @property
    def _cfg(self):
        """Return a ConfigManager given to __init__() or the global datalad.cfg
        """
        if self.__cfg:
            return self.__cfg
        return datalad.cfg

    @property
    def _keyring(self):
        """Returns the DataLad keyring wrapper

        This internal property may vanish whenever changes to the supported
        backends are made.
        """
        if self.__keyring:
            return self.__keyring
        from datalad.support.keyring_ import keyring
        self.__keyring = keyring
        return keyring

    @property
    def _cred_types(self):
        """Internal property for mapping of credential type names to fields.

        Returns
        -------
        dict
          Legacy credential type name ('token', 'user_password', etc.) as keys,
          and dictionaries as values. Each of these dicts has two keys:
          'fields' (the complete list of "fields" that the credential
          comprises), and 'secret' (the name of the field that represents the
          secret. If there is no secret, the value associated with that key is
          ``None``.
        """
        # at present the credential type specifications are built from the
        # legacy credential types, but this may change at any point in the
        # future
        # here is what that was in Mar 2022
        # 'user_password': {'fields': ['user', 'password'],
        #                   'secret': 'password'},
        # 'token':  {'fields': ['token'], 'secret': 'token'},
        # 'git':    {'fields': ['user', 'password'], 'secret': 'password'}
        # 'aws-s3': {'fields': ['key_id', 'secret_id', 'session', 'expiration'],
        #            'secret': 'secret_id'},
        # 'nda-s3': {'fields': None, 'secret': None},
        # 'loris-token': {'fields': None, 'secret': None},

        if self.__cred_types:
            return self.__cred_types

        from datalad.downloaders import CREDENTIAL_TYPES
        mapping = {}
        for cname, ctype in CREDENTIAL_TYPES.items():
            secret_fields = [
                f for f in (ctype._FIELDS or {})
                if ctype._FIELDS[f].get('hidden')
            ]
            mapping[cname] = dict(
                fields=list(ctype._FIELDS.keys()) if ctype._FIELDS else None,
                secret=secret_fields[0] if secret_fields else None,
            )
        # an implementation-independent s3-style credential (with the aim to
        # also work for MinIO and Ceph)
        mapping['s3'] = dict(
            # use boto-style names, but strip "aws" prefix, and redundant
            # non-distinguishing 'key' and 'access' terms
            fields=['key', 'secret'],
            secret='secret',
        )
        self.__cred_types = mapping
        return mapping


def _yield_legacy_credential_types():
    # query is constrained by non-secrets, no constraints means report all
    # a constraint means *exact* match on all given properties
    from datalad.downloaders.providers import (
        Providers,
        CREDENTIAL_TYPES,
    )
    type_hints = {v: k for k, v in CREDENTIAL_TYPES.items()}

    # ATTN: from Providers.from_config_files() is sensitive to the PWD
    # it will only read legacy credentials from datasets whenever
    # PWD is inside a dataset
    legacy_credentials = set(
        (p.credential.name, type(p.credential))
        # without reload, no changes in files since the last call
        # will be considered. That last call might have happened
        # in datalad-core, and may have been in another directory
        for p in Providers.from_config_files(reload=True)
        if p.credential
    )
    for name, type_ in legacy_credentials:
        yield (name, type_hints.get(type_))


def verify_property_names(names):
    """Check credential property names for syntax-compliance.

    Parameters
    ----------
    names: iterable

    Raises
    ------
    ValueError
      When any non-compliant property names were found
    """
    invalid_names = [
        k for k in names
        if not CredentialManager.valid_property_names_regex.match(k)
    ]
    if invalid_names:
        raise ValueError(
            f'Unsupported property names {invalid_names}, '
            'must match git-config variable syntax (a-z0-9 and - characters)')


def _get_cred_cfg_var(name, prop):
    """Return a config variable name for a credential property

    Parameters
    ----------
    name : str
      Credential name
    prop : str
      Property name

    Returns
    -------
    str
    """
    return f'datalad.credential.{name}.{prop}'
