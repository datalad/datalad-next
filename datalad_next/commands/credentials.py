# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See LICENSE file distributed along with the datalad_osf package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Credential management and query"""

__docformat__ = 'restructuredtext'

import json
import logging
from typing import Dict

from datalad import (
    cfg as dlcfg,
)
from datalad_next.credman.manager import (
    CredentialManager,
    verify_property_names,
)
from datalad_next.commands import (
    EnsureCommandParameterization,
    ValidatedInterface,
    Parameter,
    ParameterConstraintContext,
    build_doc,
    eval_results,
    generic_result_renderer,
    get_status_dict,
)
from datalad_next.exceptions import CapturedException
from datalad_next.datasets import datasetmethod
from datalad_next.constraints import (
    EnsureChoice,
    EnsureNone,
    EnsureStr,
)
from datalad_next.constraints.dataset import EnsureDataset
from datalad_next.utils import ParamDictator


lgr = logging.getLogger('datalad.local.credentials')

credential_actions = ('query', 'get', 'set', 'remove')


class CredentialsParamValidator(EnsureCommandParameterization):
    def __init__(self):
        super().__init__(
            param_constraints=dict(
                action=EnsureChoice(*credential_actions),
                dataset=EnsureDataset(
                    # if given, it must also exist as a source for
                    # configuration items and/or credentials
                    installed=True,
                    purpose='manage credentials',
                ),
                name=EnsureStr(),
                prompt=EnsureStr(),
            ),
            # order in joint_constraints is relevant!
            joint_constraints={
                ParameterConstraintContext(('action', 'name', 'spec'),
                                           'CLI normalization'):
                self._normalize_cli_params,
                ParameterConstraintContext(('spec',),
                                           'credential spec normalization'):
                self._normalize_spec,
                # check parameter requirements for particular actions
                ParameterConstraintContext(('action', 'name'),
                                           'remove-action requirements'):
                self._check_remove_requirements,
                ParameterConstraintContext(('action', 'name', 'spec'),
                                           'get-action requirements'):
                self._check_get_requirements,
            },
        )

    def _normalize_cli_params(self, action, name, spec):
        if action in ('get', 'set', 'remove') and not name and spec \
                and isinstance(spec, list):
            # spec came in like from the CLI (but doesn't have to be from
            # there) and we have no name set
            if spec[0][0] != ':' and '=' not in spec[0]:
                name = spec[0]
                spec = spec[1:]
        return dict(action=action, name=name, spec=spec)

    def _normalize_spec(self, spec):
        # `spec` could be many things, make uniform dict
        return dict(spec=normalize_specs(spec))

    def _check_remove_requirements(self, action, name):
        if action == 'remove' and not name:
            self.raise_for(
                dict(action=action, name=name),
                'no credential name provided',
            )

    def _check_get_requirements(self, action, name, spec):
        if action == 'get' and not name and not spec:
            self.raise_for(
                dict(action=action, name=name, spec=spec),
                'no name or credential property specified',
            )


@build_doc
class Credentials(ValidatedInterface):
    """Credential management and query

    This command enables inspection and manipulation of credentials used
    throughout DataLad.

    The command provides four basic actions:


    QUERY

    When executed without any property specification, all known credentials
    with all their properties will be yielded. Please note that this may not
    include credentials that only comprise of a secret and no other properties,
    or legacy credentials for which no trace in the configuration can be found.
    Therefore, the query results are not guaranteed to contain all credentials
    ever configured by DataLad.

    When additional property/value pairs are specified, only credentials that
    have matching values for all given properties will be reported. This can be
    used, for example, to discover all suitable credentials for a specific
    "realm", if credentials were annotated with such information.


    SET

    This is the companion to 'get', and can be used to store properties and
    secret of a credential. Importantly, and in contrast to a 'get' operation,
    given properties with no values indicate a removal request. Any matching
    properties on record will be removed. If a credential is to be stored for
    which no secret is on record yet, an interactive session will prompt a user
    for a manual secret entry.

    Only changed properties will be contained in the result record.

    The appearance of the interactive secret entry can be configured with
    the two settings `datalad.credentials.repeat-secret-entry` and
    `datalad.credentials.hidden-secret-entry`.


    REMOVE

    This action will remove any secret and properties associated with a
    credential identified by its name.


    GET (plumbing operation)

    This is a *read-only* action that will never store (updates of) credential
    properties or secrets. Given properties will amend/overwrite those already
    on record.  When properties with no value are given, and also no value for
    the respective properties is on record yet, their value will be requested
    interactively, if a ``prompt||--prompt`` text was provided too. This can be
    used to ensure a complete credential record, comprising any number of
    properties.


    Details on credentials

    A credential comprises any number of properties, plus exactly one secret.
    There are no constraints on the format or property values or the secret,
    as long as they are encoded as a string.

    Credential properties are normally stored as configuration settings in a
    user's configuration ('global' scope) using the naming scheme:

      `datalad.credential.<name>.<property>`

    Therefore both credential name and credential property name must be
    syntax-compliant with Git configuration items. For property names this
    means only alphanumeric characters and dashes. For credential names
    virtually no naming restrictions exist (only null-byte and newline are
    forbidden). However, when naming credentials it is recommended to use
    simple names in order to enable convenient one-off credential overrides
    by specifying DataLad configuration items via their environment variable
    counterparts (see the documentation of the ``configuration`` command
    for details. In short, avoid underscores and special characters other than
    '.' and '-'.

    While there are no constraints on the number and nature of credential
    properties, a few particular properties are recognized on used for
    particular purposes:

    - 'secret': always refers to the single secret of a credential
    - 'type': identifies the type of a credential. With each standard type,
      a list of mandatory properties is associated (see below)
    - 'last-used': is an ISO 8601 format time stamp that indicated the
      last (successful) usage of a credential

    Standard credential types and properties

    The following standard credential types are recognized, and their
    mandatory field with their standard names will be automatically
    included in a 'get' report.

    - 'user_password': with properties 'user', and the password as secret
    - 'token': only comprising the token as secret
    - 'aws-s3': with properties 'key-id', 'session', 'expiration', and the
      secret_id as the credential secret

    Legacy support

    DataLad credentials not configured via this command may not be fully
    discoverable (i.e., including all their properties). Discovery of
    such legacy credentials can be assisted by specifying a dedicated
    'type' property.
    """
    result_renderer = 'tailored'

    _params_ = dict(
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc="""specify a dataset whose configuration to inspect
            rather than the global (user) settings"""),
        action=Parameter(
            args=("action",),
            nargs='?',
            doc="""which action to perform"""),
        name=Parameter(
            # exclude from CLI
            args=tuple(),
            doc="""name of a credential to set, get, or remove."""),
        spec=Parameter(
            args=("spec",),
            doc="""specification of[CMD: a credential name and CMD]
            credential properties. Properties are[CMD:  either CMD] given as
            name/value pairs[CMD:  or as a property name prefixed
            by a colon CMD].
            Properties [CMD: prefixed with a colon CMD][PY: with a `None` value PY] 
            indicate a property to be deleted (action 'set'), or a
            property to be entered interactively, when no value is set
            yet, and a prompt text is given (action 'get').
            All property names are case-insensitive, must start with
            a letter or a digit, and may only contain '-' apart from
            these characters.
            [PY: Property specifications should be given a as dictionary,
            e.g., spec={'type': 'user_password'}.
            However, a CLI-like list of string arguments is also
            supported, e.g., spec=['type=user_password'] PY]""",
            nargs='*',
            metavar='[name] [:]property[=value]'),
        prompt=Parameter(
            args=("--prompt",),
            doc="""message to display when entry of missing credential
            properties is required for action 'get'. This can be used
            to present information on the nature of a credential and
            for instructions on how to obtain a credential"""),
    )

    _examples_ = [
        dict(text="Report all discoverable credentials",
             code_py="credentials()",
             code_cmd="datalad credentials"),
        dict(
            text="Set a new credential mycred & input its secret interactively",
            code_py="credentials('set', name='mycred')",
            code_cmd="datalad credentials set mycred"),
        dict(text="Remove a credential's type property",
             code_py="credentials('set', name='mycred', spec={'type': None})",
             code_cmd="datalad credentials set mycred :type"),
        dict(text="Get all information on a specific credential in a structured record",
             code_py="credentials('get', name='mycred')",
             code_cmd="datalad -f json credentials get mycred"),
        dict(text="Upgrade a legacy credential by annotating it with a 'type' property",
             code_py="credentials('set', name='legacycred', spec={'type': 'user_password')",
             code_cmd="datalad credentials set legacycred type=user_password"),
        dict(text="Set a new credential of type user_password, with a given user property, "
                  "and input its secret interactively",
             code_py="credentials('set', name='mycred', spec={'type': 'user_password', 'user': '<username>'})",
             code_cmd="datalad credentials set mycred type=user_password user=<username>"),
        dict(text="Obtain a (possibly yet undefined) credential with a minimum set of "
                  "properties. All missing properties and secret will be "
                  "prompted for, no information will be stored! "
                  "This is mostly useful for ensuring availability of an "
                  "appropriate credential in an application context",
             code_py="credentials('get', prompt='Can I haz info plz?', name='newcred', spec={'newproperty': None})",
             code_cmd="datalad credentials --prompt 'can I haz info plz?' get newcred :newproperty"),
    ]

    _validator_ = CredentialsParamValidator()

    @staticmethod
    @datasetmethod(name='credentials')
    @eval_results
    def __call__(action='query', spec=None, *, name=None, prompt=None,
                 dataset=None):
        # which config manager to use: global or from dataset
        # It makes no sense to work with a non-existing dataset's config,
        # due to https://github.com/datalad/datalad/issues/7299
        # so the `dataset` validator must not run for the default value
        # ``None``
        cfg = dataset.ds.config if dataset else dlcfg

        credman = CredentialManager(cfg)

        if action == 'set':
            try:
                updated = credman.set(name, **spec)
            except Exception as e:
                yield get_status_dict(
                    action='credentials',
                    status='error',
                    name=name,
                    message='could not set credential properties',
                    exception=CapturedException(e),
                )
                return
            # pull name out of report, if entered manually
            if not name and updated is not None:
                name = updated.pop('name', None)
            yield get_status_dict(
                action='credentials',
                status='notneeded' if updated is None else 'ok',
                name=name,
                **_prefix_result_keys(updated if updated else spec),
            )
        elif action == 'get':
            cred = credman.get(name=name, _prompt=prompt, **spec)
            if not cred:
                yield get_status_dict(
                    action='credentials',
                    status='error',
                    name=name,
                    message='credential not found',
                )
            else:
                yield get_status_dict(
                    action='credentials',
                    status='ok',
                    name=name,
                    **_prefix_result_keys(cred),
                )
        elif action == 'remove':
            try:
                removed = credman.remove(name, type_hint=spec.get('type'))
            except Exception as e:
                yield get_status_dict(
                    action='credentials',
                    status='error',
                    name=name,
                    message='could not remove credential properties',
                    exception=CapturedException(e),
                )
                return
            yield get_status_dict(
                action='credentials',
                status='ok' if removed else 'notneeded',
                name=name,
            )
        elif action == 'query':
            for name, cred in credman.query_(**spec):
                yield get_status_dict(
                    action='credentials',
                    status='ok',
                    name=name,
                    type='credential',
                    **_prefix_result_keys(cred),
                )
        else:
            raise RuntimeError('Impossible state reached')  # pragma: no cover

    @staticmethod
    def custom_result_renderer(res, **kwargs):
        # we only handle our own stuff in a custom fashion, the rest is generic
        if res['action'] != 'credentials':
            generic_result_renderer(res)
            return
        # must make a copy, because we modify the record in-place
        # https://github.com/datalad/datalad/issues/6560
        res = res.copy()
        # the idea here is to twist the result records such that the generic
        # renderer can be used
        if 'name' in res:
            res['action'] = res['name']
        res['status'] = '{} {}'.format(
            res.get('cred_type', 'secret'),
            '✓' if res.get('cred_secret') else '✗',
        )

        if res.pop('from_backend', None) == 'legacy':
            res['type'] = 'legacy-credential'
        if 'message' not in res:
            # give the names of all properties
            # but avoid duplicating the type, hide the prefix,
            # add removal marker for vanished properties
            res['message'] = ','.join(
                '{}{}{}{}'.format(
                    f':{p[5:]}' if res[p] is None else p[5:],
                    '' if res[p] is None else '=',
                    '' if res[p] is None else res[p][:25],
                    '…' if res[p] and len(res[p]) > 25 else '',
                )
                for p in sorted(res)
                if p.startswith('cred_') and p not in ('cred_type', 'cred_secret'))
        generic_result_renderer(res)


def normalize_specs(specs):
    """Normalize all supported `spec` argument values for `credentials`

    Parameter
    ---------
    specs: JSON-formatted str or list

    Returns
    -------
    dict
        Keys are the names of any property (with removal markers stripped),
        and values are `None` whenever property removal is desired, and
        not `None` for any value to be stored.

    Raises
    ------
    ValueError
      For missing values, missing removal markers, and invalid JSON input
    """
    if not specs:
        return {}
    elif isinstance(specs, str):
        try:
            specs = json.loads(specs)
        except json.JSONDecodeError as e:
            raise ValueError('Invalid JSON input') from e
    if isinstance(specs, list):
        # convert property assignment list
        specs = [
            (str(s[0]), str(s[1]))
            if isinstance(s, tuple) else
            (str(s),)
            if '=' not in s else
            (tuple(s.split('=', 1)))
            for s in specs
        ]
    if isinstance(specs, list):
        missing = [
            i for i in specs
            if (len(i) == 1 and i[0][0] != ":") or (
                len(i) > 1 and (i[0][0] == ':' and i[1] is not None))
        ]
    else:
        missing = [
            k for k, v in specs.items()
            if k[0] == ":" and v is not None
        ]
    if missing:
        raise ValueError(
            f'Value or unset flag ":" missing for property {missing!r}')
    if isinstance(specs, list):
        # expand absent values in tuples to ease conversion to dict below
        specs = [(i[0], i[1] if len(i) > 1 else None) for i in specs]
    # apply "unset marker"
    specs = {
        # this stuff goes to git-config, is therefore case-insensitive
        # and we should normalize right away
        (k[1:] if k[0] == ':' else k).lower():
        None if k[0] == ':' else v
        for k, v in (specs.items() if isinstance(specs, dict) else specs)
    }
    verify_property_names(specs)
    return specs


def _prefix_result_keys(props):
    return {
        f'cred_{k}' if not k.startswith('_') else k[1:]: v
        for k, v in props.items()
    }
