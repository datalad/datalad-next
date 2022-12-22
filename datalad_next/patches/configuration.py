"""Enable ``configuration()`` to query ``global`` scope without a dataset

"""

__docformat__ = 'restructuredtext'


import logging

from datalad import cfg as dlcfg
from datalad.distribution.dataset import require_dataset
from datalad_next.commands import (
    build_doc,
    datasetmethod,
    eval_results,
    get_status_dict,
)
from datalad.interface.common_cfg import definitions as cfg_defs
from datalad.local import configuration as conf_mod
from datalad.local.configuration import (
    config_actions,
    _dump,
    _get,
    _set,
    _unset,
)
from datalad_next.exceptions import NoDatasetFound
from datalad_next.utils import ensure_list
from datalad_next.datasets import (
    Dataset,
)

lgr = logging.getLogger('datalad.local.configuration')


@build_doc
class Configuration(conf_mod.Configuration):
    """"""
    @staticmethod
    @datasetmethod(name='configuration')
    @eval_results
    def __call__(
            action='dump',
            spec=None,
            *,
            scope=None,
            dataset=None,
            recursive=False,
            recursion_limit=None):

        # check conditions
        # - global and recursion makes no sense

        if action == 'dump':
            if scope:
                raise ValueError(
                    'Scope selection is not supported for dumping')

        # normalize variable specificatons
        specs = []
        for s in ensure_list(spec):
            if isinstance(s, tuple):
                specs.append((str(s[0]), str(s[1])))
            elif '=' not in s:
                specs.append((str(s),))
            else:
                specs.append(tuple(s.split('=', 1)))

        if action == 'set':
            missing_values = [s[0] for s in specs if len(s) < 2]
            if missing_values:
                raise ValueError(
                    'Values must be provided for all configuration '
                    'settings. Missing: {}'.format(missing_values))
            invalid_names = [s[0] for s in specs if '.' not in s[0]]
            if invalid_names:
                raise ValueError(
                    'Name must contain a section (i.e. "section.name"). '
                    'Invalid: {}'.format(invalid_names))

        ds = None
        if scope != 'global' or recursive:
            try:
                ds = require_dataset(
                    dataset,
                    check_installed=True,
                    purpose='configure')
            except NoDatasetFound:
                if action not in ('dump', 'get') or dataset:
                    raise

        res_kwargs = dict(
            action='configuration',
            logger=lgr,
        )
        if ds:
            res_kwargs['refds'] = ds.path
        yield from configuration(action, scope, specs, res_kwargs, ds)

        if not recursive:
            return

        for subds in ds.subdatasets(
                state='present',
                recursive=True,
                recursion_limit=recursion_limit,
                on_failure='ignore',
                return_type='generator',
                result_renderer='disabled'):
            yield from configuration(
                action, scope, specs, res_kwargs, Dataset(subds['path']))


def configuration(action, scope, specs, res_kwargs, ds=None):
    # go with the more specific dataset configmanager, if we are
    # operating on a dataset
    cfg = dlcfg if ds is None else ds.config

    if action not in config_actions:
        raise ValueError("Unsupported action '{}'".format(action))

    if action == 'dump':
        if not specs:
            # dumping is querying for all known keys
            specs = [
                (n,) for n in sorted(
                    set(cfg_defs.keys()).union(cfg.keys()))
            ]
        scope = None

    for spec in specs:
        if '.' not in spec[0]:
            yield get_status_dict(
                ds=ds,
                status='error',
                message=(
                    "Configuration key without a section: '%s'",
                    spec[0],
                ),
                **res_kwargs)
            continue
        # TODO without get-all there is little sense in having add
        #if action == 'add':
        #    res = _add(cfg, scope, spec)
        if action == 'get':
            res = _get(cfg, scope, spec[0])
            # `None` is a value that cannot be set in the config.
            # if it is returned, it indicates that no value was set
            # we need to communicate that back, because a None value
            # cannot be reported as such by the CLI
            # (only via, e.g. JSON, encoding).
            # It makes sense to communicate that getting this specific
            # configuration item is "impossible" (because it is not set).
            # if a caller wants to tollerate this scenario, they can
            # set on_failure='ignore'
            if res.get('value') is None:
                res['status'] = 'impossible'
                res['message'] = (
                    'key %r not set in configuration%s',
                    res['name'],
                    f" scope '{scope}'" if scope else '',
                )
        elif action == 'dump':
            res = _dump(cfg, spec[0])
        # TODO this should be there, if we want to be comprehensive
        # however, we turned this off by default in the config manager
        # because we hardly use it, and the handling in ConfigManager
        # is not really well done.
        #elif action == 'get-all':
        #    res = _get_all(cfg, scope, spec)
        elif action == 'set':
            res = _set(cfg, scope, *spec)
        elif action == 'unset':
            res = _unset(cfg, scope, spec[0])

        if ds:
            res['path'] = ds.path

        if 'status' not in res:
            res['status'] = 'ok'

        yield dict(res_kwargs, **res)

    if action in ('add', 'set', 'unset'):
        # we perform a single reload, rather than one for each modification
        # TODO: can we detect a call from cmdline? We could skip the reload.
        cfg.reload(force=True)


conf_mod.Configuration.__call__ = Configuration.__call__
conf_mod.Configuration._params_['scope']._doc = """\
    scope for getting or setting
    configuration. If no scope is declared for a query, all
    configuration sources (including overrides via environment
    variables) are considered according to the normal
    rules of precedence. A 'get' action can be constrained to
    scope 'branch', otherwise 'global' is used when not operating
    on a dataset, or 'local' (including 'global', when operating
    on a dataset.
    For action 'dump', a scope selection is ignored and all available
    scopes are considered."""
conf_mod.Configuration.__call__.__doc__ = None
conf_mod.Configuration = build_doc(conf_mod.Configuration)
