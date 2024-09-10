"""Uniform pre-execution parameter validation for commands

With this patch commands can now opt-in to receive fully validated parameters.
This can substantially simplify the implementation complexity of a command at
the expense of a more elaborate specification of the structural and semantic
properties of the parameters.

For details on implementing validation for individual commands see
:class:`datalad_next.commands.ValidatedInterface`.
"""

from __future__ import annotations

import logging
from functools import (
    partial,
    wraps,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generator,
)

from datalad import cfg as dlcfg
from datalad.interface.common_opts import eval_params

if TYPE_CHECKING:
    from datalad.interface.utils import anInterface

    from datalad_next.commands import ResultHandler

from datalad.utils import get_wrapped_class

from datalad_next.commands.legacy_result_handler import LegacyResultHandler
from datalad_next.exceptions import IncompleteResultsError
from datalad_next.patches import apply_patch
from datalad_next.utils import getargspec

# use same logger as -core
lgr = logging.getLogger('datalad.interface.utils')


# This function interface is taken from
# datalad-core@982cca549ae29a1c86a0d6736bc3d6dfec370433
def eval_results(wrapped):
    """Decorator for return value evaluation of datalad commands.

    Note, this decorator is only compatible with commands that return
    status dict sequences!

    Two basic modes of operation are supported: 1) "generator mode" that
    `yields` individual results, and 2) "list mode" that returns a sequence of
    results. The behavior can be selected via the kwarg `return_type`.
    Default is "list mode".

    This decorator implements common functionality for result rendering/output,
    error detection/handling, and logging.

    Result rendering/output configured via the `result_renderer` keyword
    argument of each decorated command. Supported modes are: 'generic' (a
    generic renderer producing one line per result with key info like action,
    status, path, and an optional message); 'json' (a complete JSON line
    serialization of the full result record), 'json_pp' (like 'json', but
    pretty-printed spanning multiple lines), 'tailored' custom output
    formatting provided by each command class (if any), or 'disabled' for
    no result rendering.

    Error detection works by inspecting the `status` item of all result
    dictionaries. Any occurrence of a status other than 'ok' or 'notneeded'
    will cause an IncompleteResultsError exception to be raised that carries
    the failed actions' status dictionaries in its `failed` attribute.

    Status messages will be logged automatically, by default the following
    association of result status and log channel will be used: 'ok' (debug),
    'notneeded' (debug), 'impossible' (warning), 'error' (error).  Logger
    instances included in the results are used to capture the origin of a
    status report.

    Parameters
    ----------
    func: function
      __call__ method of a subclass of Interface,
      i.e. a datalad command definition
    """

    @wraps(wrapped)
    def eval_func(*args, **kwargs):
        lgr.log(2, 'Entered eval_func for %s', wrapped)
        # determine the command class associated with `wrapped`
        wrapped_class = get_wrapped_class(wrapped)

        # retrieve common options from kwargs, and fall back on the command
        # class attributes, or general defaults if needed
        kwargs = get_eval_kwargs(wrapped_class, **kwargs)
        allkwargs = validate_parameters(
            interface=wrapped_class,
            cmd=wrapped,
            cmd_args=args,
            cmd_kwargs=kwargs,
        )

        # go with a custom result handler if instructed
        result_handler_cls = kwargs.pop('result_handler_cls', None)
        if result_handler_cls is None:
            # use default
            result_handler_cls = LegacyResultHandler

        result_handler = result_handler_cls(
            wrapped_class,
            cmd_kwargs=allkwargs,
        )

        return result_handler.return_results(
            # we wrap the result generating function into
            # a partial to get an argumentless callable
            # that provides an iterable. partial is a misnomer
            # here, all necessary parameters are given
            partial(
                _execute_command_,
                cmd=wrapped,
                allkwargs=allkwargs,
                result_handler=result_handler,
            ),
        )

    ret = eval_func
    ret._eval_results = True  # noqa: SLF001
    return ret


def get_eval_kwargs(cls: anInterface, **kwargs) -> dict:
    # retrieve common options from kwargs, and fall back on the command
    # class attributes, or general defaults if needed
    eval_kwargs = {
        p_name: kwargs.get(
            # go with any explicitly given default
            p_name,
            # otherwise determine the command class and pull any
            # default set in that class
            getattr(cls, p_name),
        )
        for p_name in eval_params
    }
    return dict(kwargs, **eval_kwargs)


# this is a replacement for datalad.interface.base.get_allargs_as_kwargs
# it reports which arguments were at their respective defaults
def get_allargs_as_kwargs(call, args, kwargs):
    """Generate a kwargs dict from a call signature and ``*args``, ``**kwargs``

    Basically resolving the argnames for all positional arguments, and
    resolving the defaults for all kwargs that are not given in a kwargs
    dict

    Returns
    -------
    (dict, set, set)
      The first return value is a mapping of argument names to their respective
      values.
      The second return value in the tuple is a set of argument names for
      which the effective value is identical to the default declared in the
      signature of the callable.
      The third value is a set with names of all mandatory arguments, whether
      or not they are included in the returned mapping.
    """
    argspec = getargspec(call, include_kwonlyargs=True)
    defaults = argspec.defaults
    nargs = len(argspec.args)
    defaults = defaults or []  # ensure it is a list and not None
    assert nargs >= len(defaults)  # noqa: S101
    # map any args to their name
    argmap = list(zip(argspec.args[: len(args)], args))
    kwargs_ = dict(argmap)
    # map defaults of kwargs to their names (update below)
    default_map = dict(zip(argspec.args[-len(defaults) :], defaults))
    for k, v in default_map.items():
        if k not in kwargs_:
            kwargs_[k] = v
    # update with provided kwarg args
    kwargs_.update(kwargs)
    # determine which arguments still have values identical to their declared
    # defaults
    at_default = {
        k for k in kwargs_ if k in default_map and default_map[k] == kwargs_[k]
    }
    # XXX: we cannot assert the following, because our own highlevel
    # API commands support more kwargs than what is discoverable
    # from their signature...
    # assert (nargs == len(kwargs_))
    return (
        # argument name/value mapping
        kwargs_,
        # names of arguments that are at their default
        at_default,
        # names of mandatory arguments (set for uniformity)
        set(argspec.args),
    )


# This function interface is taken from
# datalad-core@209bc319db8f34cceae4fee86493bf41927676fd
def _execute_command_(
    *,
    cmd: Callable[..., Generator[dict, None, None]],
    allkwargs: dict,
    # TODO: rather than the whole instance, pass one or more
    # method that do the thing(s) we need.
    # see `result_filter`, and `result_renderer` below
    result_handler: ResultHandler,
) -> Generator[dict, None, None]:
    """Internal helper to drive a command execution generator-style

    Parameters
    ----------
    interface:
      Interface class of associated with the `cmd` callable
    cmd:
      A DataLad command implementation. Typically the `__call__()` of
      the given `interface`.
    allkwargs:
      Keyword arguments for `cmd`.
    """
    # bookkeeping whether to raise an exception
    incomplete_results: list[dict] = []

    on_failure = allkwargs['on_failure']

    # process main results
    # execution, call with any arguments from the validated
    # set that are no result-handling related
    for res in cmd(**{k: v for k, v in allkwargs.items() if k not in eval_params}):
        if not res or 'action' not in res:
            # XXX: Yarik has to no clue on how to track the origin of the
            # record to figure out WTF, so he just skips it
            # but MIH thinks leaving a trace of that would be good
            lgr.debug('Drop result record without "action": %s', res)
            continue

        result_handler.log_result(res)
        # remove logger instance from results, as it is no longer useful
        # after logging was done, it isn't serializable, and generally
        # pollutes the output
        res.pop('logger', None)

        # output rendering
        result_handler.render_result(res)

        # error handling
        # looks for error status, and report at the end via
        # an exception
        if on_failure in ('continue', 'stop') and res['status'] in (
            'impossible',
            'error',
        ):
            incomplete_results.append(res)
            if on_failure == 'stop':
                # first fail -> that's it
                # raise will happen after the loop
                break

        yield from result_handler.run_result_hooks(res)

        if not result_handler.keep_result(res):
            continue

        yield from result_handler.transform_result(res)

    result_handler.render_result_summary()

    if incomplete_results:
        raise IncompleteResultsError(
            failed=incomplete_results, msg='Command did not complete successfully'
        )


def validate_parameters(
    interface: anInterface,
    cmd: Callable[..., Generator[dict, None, None]],
    cmd_args: tuple,
    cmd_kwargs: dict,
) -> dict[str, Any]:
    # for result filters and validation
    # we need to produce a dict with argname/argvalue pairs for all args
    # incl. defaults and args given as positionals
    allkwargs, at_default, required_args = get_allargs_as_kwargs(
        cmd,
        cmd_args,
        cmd_kwargs,
    )
    # validate the complete parameterization
    param_validator = (
        interface.get_parameter_validator()
        if hasattr(interface, 'get_parameter_validator')
        else None
    )
    if param_validator is None:
        lgr.debug(
            'Command parameter validation skipped. %s declares no validator', interface
        )
    else:
        lgr.debug('Command parameter validation for %s', interface)
        validator_kwargs = {
            'at_default': at_default,
            'required': required_args or None,
        }
        # make immediate vs exhaustive parameter validation
        # configurable
        raise_on_error = dlcfg.get('datalad.runtime.parameter-violation', None)
        if raise_on_error:
            validator_kwargs['on_error'] = raise_on_error

        allkwargs = param_validator(allkwargs, **validator_kwargs)
        lgr.debug('Command parameter validation ended for %s', interface)

    return allkwargs


# apply patch
patch_msg = 'Apply datalad-next patch to interface.(utils|base).py:eval_results'
apply_patch('datalad.interface.base', None, 'eval_results', eval_results, msg=patch_msg)
# we need to patch the datalad-next import location too
apply_patch('datalad_next.commands', None, 'eval_results', eval_results, msg=patch_msg)
