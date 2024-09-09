"""Uniform pre-execution parameter validation for commands

With this patch commands can now opt-in to receive fully validated parameters.
This can substantially simplify the implementation complexity of a command at
the expense of a more elaborate specification of the structural and semantic
properties of the parameters.

For details on implementing validation for individual commands see
:class:`datalad_next.commands.ValidatedInterface`.
"""

from functools import wraps
import inspect
import logging
from typing import (
    Callable,
    Dict,
    Generator,
)

from datalad import cfg as dlcfg
from datalad.core.local.resulthooks import (
    get_jsonhooks_from_config,
    match_jsonhook2result,
    run_jsonhook,
)
from datalad.interface.common_opts import eval_params
from datalad.interface.results import known_result_xfms
from datalad.interface.utils import (
    anInterface,
    get_result_filter,
    keep_result,
    render_action_summary,
    xfm_result,
    _process_results,
)
from datalad_next.exceptions import IncompleteResultsError
from datalad.utils import get_wrapped_class
from . import apply_patch
from datalad_next.constraints import DatasetParameter

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
        lgr.log(2, "Entered eval_func for %s", wrapped)
        # determine the command class associated with `wrapped`
        wrapped_class = get_wrapped_class(wrapped)

        # retrieve common options from kwargs, and fall back on the command
        # class attributes, or general defaults if needed
        kwargs = kwargs.copy()  # we will pop, which might cause side-effect
        common_params = {
            p_name: kwargs.pop(
                # go with any explicitly given default
                p_name,
                # otherwise determine the command class and pull any
                # default set in that class
                getattr(wrapped_class, p_name))
            for p_name in eval_params}

        # short cuts and configured setup for common options
        return_type = common_params['return_type']

        if return_type == 'generator':
            # hand over the generator
            lgr.log(2,
                    "Returning generator_func from eval_func for %s",
                    wrapped_class)
            return _execute_command_(
                interface=wrapped_class,
                cmd=wrapped,
                cmd_args=args,
                cmd_kwargs=kwargs,
                exec_kwargs=common_params,
            )
        else:
            @wraps(_execute_command_)
            def return_func(*args_, **kwargs_):
                results = _execute_command_(
                    interface=wrapped_class,
                    cmd=wrapped,
                    cmd_args=args,
                    cmd_kwargs=kwargs,
                    exec_kwargs=common_params,
                )
                if inspect.isgenerator(results):
                    # unwind generator if there is one, this actually runs
                    # any processing
                    results = list(results)
                if return_type == 'item-or-list' and \
                        len(results) < 2:
                    return results[0] if results else None
                else:
                    return results

            lgr.log(2,
                    "Returning return_func from eval_func for %s",
                    wrapped_class)
            return return_func(*args, **kwargs)

    ret = eval_func
    ret._eval_results = True
    return ret


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
    from datalad_next.utils import getargspec
    argspec = getargspec(call, include_kwonlyargs=True)
    defaults = argspec.defaults
    nargs = len(argspec.args)
    defaults = defaults or []  # ensure it is a list and not None
    assert (nargs >= len(defaults))
    # map any args to their name
    argmap = list(zip(argspec.args[:len(args)], args))
    kwargs_ = dict(argmap)
    # map defaults of kwargs to their names (update below)
    default_map = dict(zip(argspec.args[-len(defaults):], defaults))
    for k, v in default_map.items():
        if k not in kwargs_:
            kwargs_[k] = v
    # update with provided kwarg args
    kwargs_.update(kwargs)
    # determine which arguments still have values identical to their declared
    # defaults
    at_default = set(
        k for k in kwargs_
        if k in default_map and default_map[k] == kwargs_[k]
    )
    # XXX we cannot assert the following, because our own highlevel
    # API commands support more kwargs than what is discoverable
    # from their signature...
    #assert (nargs == len(kwargs_))
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
    interface: anInterface,
    cmd: Callable[..., Generator[Dict, None, None]],
    cmd_args: tuple,
    cmd_kwargs: Dict,
    exec_kwargs: Dict,
) -> Generator[Dict, None, None]:
    """Internal helper to drive a command execution generator-style

    Parameters
    ----------
    interface:
      Interface class of associated with the `cmd` callable
    cmd:
      A DataLad command implementation. Typically the `__call__()` of
      the given `interface`.
    cmd_args:
      Positional arguments for `cmd`.
    cmd_kwargs:
      Keyword arguments for `cmd`.
    exec_kwargs:
      Keyword argument affecting the result handling.
      See `datalad.interface.common_opts.eval_params`.
    """
    # for result filters and validation
    # we need to produce a dict with argname/argvalue pairs for all args
    # incl. defaults and args given as positionals
    allkwargs, at_default, required_args = get_allargs_as_kwargs(
        cmd,
        cmd_args,
        {**cmd_kwargs, **exec_kwargs},
    )

    # validate the complete parameterization
    param_validator = interface.get_parameter_validator() \
        if hasattr(interface, 'get_parameter_validator') else None
    if param_validator is None:
        lgr.debug(
            'Command parameter validation skipped. %s declares no validator',
            interface)
    else:
        lgr.debug('Command parameter validation for %s', interface)
        validator_kwargs = dict(
            at_default=at_default,
            required=required_args or None,
        )
        # make immediate vs exhaustive parameter validation
        # configurable
        raise_on_error = dlcfg.get(
            'datalad.runtime.parameter-violation', None)
        if raise_on_error:
            validator_kwargs['on_error'] = raise_on_error

        allkwargs = param_validator(
            allkwargs,
            **validator_kwargs
        )
        lgr.debug('Command parameter validation ended for %s', interface)

    # look for potential override of logging behavior
    result_log_level = dlcfg.get('datalad.log.result-level', 'debug')
    # resolve string labels for transformers too
    result_xfm = known_result_xfms.get(
        allkwargs['result_xfm'],
        # use verbatim, if not a known label
        allkwargs['result_xfm'])
    result_filter = get_result_filter(allkwargs['result_filter'])
    result_renderer = allkwargs['result_renderer']
    if result_renderer == 'tailored' and not hasattr(interface,
                                                     'custom_result_renderer'):
        # a tailored result renderer is requested, but the class
        # does not provide any, fall back to the generic one
        result_renderer = 'generic'
    if result_renderer == 'default':
        # standardize on the new name 'generic' to avoid more complex
        # checking below
        result_renderer = 'generic'

    # figure out which hooks are relevant for this command execution
    # query cfg for defaults
    # .is_installed and .config can be costly, so ensure we do
    # it only once. See https://github.com/datalad/datalad/issues/3575
    dataset_arg = allkwargs.get('dataset', None)
    ds = None
    if dataset_arg is not None:
        from datalad_next.datasets import Dataset
        if isinstance(dataset_arg, Dataset):
            ds = dataset_arg
        elif isinstance(dataset_arg, DatasetParameter):
            ds = dataset_arg.ds
        else:
            try:
                ds = Dataset(dataset_arg)
            except ValueError:
                pass
    # look for hooks
    hooks = get_jsonhooks_from_config(ds.config if ds else dlcfg)
    # end of hooks discovery

    # flag whether to raise an exception
    incomplete_results = []
    # track what actions were performed how many times
    action_summary = {}

    # if a custom summary is to be provided, collect the results
    # of the command execution
    results = []
    do_custom_result_summary = result_renderer in (
        'tailored', 'generic', 'default') and hasattr(
            interface,
            'custom_result_summary_renderer')
    pass_summary = do_custom_result_summary \
        and getattr(interface,
                    'custom_result_summary_renderer_pass_summary',
                    None)

    # process main results
    for r in _process_results(
            # execution, call with any arguments from the validated
            # set that are no result-handling related
            cmd(**{k: v for k, v in allkwargs.items()
                if k not in exec_kwargs}),
            interface,
            allkwargs['on_failure'],
            # bookkeeping
            action_summary,
            incomplete_results,
            # communication
            result_renderer,
            result_log_level,
            # let renderers get to see how a command was called
            allkwargs):
        for hook, spec in hooks.items():
            # run the hooks before we yield the result
            # this ensures that they are executed before
            # a potentially wrapper command gets to act
            # on them
            if match_jsonhook2result(hook, r, spec['match']):
                lgr.debug('Result %s matches hook %s', r, hook)
                # a hook is also a command that yields results
                # so yield them outside too
                # users need to pay attention to void infinite
                # loops, i.e. when a hook yields a result that
                # triggers that same hook again
                for hr in run_jsonhook(hook, spec, r, dataset_arg):
                    # apply same logic as for main results, otherwise
                    # any filters would only tackle the primary results
                    # and a mixture of return values could happen
                    if not keep_result(hr, result_filter, **allkwargs):
                        continue
                    hr = xfm_result(hr, result_xfm)
                    # rationale for conditional is a few lines down
                    if hr:
                        yield hr
        if not keep_result(r, result_filter, **allkwargs):
            continue
        r = xfm_result(r, result_xfm)
        # in case the result_xfm decided to not give us anything
        # exclude it from the results. There is no particular reason
        # to do so other than that it was established behavior when
        # this comment was written. This will not affect any real
        # result record
        if r:
            yield r

        # collect if summary is desired
        if do_custom_result_summary:
            results.append(r)

    # result summary before a potential exception
    # custom first
    if do_custom_result_summary:
        if pass_summary:
            summary_args = (results, action_summary)
        else:
            summary_args = (results,)
        interface.custom_result_summary_renderer(*summary_args)
    elif result_renderer in ('generic', 'default') \
            and action_summary \
            and sum(sum(s.values())
                    for s in action_summary.values()) > 1:
        # give a summary in generic mode, when there was more than one
        # action performed
        render_action_summary(action_summary)

    if incomplete_results:
        raise IncompleteResultsError(
            failed=incomplete_results,
            msg="Command did not complete successfully")


# apply patch
patch_msg = \
    'Apply datalad-next patch to interface.(utils|base).py:eval_results'
apply_patch('datalad.interface.base', None, 'eval_results',
            eval_results, msg=patch_msg)
# we need to patch the datalad-next import location too
apply_patch('datalad_next.commands', None, 'eval_results',
            eval_results, msg=patch_msg)
