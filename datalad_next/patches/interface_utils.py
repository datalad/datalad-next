"""Uniform pre-execution parameter validation for commands

With this patch commands can now opt-in to receive fully validated parameters.
This can substantially simplify the implementation complexity of a command at
the expense of a more elaborate specification of the structural and semantic
properties of the parameters.

For details on implementing validation for individual commands see
:class:`datalad_next.commands.ValidatedInterface`.
"""
from __future__ import annotations

import contextlib
import inspect
import json
import logging
import sys
from functools import (
    partial,
    wraps,
)
from os.path import relpath
from time import time
from typing import (
    Any,
    Callable,
    Generator,
)

import datalad.support.ansi_colors as ac
from datalad import cfg as dlcfg
from datalad.core.local.resulthooks import (
    get_jsonhooks_from_config,
    match_jsonhook2result,
    run_jsonhook,
)
from datalad.dochelpers import single_or_plural
from datalad.interface.base import default_logchannels
from datalad.interface.common_opts import eval_params
from datalad.interface.results import known_result_xfms
from datalad.interface.utils import (
    anInterface,
    get_result_filter,
    keep_result,
    render_action_summary,
    xfm_result,
)
from datalad.support.exceptions import CapturedException
from datalad.ui import ui
from datalad.utils import get_wrapped_class

from datalad_next.constraints import DatasetParameter
from datalad_next.exceptions import IncompleteResultsError
from datalad_next.patches import apply_patch
from datalad_next.utils import getargspec

# use same logger as -core
lgr = logging.getLogger('datalad.interface.utils')


class ResultHandler:
    def __init__(self, interface: anInterface) -> None:
        self._interface = interface
        # look for potential override of logging behavior
        self._result_log_level = dlcfg.get('datalad.log.result-level', 'debug')

        # e.g., if a custom summary is to be provided, collect the results
        self._results: list[dict] = []

        # how many repetitions to show, before suppression kicks in
        self._render_n_repetitions = \
            dlcfg.obtain('datalad.ui.suppress-similar-results-threshold') \
            if sys.stdout.isatty() \
            and dlcfg.obtain('datalad.ui.suppress-similar-results') \
            else float("inf")
        # status variables for the suppression of repeated result
        # by the generic renderer
        # counter for detected repetitions
        self._last_result_reps: int = 0
        # used to track repeated messages in the generic renderer
        self._last_result: dict | None = None
        # the timestamp of the last renderer result
        self._last_result_ts: float | None = None

        # track what actions were performed how many times
        self._action_summary: dict[str, dict] = {}

    def return_results(
        self,
        # except a generator
        get_results: Callable,
        *,
        mode: str,
    ):
        if mode == 'generator':
            # hand over the generator
            lgr.log(2,
                    "Returning generator_func from eval_func for %s",
                    self._interface)
            return get_results()

        @wraps(_execute_command_)
        def return_func():
            results = get_results()
            if inspect.isgenerator(results):
                # unwind generator if there is one, this actually runs
                # any processing
                results = list(results)
            cannot_be_item_length = 2
            if mode == 'item-or-list' and \
                    len(results) < cannot_be_item_length:
                return results[0] if results else None

            return results

        lgr.log(2,
                "Returning return_func from eval_func for %s",
                self._interface)
        return return_func()

    def log_result(self, result: dict) -> None:
        res = result
        # log message, if there is one and a logger was given
        msg = res.get('message', None)
        res_lgr = res.get('logger', None)
        if not (msg and res_lgr):
            return

        if isinstance(res_lgr, logging.Logger):
            # didn't get a particular log function, go with default
            res_lgr = getattr(
                res_lgr,
                default_logchannels[res['status']]
                if self._result_log_level == 'match-status'
                else self._result_log_level)
        msg = res['message']
        msgargs = None
        if isinstance(msg, tuple):
            msgargs = msg[1:]
            msg = msg[0]
        if 'path' in res:
            # result path could be a path instance
            path = str(res['path'])
            if msgargs:
                # we will pass the msg for %-polation, so % should be doubled
                path = path.replace('%', '%%')
            msg = '{} [{}({})]'.format(
                msg, res['action'], path)
        if msgargs:
            # support string expansion of logging to avoid runtime cost
            try:
                res_lgr(msg, *msgargs)
            except TypeError as exc:
                msg = f"Failed to render {msg!r} " \
                      f"with {msgargs!r} from {res!r}: {exc}"
                raise TypeError(msg) from exc
        else:
            res_lgr(msg)

    def want_custom_result_summary(self, mode: str) -> bool:
        return mode in (
            'tailored', 'generic', 'default') and hasattr(
                self._interface,
                'custom_result_summary_renderer')

    def render_result(
        self,
        result: dict,
        *,
        mode: str,
        cmd_kwargs: dict[str, Any],
    ) -> None:
        res = result
        result_renderer = mode
        if result_renderer == 'tailored' \
                and not hasattr(self._interface, 'custom_result_renderer'):
            # a tailored result renderer is requested, but the class
            # does not provide any, fall back to the generic one
            result_renderer = 'generic'
        if result_renderer == 'default':
            # standardize on the new name 'generic' to avoid more complex
            # checking below
            result_renderer = 'generic'

        # if a custom summary is to be provided, collect the results
        # of the command execution
        if self.want_custom_result_summary(mode):
            self._results.append(result)

        # update summary statistics
        actsum = self._action_summary.get(res['action'], {})
        if res['status']:
            actsum[res['status']] = actsum.get(res['status'], 0) + 1
            self._action_summary[res['action']] = actsum

        if result_renderer is None or result_renderer == 'disabled':
            # no rendering of individual results desired, we are done
            return

        # pass result
        if result_renderer == 'generic':
            self._last_result_reps, self._last_result, self._last_result_ts = \
                _render_result_generic(
                    res,
                    self._render_n_repetitions,
                    self._last_result_reps,
                    self._last_result,
                    self._last_result_ts,
                )
        elif result_renderer in ('json', 'json_pp'):
            _render_result_json(res, result_renderer.endswith('_pp'))
        elif result_renderer == 'tailored':
            self._interface.custom_result_renderer(res, **cmd_kwargs)
        elif callable(result_renderer):
            _render_result_customcall(res, result_renderer, cmd_kwargs)
        else:
            msg = f'unknown result renderer {result_renderer!r}'
            raise ValueError(msg)

    def render_result_summary(self, mode: str) -> None:
        # make sure to report on any issues that we had suppressed
        _display_suppressed_message(
            self._last_result_reps,
            self._render_n_repetitions,
            self._last_result_ts,
            final=True,
        )
        do_custom_result_summary = self.want_custom_result_summary(mode)

        pass_summary = do_custom_result_summary \
            and getattr(self._interface,
                        'custom_result_summary_renderer_pass_summary',
                        None)
        # result summary before a potential exception
        # custom first
        if do_custom_result_summary:
            summary_args = (self._results, self._action_summary) \
                if pass_summary else (self._results,)
            self._interface.custom_result_summary_renderer(*summary_args)
        elif mode in ('generic', 'default') \
                and self._action_summary \
                and sum(sum(s.values())
                        for s in self._action_summary.values()) > 1:
            # give a summary in generic mode, when there was more than one
            # action performed
            render_action_summary(self._action_summary)


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
            result_handler_cls = ResultHandler

        result_handler = result_handler_cls(wrapped_class)

        return result_handler.return_results(
            # we wrap the result generating function into
            # a partial to get an argumentless callable
            # that provides an iterable. partial is a misnomer
            # here, all necessary parameters are given
            partial(
                _execute_command_,
                interface=wrapped_class,
                cmd=wrapped,
                allkwargs=allkwargs,
                result_handler=result_handler,
            ),
            mode=kwargs['return_type'],
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
            getattr(cls, p_name))
        for p_name in eval_params}
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
    assert (nargs >= len(defaults))  # noqa: S101
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
    at_default = {
        k for k in kwargs_
        if k in default_map and default_map[k] == kwargs_[k]
    }
    # XXX: we cannot assert the following, because our own highlevel
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
    # resolve string labels for transformers too
    result_xfm = known_result_xfms.get(
        allkwargs['result_xfm'],
        # use verbatim, if not a known label
        allkwargs['result_xfm'])
    result_filter = get_result_filter(allkwargs['result_filter'])

    # figure out which hooks are relevant for this command execution
    hooks = get_hooks(allkwargs.get('dataset', None))

    # flag whether to raise an exception
    incomplete_results: list[dict] = []

    # process main results
    for r in _process_results(
        # execution, call with any arguments from the validated
        # set that are no result-handling related
        cmd(**{k: v for k, v in allkwargs.items()
            if k not in eval_params}),
        on_failure=allkwargs['on_failure'],
        # bookkeeping
        incomplete_results=incomplete_results,
        # communication
        result_logger=result_handler.log_result,
        result_renderer=partial(
            result_handler.render_result,
            mode=allkwargs['result_renderer'],
            cmd_kwargs=allkwargs,
        ),
    ):
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
                for hr in run_jsonhook(
                    hook, spec, r, allkwargs.get('dataset', None)
                ):
                    # apply same logic as for main results, otherwise
                    # any filters would only tackle the primary results
                    # and a mixture of return values could happen
                    if not keep_result(hr, result_filter, **allkwargs):
                        continue
                    hr_xfm = xfm_result(hr, result_xfm)
                    # rationale for conditional is a few lines down
                    if hr_xfm:
                        yield hr_xfm
        if not keep_result(r, result_filter, **allkwargs):
            continue
        r_xfm = xfm_result(r, result_xfm)
        # in case the result_xfm decided to not give us anything
        # exclude it from the results. There is no particular reason
        # to do so other than that it was established behavior when
        # this comment was written. This will not affect any real
        # result record
        if r_xfm:
            yield r_xfm

    result_handler.render_result_summary(allkwargs['result_renderer'])

    if incomplete_results:
        raise IncompleteResultsError(
            failed=incomplete_results,
            msg="Command did not complete successfully")


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
    param_validator = interface.get_parameter_validator() \
        if hasattr(interface, 'get_parameter_validator') else None
    if param_validator is None:
        lgr.debug(
            'Command parameter validation skipped. %s declares no validator',
            interface)
    else:
        lgr.debug('Command parameter validation for %s', interface)
        validator_kwargs = {
            'at_default': at_default,
            'required': required_args or None,
        }
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

    return allkwargs


def get_hooks(dataset_arg: Any) -> dict[str, dict]:
    # figure out which hooks are relevant for this command execution
    # query cfg for defaults
    # .is_installed and .config can be costly, so ensure we do
    # it only once. See https://github.com/datalad/datalad/issues/3575
    ds = None
    if dataset_arg is not None:
        from datalad_next.datasets import Dataset
        if isinstance(dataset_arg, Dataset):
            ds = dataset_arg
        elif isinstance(dataset_arg, DatasetParameter):
            ds = dataset_arg.ds
        else:
            with contextlib.suppress(ValueError):
                ds = Dataset(dataset_arg)
    # look for hooks
    return get_jsonhooks_from_config(ds.config if ds else dlcfg)


def _process_results(
    results,
    *,
    on_failure: str,
    incomplete_results,
    result_logger: Callable,
    result_renderer: Callable,
):
    # private helper pf @eval_results
    # loop over results generated from some source and handle each
    # of them according to the requested behavior (logging, rendering, ...)

    for res in results:
        if not res or 'action' not in res:
            # XXX: Yarik has to no clue on how to track the origin of the
            # record to figure out WTF, so he just skips it
            # but MIH thinks leaving a trace of that would be good
            lgr.debug('Drop result record without "action": %s', res)
            continue

        result_logger(res)
        # remove logger instance from results, as it is no longer useful
        # after logging was done, it isn't serializable, and generally
        # pollutes the output
        res.pop('logger', None)

        # output rendering
        result_renderer(res)

        # error handling
        # looks for error status, and report at the end via
        # an exception
        if on_failure in ('continue', 'stop') \
                and res['status'] in ('impossible', 'error'):
            incomplete_results.append(res)
            if on_failure == 'stop':
                # first fail -> that's it
                # raise will happen after the loop
                break
        yield res


def _display_suppressed_message(nsimilar, ndisplayed, last_ts, final=False):
    # +1 because there was the original result + nsimilar displayed.
    n_suppressed = nsimilar - ndisplayed + 1
    if n_suppressed > 0:
        ts = time()
        # rate-limit update of suppression message, with a large number
        # of fast-paced results updating for each one can result in more
        # CPU load than the actual processing
        # arbitrarily go for a 2Hz update frequency -- it "feels" good
        max_freq = 0.5
        if last_ts is None or final or (ts - last_ts > max_freq):
            ui.message('  [{} similar {} been suppressed; disable with datalad.ui.suppress-similar-results=off]'
                       .format(n_suppressed,
                               single_or_plural("message has",
                                                "messages have",
                                                n_suppressed, False)),
                       cr="\n" if final else "\r")
            return ts
    return last_ts


def _render_result_generic(
        res, render_n_repetitions,
        # status vars
        last_result_reps, last_result, last_result_ts):
    # which result dict keys to inspect for changes to discover repetitions
    # of similar messages
    repetition_keys = {'action', 'status', 'type', 'refds'}

    trimmed_result = {k: v for k, v in res.items() if k in repetition_keys}
    if res.get('status', None) != 'notneeded' \
            and trimmed_result == last_result:
        # this is a similar report, suppress if too many, but count it
        last_result_reps += 1
        if last_result_reps < render_n_repetitions:
            generic_result_renderer(res)
        else:
            last_result_ts = _display_suppressed_message(
                last_result_reps, render_n_repetitions, last_result_ts)
    else:
        # this one is new, first report on any prev. suppressed results
        # by number, and then render this fresh one
        last_result_ts = _display_suppressed_message(
            last_result_reps, render_n_repetitions, last_result_ts,
            final=True)
        generic_result_renderer(res)
        last_result_reps = 0
    return last_result_reps, trimmed_result, last_result_ts


def _render_result_json(res, prettyprint):
    ui.message(json.dumps(
        {k: v for k, v in res.items()
         if k not in ('logger')},
        sort_keys=True,
        indent=2 if prettyprint else None,
        default=str))


def _render_result_customcall(res, result_renderer, allkwargs):
    try:
        result_renderer(res, **allkwargs)
    except Exception as e:  # noqa: BLE001
        lgr.warning('Result rendering failed for: %s [%s]',
                    res, CapturedException(e))


def generic_result_renderer(res):
    if res.get('status', None) != 'notneeded':
        path = res.get('path', None)
        if path and res.get('refds'):
            # can happen, e.g., on windows with paths from different
            # drives. just go with the original path in this case
            with contextlib.suppress(ValueError):
                path = relpath(path, res['refds'])
        ui.message('{action}({status}):{path}{type}{msg}{err}'.format(
            action=ac.color_word(
                res.get('action', '<action-unspecified>'),
                ac.BOLD),
            status=ac.color_status(res.get('status', '<status-unspecified>')),
            path=f' {path}' if path else '',
            type=' ({})'.format(
                ac.color_word(res['type'], ac.MAGENTA)
            ) if 'type' in res else '',
            msg=' [{}]'.format(
                res['message'][0] % res['message'][1:]
                if isinstance(res['message'], tuple) else res[
                    'message'])
            if res.get('message', None) else '',
            err=ac.color_word(' [{}]'.format(
                res['error_message'][0] % res['error_message'][1:]
                if isinstance(res['error_message'], tuple) else res[
                    'error_message']), ac.RED)
            if res.get('error_message', None) and res.get('status', None) != 'ok' else ''))


# apply patch
patch_msg = \
    'Apply datalad-next patch to interface.(utils|base).py:eval_results'
apply_patch('datalad.interface.base', None, 'eval_results',
            eval_results, msg=patch_msg)
# we need to patch the datalad-next import location too
apply_patch('datalad_next.commands', None, 'eval_results',
            eval_results, msg=patch_msg)
