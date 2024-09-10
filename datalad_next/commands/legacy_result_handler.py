from __future__ import annotations

import contextlib
import inspect
import json
import logging
import sys
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

from datalad_next.commands import ResultHandler
from datalad_next.constraints import DatasetParameter

lgr = logging.getLogger('datalad.interface.utils')


class LegacyResultHandler(ResultHandler):
    def __init__(
        self,
        interface: anInterface,
        cmd_kwargs: dict[str, Any],
    ) -> None:
        self._interface = interface
        self._cmd_kwargs = cmd_kwargs
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

        self._hooks: dict[str, dict[Any, Any]] | None = None

        # resolve string labels for transformers too
        self._result_xfm = known_result_xfms.get(
            cmd_kwargs['result_xfm'],
            # use verbatim, if not a known label
            cmd_kwargs['result_xfm'])

        self._result_filter = get_result_filter(cmd_kwargs['result_filter'])

    def return_results(
        self,
        # except a generator
        get_results: Callable,
    ):
        mode = self._cmd_kwargs['return_type']
        if mode == 'generator':
            # hand over the generator
            lgr.log(2,
                    "Returning generator_func from eval_func for %s",
                    self._interface)
            return get_results()

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
    ) -> None:
        mode = self._cmd_kwargs['result_renderer']
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
            self._interface.custom_result_renderer(res, **self._cmd_kwargs)
        elif callable(result_renderer):
            _render_result_customcall(res, result_renderer, self._cmd_kwargs)
        else:
            msg = f'unknown result renderer {result_renderer!r}'
            raise ValueError(msg)

    def render_result_summary(self) -> None:
        mode = self._cmd_kwargs['result_renderer']
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

    def run_result_hooks(self, res) -> Generator[dict[str, Any], None, None]:
        dataset_arg = self._cmd_kwargs.get('dataset', None)
        if self._hooks is None:
            # figure out which hooks are relevant for this command execution
            self._hooks = get_hooks(dataset_arg)

        for hook, spec in self._hooks.items():
            # run the hooks before we yield the result
            # this ensures that they are executed before
            # a potentially wrapper command gets to act
            # on them
            if match_jsonhook2result(hook, res, spec['match']):
                lgr.debug('Result %s matches hook %s', res, hook)
                # a hook is also a command that yields results
                # so yield them outside too
                # users need to pay attention to void infinite
                # loops, i.e. when a hook yields a result that
                # triggers that same hook again
                for hr in run_jsonhook(
                    hook, spec, res, dataset_arg
                ):
                    # apply same logic as for main results, otherwise
                    # any filters would only tackle the primary results
                    # and a mixture of return values could happen
                    if not self.keep_result(hr):
                        continue
                    hr_xfm = xfm_result(hr, self._result_xfm)
                    # rationale for conditional is a few lines down
                    if hr_xfm:
                        yield hr_xfm

    def transform_result(self, res) -> Generator[Any, None, None]:
        r_xfm = xfm_result(res, self._result_xfm)
        # in case the result_xfm decided to not give us anything
        # exclude it from the results. There is no particular reason
        # to do so other than that it was established behavior when
        # this comment was written. This will not affect any real
        # result record
        if r_xfm:
            yield r_xfm

    def keep_result(self, res) -> bool:
        return keep_result(
            res,
            self._result_filter,
            **self._cmd_kwargs,
        )


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



