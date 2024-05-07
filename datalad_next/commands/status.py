"""
"""
from __future__ import annotations

__docformat__ = 'restructuredtext'

from dataclasses import dataclass
from enum import Enum
from logging import getLogger
from pathlib import Path
from typing import Generator

from datalad_next.commands import (
    CommandResult,
    CommandResultStatus,
    EnsureCommandParameterization,
    ValidatedInterface,
    Parameter,
    ParameterConstraintContext,
    build_doc,
    datasetmethod,
    eval_results,
)
from datalad_next.constraints import (
    EnsureChoice,
    EnsureDataset,
    WithDescription,
)

from datalad_next.iter_collections import (
    GitDiffStatus,
    GitTreeItemType,
    GitContainerModificationType,
    iter_gitstatus,
)
from datalad_next.uis import (
    ui_switcher as ui,
    ansi_colors as ac,
)

lgr = getLogger('datalad.core.local.status')


# TODO Could be `StrEnum`, came with PY3.11
class StatusState(Enum):
    """Enumeration of possible states of a status command result

    The "state" is the condition of the dataset item being reported
    on.
    """
    clean = 'clean'
    added = 'added'
    modified = 'modified'
    deleted = 'deleted'
    untracked = 'untracked'
    unknown = 'unknown'


diffstatus2resultstate_map = {
    GitDiffStatus.addition: StatusState.added,
    GitDiffStatus.copy: StatusState.added,
    GitDiffStatus.deletion: StatusState.deleted,
    GitDiffStatus.modification: StatusState.modified,
    GitDiffStatus.rename: StatusState.added,
    GitDiffStatus.typechange: StatusState.modified,
    GitDiffStatus.unmerged: StatusState.unknown,
    GitDiffStatus.unknown: StatusState.unknown,
    GitDiffStatus.other: StatusState.untracked,
}


# see base class decorator comment for why this is commented out
#@dataclass(kw_only=True)
@dataclass
class StatusResult(CommandResult):
    # TODO any of the following property are not actually optional
    # we only have to declare them such for limitations of dataclasses
    # prior PY3.10 (see kw_only command in base class

    diff_state: GitDiffStatus | None = None
    """The ``status`` of the underlying ``GitDiffItem``. It is named
    "_state" to emphasize the conceptual similarity with the legacy
    property 'state'
    """
    gittype: GitTreeItemType | None = None
    """The ``gittype`` of the underlying ``GitDiffItem``."""
    prev_gittype: GitTreeItemType | None = None
    """The ``prev_gittype`` of the underlying ``GitDiffItem``."""
    modification_types: tuple[GitContainerModificationType] | None = None
    """Qualifiers for modification types of container-type
    items (directories, submodules)."""

    @property
    def state(self) -> StatusState:
        """A (more or less legacy) simplified representation of the subject
        state. For a more accurate classification use the ``diff_status``
        property.
        """
        return diffstatus2resultstate_map[self.diff_state]

    # the previous status-implementation did not report plain git-types
    # we establish a getter to perform this kind of inference/mangling,
    # when it is needed
    @property
    def type(self) -> str | None:
        """
        """
        # TODO this is just a placeholder
        return self.gittype.value if self.gittype else None

    # we need a setter for this `type`-override stunt
    @type.setter
    def type(self, value):
        self.gittype = value

    @property
    def prev_type(self) -> str:
        """
        """
        return self.prev_gittype.value if self.prev_gittype else None

    @property
    def type_src(self) -> str | None:
        """Backward-compatibility adaptor"""
        return self.prev_type


opt_untracked_values = ('no', 'whole-dir', 'no-empty-dir', 'normal', 'all')
opt_recursive_values = ('no', 'repository', 'datasets', 'mono')
opt_eval_subdataset_state_values = ('no', 'commit', 'full')


class StatusParamValidator(EnsureCommandParameterization):
    def __init__(self):
        super().__init__(
            param_constraints=dict(
                # if given, it must also exist
                dataset=EnsureDataset(installed=True),
                untracked=EnsureChoice(*opt_untracked_values),
                recursive=EnsureChoice(*opt_recursive_values),
                eval_subdataset_state=EnsureChoice(
                    *opt_eval_subdataset_state_values)
            ),
            validate_defaults=('dataset',),
            joint_constraints={
                ParameterConstraintContext(('untracked', 'recursive'),
                                           'option normalization'):
                self.normalize_options,
            },
        )

    def normalize_options(self, **kwargs):
        if kwargs['untracked'] == 'no':
            kwargs['untracked'] = None
        if kwargs['untracked'] == 'normal':
            kwargs['untracked'] = 'no-empty-dir'
        if kwargs['recursive'] == 'datasets':
            kwargs['recursive'] = 'submodules'
        if kwargs['recursive'] == 'mono':
            kwargs['recursive'] = 'monolithic'
        return kwargs


@build_doc
class Status(ValidatedInterface):
    """Report on the (modification) status of a dataset

    .. note::

        This is a preview of an command implementation aiming to replace
        the DataLad ``status`` command.

        For now, expect anything here to change again.

    This command provides a report that is roughly identical to that of
    ``git status``. Running with default parameters yields a report that
    should look familiar to Git and DataLad users alike, and contain
    the same information as offered by ``git status``.

    The main difference to ``git status`` are:

    - Support for recursion into submodule. ``git status`` does that too,
      but the report is limited to the global state of an entire submodule,
      whereas this command can issue detailed reports in changes inside
      a submodule (any nesting depth).

    - Support for directory-constrained reporting. Much like ``git status``
      limits its report to a single repository, this command can optionally
      limit its report to a single directory and its direct children. In this
      report subdirectories are considered containers (much like) submodules,
      and a change summary is provided for them.

    - Support for a "mono" (monolithic repository) report. Unlike a standard
      recursion into submodules, and checking each of them for changes with
      respect to the HEAD commit of the worktree, this report compares a
      submodule with respect to the state recorded in its parent repository.
      This provides an equally comprehensive status report from the point of
      view of a queried repository, but does not include a dedicated item on
      the global state of a submodule. This makes nested hierarchy of
      repositories appear like a single (mono) repository.

    - Support for "adjusted mode" git-annex repositories. These utilize a
      managed branch that is repeatedly rewritten, hence is not suitable
      for tracking within a parent repository. Instead, the underlying
      "corresponding branch" is used, which contains the equivalent content
      in an un-adjusted form, persistently. This command detects this condition
      and automatically check a repositories state against the corresponding
      branch state.

    *Presently missing/planned features*

    - There is no support for specifying paths (or pathspecs) for constraining
      the operation to specific dataset parts. This will be added in the
      future.

    - There is no reporting of git-annex properties, such as tracked file size.
      It is undetermined whether this will be added in the future. However,
      even without a dedicated switch, this command has support for
      datasets (and their submodules) in git-annex's "adjusted mode".

    *Differences to the ``status`` command implementation prior DataLad v2*

    - Like ``git status`` this implementation reports on dataset modification,
      whereas the previous ``status`` also provided a listing of unchanged
      dataset content. This is no longer done. Equivalent functionality for
      listing dataset content is provided by the ``ls_file_collection``
      command.
    - The implementation is substantially faster. Depending on the context
      the speed-up is typically somewhere between 2x and 100x.
    - The implementation does not suffer from the limitation re type change
      detection.
    - Python and CLI API of the command use uniform parameter validation.
    """
    # Interface.validate_args() will inspect this dict for the presence of a
    # validator for particular parameters
    _validator_ = StatusParamValidator()

    # this is largely here for documentation and CLI parser building
    _params_ = dict(
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc="""Dataset to be used as a configuration source. Beyond
            reading configuration items, this command does not interact with
            the dataset."""),
        untracked=Parameter(
            args=('--untracked',),
            choices=opt_untracked_values,
            doc="""Determine how untracked content is considered and reported
            when comparing a revision to the state of the working tree.
            'no': no untracked content is considered as a change;
            'normal': untracked files and entire untracked directories are
            reported as such;
            'all': report individual files even in fully untracked directories.
            In addition to these git-status modes,
            'whole-dir' (like normal, but include empty directories), and
            'no-empty-dir' (alias for 'normal') are understood."""),
        recursive=Parameter(
            args=('-r', '--recursive'),
            nargs='?',
            const='datasets',
            choices=opt_recursive_values,
            doc="""Mode of recursion for status reporting.
            With 'no' the report is restricted to a single directory and
            its direct children.
            With 'repository', the report comprises all repository content
            underneath current working directory or root of a given dataset,
            but is limited to items directly contained in that repository.
            With 'datasets', the report also comprises any content in any
            subdatasets. Each subdataset is evaluated against its respective
            HEAD commit.
            With 'mono', a report similar to 'datasets' is generated, but
            any subdataset is evaluate with respect to the state recorded
            in its parent repository. In contrast to the 'datasets' mode,
            no report items on a joint submodule are generated.
            [CMD: If no particular value is given with this option the
            'datasets' mode is selected. CMD]
            """),
        eval_subdataset_state=Parameter(
            args=("-e", "--eval-subdataset-state",),
            choices=opt_eval_subdataset_state_values,
            doc="""Evaluation of subdataset state (modified or untracked
            content) can be expensive for deep dataset hierarchies
            as subdataset have to be tested recursively for
            uncommitted modifications. Setting this option to
            'no' or 'commit' can substantially boost performance
            by limiting what is being tested.
            With 'no' no state is evaluated and subdataset are not
            investigated for modifications.
            With 'commit' only a discrepancy of the HEAD commit
            gitsha of a subdataset and the gitsha recorded in the
            superdataset's record is evaluated.
            With 'full' any other modifications are considered
            too."""),
    )

    _examples_ = [
    ]

    @staticmethod
    @datasetmethod(name="next_status")
    @eval_results
    def __call__(
        # TODO later
        #path=None,
        *,
        dataset=None,
        # TODO possibly later
        #annex=None,
        untracked='normal',
        recursive='repository',
        eval_subdataset_state='full',
    ) -> Generator[StatusResult, None, None] | list[StatusResult]:
        ds = dataset.ds
        rootpath = Path.cwd() if dataset.original is None else ds.pathobj

        for item in iter_gitstatus(
            path=rootpath,
            untracked=untracked,
            recursive=recursive,
            eval_submodule_state=eval_subdataset_state,
        ):
            yield StatusResult(
                action='status',
                status=CommandResultStatus.ok,
                path=rootpath / (item.path or item.prev_path),
                gittype=item.gittype,
                prev_gittype=item.prev_gittype,
                diff_state=item.status,
                modification_types=item.modification_types,
                refds=ds,
                logger=lgr,
            )

    def custom_result_renderer(res, **kwargs):
        # we are guaranteed to have dataset-arg info through uniform
        # parameter validation
        dsarg = kwargs['dataset']
        rootpath = Path.cwd() if dsarg.original is None else dsarg.ds.pathobj
        # because we can always determine the root path of the command
        # execution environment, we can report meaningful relative paths
        # unconditionally
        path = res.path.relative_to(rootpath)
        # collapse item type information across current and previous states
        type_ = res.type or res.prev_type or ''
        max_len = len('untracked')
        state = res.state.value
        # message format is same as for previous command implementation
        ui.message(u'{fill}{state}: {path}{type_}{annot}'.format(
            fill=' ' * max(0, max_len - len(state)),
            state=ac.color_word(
                res.state.value,
                _get_result_status_render_color(res)),
            path=path,
            type_=' ({})'.format(ac.color_word(type_, ac.MAGENTA))
            if type_ else '',
            annot=f' [{", ".join(q.value for q in res.modification_types)}]'
            if res.modification_types else '',
        ))

    @staticmethod
    def custom_result_summary_renderer(results):
        # no reports, no changes
        if len(results) == 0:
            ui.message("nothing to save, working tree clean")


def _get_result_status_render_color(res):
    if res.state == StatusState.deleted:
        return ac.RED
    elif res.state == StatusState.modified:
        return ac.CYAN
    elif res.state == StatusState.added:
        return ac.GREEN
    else:
        return ac.BOLD
