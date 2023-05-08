# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 et:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Basic constraints for declaring essential data types, values, and ranges"""

from __future__ import annotations

__docformat__ = 'restructuredtext'

from pathlib import Path
import re

from datalad_next.datasets import resolve_path

from .base import (
    Constraint,
    DatasetParameter,
)
from .utils import _type_str


class NoConstraint(Constraint):
    """A constraint that represents no constraints"""
    def short_description(self):
        return ''

    def __call__(self, value):
        return value


class EnsureValue(Constraint):
    """Ensure an input is a particular value"""
    def __init__(self, value):
        super().__init__()
        self._target_value = value

    def __call__(self, value):
        if value == self._target_value:
            return value
        else:
            self.raise_for(
                value,
                "must be {target_value!r}",
                target_value=self._target_value,
            )

    def short_description(self):
        return f'{self._target_value!r}'

    def long_description(self):
        return f'value must be {self.short_description()}'


class EnsureDType(Constraint):
    """Ensure that an input (or several inputs) are of a particular data type.
    Examples:

        >>> c = EnsureDType(float)
        >>> type(c(8))                  # doctest: +SKIP
        float
        >>> import numpy as np          # doctest: +SKIP
        >>> c = EnsureDType(np.float64) # doctest: +SKIP
        >>> type(c(8))                  # doctest: +SKIP
        numpy.float64
    """
    def __init__(self, dtype):
        """
        Parameters
        ----------
        dtype : functor
        """
        self._dtype = dtype

    def __call__(self, value):
        try:
            return self._dtype(value)
        except Exception as e:
            self.raise_for(
                value,
                str(e),
            )

    def short_description(self):
        return _type_str(self._dtype)

    def long_description(self):
        return "value must be convertible to type '%s'" % self.short_description()


class EnsureInt(EnsureDType):
    """Ensure that an input (or several inputs) are of a data type 'int'.
    """
    def __init__(self):
        """Initializes EnsureDType with int"""
        EnsureDType.__init__(self, int)


class EnsureFloat(EnsureDType):
    """Ensure that an input (or several inputs) are of a data type 'float'.
    """
    def __init__(self):
        """Initializes EnsureDType with float"""
        EnsureDType.__init__(self, float)


class EnsureBool(Constraint):
    """Ensure that an input is a bool.

    A couple of literal labels are supported, such as:
    False: '0', 'no', 'off', 'disable', 'false'
    True: '1', 'yes', 'on', 'enable', 'true'
    """
    def __call__(self, value):
        if isinstance(value, bool):
            return value
        elif isinstance(value, (bytes, str)):
            value = value.lower()
            if value in ('0', 'no', 'off', 'disable', 'false'):
                return False
            elif value in ('1', 'yes', 'on', 'enable', 'true'):
                return True
        self.raise_for(value, "must be convertible to boolean")

    def long_description(self):
        return 'value must be convertible to type bool'

    def short_description(self):
        return 'bool'


class EnsureStr(Constraint):
    """Ensure an input is a string of some min. length and matching a pattern

    Pattern matching is optional and minimum length is zero (empty string is
    OK).

    No type conversion is performed.
    """
    def __init__(self, min_len: int = 0, match: str | None = None):
        """
        Parameters
        ----------
        min_len: int, optional
           Minimal length for a string.
        match:
           Regular expression used to match any input value against.
           Values not matching the expression will cause a
           `ValueError` to be raised.
        """
        assert min_len >= 0
        self._min_len = min_len
        self._match = match
        super().__init__()
        if match is not None:
            self._match = re.compile(match)

    def __call__(self, value) -> str:
        if not isinstance(value, (bytes, str)):
            # do not perform a blind conversion ala str(), as almost
            # anything can be converted and the result is most likely
            # unintended
            self.raise_for(value, "must be a string")
        if len(value) < self._min_len:
            self.raise_for(value, "must have minimum length {len}",
                                 len=self._min_len)
        if self._match:
            if not self._match.match(value):
                self.raise_for(
                    value,
                    'does not match {pattern}',
                    pattern=self._match.pattern,
                )
        return value

    def long_description(self):
        return 'must be a string{}'.format(
            f' and match {self._match.pattern}' if self._match else '',
        )

    def short_description(self):
        return 'str{}'.format(
            f'({self._match.pattern})' if self._match else '',
        )


# TODO possibly consolidate on EnsureStr from -gooey, which can take
# a regex that could perform this. CON: documentation less clear.
# But if custom documentation will be supported, it might get even
# more clear nevertheless
class EnsureStrPrefix(EnsureStr):
    """Ensure an input is a string that starts with a given prefix.
    """
    def __init__(self, prefix):
        """
        Parameters
        ----------
        prefix : str
           Mandatory prefix.
        """
        self._prefix = prefix
        super().__init__()

    def __call__(self, value):
        super().__call__(value)
        if not value.startswith(self._prefix):
            self.raise_for(
                value,
                "does not start with {prefix!r}",
                prefix=self._prefix,
            )
        return value

    def long_description(self):
        return "value must start with '{}'".format(self._prefix)

    def short_description(self):
        return '{}...'.format(self._prefix)


class EnsureNone(EnsureValue):
    """Ensure an input is of value `None`"""
    def __init__(self):
        super().__init__(None)


class EnsureCallable(Constraint):
    """Ensure an input is a callable object"""
    def __call__(self, value):
        if hasattr(value, '__call__'):
            return value
        else:
            self.raise_for(value, "must be a callable")

    def short_description(self):
        return 'callable'

    def long_description(self):
        return 'value must be a callable'


class EnsureChoice(Constraint):
    """Ensure an input is element of a set of possible values"""

    def __init__(self, *values):
        """
        Parameters
        ----------
        *values
           Possible accepted values.
        """
        self._allowed = values
        super(EnsureChoice, self).__init__()

    def __call__(self, value):
        if value not in self._allowed:
            self.raise_for(
                value,
                "is not one of {allowed}",
                allowed=self._allowed,
            )
        return value

    def long_description(self):
        return 'value must be one of [CMD: %s CMD][PY: %s PY]' % (
            str(tuple(i for i in self._allowed if i is not None)),
            str(self._allowed)
        )

    def short_description(self):
        return '{%s}' % ', '.join([repr(c) for c in self._allowed])


class EnsureKeyChoice(EnsureChoice):
    """Ensure value under a key in an input is in a set of possible values"""

    def __init__(self, key, values):
        """
        Parameters
        ----------
        key : str
          The to-be-tested values are looked up under the given key in
          a dict-like input object.
        values : tuple
           Possible accepted values.
        """
        self._key = key
        super(EnsureKeyChoice, self).__init__(*values)

    def __call__(self, value):
        if self._key not in value:
            self.raise_for(value, "must be dict-like")
        super(EnsureKeyChoice, self).__call__(value[self._key])
        return value

    def long_description(self):
        return "value in '%s' must be one of %s" % (self._key, str(self._allowed),)

    def short_description(self):
        return '%s:{%s}' % (self._key, ', '.join([repr(c) for c in self._allowed]))


class EnsureRange(Constraint):
    """Ensure an input is within a particular range

    No type checks are performed.
    """
    def __init__(self, min=None, max=None):
        """
        Parameters
        ----------
        min
            Minimal value to be accepted in the range
        max
            Maximal value to be accepted in the range
        """
        self._min = min
        self._max = max
        if self._min is None and self._max is None:
            raise ValueError('No range given, min == max == None')
        super(EnsureRange, self).__init__()

    def __call__(self, value):
        if self._min is not None:
            if self._max is not None:
                if value < self._min or value > self._max:
                    self.raise_for(
                        value,
                        f"must be in range from {self._min!r} to {self._max!r}"
                    )
            else:
                if value < self._min:
                    self.raise_for(value, f"must be at least {self._min!r}")
        if self._max is not None:
            if value > self._max:
                self.raise_for(value, f"must be at most {self._max!r}")
        return value

    def long_description(self):
        return self.short_description()

    def short_description(self):
        if self._max is None:
            return f'not less than {self._min!r}'
        elif self._min is None:
            return f'not greater than {self._max!r}'
        else:
            # it is inclusive, but spelling it out would be wordy
            return f'in range from {self._min!r} to {self._max!r}'


class EnsurePath(Constraint):
    """Ensures input is convertible to a (platform) path and returns a `Path`

    Optionally, the path can be tested for existence and whether it is absolute
    or relative.
    """
    def __init__(self,
                 *,
                 path_type: type = Path,
                 is_format: str | None = None,
                 lexists: bool | None = None,
                 is_mode: callable | None = None,
                 ref: Path | None = None,
                 ref_is: str = 'parent-or-same-as',
                 dsarg: DatasetParameter | None = None):
        """
        Parameters
        ----------
        path_type:
          Specific pathlib type to convert the input to. The default is `Path`,
          i.e. the platform's path type. Not all pathlib Path types can be
          instantiated on all platforms, and not all checks are possible with
          all path types.
        is_format: {'absolute', 'relative'} or None
          If not None, the path is tested whether it matches being relative or
          absolute.
        lexists:
          If not None, the path is tested to confirmed exists or not. A symlink
          need not point to an existing path to fulfil the "exists" condition.
        is_mode:
          If set, this callable will receive the path's `.lstat().st_mode`,
          and an exception is raised, if the return value does not evaluate
          to `True`. Typical callables for this feature are provided by the
          `stat` module, e.g. `S_ISDIR()`
        ref:
          If set, defines a reference Path any given path is compared to. The
          comparison operation is given by `ref_is`.
        ref_is: {'parent-or-same-as', 'parent-of'}
          Comparison operation to perform when `ref` is given.
        dsarg: DatasetParameter, optional
          If given, incoming paths are resolved in the following fashion:
          If, and only if, the original "dataset" parameter was a
          ``Dataset`` object instance, relative paths are interpreted as
          relative to the given dataset. In all other cases, relative paths
          are treated as relative to the current working directory.
        """
        super().__init__()
        self._path_type = path_type
        self._is_format = is_format
        self._lexists = lexists
        self._is_mode = is_mode
        self._ref = ref
        self._ref_is = ref_is
        self._dsarg = dsarg
        assert self._ref_is in ('parent-or-same-as', 'parent-of'), \
            'Unrecognized `ref_is` operation label'

    def __call__(self, value):
        # turn it into the target type to make everything below
        # more straightforward
        path = self._path_type(value)

        # we are testing the format first, because resolve_path()
        # will always turn things into absolute paths
        if self._is_format is not None:
            is_abs = path.is_absolute()
            if self._is_format == 'absolute' and not is_abs:
                self.raise_for(path, 'is not an absolute path')
            elif self._is_format == 'relative' and is_abs:
                self.raise_for(path, 'is not a relative path')

        # resolve relative paths against a dataset, if given
        if self._dsarg:
            path = resolve_path(
                path,
                self._dsarg.original,
                self._dsarg.ds)

        mode = None
        if self._lexists is not None or self._is_mode is not None:
            try:
                mode = path.lstat().st_mode
            except FileNotFoundError:
                # this is fine, handled below
                pass
        if self._lexists is not None:
            if self._lexists and mode is None:
                self.raise_for(path, 'does not exist')
            elif not self._lexists and mode is not None:
                self.raise_for(path, 'does (already) exist')
        if self._is_mode is not None:
            if not self._is_mode(mode):
                self.raise_for(path, 'does not match desired mode')
        if self._ref:
            ok = True
            if self._ref_is == 'parent-or-same-as':
                ok = (path == self._ref or self._ref in path.parents)
            elif self._ref_is == 'parent-of':
                ok = self._ref in path.parents
            else:  # pragma: nocover
                # this code cannot be reached with normal usage.
                # it is prevented by an assertion in __init__()
                raise RuntimeError('Unknown `ref_is` operation label')

            if not ok:
                self.raise_for(
                    path,
                    '{ref} is not {ref_is} {path}',
                    ref=self._ref,
                    ref_is=self._ref_is,
                )
        return path

    def for_dataset(self, dataset: DatasetParameter) -> Constraint:
        """Return an similarly parametrized variant that resolves
        paths against a given dataset (argument)


        """
        return self.__class__(
            path_type=self._path_type,
            is_format=self._is_format,
            lexists=self._lexists,
            is_mode=self._is_mode,
            ref=self._ref,
            ref_is=self._ref_is,
            dsarg=dataset,
        )

    def short_description(self):
        return '{}{}path{}'.format(
            'existing '
            if self._lexists
            else 'non-existing '
            if self._lexists else '',
            'absolute '
            if self._is_format == 'absolute'
            else 'relative'
            if self._is_format == 'relative'
            else '',
            f' that is {self._ref_is} {self._ref}'
            if self._ref
            else '',
        )
