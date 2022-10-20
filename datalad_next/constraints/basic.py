# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 et:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Helper for parameter validation, documentation and conversion"""

__docformat__ = 'restructuredtext'

from pathlib import Path
import re
from typing import Dict

from .api import (
    Constraint,
    aDataset,
)
from .utils import _type_str


class NoConstraint(Constraint):
    """A contraint that represents no constraints"""
    def short_description(self):
        return ''

    def __call__(self, value):
        return value


class EnsureDType(Constraint):
    """Ensure that an input (or several inputs) are of a particular data type.
    """
    # TODO extend to support numpy-like dtype specs, e.g. 'int64'
    # in addition to functors
    def __init__(self, dtype):
        """
        Parameters
        ----------
        dtype : functor
        """
        self._dtype = dtype

    def __call__(self, value):
        return self._dtype(value)

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


class EnsureIterableOf(Constraint):
    """Ensure that an input is a list of a particular data type
    """
    # TODO support a delimiter to be able to take str-lists?
    def __init__(self,
                 iter_type: type,
                 item_constraint: callable,
                 min_len: int or None = None,
                 max_len: int or None = None):
        """
        Parameters
        ----------
        iter_type:
          Target type of iterable. Common types are `list`, or `tuple`,
          but also generator type iterables are possible. Type constructor
          must take an iterable with items as the only required positional
          argument.
        item_constraint:
          Each incoming item will be mapped through this callable
          before being passed to the iterable type constructor.
        min_len:
          If not None, the iterable will be verified to have this minimum
          number of items. The iterable type must implement `__len__()`
          for this check to be supported.
        max_len:
          If not None, the iterable will be verified to have this maximum
          number of items. The iterable type must implement `__len__()`
          for this check to be supported.
        """
        if min_len is not None and max_len is not None and min_len > max_len:
            raise ValueError(
                'Given minimum length exceeds given maximum length')
        self._iter_type = iter_type
        self._item_constraint = item_constraint
        self._min_len = min_len
        self._max_len = max_len
        super().__init__()

    @property
    def item_constraint(self):
        return self._item_constraint

    def __call__(self, value):
        iter = self._iter_type(
            self._item_constraint(i) for i in value
        )
        if self._min_len is not None or self._max_len is not None:
            # only do this if necessary, generators will not support
            # __len__, for example
            iter_len = len(iter)
            if self._min_len is not None and iter_len < self._min_len:
                raise ValueError(
                    f'Length-{iter_len} iterable is shorter than '
                    f'required minmum length {self._min_len}')
            if self._max_len is not None and iter_len > self._max_len:
                raise ValueError(
                    f'Length-{iter_len} iterable is longer than '
                    f'required maximum length {self._max_len}')
        return iter

    def short_description(self):
        return f'{self._iter_type}({self._item_constraint})'


class EnsureListOf(EnsureIterableOf):
    def __init__(self,
                 item_constraint: callable,
                 min_len: int or None = None,
                 max_len: int or None = None):
        """
        Parameters
        ----------
        item_constraint:
          Each incoming item will be mapped through this callable
          before being passed to the list constructor.
        min_len:
          If not None, the list will be verified to have this minimum
          number of items.
        max_len:
          If not None, the list will be verified to have this maximum
          number of items.
        """
        super().__init__(list, item_constraint,
                         min_len=min_len, max_len=max_len)

    def short_description(self):
        return f'list({self._item_constraint})'


class EnsureTupleOf(EnsureIterableOf):
    def __init__(self,
                 item_constraint: callable,
                 min_len: int or None = None,
                 max_len: int or None = None):
        """
        Parameters
        ----------
        item_constraint:
          Each incoming item will be mapped through this callable
          before being passed to the tuple constructor.
        min_len:
          If not None, the tuple will be verified to have this minimum
          number of items.
        max_len:
          If not None, the tuple will be verified to have this maximum
          number of items.
        """
        super().__init__(tuple, item_constraint,
                         min_len=min_len, max_len=max_len)

    def short_description(self):
        return f'tuple({self._item_constraint})'


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
        raise ValueError(
            "value '{}' must be convertible to boolean".format(
                value))

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
    def __init__(self, min_len: int = 0, match: str = None):
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
            raise ValueError("%s is not a string" % repr(value))
        if len(value) < self._min_len:
            raise ValueError("%r is shorter than of minimal length %d"
                             % (value, self._min_len))
        if self._match:
            if not self._match.match(value):
                raise ValueError(
                    f'{value} does not match {self._match.pattern}')
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
            raise ValueError("%r does not start with '%s'"
                             % (value, self._prefix))
        return value

    def long_description(self):
        return "value must start with '{}'".format(self._prefix)

    def short_description(self):
        return '{}...'.format(self._prefix)


class EnsureNone(Constraint):
    """Ensure an input is of value `None`"""
    def __call__(self, value):
        if value is None:
            return None
        else:
            raise ValueError("value must be `None`")

    def short_description(self):
        return 'None'

    def long_description(self):
        return 'value must be `None`'


class EnsureCallable(Constraint):
    """Ensure an input is of value `None`"""
    def __call__(self, value):
        if hasattr(value, '__call__'):
            return value
        else:
            raise ValueError("value must be a callable")

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
            raise ValueError(f"value {value} is not one of {self._allowed}")
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
            raise ValueError("value not dict-like")
        super(EnsureKeyChoice, self).__call__(value[self._key])
        return value

    def long_description(self):
        return "value in '%s' must be one of %s" % (self._key, str(self._allowed),)

    def short_description(self):
        return '%s:{%s}' % (self._key, ', '.join([str(c) for c in self._allowed]))


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
        if self._min == self._max == None:
            raise ValueError('No range given, min == max == None')
        super(EnsureRange, self).__init__()

    def __call__(self, value):
        if self._min is not None:
            if value < self._min:
                raise ValueError("value must be at least %s" % (self._min,))
        if self._max is not None:
            if value > self._max:
                raise ValueError("value must be at most %s" % (self._max,))
        return value

    def long_description(self):
        return self.short_description()

    def short_description(self):
        if self._max is None:
            return f'not less than {self._min}'
        elif self._min is None:
            return f'not greater than {self._max}'
        else:
            # it is inclusive, but spelling it out would be wordy
            return f'between {self._min} and {self._max}'


class EnsurePath(Constraint):
    """Ensures input is convertible to a (platform) path and returns a `Path`

    Optionally, the path can be tested for existence and whether it is absolute
    or relative.
    """
    def __init__(self,
                 path_type: type = Path,
                 is_format: str or None = None,
                 lexists: bool or None = None,
                 is_mode: callable = None,
                 ref: Path = None,
                 ref_is: str = 'parent-or-same-as'):
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
          need not point to an existing path to fullfil the "exists" condition.
        is_mode:
          If set, this callable will receive the path's `.lstat().st_mode`,
          and an exception is raised, if the return value does not evaluate
          to `True`. Typical callables for this feature are provided by the
          `stat` module, e.g. `S_ISDIR()`
        ref:
          If set, defines a reference Path any given path is compared to. The
          comparison operation is given by `ref_is`.
        ref_is: {'parent-or-identical'}
          Comparison operation to perform when `ref` is given.
        """
        super().__init__()
        self._path_type = path_type
        self._is_format = is_format
        self._lexists = lexists
        self._is_mode = is_mode
        self._ref = ref
        self._ref_is = ref_is

    def __call__(self, value):
        path = self._path_type(value)
        mode = None
        if self._lexists is not None or self._is_mode is not None:
            try:
                mode = path.lstat().st_mode
            except FileNotFoundError:
                # this is fine, handled below
                pass
        if self._lexists is not None:
            if self._lexists and mode is None:
                raise ValueError(f'{path} does not exist')
            elif not self._lexists and mode is not None:
                raise ValueError(f'{path} does (already) exist')
        if self._is_format is not None:
            is_abs = path.is_absolute()
            if self._is_format == 'absolute' and not is_abs:
                raise ValueError(f'{path} is not an absolute path')
            elif self._is_format == 'relative' and is_abs:
                raise ValueError(f'{path} is not a relative path')
        if self._is_mode is not None:
            if not self._is_mode(mode):
                raise ValueError(f'{path} does not match desired mode')
        if self._ref:
            ok = True
            if self._ref_is == 'parent-or-same-as':
                ok = (path == self._ref or self._ref in path.parents)
            elif self._ref_is == 'parent-of':
                ok = self._ref in path.parents
            else:
                raise ValueError('Unknown `ref_is` operation label')

            if not ok:
                raise ValueError(
                    f'{self._ref} is not {self._ref_is} {path}')
        return path

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


class EnsureMapping(Constraint):
    """Ensure a mapping of a key to a value of a specific nature"""

    def __init__(self,
                 key: Constraint,
                 value: Constraint,
                 delimiter: str = ':'):
        """
        Parameters
        ----------
        key:
          Key constraint instance.
        value:
          Value constraint instance.
        delimiter:
          Delimiter to use for splitting a key from a value for a `str` input.
        """
        super().__init__()
        self._key_constraint = key
        self._value_constraint = value
        self._delimiter = delimiter

    def short_description(self):
        return 'mapping of {} -> {}'.format(
            self._key_constraint.short_description(),
            self._value_constraint.short_description(),
        )

    def __call__(self, value) -> Dict:
        # determine key and value from various kinds of input
        if isinstance(value, str):
            # will raise if it cannot split into two
            key, val = value.split(sep=self._delimiter, maxsplit=1)
        elif isinstance(value, dict):
            if not len(value):
                raise ValueError('dict does not contain a key')
            elif len(value) > 1:
                raise ValueError(f'{value} contains more than one key')
            key, val = value.copy().popitem()
        elif isinstance(value, (list, tuple)):
            if not len(value) == 2:
                raise ValueError('key/value sequence does not have length 2')
            key, val = value

        key = self._key_constraint(key)
        val = self._value_constraint(val)
        return {key: val}

    def for_dataset(self, dataset: aDataset):
        # tailor both constraints to the dataset and reuse delimiter
        return EnsureMapping(
            key=self._key_constraint.for_dataset(dataset),
            value=self._value_constraint.for_dataset(dataset),
            delimiter=self._delimiter,
        )
