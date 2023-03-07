"""Constraints that wrap or contain other constraints"""

from __future__ import annotations

from pathlib import Path
import sys
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Tuple,
)

from .base import (
    Constraint,
    DatasetParameter,
)

from .basic import NoConstraint

class EnsureIterableOf(Constraint):
    """Ensure that an input is a list of a particular data type
    """
    # TODO support a delimiter to be able to take str-lists?
    def __init__(self,
                 iter_type: type,
                 item_constraint: Callable,
                 min_len: int | None = None,
                 max_len: int | None = None,
                 delimiter: str | None = None):
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
        self._delimiter = delimiter
        super().__init__()

    def __repr__(self):
        # not showing iter_type here, will come via class.name
        # in general
        return (
            f'{self.__class__.__name__}('
            f'item_constraint={self._item_constraint!r}'
            f', min_len={self._min_len!r}'
            f', max_len={self._max_len!r}'
            f', delimiter={self._delimiter!r})'
        )

    @property
    def item_constraint(self):
        return self._item_constraint

    def __call__(self, value):
        if self._delimiter is not None and isinstance(value, str):
            value = self._split_values(value)
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

    def _split_values(self, value):
        return value.split(sep=self._delimiter)

    def short_description(self):
        return f'{self._iter_type}({self._item_constraint})'


class EnsureListOf(EnsureIterableOf):
    def __init__(self,
                 item_constraint: Callable,
                 min_len: int | None = None,
                 max_len: int | None = None):
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
                 item_constraint: Callable,
                 min_len: int | None = None,
                 max_len: int | None = None):
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


class EnsureNTuple(EnsureIterableOf):
    """Ensure that an input is a tuple of arbitrary length, with a specific
     constraint for each item."""

    def __init__(self,
                 itemconstraints: List,
                 delimiter: str | None = None):
        """
        Parameters
        ----------
        itemconstraints: list
          A list of itemconstraints to be applied. Must match the number
          of input values, as itemconstraints will be mapped to values.
        """
        super().__init__(tuple, item_constraint=NoConstraint(),
                         min_len=len(itemconstraints),
                         max_len=len(itemconstraints),
                         delimiter=delimiter)
        self._itemconstraints = itemconstraints
        self._delimiter = delimiter

    def __repr__(self):
        return (
            f'{self.__class__.__name__}('
            f'itemconstraints={self._itemconstraints!r})'
        )

    def short_description(self):
        return 'mapping to the following constraints: {}'.format(
            [constraint.short_description() for constraint in
             self._itemconstraints],
        )

    def __call__(self, value) -> Tuple:
        if self._delimiter is not None and isinstance(value, str):
            value = self._split_values(value)
        if hasattr(value, '__len__'):
            if len(value) != len(self._itemconstraints):
                raise ValueError(f'Number of provided values ({value}) does not '
                                 f'match the number of defined constraints '
                                 f'({self._itemconstraints}')
        else:
            # float or ints have no len. the number of constraints must now be 1
            if len(self._itemconstraints) > 1:
                raise ValueError(f'The number of item constraints'
                                 f' ({self._itemconstraints}) exceeds the'
                                 f' number of provided values ({value}')
        return tuple(constraint(val) for val, constraint in
                     zip(value, self._itemconstraints))


class EnsureMapping(Constraint):
    """Ensure a mapping of a key to a value of a specific nature"""

    def __init__(self,
                 key: Constraint,
                 value: Constraint,
                 delimiter: str = ':',
                 allow_length2_sequence: bool = True):
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
        self._allow_length2_sequence = allow_length2_sequence

    def __repr__(self):
        return (
            f'{self.__class__.__name__}('
            f'key={self._key_constraint!r}'
            f', value={self._value_constraint!r}'
            f', delimiter={self._delimiter!r})'
        )

    def short_description(self):
        return 'mapping of {} -> {}'.format(
            self._key_constraint.short_description(),
            self._value_constraint.short_description(),
        )

    def _get_key_value(self, value) -> tuple:
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
        elif self._allow_length2_sequence and isinstance(value, (list, tuple)):
            if not len(value) == 2:
                raise ValueError('key/value sequence does not have length 2')
            key, val = value
        else:
            raise ValueError(f'Unsupported data type for mapping: {value!r}')

        return key, val

    def __call__(self, value) -> Dict:
        key, val = self._get_key_value(value)
        key = self._key_constraint(key)
        val = self._value_constraint(val)
        return {key: val}

    def for_dataset(self, dataset: DatasetParameter) -> Constraint:
        # tailor both constraints to the dataset and reuse delimiter
        return EnsureMapping(
            key=self._key_constraint.for_dataset(dataset),
            value=self._value_constraint.for_dataset(dataset),
            delimiter=self._delimiter,
        )


class EnsureMappings(Constraint):
    """Ensure several mappings of keys to values of a specific nature"""

    def __init__(self,
                 key: Constraint,
                 value: Constraint,
                 delimiter: str = ':',
                 pair_delimiter: str = ',',
                 allow_length2_sequence: bool = True
                 ):
        """
        Parameters
        ----------
        key:
          Key constraint instance.
        value:
          Value constraint instance.
        delimiter:
          Delimiter to use for splitting a key from a value for a `str` input.
        pair_delimiter:
          Delimiter to use for splitting key-value pairs from another for a str
          input.
        """
        super().__init__()
        self._key_constraint = key
        self._value_constraint = value
        self._delimiter = delimiter
        self._pair_delimiter = pair_delimiter
        self._allow_length2_sequence: bool = True

    def short_description(self):
        return 'mapping of {} -> {}'.format(
            self._key_constraint.short_description(),
            self._value_constraint.short_description(),
        )

    def _get_keys_values(self, value) -> tuple:
        # determine key and value from various kinds of input
        if isinstance(value, str):
            # split into pairs
            # TODO: what about whitespace? e.g., 'key: val', 'morekey': moreval'
            pairs = value.split(sep=self._pair_delimiter)
            keys_n_values = [p.split(sep=self._delimiter, maxsplit=1) for p in pairs]
            try:
                keys = [p[0] for p in keys_n_values]
                vals = [p[1] for p in keys_n_values]
            except IndexError as e:
                raise(ValueError('Could not cast input to key value pairs'))
        elif isinstance(value, dict):
            if not len(value):
                raise ValueError('dict does not contain a key')
            keys = list(value.keys())
            vals = list(value.values())
        elif self._allow_length2_sequence and isinstance(value, (list, tuple)):
            if not len(value) % 2 == 0 or len(value) < 2:
                raise ValueError('sequence can not be chunked into pairs by 2')
            keys = value[::2]
            vals = value[1::2]
        else:
            raise ValueError(f'Unsupported data type for mapping: {value!r}')

        return keys, vals

    def __call__(self, value) -> Dict:
        keys, vals = self._get_keys_values(value)
        keys = [self._key_constraint(k) for k in keys]
        vals = [self._value_constraint(v) for v in vals]
        return dict(zip(keys, vals))

    def for_dataset(self, dataset: DatasetParameter) -> Constraint:
        # tailor both constraints to the dataset and reuse delimiter
        return EnsureMappings(
            key=self._key_constraint.for_dataset(dataset),
            value=self._value_constraint.for_dataset(dataset),
            delimiter=self._delimiter,
            pair_delimiter=self._pair_delimiter,
            allow_length2_sequence=self._allow_length2_sequence
        )


class EnsureGeneratorFromFileLike(Constraint):
    """Ensure a constraint for each item read from a file-like.

    A given value can either be a file-like (the outcome of `open()`,
    or `StringIO`), or `-` as an alias of STDIN, or a path to an
    existing file to be read from.
    """

    def __init__(
        self,
        item_constraint: Callable,
        exc_mode: str = 'raise',
    ):
        """
        Parameters
        ----------
        item_constraint:
          Each incoming item will be mapped through this callable
          before being yielded by the generator.
        exc_mode: {'raise', 'yield'}, optional
          How to deal with exceptions occurring when processing
          individual lines/items. With 'yield' the respective
          exception instance is yielded, and processing continues.
          A caller can then decide whether to ignore, report, or raise
          the exception. With 'raise', an exception is raised immediately
          and processing stops.
        """
        assert exc_mode in ('raise', 'yield')
        self._item_constraint = item_constraint
        self._exc_mode = exc_mode
        super().__init__()

    def __repr__(self):
        # not showing iter_type here, will come via class.name
        # in general
        return (
            f'{self.__class__.__name__}('
            f'item_constraint={self._item_constraint!r})'
        )

    def short_description(self):
        return \
            f'items of type "{self._item_constraint.short_description()}" ' \
            'read from a file-like'

    def __call__(self, value) -> Generator[Any, None, None]:
        # we only support a single file-like source. If we happend to get
        # a length-1 sequence (for technical reasons, such as argparse
        # having collected the value), we unpack it.
        if isinstance(value, (list, tuple)) and len(value) == 1:
            value = value[0]
        opened_file = False
        if value == '-':
            value = sys.stdin
        elif isinstance(value, (str, Path)):
            # we covered the '-' special case, so this must be a Path
            path = Path(value) if not isinstance(value, Path) else value
            if not path.is_file():
                raise ValueError(f'{value} is not an existing file')
            value = path.open()
            opened_file = True
        return self._item_yielder(value, opened_file)

    def _item_yielder(self, fp, close_file):
        try:
            for line in fp:
                try:
                    yield self._item_constraint(
                        # splitlines() removes the newline at the end of
                        # the string that is left in by __iter__()
                        line.splitlines()[0]
                    )
                except Exception as e:
                    if self._exc_mode == 'raise':
                        raise
                    else:
                        yield e
        finally:
            if close_file:
                fp.close()


class ConstraintWithPassthrough(Constraint):
    """Regular contraint, but with a "pass-through" value that is not processed

    This is different from a `Constraint() | EnsureValue(...)` construct,
    because the pass-through value is not communicated. This can be useful
    when a particular value must be supported for technical reasons, but
    need not, or must not be included in (error) messages.

    The pass-through is returned as-is, and is not processed except for an
    identity check (`==`).

    For almost all reporting (`__str__`, descriptions, ...) the wrapped
    value constraint is used, making this class virtually invisible.
    Only ``__repr__`` reflects the wrapping.
    """
    def __init__(self,
                 constraint: Constraint,
                 passthrough: Any):
        """
        Parameters
        ----------
        constraint: Constraint
          Any ``Constraint`` subclass instance that will be used to validate
          values.
        passthrough:
          A value that will not be subjected to validation by the value
          constraint, but is returned as-is. This value is not copied.
          It is a caller's responsibility to guarantee immutability if that
          is desired.
        """
        super().__init__()
        self._constraint = constraint
        self._passthrough = passthrough

    @property
    def constraint(self) -> Constraint:
        """Returns the wrapped constraint instance"""
        return self._constraint

    @property
    def passthrough(self) -> Any:
        """Returns the set pass-through value"""
        return self._passthrough

    def __call__(self, value) -> Any:
        if value == self._passthrough:
            val = value
        else:
            val = self._constraint(value)
        return val

    def __str__(self) -> str:
        return self._constraint.__str__()

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}' \
               f'({self._constraint!r}, passthrough={self._passthrough!r})'

    def for_dataset(self, dataset: DatasetParameter) -> Constraint:
        """Wrap the wrapped constraint again after tailoring it for the dataset

        The pass-through value is re-used.
        """
        return self.__class__(
            self._constraint.for_dataset(dataset),
            passthrough=self._passthrough,
        )

    def long_description(self) -> str:
        return self._constraint.long_description()

    def short_description(self) -> str:
        return self._constraint.short_description()
