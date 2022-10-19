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


from .api import Constraint
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
        if hasattr(value, '__iter__') and \
                not (isinstance(value, (bytes, str))):
            return list(map(self._dtype, value))
        else:
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


class EnsureListOf(Constraint):
    """Ensure that an input is a list of a particular data type
    """
    def __init__(self, dtype):
        """
        Parameters
        ----------
        dtype : functor
        """
        self._dtype = dtype
        super(EnsureListOf, self).__init__()

    def __call__(self, value):
        return list(map(self._dtype, value))

    def short_description(self):
        return 'list(%s)' % _type_str(self._dtype)

    def long_description(self):
        return "value must be convertible to %s" % self.short_description()


class EnsureTupleOf(Constraint):
    """Ensure that an input is a tuple of a particular data type
    """
    def __init__(self, dtype):
        """
        Parameters
        ----------
        dtype : functor
        """
        self._dtype = dtype
        super(EnsureTupleOf, self).__init__()

    def __call__(self, value):
        return tuple(map(self._dtype, value))

    def short_description(self):
        return 'tuple(%s)' % _type_str(self._dtype)

    def long_description(self):
        return "value must be convertible to %s" % self.short_description()


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
    """Ensure an input is a string.

    No automatic conversion is attempted.
    """
    def __init__(self, min_len=0):
        """
        Parameters
        ----------
        min_len: int, optional
           Minimal length for a string.
        """
        assert min_len >= 0
        self._min_len = min_len
        super(EnsureStr, self).__init__()

    def __call__(self, value):
        if not isinstance(value, (bytes, str)):
            # do not perform a blind conversion ala str(), as almost
            # anything can be converted and the result is most likely
            # unintended
            raise ValueError("%s is not a string" % repr(value))
        if len(value) < self._min_len:
            raise ValueError("%r is shorter than of minimal length %d"
                             % (value, self._min_len))
        return value

    def long_description(self):
        return 'value must be a string'

    def short_description(self):
        return 'str'


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
        min_str = '-inf' if self._min is None else str(self._min)
        max_str = 'inf' if self._max is None else str(self._max)
        return 'value must be in range [%s, %s]' % (min_str, max_str)

    def short_description(self):
        return None
