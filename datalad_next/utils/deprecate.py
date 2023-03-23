from __future__ import annotations

from collections.abc import Callable
from functools import wraps
import warnings

__all__ = ['deprecated']


_base_tmpl = "{mod}.{func} was deprecated in version {version}. {msg}"
_kwarg_tmpl = f"argument {{kwarg!r}} of {_base_tmpl}"
_kwarg_val_tmpl = f"Use of values {{kwarg_values!r}} for {_kwarg_tmpl}"


# we must have a secret value that indicates "no value deprecation", otherwise
# we cannot tell whether `None` is deprecated or not
class _NoDeprecatedValue:
    pass


def deprecated(
        msg: str,
        *,
        version: str,
        kwarg: str | None = None,
        kwarg_values: list | _NoDeprecatedValue = _NoDeprecatedValue,
) -> Callable:
    """Annotate functions, classes, or (required) keyword-arguments
    with standardized deprecation warnings.

    Support for deprecation messages on individual keyword arguments
    is limited to calls with explicit keyword-argument use, not (implicit)
    use as a positional argument.

    Parameters
    ----------
    msg: str
      Custom message to append to a deprecation warning
    version: str
      Software version number at which the deprecation was made
    kwarg: str, optional
      Name of the particular deprecated keyword argument (instead of entire
      function/class)
    kwarg_values: list, optional
      Particular deprecated values of the specified keyword-argument
    """
    # normalize to a set(), when the set is empty, no particular value
    # was deprecated
    kwarg_values = set() \
        if kwarg_values is _NoDeprecatedValue else set(kwarg_values)

    def decorator(func):
        @wraps(func)
        def func_with_deprecation_warning(*args, **kwargs):
            # this is the layer that run for a deprecated call

            # do we have a deprecated kwargs, but it has not been used?
            # -> quick way out
            if kwarg is not None and kwarg not in kwargs.keys():
                # there is nothing to deprecate
                return func(*args, **kwargs)

            # pick the right message template
            # whole thing deprecated, or kwargs, or particular kwarg-value
            template = _base_tmpl if kwarg is None \
                else _kwarg_tmpl if not kwarg_values \
                else _kwarg_val_tmpl

            # deprecated value to compare against
            val = kwargs.get(kwarg, _NoDeprecatedValue)

            # comprehensive set of conditions when to issue deprecation
            # warning
            # - no particular kwarg is deprecated, but the whole callable
            # - no particular value is deprecated, but the whole argument
            # - given value matches any deprecated value
            # - given list/tuple/dict-keys match any deprecated value
            if (# no particular kwarg is deprecated, but the whole callable
                kwarg is None
                # no particular value is deprecated, but the whole argument
                or not kwarg_values
                # given value matches any deprecated value
                # exluce tuple/list, because they are not hashable
                or (not isinstance(val, (list, dict))
                    and val in kwarg_values)
                # given list/tuple-item or dict-key match any deprecated value
                or (isinstance(val, (tuple, list, dict))
                    and kwarg_values.intersection(val))
            ):
                warnings.warn(
                    template.format(
                        mod=func.__module__,
                        func=func.__name__,
                        kwarg=kwarg,
                        kwarg_values=kwarg_values,
                        version=version,
                        msg=msg,
                    ),
                    DeprecationWarning,
                )
            return func(*args, **kwargs)

        return func_with_deprecation_warning

    return decorator
