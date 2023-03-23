import warnings
from functools import wraps

__all__ = ['deprecated']


_base_tmpl = "{mod}.{func} was deprecated in version {version}. {msg}"
_kwarg_tmpl = f"argument {{kwarg!r}} of {_base_tmpl}"
_kwarg_val_tmpl = f"Use of value {{kwarg_value!r}} for {_kwarg_tmpl}"


# we must have a secret value that indicates "no value deprecation", otherwise
# we cannot tell whether `None` is deprecated or not
class _NoDeprecatedValue:
    pass


def deprecated(msg, version, kwarg=None, kwarg_value=_NoDeprecatedValue):
    """Annotate functions, classes, or (required) keyword-arguments
    with standardized deprecation warnings.

    Support for deprecation messages on individual keyword arguments
    is limited to calls with explicit keyword-argument use, not (implicit)
    use as a positional argument.

    Parameters
    ----------
    version: str
      Software version number at which the deprecation was made
    msg: str
      Custom message to append to a deprecation warning
    kwarg: str
      Name of the particular deprecated keyword argument (instead of entire
      function/class)
    kwarg_value: str
      Particular deprecated value of the specified keyword-argument
    """

    if kwarg is None:
        # the entire class/function is deprecated
        def decorator(func):
            @wraps(func)
            def func_with_deprecation_warning(*args, **kwargs):
                warnings.warn(
                    _base_tmpl.format(
                        mod=func.__module__,
                        func=func.__name__,
                        version=version,
                        msg=msg,
                    ),
                    DeprecationWarning,
                )
                return func(*args, **kwargs)

            return func_with_deprecation_warning

        return decorator

    # a single kwarg, or kwarg value is deprecated
    def decorator(func):
        @wraps(func)
        def func_with_deprecation_warning(*args, **kwargs):
            # this is the layer that run for a deprecated call

            # has a deprecated kwarg been used? if not, quick way out
            if kwarg not in kwargs.keys():
                # there is nothing to deprecate
                return func(*args, **kwargs)

            # has a deprecated kwarg value been used?
            # pick the right message template
            template = _kwarg_tmpl if kwarg_value is _NoDeprecatedValue \
                else _kwarg_val_tmpl

            # deprecated value to compare against, we know it is in kwargs
            val = kwargs[kwarg]

            # comprehensive set of conditions when to issue deprecation
            # warning
            # - no particular value is deprecated, but the whole argument
            # - given list contains deprecated value
            # - given value in list of deprecated value
            # - given value matches deprecated value
            if kwarg_value is _NoDeprecatedValue \
                    or (isinstance(val, list) and kwarg_value in val) \
                    or (isinstance(kwarg_value, list) and val in kwarg_value) \
                    or val == kwarg_value:
                warnings.warn(
                    template.format(
                        mod=func.__module__,
                        func=func.__name__,
                        kwarg=kwarg,
                        kwarg_value=kwarg_value,
                        version=version,
                        msg=msg,
                    ),
                    DeprecationWarning,
                )
            return func(*args, **kwargs)

        return func_with_deprecation_warning

    return decorator
