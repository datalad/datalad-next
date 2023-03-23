import warnings
from functools import wraps


def deprecated(msg, version, kwarg=None, kwarg_value=None):
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

    base_template = "{func} was deprecated in version {version}. {msg}"

    if kwarg is None:
        # the entire class/function is deprecated
        def decorator(func):
            @wraps(func)
            def func_with_deprecation_warning(*args, **kwargs):
                warnings.warn(
                    base_template.format(
                        func=f'{func.__module__}.{func.__name__}',
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
            # has a deprecated kwarg been used?
            if kwarg not in kwargs.keys():
                # there is nothing to deprecate
                return func(*args, **kwargs)
            # has a deprecated kwarg value been used?
            if kwarg_value is not None:
                val = kwargs[kwarg]
                if isinstance(val, list):
                    if kwarg_value not in val:
                        # there is nothing to deprecate
                        return func(*args, **kwargs)
                elif isinstance(kwarg_value, list):
                    if val not in kwarg_value:
                        # there is nothing to deprecate
                        return func(*args, **kwargs)
                else:
                    if val != kwarg_value:
                        # there is nothing to deprecate
                        return func(*args, **kwargs)
                template = (
                    f"Use of value {kwarg_value!r} for "
                    f"argument {kwarg!r} of " + base_template
                )
            else:
                template = "The {kwarg!r} parameter of " + base_template
            warnings.warn(
                template.format(
                    kwarg=kwarg,
                    func=f'{func.__module__}.{func.__name__}',
                    version=version,
                    msg=msg,
                ),
                DeprecationWarning,
            )
            return func(*args, **kwargs)

        return func_with_deprecation_warning

    return decorator
