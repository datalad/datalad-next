import warnings
from functools import wraps


def deprecated(msg, version, kwarg=None, kwarg_choice=None):
    """Annotate functions, classes, or keyword-arguments with standardized
    deprecation warnings.

    Parameters
    ----------
    version: str
      Software version number at which the deprecation was made
    msg: str
      Custom message to append to a deprecation warning
    kwarg: str
      Individual keyword argument being deprecated (instead of entire
      function/class)
    kwarg_choice: str
      Individual choice of the specified keyword-argument being deprecated
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

    # a single kwarg, or kwarg choice is deprecated
    def decorator(func):
        @wraps(func)
        def func_with_deprecation_warning(*args, **kwargs):
            # has a deprecated kwarg been used?
            if kwarg not in kwargs.keys():
                # there is nothing to deprecate
                return func(*args, **kwargs)
            # has a deprecated kwarg choice been used?
            if kwarg_choice is not None:
                val = kwargs[kwarg]
                if isinstance(val, list):
                    if kwarg_choice not in val:
                        # there is nothing to deprecate
                        return func(*args, **kwargs)
                elif isinstance(kwarg_choice, list):
                    if val not in kwarg_choice:
                        # there is nothing to deprecate
                        return func(*args, **kwargs)
                else:
                    if val != kwarg_choice:
                        # there is nothing to deprecate
                        return func(*args, **kwargs)
                template = (
                    f"Use of value {kwarg_choice!r} for "
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
