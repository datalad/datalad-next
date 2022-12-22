from __future__ import annotations

from importlib import import_module
import logging
from typing import Any

lgr = logging.getLogger('datalad.ext.next.utils.patch')


def apply_patch(
        modname: str,
        objname: str | None,
        attrname: str,
        patch: Any,
        msg: str | None = None,
        expect_attr_present=True,
):
    """
    Monkey patch helper

    Parameters
    ----------
    modname: str
      Importable name of the module with the patch target.
    objname: str or None
      If `None`, patch will target an attribute of the module,
      otherwise an object name within the module can be specified.
    attrname: str
      Name of the attribute to replace with the patch, either
      in the module or the given object (see ``objname``)
    patch:
      The identified attribute will be replaced with this object.
    msg: str or None
      If given, a debug-level log message with this text will be
      emitted, otherwise a default message is generated.
    expect_attr_present: bool
      If True (default) an exception is raised when the target
      attribute is not found.

    Returns
    -------
    object
      The original, unpatched attribute -- if ``expect_attr_present``
      is enabled, or ``None`` otherwise.

    Raises
    ------
    ImportError
      When the target module cannot be imported
    AttributedError
      When the target object is not found, or the target attribute is
      not found.
    """
    orig_attr = None

    msg = msg or f'Apply patch to {modname}.{attrname}'
    lgr.debug(msg)

    # we want to fail on ImportError
    mod = import_module(modname, package='datalad')
    if objname:
        # we want to fail on a missing object
        obj = getattr(mod, objname)
    else:
        # the target is the module itself
        obj = mod

    if expect_attr_present:
        # we want to fail on a missing attribute/object
        orig_attr = getattr(obj, attrname)

    setattr(obj, attrname, patch)

    return orig_attr
