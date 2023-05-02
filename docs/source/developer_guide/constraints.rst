.. _constraints:

``datalad-next``'s Constraint System
************************************

``datalad_next.constraints`` implements a system to perform data validation, coercion, and parameter documentation for commands via a flexible set of "Constraints".
You can find an overview of available Constraints in the respective module overview of the :ref:`pyutils`.

Adding parameter validation to a command
----------------------------------------

In order to equip an existing or new command with the constraint system, the following steps are required:

* Set the commands base class to ``ValidatedInterface``:

.. code-block:: python

   from datalad_next.commands import ValidatedInterface

   @build_doc
   class MyCommand(ValidatedInterface):
       """Download from URLs"""

* Declare a ``_validator_`` class member:

.. code-block:: python

   from datalad_next.commands import (
       EnsureCommandParameterization,
       ValidatedInterface,
   )

   @build_doc
   class MyCommand(ValidatedInterface):
       """Download from URLs"""

   _validator_ = EnsureCommandParameterization(dict(
        [...]
    ))


* Determine for each parameter of the command whether it has constraints, and what those constraints are.
  If you're transitioning an existing command, remove any ``constraints=`` declaration in the ``_parameter_`` class member.
* Add a fitting Constraint declaration for each parameter into the ``_validator_`` as a key-value pair where the key is the parameter and its value is a Constraint.
  There does not need to be a Constraint per parameter; only add entries for parameters that need validation.

.. code-block:: python

   from datalad_next.commands import (
       EnsureCommandParameterization,
       ValidatedInterface,
   )
   from datalad_next.constraints import EnsureChoice
   from datalad_next.constraints.dataset import EnsureDataset

   @build_doc
   class Download(ValidatedInterface):
       """Download from URLs"""

   _validator_ = EnsureCommandParameterization(dict(
        dataset=EnsureDataset(installed=True),
        force=EnsureChoice('yes','no','maybe'),
    ))

Combining constraints
"""""""""""""""""""""

Constraints can be combined in different ways.
The ``|``, ``&``, and ``()`` operators allow ``AND``, ``OR``, and grouping of Constraints.
The following example from the ``download`` command defines a chain of possible Constraints:

.. code-block:: python

   spec_item_constraint = url2path_constraint | (
        (
            EnsureJSON() | EnsureURLFilenamePairFromURL()
        ) & url2path_constraint)

Constrains can also be combined using ``AnyOf`` or ``AllOf`` MultiConstraints, which correspond almost entirely to ``|`` and ``&``.
Here's another example from the ``download`` command:

.. code-block:: python

    spec_constraint = AnyOf(
        spec_item_constraint,
        EnsureListOf(spec_item_constraint),
        EnsureGeneratorFromFileLike(
            spec_item_constraint,
            exc_mode='yield',
        ),

One can combine an arbitrary number of Constraints.
They are evaluated in the order in which they were specified.
Logical OR constraints will return the value from the first constraint that does not raise an exception, and logical AND constraints pass the return values of each constraint into the next.

Implementing additional constraints
-----------------------------------

TODO

Parameter Documentation
-----------------------

TODO

