.. _generator_runner:


How to use ``datalad``'s runner with Generator Protocols
********************************************************

``datalad_next`` executes a lot of subprocesses to do its work. This is performed by the runner class: ``datalad_next.runner.ThreadedRunner`` and a small shell around it: ``datalad_next.runner.Runner`` (we refer to the latter as "runner" throughout the rest of the document).

This document is intended for ``datalad``-developers who want to understand how the runner code works and how they can use it to efficiently execute subprocesses or to build higher level tools, e.g. batch-command classes or context-manager that provide a running subprocess.


General functionality
=====================
The runner will execute a subprocess and communicate with it. Communication means that it might send input the the stdin-descriptor of the subprocess, if that is specified, and read output from the stderr- and stdout-descriptors of the subprocess, if that is specified.

All input to the subprocess is directly written the stdin-descriptor of the subprocess.
The output of the subprocess is passed to an instance of a `Protocol`-class in the thread that executed the ``run``-method of the runner. The protocol classes are modelled after the class ``asyncio.protocol.SubprocessProtocol``. It provides callbacks for the following events:

- data received from stdout or stderr of the subprocess: ``pipe_data_received``
- data connection to subprocess closed: ``connection_lost``
- subprocess exited
- timeout occurred

When the subprocess is executed the runner will invoke the appropriate callbacks.

``datalad_next`` provides some pre-made protocols-classes that do thinks like, returning a dictionary with all stdout- and stderr-output of the subprocess. But you are free to implement your own protocol classes that could implement any operation, e.g. calculate a letter-histogram, trace response times, send all output to a file, or send all output into another process.


Operation modes
---------------

The previous section described the general interaction between a runner and a protocol instance. In ``datalad_next`` the protocol classes come in two flavors that determine when the runner returns control to its caller and how the results are processed. The two flavors are `synchronous` and `asynchronous`. We also refer to the synchronous behavior are `return-based` and to the asynchronous behavior as `generator-based`.

The following section will shortly describe the synchronous flavor, before the remainder of the document focuses on the asynchronous flavor.


Synchronous runner execution
----------------------------

In the synchronous mode, the runner is started with a protocol class that inherits from ``datalad_next.runners.Protocol``, but not from the mixin-class ``datalad_next.runners.GeneratorMixIn``. The runner will create an instance of the protocol-class, start the subprocess and call the appropriate callbacks whenever data is received from the subprocess. If no exception occurs, it will continue to do that until the subprocess exits.
After the process exited, the runner will call a callback on the protocol instance that is specific to `synchronous` execution: ``_prepare_result(self) -> dict:``.
This callback performs whatever operations it needs to assemble some result of the subprocess execution and return that result as a dictionary.

For example, the protocol ``datalad_next.runners.StdOutErrCapture`` will store all stdout- and stderr-output from the subprocess until the subprocess exits. When ``datalad_next.runners.StdOutErrCapture._prepare_result`` is called, it decodes the bytes that it received and return a dictionary containing the decoded bytes from stdout, from stderr, and the process exit code.

This flavor is called `synchronous` because the ``run``-method of the runner will only exit after the subprocess has exited. It will return the result of the invocation of ``_prepare_result``.

While this protocol and its siblings ``datalad_next.runners.StdOutCapture``, ``datalad_next.runners.StdErrCapture``, and ``datalad_next.runners.NoCapture``, are useful out-of-the-box, an obvious extension would be a protocol that returns un-decoded subprocess output, i.e. bytes, to the caller. This could be implemented by sub-classing ``datalad_next.runners.Protocol`` and overwriting the method ``_prepare_result`` to not decode the received data.


Asynchronous runner execution
-----------------------------

In the asynchronous mode, the runner is started with a protocol class that inherits from from the mixin-class ``datalad_next.runners.GeneratorMixIn``. The protocol class should of course also provide all callbacks defined in ``datalad_next.runners.Protocol``. So it could inherit from ``datalad_next.runners.GeneratorMixIn`` and a subclass of ``datalad_next.runners.Protocol``, e.g. ``datalad_next.runners.StdOutErrCapture``.

How does the asynchronous operation work? The following picture gives an overview of the elements in the runner and will help to explain the ins and outs of the operation of the runner:

.. image:: docs/source/developer_guide/runner_arch.png


The right side of the figure shows three threads that are interacting with the subprocess.
Two of those threads, i.e. "Stdout Reader Thread" and "Stderr Reader Thread" read stdout and stderr from the subprocess and enqueue all data they read into the "From Process Queue".
The third thread, i.e. "Stdin Writer Thread", reads all data from the "Stdin Queue" and writes that data to stdin of the subprocess (the "Stdin Queue" could have been named "To Process Queue", but in the source code it is often named ``stdin_queue``).
The execution of these threads is independent from the execution of the calling thread, i.e. the thread that executes the ``run``-method of the runner.
In the figure the calling thread is identified as "Main Thread".


If the protocol that is provided to the ``run``-method inherits from ``GeneratorMixIn``, the ``run``-method behaves quite differently from the synchronous case described in a previous section.
Instead of blocking, it immediately returns a generator object to the caller, i.e. "Result Generator" in the figure.
Whenever the caller invokes ``send()`` on the generator, e.g. by iterating over it or by calling ``next(generator)``, the generator will do the following:

1. Check whether its internal result queue, i.e. "Result Queue" in the figure, has entries.
   If that is the case, it yields the next entry

2. While the "Result Queue" is empty and the subprocess is still running, it will trigger a single step in the "Queue Reading & Timeout Creation"-logic (this logic is also used in the synchronous execution mode). The "Queue Reading & Timeout Creation"-logic might either block or invoke one of the protocol callbacks.
   In the figure, the callbacks ``pipe_data_received`` and ``timeout`` are explicitly named, but any callback would be called from here.
   if the internal result queue is empty, and the process is not running any more, the generator will continue with step 5.

3. The protocol-object might invoke the method ``send_result``.
   This method is defined in the ``GeneratorMixIn``-class. Any object that is passed to ``send_result`` is enqueued into the "Result Queue" of the Generator.
   To be able to do this, the ``GeneratorMixIn``-objects have an attribute that points to the internal result queue of the generator.

4. If the internal result queue of the generator is not empty, the generator will yield the first element of the result queue and go back to step 2.

5. The subprocess has exited, the next call to the ``send()`` method of the generator will retrieve the exit code of the subprocess and end the iteration, i.e. it will raise ``StopIteration``.

From the description above and from the figure it should be clear, that, if a user wants to receive output from a subprocess and not just wait for its exit, the user has to send
data that is received via ``pipe_data_received`` to the result queue of the result generator.
That means basically he has to call ``send_result()``.
This is not automatically mixed into the protocol class by ``GeneratorMixIn``. A minimal generator protocol would therefore look like this.

.. code-block:: python

    from datalad_next.runners import GeneratorMixIn, StdOutErrCapture

    class StdOutErrCaptureGeneratorProtocol(StdOutErrCapture, GeneratorMixIn):
        def __init__(self, done_future=None, encoding=None):
            StdOutCapture.__init__(self, done_future, encoding)
            GeneratorMixIn.__init__(self)

        def pipe_data_received(self, fd: int, data: bytes):
            self.send_result((fd, data))


The elements that a caller would read from the generator would then be tuples where the first element is the source file descriptor and the second elements are the bytes that the subprocess wrote to this descriptor.


.. note::
    Remark: you might not want to inherit from any of the ``datalad_next.runners.Protocol`` subclasses, because they contain code that is never used during asynchronous runner execution.


Programming examples
====================
TODO
