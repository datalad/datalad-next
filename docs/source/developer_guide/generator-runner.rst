.. _generator_runner:


How to use ``datalad``'s runner with Generator Protocols
********************************************************

``datalad_next`` executes a lot of subprocesses to get its work done.
Subprocess execution is done by the runner class, i.e. ``datalad_next.runner.ThreadedRunner`` and a small shell around it: ``datalad_next.runner.Runner`` (we refer to the latter as "runner" throughout the rest of the document).

This document is intended for ``datalad``-developers who want to understand how the runner code works and how they can use it to efficiently execute subprocesses or to build higher level tools, e.g. batch-command classes.


General functionality
=====================
The runner will execute a subprocess and communicate with it. Communication means that it might send input the the stdin-descriptor of the subprocess, if that is specified, and read output from the stderr- and stdout-descriptors of the subprocess, if that is specified.

All input to the subprocess is directly written the stdin-descriptor of the subprocess-object (which is usually the write end of a pipe-like structure).
The output of the subprocess is passed to an instance of a `protocol`-class in the thread that executed the ``run``-method of the runner. The protocol classes are modelled after the class ``asyncio.protocol.SubprocessProtocol``. They provide callbacks for the following events:


- data received from stdout or stderr of the subprocess: ``pipe_data_received``
- data connection lost: ``pipe_connection_lost``
- connection to subprocess established: ``connection_made``
- connection to subprocess closed: ``connection_lost``
- subprocess exited: ``process_exited``
- timeout occurred: ``timeout``

When the subprocess is executed the runner will invoke the appropriate callbacks.

``datalad_next`` provides some pre-defined protocol-classes for common use cases, e.g. ``datalad_next.runners.StdOutErrCapture``, which executes a subprocess and returns a dictionary with the ``stdout``- and ``stderr``-output, and the `return code` (also referred to as `exit status`) of the subprocess.
You are not limited to the existing protocol-classes.
Instead you may implement your own protocol-classes that could implement arbitrary operation, e.g. calculate a letter-histogram, trace response times, send all output to a file, or send all output to another process.


Operation modes
---------------

The previous section described the general interaction between a runner and a protocol instance. In ``datalad_next`` the protocol classes come in two flavors that determine when the runner returns control to its caller and how the results are processed. The two flavors are `synchronous` and `asynchronous`. We also refer to the synchronous behavior are `return-based` and to the asynchronous behavior as `generator-based`.

The following section will shortly describe the synchronous flavor, before the remainder of the document focuses on the asynchronous flavor.


Synchronous runner execution
----------------------------

In synchronous mode, the runner is started with a protocol class that inherits from ``datalad_next.runners.Protocol``, but not from the mixin-class ``datalad_next.runners.GeneratorMixIn``. The runner will create an instance of the protocol-class, start the subprocess and call the appropriate callbacks whenever data is received from the subprocess. If no exception occurs, it will continue to do that until the subprocess exits.
After the process exited, the runner will execute the ``_prepare_result()`` callback on the protocol instance. This callback is specific to the synchronous mode.
It performs whatever operations is required to assemble the result of the subprocess execution and returns that result. The return value of the callback will be returned as result of the synchronous execution.

For example, the protocol ``datalad_next.runners.StdOutErrCapture`` will store all ``stdout``- and ``stderr``-output from the subprocess until the subprocess exits. When ``datalad_next.runners.StdOutErrCapture._prepare_result`` is called, it decodes the bytes that it received and returns a dictionary containing the decoded bytes from ``stdout``, from ``stderr``, and the return code of the process.

This flavor is called `synchronous` because the ``run``-method of the runner will only return control to its caller after the subprocess has exited. It will return the result of the invocation of ``_prepare_result`` as its result.

While this protocol and its siblings ``datalad_next.runners.StdOutCapture``, ``datalad_next.runners.StdErrCapture``, and ``datalad_next.runners.NoCapture``, are useful out-of-the-box, an obvious extension would be a protocol that returns un-decoded subprocess output, i.e. bytes, to the caller. This could be implemented by sub-classing ``datalad_next.runners.Protocol`` and overwriting the method ``_prepare_result`` to not decode the received data.


Asynchronous runner execution
-----------------------------

In the asynchronous mode, the runner is started with a protocol class that inherits from the mixin-class ``datalad_next.runners.GeneratorMixIn``. The protocol class must provide all callbacks defined in ``datalad_next.runners.Protocol``. So it could inherit from ``datalad_next.runners.GeneratorMixIn`` and a subclass of ``datalad_next.runners.Protocol``, e.g. ``datalad_next.runners.StdOutErrCapture``.

How does the asynchronous operation work? The following picture gives an overview of the elements in the runner and will help to explain the ins and outs of the operation of the runner:

.. image:: /_static/runner_arch.png
  :alt: architecture of the runner in asynchronous mode

The right side of the figure shows three threads that are interacting with the subprocess.
Two of those threads, i.e. "Stdout Reader Thread" and "Stderr Reader Thread" read stdout and stderr from the subprocess and enqueue all data they have read into the "From Process Queue".
The third thread, i.e. "Stdin Writer Thread", reads all data from the "Stdin Queue" and writes that data to stdin of the subprocess (the "Stdin Queue" could have been named "To Process Queue", but in the source code it is often named ``stdin_queue``).
If the "Stdin Writer Thread" reads ``None`` from the "Stdin Queue", it will close the stdin-descriptor of the subprocess (usually the writing-end of a pipe-like structure).
The execution of these threads is independent from the execution of the calling thread, i.e. the thread that executes the ``run``-method of the runner.
In the figure the calling thread is identified as "Main Thread".


If the protocol that is provided to the ``run``-method inherits from ``GeneratorMixIn``, the ``run``-method behaves differently from the synchronous case described in a previous section.
Instead of blocking, it immediately returns a generator object to the caller, referred to as "Result Generator" in the figure.
Whenever the caller invokes ``send()`` on the generator, e.g. by iterating over it or by calling ``next(generator)``, the generator will do the following:

1. Check whether its internal result queue, referred to as "Result Queue" in the figure, contains entries.
   If that is the case, it removes the first entry from the result queue and yields it.

2. As long as the "Result Queue" is empty and the subprocess is still running, it will trigger a single step in the "Queue Reading & Timeout Creation"-logic (implementation remark: this logic is also used in the synchronous execution mode).
   The "Queue Reading & Timeout Creation"-logic might either block or invoke one of the protocol callbacks.
   In the figure, the callbacks ``pipe_data_received`` and ``timeout`` are explicitly named, but any callback would be called from here.
   If the internal result queue is empty, and the process is not running any more, the generator will continue with step 5.

3. The protocol-object might invoke the method ``send_result``, which is defined in the ``GeneratorMixIn``-class.
   Any object that is passed to ``send_result`` is enqueued into the "Result Queue" of the Generator.
   To be able to do this, the ``GeneratorMixIn``-objects posses an attribute that points to the internal result queue of the generator.

4. If the internal result queue of the generator is not empty, the generator will yield the first element of the result queue and go back to step 2.

5. The subprocess has exited, the next call to the its ``send()``-method will retrieve the exit status of the subprocess and end the iteration by raising ``StopIteration``.

From the description above and from the figure it should be clear, that, if a user wants to receive output from a subprocess and not just wait for its exit, the user has to send
data that is received via ``pipe_data_received`` to the result queue of the result generator.
That means basically he has to call ``send_result()``.
This is not automatically mixed into the protocol class by ``GeneratorMixIn``.
A minimal generator protocol would therefore look like this.

.. code-block:: python

    from datalad_next.runners import GeneratorMixIn, StdOutErrCapture

    class StdOutErrCaptureGeneratorProtocol(StdOutErrCapture, GeneratorMixIn):
        def __init__(self, done_future=None, encoding=None):
            StdOutCapture.__init__(self, done_future, encoding)
            GeneratorMixIn.__init__(self)

        def pipe_data_received(self, fd: int, data: bytes):
            self.send_result((fd, data))


Given the code above, the elements that a caller would read from the generator would be tuples where the first element is the source file descriptor and the second elements are the bytes that the subprocess wrote to specified file descriptor.


.. note::
    Remark: you might not want to inherit from any of the ``datalad_next.runners.Protocol`` subclasses, because they contain code that is never used during asynchronous runner execution.
    Nevertheless, if you use your own class with the callbacks defined in ``datalad.next.runners.Protocol``, you will have to add the two class variables: ``proc_out``, and ``proc_err`` and set them to ``True``, if you want ``stdout``- or ``stderr``-output to be sent to the "From Process Queue", from which it can eventually be sent to the calling code.


Programming examples
====================


Simple data reading from a subprocess
-------------------------------------

The following code snippet creates a runner with the protocol-class `StdOutCaptureGeneratorProtocol` to read the output of the command ``ls -l /etc``.
The class ``StdOutCapture`` indicates that only ``stdout`` of the subprocess should be captured.
The class ``GeneratorMixIn`` indicates to the runner that it should run in asynchronous mode.

.. code-block:: python

    from datalad_next.runners import Runner, StdOutCaptureGeneratorProtocol as Prot

    for data in Runner().run(cmd=['ls', '-l', '/etc'], protocol=Prot):
        print(data)



Getting the exit code from a subprocess
---------------------------------------

The previous example did not capture the exit code of the subprocess.
After the subprocess has exited, its exit code is stored in the generator (if the runner was started in asynchronous mode. To read it, just keep a reference to the generator:

.. code-block:: python

    from datalad_next.runners import Runner, StdOutCaptureGeneratorProtocol as Prot

    result_generator = Runner().run(cmd=['ls', '-l', '/etc'], protocol=Prot, exception_on_error=False)
    for line in result_generator:
        print(line)
    print(f'Subprocess exited with exit code: {result_generator.return_code}')

The actual return code can be read from ``result_generator.return_code`` after the subprocess terminated.
Note that we set the ``exception_on_error``-argument to ``False``.
This prevents raising a ``CommandError``, if the program exits with a non-zero return code, and ensures that we reach the last line, where the return code is printed, independent from the actual return code.


Use timeouts to ensure process termination
------------------------------------------
Every subprocess that is executed requires resources. In order to not leak resources, all subprocesses should be ended, once their task is performed.
Some processes perform their task based on given finite input, e.g. certain options, and exit.
Other processes read input from a file-descriptor and perform actions based on that input.
The latter usually can be instructed to terminate via closing the file-descriptor or via a specific input.

Although each subprocess usually has a defined path to termination, this might not work under error conditions.
For example, if network connections are down or if file systems are not available a process might stall.
Furthermore, the executed programs might contain bugs that keep it from exiting.
As a result some subprocesses might continue to execute after their termination condition was met.

To ensure that subprocesses are actually terminated and that their exit status is read (which is required to prevent zombie-processes) we can use timeouts.
The following example uses a 4-second timeout to send a terminate signal to a subprocess and a 6-second timeout to send a kill signal to the subprocess.

The example (which works on Posix-system) executes a subprocess in generator-mode, using a "buggy" shell command that runs forever.
The shell command will not terminate on a termination request, but print the message ``'terminate'``.
It can only be stopped by a kill signal.
In order to implement our timeout strategy, we derive a protocol class from ``StdOutCaptureGeneratorProtocol`` and overwrite the ``timeout``-callback.

If the process is terminated or killed, the result generator will fetch its return code, perform clean up operations, and stop the iteration.
To allow the result generator to perform these tasks, it has to be "called", i.e. its ``send``-method has to be invoked until it raises ``StopIteration``.
In this examples, this is achieved by the ``for``-loop:

.. code-block:: python

    from datalad_next.runners import Runner, StdOutCaptureGeneratorProtocol as BaseProt


    class TimeoutProt(BaseProt):
        def __init__(self, done_future=None, encoding=None):
            BaseProt.__init__(self, done_future, encoding)
            self.timeout_counter = 0
            self.process = None

        def connection_made(self, process):
            self.process = process

        def timeout(self, source_id):
            # Only count process timeouts
            if source_id is None:
                self.timeout_counter += 1
                if self.timeout_counter == 6:
                    self.process.kill()
                elif self.timeout_counter == 4:
                    self.process.terminate()
            return False


    command = 'trap "echo terminate" TERM; while [ "1" ]; do echo $(date) example output; sleep 1; done'
    result_generator = Runner().run(['bash', '-c', command], TimeoutProt, timeout=1.0, exception_on_error=False)
    for output in result_generator:
        print(output)
    print('return code:', result_generator.return_code)

Note that we set the ``timeout`` argument to ``1.0`` to activate timeouts.
This will trigger timeouts after one-second of inactivity of ``stderr`` and ``stdout``
It will also trigger a _process_-timeout every second, while the process is executing.
The program will generate output similar to the following:

.. code-block:: console

    b'Do 19. Okt 12:17:44 CEST 2023 example output\n'
    b'Do 19. Okt 12:17:45 CEST 2023 example output\n'
    b'Do 19. Okt 12:17:46 CEST 2023 example output\n'
    b'Do 19. Okt 12:17:47 CEST 2023 example output\n'
    b'terminate\n'
    b'Do 19. Okt 12:17:48 CEST 2023 example output\n'
    b'Do 19. Okt 12:17:49 CEST 2023 example output\n'
    return code: -9

Line five in the output above shows that a terminate signal was sent after four seconds.
On a Posix-system, the return code ``-9`` indicates that the process was terminated by signal number nine, which is ``SIGKILL``.


Which timeout should one use?
.............................

Which timeout strategy and which timeout values one should use depends on the subprocess in question.
These considerations are somewhat independent from the runner-implementation, but here are a few general recommendations.

If the subprocess is not expected to generate output on ``stdout`` or ``stderr`` and you know that the process should be finished in ``x`` seconds, you could use something like ``1.5 * x`` as timeout for a termination-signal.
Alternatively, one could use ``timeouts``-callbacks to track progress of the process by observing side effects like disk-file changes etc.
However a termination is triggered, the process should be given enough time to get into a consistent state, e.g. flush buffers, clean up temporary resources etc., before sending a kill-signal.
How much time that should be is again very much depending on the process in question.
In the end a kill-signal will be the only guarantee that the subprocess is not running anymore.

If the subprocess generates ``stdout`` or ``stderr``-output, the timeout strategy can be based on ``stdout``- and ``stderr``-timeouts, and a fallback based on the strategies mentioned above can be used.


Manage all events in a unified way
----------------------------------

If individual protocol callbacks operate differently, the overall protocol behavior can be quite unexpected.
For example, if a ``pipe_data_received``-callback sends data to the result queue, the data will be available via the result-generator.
So, in this example, iterating over the result generator is the way to access subprocess output.
Let's assume that a ``timeout``-callback raises an exception.
Then the exception will be raised in statement that iterates over the result generator, e.g. in ``for x in result_generator:``.-statement.
Getting "access" to the exception will therefore require a ``try``-``except``-clause.
This is different from accessing the output of the subprocess.
Overall, this "mixed behavior situation", where some callbacks enqueue data into the result queue, while others raise esceptions, can lead to complicated try-except-clauses and a hard-to-grasp control-flow, for example, in case of timeouts.

An alternative way would be to send all events, including timeouts, to the result queue of the result generator, and handle all events, in a the same way.
If all events are enqueued in the result queue, then all events can be handled inside the body of the ``for x in result_generator:``-statement.
Below is the definition of a generic generator-protocol that sends all events to the result queue:

.. code-block:: python

    from datalad_next.runners import GeneratorMixIn, Runner, StdOutErrCapture


    class GenericGeneratorProtocol(StdOutErrCapture, GeneratorMixIn):
        def __init__(self,
                     done_future=None,
                     encoding= None
                     ):
            StdOutErrCapture.__init__(self, done_future, encoding)
            GeneratorMixIn.__init__(self)
            self.process=None
            self.return_code=None

        def connection_made(self, process) -> None:
            self.process = process
            self.send_result(('connection_made', process))

        def connection_lost(self, exc):
            self.send_result(('connection_lost', exc))

        def pipe_data_received(self, fd, data):
            self.send_result(('data', fd, data))

        def pipe_connection_lost(self, fd, exc):
            self.send_result(('pipe_connection_lost', fd, exc))

        def timeout(self, fd):
            self.send_result(('timeout', fd))
            return False

        def process_exited(self):
            self.return_code = self.process.poll()
            self.send_result(('process_exited', self.return_code))


With this protocol we can handle all events inside a ``for``-loop:


.. code-block:: python

    event_source = Runner().run(['find', '/etc'], GenericGeneratorProtocol, timeout=.2, exception_on_error=False)
    for event in event_source:
        if event[0] == 'data':
            if event[1] == 1:
                # handle stdout data here
                print(event)
            else:
                # handle stderr data here
                print(event)

        elif event[0] == 'timeout':
            # handle timeouts here
            print(event)

        elif event[0] == 'connection_made':
            # Store the process object
            process = event[1]
            print(event)

        elif event[0] == 'process_exited':
            # Get the return code of the process and delete the process object reference
            return_code = event[1]
            process = None
            print(event)

        else:
            # ignore all other events
            print(event)

    print(return_code)

Running the code above would generate an output similar to the following (output shortened):

.. code-block:: console

    ('connection_made', <Popen: returncode: None args: ['find', '/etc']>)
    ('data', 1, b'/etc\n/etc/snapper\n/etc/snapper/configs\n ... ')
        ...
    ('data', 2, b'find: /etc/ppp: Keine Berechtigung\n')
        ...
    ('data', 1, b'/etc/ipsec.d/reqs\n ...')
    ('pipe_connection_lost', 2, None)
    ('pipe_connection_lost', 1, None)
    ('connection_lost', None)
    1

This approach allows a unified handling of all events and limits the number of protocol-class definitions.
For example, timeout-events could be counted and if a certain threshold is reached, a termination- or kill-signal could be sent to the process.
(Obviously, there are better ways to dispatch the events, the code above is just an example to illustrate the principle).
