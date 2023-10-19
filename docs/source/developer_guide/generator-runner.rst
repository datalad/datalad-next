.. _generator_runner:


How to use ``datalad``'s runner with Generator Protocols
********************************************************

``datalad_next`` executes a lot of subprocesses to do its work. This is performed by the runner class: ``datalad_next.runner.ThreadedRunner`` and a small shell around it: ``datalad_next.runner.Runner`` (we refer to the latter as "runner" throughout the rest of the document).

This document is intended for ``datalad``-developers who want to understand how the runner code works and how they can use it to efficiently execute subprocesses or to build higher level tools, e.g. batch-command classes.


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

.. image:: /_static/runner_arch.png
  :alt: architecture of the runner in asynchronous mode


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
    Remark: you might not want to inherit from any of the ``datalad_next.runners.Protocol`` subclasses, because they contain code that is never used during asynchronous runner execution
    Nevertheless, if you use your own class with the callbacks defined in ``datalad.next.runners.Protocol``, you will have to add the two class variables: ``proc_out``, and ``proc_err`` and set them to ``True``, if you want stdout-output and stderr-output to be sent to the "From Process Queue" and eventually to the user code.


Programming examples
====================

Simple data reading from a subprocess
-------------------------------------

The following code snippet creates a runner with the protocol-class `StdOutCaptureGeneratorProtocol` to read the output of the command ``ls -l /etc``.
The protocol is derived from the two classes ``StdOutCapture``, and ``GeneratorMixIn``.
The class ``StdOutCapture`` indicates that only ``stdout`` of the subprocess should be captured.
The class ``GeneratorMixIn`` indicates to the runner that it should run in generator-mode.

.. code-block:: python

    from datalad_next.runners import Runner, StdOutCaptureGeneratorProtocol as Prot

    for data in Runner().run(cmd=['ls', '-l', '/etc'], protocol=Prot):
        print(data)



Getting the exit code from a subprocess
---------------------------------------

The previous example did not capture the exit code of the subprocess.
After the subprocess has exited, its exit code is stored in the generator (if the runner was started in generator-mode). To read it, just keep a reference to the generator:

.. code-block:: python

    from datalad_next.runners import Runner, StdOutCaptureGeneratorProtocol as Prot

    result_generator = Runner().run(cmd=['ls', '-l', '/etc'], protocol=Prot)
    for line in result_generator:
        print(line)
    print(f'Subprocess exited with exit code: {result_generator.return_code}')


Getting decoded lines from a subprocess
---------------------------------------
You may notice that the data is neither decoded, i.e. you receive bytes and not strings, and that multiple lines or incomplete lines might be returned in a single data packet the is yielded from the generator.
Although this is not strictly runner-related, it should be noted that the runner will yield the output from the subprocess in arbitrarily sized chunks of data.
If you want to work with decoded, line-based data, this has to be ensured by additional code.
There is the possibility to create a ``DecodedLineStdOutCaptureProtocol`` and implement the required functionality in the ``pipe_data_received``-callback before sending the processed data to the result-queue.
Then the code that iterates over the result generator would receive decoded lines.
One problem with this approach is that it blows up the number of class-definitions because each combination of desired properties requires a new class.
``datalad-next`` offers an alternative, the ability to wrap the result generator into another generator that receives data from a generator, executes a number of transformation processes on the data, and the yields the result of the final transformation step.
The wrapper is called ``process_from``. In the example below it is used with two data processors, ``decode_utf8_processor`, and ``splitlines_processor``.
``docode_utf8_processor`` will decode UTF-8 encoded strings, even if the encoded characters are split by a data chunk border. ``splitlines_processor`` will split incoming bytes or strings at line ending-characters, e.g. ``\n``, and leave the line ending-character in the result.
Using the wrapping generator and the two data processors, the following programm will print out individual decoded lines:

.. code-block:: python

    from datalad_next.runners import Runner, StdOutCaptureGeneratorProtocol as Prot
    from datalad_next.runners.data_processors import process_from, splitlines_processor, decode_utf8_processor

    result_generator = Runner().run(cmd=['ls', '-l', '/etc'], protocol=Prot)
    for line in process_from(result_generator, [decode_utf8_processor, splitlines_processor]):
        print(line, end='')
    print(f'Subprocess exited with exit code: {result_generator.return_code}')



Use timeouts to ensure process termination
------------------------------------------
Every subprocess that is executed requires resources. In order to not leak resources, all subprocesses should be ended, once their task is performed. Some processes perform their task based on given finite input, e.g. certain options, and exit. Other processes read input from a file-descriptor and perform actions based on that input. The latter usually can be instructed to terminate via closing the file-descriptor or via a specific input.

Although each subprocess usually has a defined path to termination, this might not work under error conditions. For example, if network connections are down or if file-systems are not available a process may stall. Furthermore, the executed programs might contain bugs that keeps them running. As a result some subprocesses might continue to execute after their termination condition was met.

To ensure that subprocesses are actually terminated and that their exit-status is read (which is required to prevent zombie-processes) we can use timeouts. The following example uses a 4-second timeout to send a terminate signal to a subprocess and a 6-second timeout to send a kill signal to the subprocess.

In the example below (for a Posix-system) we start a subprocess in generator-mode with a "buggy" shell command that would run forever. The shell command will not terminate on a termination request, but print the message ``'terminate'``. In order to implement our timeout strategy, we derive a protocol class from ``StdOutCaptureGeneratorProtocol`` and overwrite the ``timeout``-callback.

If the process is terminated or killed, the result generator will fetch its return code, perform clean up operations, and stop the iteration. In oder to allow the result generator to perform these tasks, it has to be "called". This is done here in a for-loop:

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

This program will generate output similar to the following:

.. code-block:: console

    b'Do 19. Okt 12:17:44 CEST 2023 example output\n'
    b'Do 19. Okt 12:17:45 CEST 2023 example output\n'
    b'Do 19. Okt 12:17:46 CEST 2023 example output\n'
    b'Do 19. Okt 12:17:47 CEST 2023 example output\n'
    b'terminate\n'
    b'Do 19. Okt 12:17:48 CEST 2023 example output\n'
    b'Do 19. Okt 12:17:49 CEST 2023 example output\n'
    return code: -9

On a Posix-system, the return code ``-9`` indicates that the process was terminated by signal number nine, which is ``SIGKILL``.

Which timeout should you use?
.............................

Which timeout strategy and which timeout values you should use depends on the subprocess in question. These considerations are somewhat independent from the runner-implementation, but here are a few general recommendations.

If the subprocess is not expected to generate output on ``stdout`` or ``stderr`` and you know that the process should be finished in ``x`` seconds, you could use something like ``x + 0.3 * x`` as timeout for a termination-signal.
Alternatively, if possible, one could use timeouts to track progress of the process by observing side effects like disk-file changes etc.
However a termination is triggered, the process should be given enough time to get into a consistent state, e.g. flush buffers, clean up temporary resources etc., before sending a kill-signal.
How much time that should be is again very much depending on the process.
In the end a kill-signal will be the only guarantee that the process is not running anymore.

If the subprocess generates ``stdout`` or ``stderr``-output, the timeout strategy can be based on ``stdout``- and ``stderr``-timeouts, and a fallback based on the strategies mentioned above can be used.



Manage all events in a unified way
----------------------------------

If individual protocol callbacks operate differently, the overall protocol behavior can be quite unexpected. For example, if a ``pipe_data_received``-callback sends data to the result queue, the data will be available via the result-generator.
If, for example, a ``timeout``-callback raises an exception, the exception will be raised in statement that iterates over the result generator, e.g. in ``for x in result_generator:``.-statement.
This can lead to complicated try-except-clauses and a hard-to-grasp control-flow in case of timeouts.

An alternative way is to send all event, including the timeouts, to the result_queue and handle all events, i.e. data-events and timeout-events in the same place.
That means all events are handled inside the body of the ``for x in result_generator:``-statement.
In fact we can define a generator-protocol that sends all events to the result_queue:

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


With this protocol we can handle all events, for example, inside a run-context (or inside a for loop).


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

This approach allows a unified handling of all events and limits the number of protocol-class definitions
For example, timeout-events could be counted and if a certain threshold is reached, a termination- or kill-signal could be sent to the process (obviously, there are better ways to dispatch the events, the code above is just an example to illustrate the principle).
