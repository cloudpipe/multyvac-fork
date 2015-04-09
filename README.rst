========
Multyvac
========

Multyvac gives Python developers a simple interface for offloading
computational workloads to the cloud.

Getting Started
===============

You need to set an Api Key for your machine. To do this, run the following from your shell:

.. code-block:: bash

   $ python -m multyvac.setup
   
Follow the on screen instructions to complete the setup process.

Verify Installation
---------------------

Open up Python, and import multyvac:

.. code-block:: python

   >>> import multyvac

If you get an ImportError, something went wrong. Try running the setup again.

Running a Job
=============

The idea is to take a function that you would normally run on your own machine, but instead run it on Multyvac. Here's an example of how you would offload a function for adding two numbers:

.. code-block:: python

   >>> def add(x, y):
   ...     return x + y
   >>> # run add on your machine
   >>> add(1, 2)
   3
   >>> # submit add to Multyvac
   >>> job_id = multyvac.submit(add, 1, 2)
   >>> # get job object
   >>> job = multyvac.get(job_id)
   >>> # wait for job to finish processing
   >>> job.wait()
   >>> # verify the result is the same
   >>> job.result
   3
   