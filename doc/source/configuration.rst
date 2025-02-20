Initial Configuration
=====================

There are a few configuration variables that must be set before the project can be used.


Local Timezone
--------------

The local timezone must also be set.  This can be done with the :option:`granicus-archiver --local-timezone`
option, but will also be prompted for it.

.. code-block:: bash

   $ granicus-archiver
   Please enter the local timezone name: <your-timezone>


.. _config-root-directories:

Root Directories
----------------

The root directories to downloaded files are set with the :option:`granicus-archiver --out-dir`
and :option:`granicus-archiver --legistar-out-dir` options.  These can be set in the
configuration file, or passed as command line arguments.


.. code-block:: bash

   $ granicus-archiver --out-dir <path/to/granicus/files> --legistar-out-dir <path/to/legistar/files>


.. important::

   Both of these directories must be relative to the current working directory.
   It will be assumed that any invocations of the CLI will be done from the same
   working directory from this point forward.

   The working directory will be checked on every invocation and will warn you if
   things don't match.


.. _config-granicus-data-url:

Granicus Data URL
-----------------

This is the URL of the view created in the :ref:`view-publishing-url` section of the Granicus Setup.
It can be set with the :option:`granicus-archiver --granicus-data-url` option.


.. code-block:: bash

   $ granicus-archiver --granicus-data-url <view-url>
