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
   things don't match:

   .. code-block:: bash

      $ granicus-archiver clips download --max-clips 5
      Current working directory does not match your config. Continue? [y/N]


.. _config-granicus-data-url:

Granicus Data URL
-----------------

This is the URL of the view created in the :ref:`view-publishing-url` section of the Granicus Setup.
It can be set with the :option:`granicus-archiver --granicus-data-url` option.


.. code-block:: bash

   $ granicus-archiver --granicus-data-url <view-url>


.. _config-googledrive:

Google Drive
------------

.. note::

   This is optional and can be skipped or configured later.


The Google Drive integration requires setup of a Google Drive API project and OAuth2 credentials.
This is a one-time setup and can be done at any time.


Account Setup
^^^^^^^^^^^^^

1. Create a new project in the `Google APIs and Services Dashboard`_.
2. Enable an API in the `Google API Library`_ (e.g. the `Google Drive API`_).
3. Create credentials in the `Google Credentials Wizard`_.
4. Pick an API in the `Google APIs Explorer`_ and click "Authorize" to get the OAuth2 credentials.



OAuth2 Credentials
^^^^^^^^^^^^^^^^^^

The credentials created above must be set as environment variables:

- ``OAUTH_CLIENT_ID`` (e.g. ``1234567890-abcdefghijklmnopqrstuvwxyz.apps.googleusercontent.com``)
- ``OAUTH_CLIENT_SECRET``
- ``OAUTH_CLIENT_EMAIL`` (the email address of the service account)

Setting these environment variables can be done in the shell or by using a ``.env`` file.


Authorization
^^^^^^^^^^^^^

Once the credentials are set, the Google Drive API must be authorized.
The :ref:`cli-drive-auth` command will open a browser window to authorize the API.
The authorization token will then be saved to ``~/.granicus-oauth-user.json``.

.. code-block:: bash

   $ granicus-archiver drive authorize

.. todo::

   This should be placed in the user-config appdirs directory instead.


.. _config-aws-s3:

AWS S3
------

The S3 integration assumes that the necessary credentials are stored in the appropriate
location.  See the `boto3 documentation`_ for more information.

The S3 bucket name and other settings can be set with the :ref:`cli-aws-config` command:

.. code-block:: bash

   $ granicus-archiver aws config --bucket-name <bucket-name>




.. _Google APIs and Services Dashboard: https://console.cloud.google.com/projectselector/apis/dashboard
.. _Google API Library: https://console.cloud.google.com/apis/library
.. _Google Credentials Wizard: https://console.cloud.google.com/apis/credentials/wizard
.. _Google Drive API: https://console.cloud.google.com/apis/library/drive.googleapis.com
.. _Google APIs Explorer: https://developers.google.com/apis-explorer/
.. _boto3 documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration
