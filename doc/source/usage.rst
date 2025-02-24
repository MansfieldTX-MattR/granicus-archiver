.. _usage:

Usage
#####


Granicus
********


Downloading
===========

Granicus items are downloaded using the :ref:`cli-clips-download` command.
This command will download any items that have not been downloaded yet.

Items are initially downloaded to a temporary directory and then moved to the final directory when the download is complete.
This is to handle any errors that may occur during the download process.

A temporary directory may be specified with the :option:`--temp-dir <granicus-archiver clips download --temp-dir>` option.
This is useful if you are downloading into a separate drive or volume and want to avoid moving files across drives.

The :option:`--max-clips <granicus-archiver clips download --max-clips>` option is used to limit the number of clips to
download in one invocation.


.. code-block:: bash

   $ granicus-archiver clips download --max-clips 10



Checking Local Files
=====================

The :ref:`cli-clips-check` command will check local files against what's stored in the data file.
This is useful to see if any files have been deleted or moved.


.. note::

   The same checks are also performed when running the :ref:`cli-clips-download` command.


Uploading to Google Drive
=========================

.. important::

   The :ref:`config-googledrive` setup steps must be completed before using this feature.

The :ref:`cli-clips-upload` command will upload the Granicus items to Google Drive.

.. todo::

   Add more information about the Google Drive upload feature.


Legistar
********

There are two variants to most of the Legistar commands. The main commands are from the initial implementation of the tool
and rely on an assumption that the :obj:`~granicus_archiver.legistar.types.GUID` of the RSS items was unique.

This was a flawed assumption: the GUIDs change on every update made in the system, but there is in fact a unique ID
which I refer to as the :obj:`~granicus_archiver.legistar.types.REAL_GUID` in the codebase.

Consequently, the "REAL_GUID" variants for each command were added and are suffixed with "-rguid" in most cases.
These variants are recommended and will be the default in the future.


Downloading
===========

Legistar items are downloaded using the :ref:`cli-legistar-download`
(or :ref:`cli-legistar-download-rguid`) command.
This command will download any items that have not been downloaded yet.

Items are initially downloaded to a temporary directory and then moved to the final directory when the download is complete.
This is to handle any errors that may occur during the download process.

A temporary directory may be specified with the :option:`--temp-dir <granicus-archiver legistar download --temp-dir>` option.
This is useful if you are downloading into a separate drive or volume and want to avoid moving files across drives.

The :option:`--max-clips <granicus-archiver legistar download --max-clips>` option is used to limit the number of items to
download in one invocation.

The PDF files contain embedded links which may or may not be desired. They will by default be removed from the document,
but this can be changed with the :option:`--no-strip-pdf-links <granicus-archiver legistar download --no-strip-pdf-links>` option.

.. code-block:: bash

   $ granicus-archiver legistar download --max-clips 10 --no-strip-pdf-links

Or:

.. code-block:: bash

   $ granicus-archiver legistar download-rguid --max-clips 10 --no-strip-pdf-links


.. note::

   When first running the download command, it may take a while.  This is because the data for each item is gathered
   from a web page on the Legistar site.

   Subsequent runs will be much faster as the data is cached locally.


Checking Local Files
=====================

The :ref:`cli-legistar-check` (or :ref:`cli-legistar-check-rguid`) command will check local files against what's
stored in the data file. This is useful to see if any files have been deleted or moved.

.. note::

   The same checks are also performed when running the :ref:`cli-legistar-download` command.


Handling Updates
================

When items have changed in Legistar, this is detected, but the items are not automatically updated.
This is to ensure that the user has control over when the updates are applied.

When updates are detected, you will see something like this:


.. grid:: 1

   .. grid-item::

      .. figure:: images/04-legistar-update-warning-01.jpg
         :name: legistar-update-warning

         Legistar Update Warning


In the output above under "REAL GUID COLLISIONS", you will see a list of items that have changed.
Most of the time it will be a status change ``setattr(obj, "minutes_status", Draft)``
and possibly a change in the URLs available ``setattr(obj.links, "minutes", <url>)``.

These cases are safe to update, so you can run the command again with the
:option:`--allow-updates <granicus-archiver legistar download --allow-updates>` option as suggested:


.. grid:: 1

   .. grid-item::

      .. figure:: images/04-legistar-update-complete-01.jpg
         :name: legistar-update-complete

         Legistar Update Complete



Uploading to Google Drive
=========================

.. important::

   The :ref:`config-googledrive` setup steps must be completed before using this feature.


The :ref:`cli-legistar-upload` (or :ref:`cli-legistar-upload-rguid`) command will upload the Legistar items to Google Drive.


.. todo::

   Add more information about the Google Drive upload feature.
