.. granicus-archiver documentation master file, created by
   sphinx-quickstart on Tue Sep 17 13:06:51 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

granicus-archiver documentation
###############################


What is this?
*************

granicus-archiver is a tool for users of the `Granicus`_ and `Legistar`_ platforms
for Government meeting management. It provides a way to archive meeting data
locally for backup purposes or other uses.

The archived content for each meeting may include:

- Agenda (PDF)
- Agenda Packet (PDF)
- Minutes (PDF)
- Video (MP4)
- Audio (MP3)
- Video timestamps (:term:`WebVTT`)

Data for all items will be stored in human-readable JSON format with the local filenames
and URLs for each meeeting as well as the following metadata:

- Meeting date
- Meeting title
- Meeting location

For :term:`Legistar Items <Legistar Item>`, the following additional metadata will be included:

- Agenda Status (e.g. "Final", "Draft", etc.)
- Minutes Status (e.g. "Final", "Draft", etc.)


How do I use it?
****************

The tool was written in `Python`_ and must be run from the command line,
so familiartiy with both is recommended.  See the :ref:`installation`
documentation for more information on how to install the tool.


Some initial setup is required within :term:`Granicus` beforehand which
will require a user with administrative permissions.
See the documentation for :ref:`granicus-setup` and :ref:`legistar-setup`
for more information on this.

If you have many years of data (especially video) to archive, it is recommended
to run the tool on a machine with a fast internet connection and plenty of disk space.
For example, with around 7-10 years of data, you may need 1-2 TB of disk space.

.. tip::

  External SSD's are pretty inexpensive and can be a good option, not only for
  speed and portability, but for their longevity and shelf-life.


Other Features
**************

- **Google Drive Integration**:  The tool can upload the archived content to Google Drive
  for backup purposes.  This is useful for both redundancy and for sharing the data with
  others.
- **AWS S3 Integration**:  The tool can upload the archived content to an AWS S3 bucket.
  This can be used for redundancy as well as serving the data to a website or other
  application.
- **Web Interface**: There is a built-in web application that allows you to view the
  archived content in a browser.  This is useful for quickly finding and viewing
  content without having to navigate the file system.


Project Links
*************

- `Source Code <https://github.com/MansfieldTX-MattR/granicus-archiver>`_
- `Documentation <https://granicus-archiver.readthedocs.io>`_


Setup
*****

.. toctree::
   :maxdepth: 2
   :caption: Setup:

   installation
   granicus-setup
   legistar-setup
   configuration

Usage
*****

.. toctree::
   :maxdepth: 1
   :caption: Usage:

   usage


Reference
*********

.. toctree::
   :maxdepth: 1
   :caption: Reference:

   cli
   reference/index
   glossary


.. todolist::



.. _Granicus: https://granicus.com
.. _Legistar: https://granicus.com/product/legistar-agenda-management/
.. _Python: https://www.python.org
