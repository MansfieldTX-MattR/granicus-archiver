.. _granicus-setup:

Granicus Setup
##############

In order to gather data from Granicus, you will need to create a templated view
in the Granicus admin interface.  This view will be used to gather the necessary
data for each meeting.

.. _granicus-setup-template:

Template
========

The template for the view is stored in this respository as ``/clip-dump-template.html``.

.. dropdown:: Template Contents

  .. literalinclude:: ../../clip-dump-template.html


In the Admin page, go to "Templates" and create a new template.  Make sure to set
the ``Template Type`` to "View":

.. grid:: 2

  .. grid-item::

    .. figure:: images/01-template-01.jpg
      :name: granicus-admin-templates

      Granicus Admin Templates

  .. grid-item::

    .. figure:: images/01-template-02b.jpg
      :name: new-template-dialog

      New Template Dialog

After creating the template, go to the "Editor" tab, paste the contents into the
text area and save the template:

.. grid:: 1

  .. grid-item::

    .. figure:: images/01-template-03.jpg
      :name: template-editor

      Template Editor


View
====

Next, go to the "Views" page and create a new view.  Select the template you just
created as the "View Template" and check the "Active (Publishing)" box:

.. grid:: 2

  .. grid-item::

    .. figure:: images/02-view-01.jpg
      :name: granicus-admin-views

      Granicus Admin Views

  .. grid-item::

    .. figure:: images/02-view-02b.jpg
      :name: new-view-dialog

      New View Dialog


Contents
--------

Under the "Contents" tab, select any Folders that you want to include in the archive:

.. grid:: 1

  .. grid-item::

    .. figure:: images/02-view-03.jpg
      :name: view-contents

      View Contents



View Publishing URL
-------------------

After creating the view, make a note of the "Publishing URL" for it.
This will be needed during the configuration:

.. grid:: 1

  .. grid-item::

    .. figure:: images/02-view-04.jpg
      :name: view-publishing-url

      View Publishing URL


Rendered Result
===============

After setting up the template and the view, you can preview it from
the :ref:`view-publishing-url` and see something that looks like this:

.. grid:: 1

  .. grid-item::

    .. figure:: images/02-result-01.jpg

      Rendered View


.. note::

  This is a very basic (and ugly) table, but it isn't meant to be seen
  by the public.  It has data attributes embedded in the HTML that the
  archiver will use later on in the
  :ref:`configuration section <config-granicus-data-url>`.
