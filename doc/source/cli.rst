CLI
===


.. _cli-root:
.. click:: granicus_archiver.cli:cli
    :prog: granicus-archiver
    :nested: none


.. _cli-show-config:
.. click:: granicus_archiver.cli:show_config
    :prog: granicus-archiver show-config
    :nested: none

.. _cli-configure:
.. click:: granicus_archiver.cli:configure
    :prog: granicus-archiver configure
    :nested: none

.. _cli-save-config:
.. click:: granicus_archiver.cli:save_config
    :prog: granicus-archiver save-config
    :nested: none


Clips
-----

.. _cli-clips:
.. click:: granicus_archiver.clips.cli:cli
    :prog: granicus-archiver clips
    :nested: none

.. _cli-clips-download:
.. click:: granicus_archiver.clips.cli:download_clips
    :prog: granicus-archiver clips download
    :nested: none

.. _cli-clips-check:
.. click:: granicus_archiver.clips.cli:check_clips
    :prog: granicus-archiver clips check
    :nested: none

.. _cli-clips-upload:
.. click:: granicus_archiver.clips.cli:upload_clips
    :prog: granicus-archiver clips upload
    :nested: none


Legistar
--------

.. _cli-legistar:
.. click:: granicus_archiver.legistar.cli:cli
    :prog: granicus-archiver legistar
    :nested: none


.. _cli-legistar-add-feed-url:
.. click:: granicus_archiver.legistar.cli:add_feed_url
    :prog: granicus-archiver legistar add-feed-url
    :nested: none


.. _cli-legistar-list-feed-urls:
.. click:: granicus_archiver.legistar.cli:list_feed_urls
    :prog: granicus-archiver legistar list-feed-urls
    :nested: none


.. _cli-legistar-add-category-map:
.. click:: granicus_archiver.legistar.cli:add_category_map
    :prog: granicus-archiver legistar add-category-map
    :nested: none


"Fake GUID" Commands
^^^^^^^^^^^^^^^^^^^^

.. _cli-legistar-download:
.. click:: granicus_archiver.legistar.cli:download_legistar
    :prog: granicus-archiver legistar download
    :nested: none

.. _cli-legistar-check:
.. click:: granicus_archiver.legistar.cli:check_legistar
    :prog: granicus-archiver legistar check
    :nested: none


.. _cli-legistar-upload:
.. click:: granicus_archiver.legistar.cli:upload_legistar
    :prog: granicus-archiver legistar upload
    :nested: none


"Real GUID" Commands
^^^^^^^^^^^^^^^^^^^^

.. _cli-legistar-download-rguid:
.. click:: granicus_archiver.legistar.cli:download_legistar_rguid
    :prog: granicus-archiver legistar download-rguid
    :nested: none

.. _cli-legistar-check-rguid:
.. click:: granicus_archiver.legistar.cli:check_legistar_rguid
    :prog: granicus-archiver legistar check-rguid
    :nested: none


.. _cli-legistar-upload-rguid:
.. click:: granicus_archiver.legistar.cli:upload_legistar_rguid
    :prog: granicus-archiver legistar upload-rguid
    :nested: none



Drive
-----

.. _cli-drive:
.. click:: granicus_archiver.cli:drive
    :prog: granicus-archiver drive
    :nested: none


.. _cli-drive-auth:
.. click:: granicus_archiver.cli:authorize
    :prog: granicus-archiver drive authorize
    :nested: none


AWS
---

.. _cli-aws:
.. click:: granicus_archiver.aws.cli:cli
    :prog: granicus-archiver aws
    :nested: none

.. _cli-aws-config:
.. click:: granicus_archiver.aws.cli:config
    :prog: granicus-archiver aws config
    :nested: none

.. _cli-aws-upload-clips:
.. click:: granicus_archiver.aws.cli:upload_clips
    :prog: granicus-archiver aws upload-clips
    :nested: none

.. _cli-aws-upload-legistar:
.. click:: granicus_archiver.aws.cli:upload_legistar
    :prog: granicus-archiver aws upload-legistar
    :nested: none

.. _cli-aws-upload-legistar-rguid:
.. click:: granicus_archiver.aws.cli:upload_legistar_rguid
    :prog: granicus-archiver aws upload-legistar-rguid
    :nested: none


Web
---

.. _cli-web:
.. click:: granicus_archiver.web.app:cli
    :prog: granicus-archiver web
    :nested: none

.. _cli-web-collect-static:
.. click:: granicus_archiver.web.app:collect_static
    :prog: granicus-archiver web collect-static
    :nested: none

.. _cli-web-serve:
.. click:: granicus_archiver.web.app:serve
    :prog: granicus-archiver web serve
    :nested: none
