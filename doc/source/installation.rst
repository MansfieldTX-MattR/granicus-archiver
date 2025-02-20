.. _installation:

Installation
############

These instructions assume you have `Python`_ installed on your system (>=3.11 recommended).

Since this project is fairly niche, it will likely never be published to `PyPI`_, so it
will need to be installed from source.

.. note::

   It is **highly** recommended to install the project in a virtual environment.
   This can be done with the :mod:`venv` module that comes with Python.


Installing from source
***********************

To install the project from source, you can clone the repository and install it with `pip`:

.. code-block:: bash

   git clone https://github.com/MansfieldTX-MattR/granicus-archiver.git
   cd granicus-archiver
   # Create a virtual environment and activate it
   python -m venv .venv
   source .venv/bin/activate
   # Install the project in editable mode
   pip install -e .


This will install the project and its dependencies in your Python environment.
If successful, you should be able to run the CLI with the following command:

.. code-block:: bash

   $ granicus-archiver --help
   Usage: granicus-archiver [OPTIONS] COMMAND [ARGS]...
   ...




.. _Python: https://www.python.org/
.. _PyPI: https://pypi.org/
