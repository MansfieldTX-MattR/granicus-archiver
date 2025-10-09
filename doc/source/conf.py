# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'granicus-archiver'
copyright = '2024, Matthew Reid'
author = 'Matthew Reid'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinx.ext.intersphinx',
    'sphinx.ext.extlinks',
    'sphinx.ext.todo',
    'sphinx_design',
    'sphinx_click',
]

numfig = True
todo_include_todos = True
todo_link_only = True

autodoc_member_order = 'bysource'
autodoc_default_options = {
    'show-inheritance':True,
}
autodoc_typehints = 'both'
autodoc_docstring_signature = True

templates_path = ['_templates']
exclude_patterns = []



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'furo'
html_static_path = ['_static']

intersphinx_mapping = {
    'python':('https://docs.python.org', None),
    'aiohttp':('https://docs.aiohttp.org/en/stable', None),
    'aiojobs':('https://aiojobs.readthedocs.io/en/stable', None),
    'yarl':('https://yarl.aio-libs.org/en/latest', None),
    'pyquery':('https://pyquery.readthedocs.io/en/latest', None),
    'aiogoogle':('https://aiogoogle.readthedocs.io/en/latest', None),
    'whoosh':('https://sygil-dev.github.io/whoosh-reloaded', None),
    'pypdf':('https://pypdf.readthedocs.io/en/latest', None),
}
