# -*- coding: utf-8 -*-
# 
# NAME OF THE PROGRAM THIS FILE BELONGS TO 
#  
# file: mimic-preprocessing
#  
# Authors: Brandon Malone (Brandon.malone@neclab.eu
#               Jun Cheng (jun.cheng@neclab.eu)
# 
# NEC Laboratories Europe GmbH, Copyright (c) 2020, All rights reserved. 
#     THIS HEADER MAY NOT BE EXTRACTED OR MODIFIED IN ANY WAY.
#  
#     PROPRIETARY INFORMATION --- 
# 
# SOFTWARE LICENSE AGREEMENT
# ACADEMIC OR NON-PROFIT ORGANIZATION NONCOMMERCIAL RESEARCH USE ONLY
# BY USING OR DOWNLOADING THE SOFTWARE, YOU ARE AGREEING TO THE TERMS OF THIS LICENSE AGREEMENT.  IF YOU DO NOT AGREE WITH THESE TERMS, YOU MAY NOT USE OR DOWNLOAD THE SOFTWARE.
# 
# This is a license agreement ("Agreement") between your academic institution or non-profit organization or self (called "Licensee" or "You" in this Agreement) and NEC Laboratories Europe GmbH (called "Licensor" in this Agreement).  All rights not specifically granted to you in this Agreement are reserved for Licensor. 
# RESERVATION OF OWNERSHIP AND GRANT OF LICENSE: Licensor retains exclusive ownership of any copy of the Software (as defined below) licensed under this Agreement and hereby grants to Licensee a personal, non-exclusive, non-transferable license to use the Software for noncommercial research purposes, without the right to sublicense, pursuant to the terms and conditions of this Agreement. NO EXPRESS OR IMPLIED LICENSES TO ANY OF LICENSOR’S PATENT RIGHTS ARE GRANTED BY THIS LICENSE. As used in this Agreement, the term "Software" means (i) the actual copy of all or any portion of code for program routines made accessible to Licensee by Licensor pursuant to this Agreement, inclusive of backups, updates, and/or merged copies permitted hereunder or subsequently supplied by Licensor,  including all or any file structures, programming instructions, user interfaces and screen formats and sequences as well as any and all documentation and instructions related to it, and (ii) all or any derivatives and/or modifications created or made by You to any of the items specified in (i).
# CONFIDENTIALITY/PUBLICATIONS: Licensee acknowledges that the Software is proprietary to Licensor, and as such, Licensee agrees to receive all such materials and to use the Software only in accordance with the terms of this Agreement.  Licensee agrees to use reasonable effort to protect the Software from unauthorized use, reproduction, distribution, or publication. All publication materials mentioning features or use of this software must explicitly include an acknowledgement the software was developed by NEC Laboratories Europe GmbH.
# COPYRIGHT: The Software is owned by Licensor.  
# PERMITTED USES:  The Software may be used for your own noncommercial internal research purposes. You understand and agree that Licensor is not obligated to implement any suggestions and/or feedback you might provide regarding the Software, but to the extent Licensor does so, you are not entitled to any compensation related thereto.
# DERIVATIVES: You may create derivatives of or make modifications to the Software, however, You agree that all and any such derivatives and modifications will be owned by Licensor and become a part of the Software licensed to You under this Agreement.  You may only use such derivatives and modifications for your own noncommercial internal research purposes, and you may not otherwise use, distribute or copy such derivatives and modifications in violation of this Agreement.
# BACKUPS:  If Licensee is an organization, it may make that number of copies of the Software necessary for internal noncommercial use at a single site within its organization provided that all information appearing in or on the original labels, including the copyright and trademark notices are copied onto the labels of the copies.
# USES NOT PERMITTED:  You may not distribute, copy or use the Software except as explicitly permitted herein. Licensee has not been granted any trademark license as part of this Agreement. Neither the name of NEC Laboratories Europe GmbH nor the names of its contributors may be used to endorse or promote products derived from this Software without specific prior written permission.
# You may not sell, rent, lease, sublicense, lend, time-share or transfer, in whole or in part, or provide third parties access to prior or present versions (or any parts thereof) of the Software.
# ASSIGNMENT: You may not assign this Agreement or your rights hereunder without the prior written consent of Licensor. Any attempted assignment without such consent shall be null and void.
# TERM: The term of the license granted by this Agreement is from Licensee's acceptance of this Agreement by downloading the Software or by using the Software until terminated as provided below.
# The Agreement automatically terminates without notice if you fail to comply with any provision of this Agreement.  Licensee may terminate this Agreement by ceasing using the Software.  Upon any termination of this Agreement, Licensee will delete any and all copies of the Software. You agree that all provisions which operate to protect the proprietary rights of Licensor shall remain in force should breach occur and that the obligation of confidentiality described in this Agreement is binding in perpetuity and, as such, survives the term of the Agreement.
# FEE: Provided Licensee abides completely by the terms and conditions of this Agreement, there is no fee due to Licensor for Licensee's use of the Software in accordance with this Agreement.
# DISCLAIMER OF WARRANTIES:  THE SOFTWARE IS PROVIDED "AS-IS" WITHOUT WARRANTY OF ANY KIND INCLUDING ANY WARRANTIES OF PERFORMANCE OR MERCHANTABILITY OR FITNESS FOR A PARTICULAR USE OR PURPOSE OR OF NON-INFRINGEMENT.  LICENSEE BEARS ALL RISK RELATING TO QUALITY AND PERFORMANCE OF THE SOFTWARE AND RELATED MATERIALS.
# SUPPORT AND MAINTENANCE: No Software support or training by the Licensor is provided as part of this Agreement.  
# EXCLUSIVE REMEDY AND LIMITATION OF LIABILITY: To the maximum extent permitted under applicable law, Licensor shall not be liable for direct, indirect, special, incidental, or consequential damages or lost profits related to Licensee's use of and/or inability to use the Software, even if Licensor is advised of the possibility of such damage.
# EXPORT REGULATION: Licensee agrees to comply with any and all applicable export control laws, regulations, and/or other laws related to embargoes and sanction programs administered by law.
# SEVERABILITY: If any provision(s) of this Agreement shall be held to be invalid, illegal, or unenforceable by a court or other tribunal of competent jurisdiction, the validity, legality and enforceability of the remaining provisions shall not in any way be affected or impaired thereby.
# NO IMPLIED WAIVERS: No failure or delay by Licensor in enforcing any right or remedy under this Agreement shall be construed as a waiver of any future or other exercise of such right or remedy by Licensor.
# GOVERNING LAW: This Agreement shall be construed and enforced in accordance with the laws of Germany without reference to conflict of laws principles.  You consent to the personal jurisdiction of the courts of this country and waive their rights to venue outside of Germany.
# ENTIRE AGREEMENT AND AMENDMENTS: This Agreement constitutes the sole and entire agreement between Licensee and Licensor as to the matter set forth herein and supersedes any previous agreements, understandings, and arrangements between the parties relating hereto.
###
#
# Configuration file for the Sphinx documentation builder.
#
# This file does only contain a selection of the most common options. For a
# full list see the documentation:
# http://www.sphinx-doc.org/en/master/config

###
# CUSTOM AUTODOC COMMANDS
#
# This conf.py file also adds a few custom autodoc commands. While these have
# been somewhat tested, they are all a bit hacky. User beware.
#
#   * `autoclass_docstr_only`: This can be used to show only the docstring for
#       the following class. Presumably, this is used at the top of the
#       documentation when the class is just introduced. In particular, this
#       also suppresses the name of the class. Example usage:
#
#       ```
#       .. autoclass_docstr_only:: aa_encode.aa_encoder.AAEncoder
#       ```
#
#   * `autono_docstr_text`: This can be used to suppress the docstring for
#       the following class. Presumably, this is used when giving the method
#       definitions for a class. Example usage:
#
#       ```
#       .. autono_docstr_text:: aa_encode.aa_encoder.AAEncoder
#       ```
#
#   * `:exclude-members: module_doc_str`: This option can be used to suppress
#       the module-level docstring for a module. This is again intended for
#       use in the "Definitions" section after the module has already been
#       introduced. Example usage:
#
#       ```
#       .. automodule:: aa_encode.utils
#           :members:
#           :exclude-members: module_doc_str
#       ```
###

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))


# -- Project information -----------------------------------------------------

project = 'immunogenicity_scorers'
copyright = '2020, Brandon Malone'
author = 'Brandon'

# The short X.Y version
version = '0.1'
# The full version, including alpha/beta/rc tags
release = '0.1.0'

###
# These are used for LaTeX and man page documents
###
base_filename = 'immunogenicity_scorers'
short_description=  'Documentation for immunogenicity_scorers'
category = 'Miscellaneous'

# -- General configuration ---------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
#
# needs_sphinx = '1.0'

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.intersphinx',
    'sphinx.ext.todo',
    'sphinx.ext.mathjax',
    'sphinx.ext.viewcode',
    'sphinx.ext.githubpages',
    'sphinx.ext.napoleon',
    'sphinx.ext.autosummary',
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
#
# source_suffix = ['.rst', '.md']
source_suffix = '.rst'

# The master toctree document.
master_doc = 'index'

# The language for content autogenerated by Sphinx. Refer to documentation
# for a list of supported languages.
#
# This is also used if you do content translation via gettext catalogs.
# Usually you set "language" from the command line for these cases.
language = None

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['.ipynb_checkpoints']

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = None


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
#html_theme = 'alabaster'
html_theme = "sphinx_rtd_theme"


# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
#
html_theme_options = {
    'navigation_depth': 4,
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# Custom sidebar templates, must be a dictionary that maps document names
# to template names.
#
# The default sidebars (for documents that don't match any pattern) are
# defined by theme itself.  Builtin themes are using these templates by
# default: ``['localtoc.html', 'relations.html', 'sourcelink.html',
# 'searchbox.html']``.
#
# html_sidebars = {}


# -- Options for HTMLHelp output ---------------------------------------------

# Output file base name for HTML help builder.
htmlhelp_basename = 'Utilitiesdoc'


# -- Options for LaTeX output ------------------------------------------------

latex_elements = {
    # The paper size ('letterpaper' or 'a4paper').
    #
    # 'papersize': 'letterpaper',

    # The font size ('10pt', '11pt' or '12pt').
    #
    # 'pointsize': '10pt',

    # Additional stuff for the LaTeX preamble.
    #
    # 'preamble': '',

    # Latex figure (float) alignment
    #
    # 'figure_align': 'htbp',
}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title,
#  author, documentclass [howto, manual, or own class]).
latex_documents = [
    (master_doc,
    '{}.tex'.format(base_filename),
    short_description,
     author,
     'manual'),
]


# -- Options for manual page output ------------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [
    (master_doc,
    base_filename,
    short_description,
     [author], 1)
]


# -- Options for Texinfo output ----------------------------------------------

# Grouping the document tree into Texinfo files. List of tuples
# (source start file, target name, title, author,
#  dir menu entry, description, category)
texinfo_documents = [
    (master_doc,
     base_filename,
     short_description,
     author,
     short_description,
     'One line description of project.',
     category),
]


# -- Options for Epub output -------------------------------------------------

# Bibliographic Dublin Core info.
epub_title = project

# The unique identifier of the text. This can be a ISBN number
# or the project homepage.
#
# epub_identifier = ''

# A unique identification for the text.
#
# epub_uid = ''

# A list of files that should not be packed into the epub file.
epub_exclude_files = ['search.html']


# -- Extension configuration -------------------------------------------------

# -- Options for intersphinx extension ---------------------------------------

# Example configuration for intersphinx: refer to the Python standard library.
intersphinx_mapping = {
    'scipy': ('https://docs.scipy.org/doc/scipy/reference', None),
    'matplotlib': ('https://matplotlib.org/', None),
    'sklearn': ('https://scikit-learn.org/0.20', None),
    'python': ('http://docs.python.org/', None),
    'pandas': ('http://pandas.pydata.org/pandas-docs/stable/',
              'http://pandas.pydata.org/pandas-docs/stable/objects.inv'),
    'numpy': ('https://docs.scipy.org/doc/numpy/',
             'https://docs.scipy.org/doc/numpy/objects.inv')
}

# -- Options for todo extension ----------------------------------------------

# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = True

# -- Custom sphinx autodoc helpers -------------------------------------------
from sphinx.ext import autodoc
from sphinx.deprecation import RemovedInSphinx40Warning
import warnings


ATTRIBUTES_HEADER_MARKER = "__ATTRIBUTES_HEADER_MARKER__"

ATTRIBUTES_HEADER_STR = '**Attributes**'
METHODS_HEADER_STR = '**Methods**'

class ClassDocstrOnlyDocumenter(autodoc.ClassDocumenter):
    objtype = "class_docstr_only"
    directivetype = 'class'

    #do not indent the content
    content_indent = ""
    
    def get_doc(self, encoding: str = None, ignore: int = 1) :
        if encoding is not None:
            msg = ("The 'encoding' argument to autodoc.{}.get_doc() is "
                "deprecated.".format(self.__class__.__name__))
            warnings.warn(msg, RemovedInSphinx40Warning)
            
        ds = super().get_doc(encoding, ignore)
        
        if 'Attributes' in ds[0]:
            i = ds[0].index('Attributes')
            ds[0] = ds[0][:i]
            
        return ds
    
    #do not add a header to the docstring
    def add_directive_header(self, sig):
        pass

class ClassNoDocstrTextDocumenter(autodoc.ClassDocumenter):
    objtype = "no_docstr_text"
    directivetype = 'class'
    
    def get_doc(self, encoding: str = None, ignore: int = 1) :
        if encoding is not None:
            msg = ("The 'encoding' argument to autodoc.{}.get_doc() is "
                "deprecated.".format(self.__class__.__name__))
            warnings.warn(msg, RemovedInSphinx40Warning)
            
        ds = super().get_doc(encoding, ignore)
        
        if 'Attributes' in ds[0]:
            i = ds[0].index('Attributes')
            ds[0] = ds[0][i-1:]
            ds[0].insert(0, ATTRIBUTES_HEADER_MARKER)
            
        else:
            ds = [[
                ATTRIBUTES_HEADER_MARKER, '',
                'This class has no documented attributes.', '',
            ]]
            
        return ds
    
def process_docstring(app, what, name, obj, options, lines):
    
    # a bit of a hack
    #
    # if we start with the "attributes" text, then
    # we assume the methods will follow.
    if lines[0] == ATTRIBUTES_HEADER_MARKER:
        lines[0] = ATTRIBUTES_HEADER_STR
        lines.append(METHODS_HEADER_STR)
        lines.append('')
    
    # If we are told to exclude the "module_doc_str", then delete
    # the lines for this docstring (if this is a module).
    
    # Since this modified `lines` in-place, this causes a problem if
    # we want to show the docstring again later on.
    #
    # On further checking, this _seems_ to behave as desired. I am leaving
    # this comment here in case problems arise in the future, though.
    if what == 'module':
        if 'exclude-members' in options:
            if 'module_doc_str' in options['exclude-members']:
                del lines[:]
    
def setup(app):
    app.add_autodocumenter(ClassNoDocstrTextDocumenter)
    app.add_autodocumenter(ClassDocstrOnlyDocumenter)
    
    app.connect('autodoc-process-docstring', process_docstring)