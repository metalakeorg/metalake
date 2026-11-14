"""Builds the extension against the installed JupyterLab's own core metadata, read from its package rather than
fetched: the builder otherwise asks npm, then GitHub, which limits how often a runner may ask."""

import os
import sys

import jupyterlab
from jupyter_builder.main import main

core = os.path.join(os.path.dirname(jupyterlab.__file__), "staging", "package.json")
sys.argv = ["jupyter-builder", "build", ".", "--core-package-file", core]
sys.exit(main())
