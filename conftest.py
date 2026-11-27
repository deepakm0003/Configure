# Root conftest.py – adds the workspace root to sys.path so that
# `pipeline` and `simulator` are importable without installation.
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
