"""
Pytest configuration to make the project root importable (src-layout support).

Ensures that `import src...` works when running `pytest` from the
repository root without needing to set PYTHONPATH manually.
"""
import os
import sys


def _add_project_root_to_syspath():
    tests_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(tests_dir, ".."))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)


_add_project_root_to_syspath()

