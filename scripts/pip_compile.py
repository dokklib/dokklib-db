#!/usr/bin/env python3
"""Compile pinned dependencies with pip-compile."""
import subprocess
from pathlib import Path


_DEPS_ORDER = [
    'test-requirements.in', 'dev-requirements.in'
]


this_file = Path(__file__)
repo_root = this_file.parents[1]
deps_dir = repo_root / 'requirements'

# The --allow-unsafe flag lets us pin setuptools.
# The flag will be soon deprecated and made the default behaviour.
# More info: https://github.com/jazzband/pip-tools/issues/989
# TODO (abiro) remove --allow-unsafe once it's deprecated
cmd_template = 'pip-compile -q --allow-unsafe --generate-hashes ' \
               '--output-file {out_file} {in_file}'

for dep in _DEPS_ORDER:
    p = deps_dir / dep
    out_file = p.with_suffix('.txt')
    cmd = cmd_template.format(in_file=p, out_file=out_file)
    subprocess.run(cmd.split(' '), check=True)
