# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Everything about the package is in pyproject.toml, except one thing setuptools cannot say there: the wheel carries
``00_invaract_attach.pth`` at the top of ``site-packages``, where Python reads it as it starts (``_invaract_attach``)."""
import os
import shutil

from setuptools import setup
from setuptools.command.build_py import build_py

START_UP_HOOK = "00_invaract_attach.pth"


class BuildWithStartUpHook(build_py):
    def run(self):
        super().run()
        shutil.copyfile(os.path.join(os.path.dirname(os.path.abspath(__file__)), START_UP_HOOK),
                        os.path.join(self.build_lib, START_UP_HOOK))

    def get_outputs(self, include_bytecode=True):
        return super().get_outputs(include_bytecode) + [os.path.join(self.build_lib, START_UP_HOOK)]


setup(cmdclass={"build_py": BuildWithStartUpHook})
