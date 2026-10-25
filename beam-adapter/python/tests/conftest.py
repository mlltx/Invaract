# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
import os
import sys

# so a test can name its own delegate runner by module path, as a job names a runner
sys.path.insert(0, os.path.dirname(__file__))
