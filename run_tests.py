"""This module runs all tests for the datagrunt package."""

import subprocess
import sys
from pathlib import Path

if __name__ == "__main__":
    tests_dir = Path(__file__).parent / "tests"
    test_script = tests_dir / "run_tests.py"
    sys.exit(subprocess.call([sys.executable, str(test_script)]))
