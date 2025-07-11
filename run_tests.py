"""This module runs all tests for the datagrunt package."""

from pathlib import Path
import sys
import subprocess

if __name__ == "__main__":
    tests_dir = Path(__file__).parent / "tests"
    test_script = tests_dir / "run_tests.py"
    sys.exit(subprocess.call([sys.executable, str(test_script)]))
