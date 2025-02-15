import pytest
import sys
from pathlib import Path

def main():
    """Run all tests."""
    # Get the root directory (parent of tests directory)
    root_dir = Path(__file__).parent.parent

    # Add the root directory to Python path
    sys.path.insert(0, str(root_dir))

    args = [
        "-v",
        f"--cov={root_dir}/src/datagrunt",
        "--cov-report=term-missing",
        str(root_dir / "tests")
    ]
    return pytest.main(args)

if __name__ == "__main__":
    sys.exit(main())
