import pytest
import sys

def main():
    """Run all tests."""
    args = [
        "-v",
        "--cov=src/datagrunt",
        "--cov-report=term-missing",
        "tests/"
    ]
    return pytest.main(args)

if __name__ == "__main__":
    sys.exit(main())
