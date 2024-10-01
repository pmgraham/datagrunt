import pytest
import sys
sys.path.append('../')  # Add the parent directory to the search path
sys.path.append('../src/datagrunt')  # Add the parent directory to the search path

if __name__ == "__main__":
    # sys.exit(pytest.main(["-v", "--cov=src.datagrunt", "--cov-report=term-missing", "--cov-fail-under=90", "."]))
    print("Python version:", sys.version)
    print("Pytest version:", pytest.__version__)
    
    try:
        import pytest_cov
        print("Pytest-cov version:", pytest_cov.__version__)
    except ImportError:
        print("Pytest-cov not found!")

    args = ["-v", "--cov=src.datagrunt", "--cov-report=term-missing", "."]
    print("Running pytest with args:", args)
    
    exit_code = pytest.main(args)
    print("Pytest exit code:", exit_code)
    sys.exit(exit_code)
