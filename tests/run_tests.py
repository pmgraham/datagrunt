import pytest
import sys
import os

# Get the absolute path of the directory containing your test file
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the src directory to the Python path
src_dir = os.path.abspath(os.path.join(current_dir, '..', 'src'))
sys.path.insert(0, src_dir)

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
