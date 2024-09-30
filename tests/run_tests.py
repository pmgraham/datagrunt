import pytest
import sys
sys.path.append('../')  # Add the parent directory to the search path
sys.path.append('../src/datagrunt')  # Add the parent directory to the search path

if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "."]))
