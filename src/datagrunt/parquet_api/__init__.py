# datagrunt.core before any module inside it (parquetreader imports datagrunt.core.parquet_io.engines): Python takes
# a module's import lock before its package's, and datagrunt.core imports every subpackage, so entering it below
# its root could deadlock with another thread's first import of another API (each holding a lock the other needs).
import datagrunt.core  # noqa: F401

# Import key classes that should be available at the package level
from datagrunt.parquet_api.parquetreader import ParquetReader
from datagrunt.parquet_api.parquetwriter import ParquetWriter

__all__ = ["ParquetReader", "ParquetWriter"]
