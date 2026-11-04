"""Regression tests for issue #224 robustness fixes (item 4).

Items 6 and 7, and the item 3+5 consistency case, covered v4 PDF code that the 5.0 flip (#373) deleted.
"""

import pytest


def test_load_spatial_flag_not_set_when_load_fails(monkeypatch, tmp_path):
    """Item 4: if INSTALL succeeds but LOAD fails, _spatial_installed stays False
    so the next connection retries instead of LOADing a never-installed extension.
    """
    from datagrunt.core.databases.databases import DuckDBQueries

    monkeypatch.setattr(DuckDBQueries, "_spatial_installed", False)  # auto-restored
    csv = tmp_path / "x.csv"
    csv.write_text("a,b\n1,2\n")
    q = DuckDBQueries(str(csv))

    class FakeConn:
        def __init__(self):
            self.calls = []

        def execute(self, sql):
            self.calls.append(sql)
            if sql.startswith("LOAD"):
                raise RuntimeError("load failed")

    conn = FakeConn()
    with pytest.raises(RuntimeError, match="load failed"):
        q.load_spatial_extension(conn)

    assert DuckDBQueries._spatial_installed is False  # NOT set after LOAD failure
    assert conn.calls == ["INSTALL spatial;", "LOAD spatial;"]  # INSTALL ran, LOAD attempted
