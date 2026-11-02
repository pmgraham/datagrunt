"""Tests for the v5 PDFReader (#372, A8b): ``datagrunt.pdf.reader``.

Fixtures are built with the ReportLab toolkit (``pdf_tests.builders``): a
digital PDF of two mixed pages (heading, prose, an image, a ruled table and
a borderless one), its bundle (A7a's ``write_bundle``) and its document
JSON (A7a's codec). A stage the reader calls is a ``monkeypatch`` of the
reader module's name for it.
"""

import copy
import errno
import gc
import os
import re
import sys
import weakref
from concurrent.futures import ThreadPoolExecutor
from types import MappingProxyType, SimpleNamespace

import duckdb
import polars as pl
import pyarrow as pa
import pytest

from datagrunt.pdf import _database
from datagrunt.pdf import reader as reader_module
from datagrunt.pdf.bundle import load_bundle, write_bundle
from datagrunt.pdf.codec import document_from_json, document_to_json
from datagrunt.pdf.frame import ARROW_SCHEMA, FRAME_SCHEMA, element_arrow, element_frame
from datagrunt.pdf.pipeline import extract_document
from datagrunt.pdf.reader import PDFReader
from pdf_tests.builders import build_pdf
from pdf_tests.documents import empty_document
from pdf_tests.test_pipeline import mixed_page

# A figure at 32 px (mixed_page's image is 64 x 32 px): every element kind the page holds is read.
OPTIONS = {"min_image_dimension": 32, "ocr": "never"}
TEXT = b"Name,Qty\nBolt,4\n"


@pytest.fixture(scope="module")
def made(tmp_path_factory):
    """A digital PDF of two mixed pages, its document, its bundle, and its document JSON."""
    directory = tmp_path_factory.mktemp("reader")
    path = build_pdf(directory / "parts list.pdf", [mixed_page(), mixed_page(top=100.0)])
    document = extract_document(path, **OPTIONS)
    bundle = directory / "bundle"
    write_bundle(document, bundle, source=path)
    return SimpleNamespace(path=path, document=document, bundle=bundle, json=document_to_json(document))


def calls_of(monkeypatch, name: str) -> list:
    """Record each call of the reader module's ``name`` (its arguments) and let it run."""
    calls: list = []
    real = getattr(reader_module, name)

    def recorded(*args, **kwargs):
        calls.append((args, kwargs))
        return real(*args, **kwargs)

    monkeypatch.setattr(reader_module, name, recorded)
    return calls


def refuse_work(monkeypatch) -> None:
    """Fail the test if the reader extracts or loads anything."""

    def refused(*args, **kwargs):
        raise AssertionError("work was done before the checks")

    for name in ("extract_document", "load_bundle", "document_from_json"):
        monkeypatch.setattr(reader_module, name, refused)


class TestSources:
    """A ``.pdf`` path is extracted (A8a), a bundle loaded (A7a), a Mapping decoded (A7a's codec)."""

    @pytest.mark.parametrize("kind", [str, os.PathLike], ids=["str", "path"])
    def test_a_pdf_path_is_extracted_with_the_readers_options(self, made, kind):
        source = str(made.path) if kind is str else made.path

        assert PDFReader(source, **OPTIONS).document == made.document

    def test_every_option_and_the_password_reach_the_extraction(self, made, monkeypatch):
        calls = calls_of(monkeypatch, "extract_document")
        options = {
            "workers": 2,
            "ocr": "never",
            "dpi": 100,
            "vision_dpi": 150,
            "max_render_pixels": 1_000_000,
            "min_image_dimension": 32,
            "strict": True,
        }

        PDFReader(made.path, password="not-needed", **options).document

        assert calls == [((made.path.absolute(),), {"password": "not-needed", **options})]

    def test_a_bundle_directory_is_loaded(self, made):
        assert PDFReader(made.bundle).document == load_bundle(made.bundle)

    def test_a_bundles_manifest_is_loaded(self, made):
        assert PDFReader(made.bundle / "manifest.json").document == load_bundle(made.bundle)

    @pytest.mark.parametrize("wrap", [dict, MappingProxyType], ids=["dict", "mapping"])
    def test_a_mapping_is_the_codecs_document_json(self, made, wrap):
        assert PDFReader(wrap(copy.deepcopy(made.json))).document == document_from_json(made.json)

    def test_a_mapping_changed_after_construction_does_not_change_the_reader(self, made):
        data = copy.deepcopy(made.json)
        reader = PDFReader(data)

        data["pages"].clear()

        assert reader.document == made.document

    def test_a_path_is_made_absolute_at_construction(self, made, monkeypatch, tmp_path):
        monkeypatch.chdir(made.path.parent)
        reader = PDFReader(made.path.name, **OPTIONS)

        monkeypatch.chdir(tmp_path)

        assert reader.document == made.document

    def test_a_pdf_with_bytes_before_its_header_is_a_pdf(self, made, tmp_path):
        path = tmp_path / "prefixed.pdf"
        path.write_bytes(b"\x00" * 1000 + made.path.read_bytes())

        reader = PDFReader(path, **OPTIONS)

        assert reader.document.pages == made.document.pages


class TestExtraction:
    """The document is read on first use, then cached; construction does no work."""

    def test_construction_does_not_extract(self, made, monkeypatch):
        calls = calls_of(monkeypatch, "extract_document")

        PDFReader(made.path, **OPTIONS)

        assert calls == []

    def test_the_document_is_extracted_once(self, made, monkeypatch):
        calls = calls_of(monkeypatch, "extract_document")
        reader = PDFReader(made.path, **OPTIONS)

        first, second = reader.document, reader.document

        assert first is second
        assert len(calls) == 1

    def test_a_bundle_is_loaded_on_first_use_then_cached(self, made, monkeypatch):
        calls = calls_of(monkeypatch, "load_bundle")
        reader = PDFReader(made.bundle)

        assert calls == []
        assert reader.document is reader.document
        assert len(calls) == 1

    def test_a_failed_extraction_is_not_cached(self, made, monkeypatch):
        failures = iter([OSError("disk gone")])
        real = reader_module.extract_document

        def once_failing(*args, **kwargs):
            for failure in failures:
                raise failure
            return real(*args, **kwargs)

        monkeypatch.setattr(reader_module, "extract_document", once_failing)
        reader = PDFReader(made.path, **OPTIONS)

        with pytest.raises(OSError, match="disk gone"):
            reader.document
        assert reader.document == made.document


class TestConstructionChecks:
    """Spec invariants 1-2: a bad source or option raises at construction, before any work."""

    def test_a_missing_path_is_file_not_found(self, tmp_path, monkeypatch):
        refuse_work(monkeypatch)

        with pytest.raises(FileNotFoundError) as caught:
            PDFReader(tmp_path / "missing.pdf")
        assert caught.value.errno == errno.ENOENT

    def test_a_missing_manifest_is_file_not_found(self, tmp_path, monkeypatch):
        refuse_work(monkeypatch)

        with pytest.raises(FileNotFoundError):
            PDFReader(tmp_path / "manifest.json")

    @pytest.mark.parametrize(
        "content",
        [TEXT, b"", b" " * 1025 + b"%PDF-1.7\n", b"%pdf-1.7\n", b"%PD" + b" " * 2000],
        ids=["csv", "empty", "header-at-offset-1025", "lower-case", "cut-marker"],
    )
    def test_a_file_that_is_not_a_pdf_is_a_value_error(self, tmp_path, monkeypatch, content):
        refuse_work(monkeypatch)
        path = tmp_path / "parts.pdf"
        path.write_bytes(content)

        message = f"source {path} is not a PDF: no %PDF at any offset from 0 to 1024"
        with pytest.raises(ValueError, match=re.escape(message)):
            PDFReader(path)

    @pytest.mark.parametrize("offset", [0, 1, 1020, 1024], ids=lambda offset: f"offset-{offset}")
    @pytest.mark.parametrize("marker", [b"%PDF-1.7\n", b"%PDF"], ids=["versioned", "bare"])
    def test_a_file_with_the_marker_at_offsets_0_to_1024_passes_the_check(self, tmp_path, monkeypatch, offset, marker):
        refuse_work(monkeypatch)
        path = tmp_path / "parts.pdf"
        path.write_bytes(b" " * offset + marker)

        PDFReader(path)  # checked, not read: extraction is on first use

    def test_at_most_1028_bytes_are_read_to_check_the_header(self, tmp_path, monkeypatch):
        path = tmp_path / "parts.pdf"
        path.write_bytes(b" " * 4096)
        reads = []
        real = os.read
        monkeypatch.setattr(reader_module.os, "read", lambda fd, size: reads.append(size) or real(fd, size))

        with pytest.raises(ValueError, match="is not a PDF"):
            PDFReader(path)
        assert reads == [1028]

    def test_a_directory_without_a_manifest_is_a_value_error(self, tmp_path, monkeypatch):
        refuse_work(monkeypatch)

        with pytest.raises(ValueError, match=re.escape(f"source {tmp_path} is a directory without a manifest.json")):
            PDFReader(tmp_path)

    @pytest.mark.parametrize("target", ["directory", "manifest"])
    def test_a_bundle_whose_manifest_is_not_valid_is_a_value_error(self, made, tmp_path, monkeypatch, target):
        refuse_work(monkeypatch)
        bundle = tmp_path / "bundle"
        bundle.mkdir()
        (bundle / "manifest.json").write_bytes(b'{"bundle_format_version": "4.0"}')

        source = bundle if target == "directory" else bundle / "manifest.json"
        with pytest.raises(ValueError, match=re.escape(f"source {bundle} is not a valid bundle: manifest.json: ")):
            PDFReader(source)

    @pytest.mark.skipif(sys.platform == "win32", reason="named pipes are POSIX")
    def test_a_named_pipe_is_refused_without_blocking(self, tmp_path, monkeypatch):
        refuse_work(monkeypatch)
        fifo = tmp_path / "pipe.pdf"
        os.mkfifo(fifo)

        with pytest.raises(ValueError, match=re.escape(f"source {fifo} is not a regular file")):
            PDFReader(fifo)

    @pytest.mark.parametrize(
        "source,kind",
        [(42, "int"), (b"parts.pdf", "bytes"), (None, "NoneType"), (["parts.pdf"], "list")],
        ids=["int", "bytes", "none", "list"],
    )
    def test_a_source_of_another_type_is_a_type_error(self, monkeypatch, source, kind):
        refuse_work(monkeypatch)

        with pytest.raises(TypeError, match=re.escape(f"source must be a str, os.PathLike or Mapping, got {kind}")):
            PDFReader(source)

    def test_a_mapping_that_is_not_a_document_is_refused(self, made):
        with pytest.raises(ValueError, match=re.escape("document has missing key(s)")):
            PDFReader({"format": "datagrunt.pdf.document"})
        with pytest.raises(ValueError, match=re.escape("format must be 'datagrunt.pdf.document', got 'other'")):
            PDFReader({**made.json, "format": "other"})

    @pytest.mark.parametrize(
        "option,value,error,message",
        [
            ("workers", 0, ValueError, "workers must be >= 1, got 0"),
            ("dpi", 35, ValueError, "dpi must be >= 36, got 35"),
            ("vision_dpi", 199, ValueError, "vision_dpi must be >= dpi (200), got 199"),
            ("max_render_pixels", 262_143, ValueError, "max_render_pixels must be >= 262144, got 262143"),
            ("min_image_dimension", 0, ValueError, "min_image_dimension must be >= 1, got 0"),
            ("ocr", "sometimes", ValueError, "ocr must be 'auto' or 'always' or 'never', got 'sometimes'"),
            ("strict", 1, TypeError, "strict must be a bool"),
            ("workers", True, TypeError, "workers must be an int"),
            ("password", b"pw", TypeError, "password must be a str or None, got bytes"),
            ("password", "p\x00w", ValueError, "password must not contain NUL"),
        ],
    )
    @pytest.mark.parametrize("source", ["pdf", "bundle", "mapping", "missing"])
    def test_an_option_out_of_bounds_raises_first_for_every_source(
        self, made, tmp_path, monkeypatch, source, option, value, error, message
    ):
        refuse_work(monkeypatch)
        given = {"pdf": made.path, "bundle": made.bundle, "mapping": made.json, "missing": tmp_path / "gone.pdf"}

        with pytest.raises(error, match=re.escape(message)):
            PDFReader(given[source], **{option: value})


COLLECTIONS = (
    "pages",
    "headings",
    "paragraphs",
    "key_values",
    "tables",
    "lists",
    "figures",
    "page_images",
    "sections",
    "header_context",
    "metadata",
)


@pytest.fixture(scope="module")
def read(made):
    """A reader of the digital PDF, its document read once for the module."""
    reader = PDFReader(made.path, **OPTIONS)
    reader.document
    return reader


class TestCollections:
    """Each collection the spec lists is the document's own."""

    @pytest.mark.parametrize("name", COLLECTIONS)
    def test_each_collection_is_the_documents(self, read, name):
        assert getattr(read, name) is getattr(read.document, name)

    def test_the_fixture_holds_every_element_kind_but_lists(self, read):
        kinds = {name: len(getattr(read, name)) for name in ("headings", "paragraphs", "tables", "figures")}

        assert all(count > 0 for count in kinds.values()), kinds
        assert len(read.pages) == 2

    @pytest.mark.parametrize("name", COLLECTIONS)
    def test_a_collection_extracts_on_first_use(self, made, monkeypatch, name):
        calls = calls_of(monkeypatch, "extract_document")
        reader = PDFReader(made.path, **OPTIONS)

        getattr(reader, name)

        assert len(calls) == 1


class TestFrame:
    """A7a's element frame: as polars, Arrow, row dicts and a sample; and the document's Markdown."""

    def test_the_dataframe_is_the_documents_element_frame(self, read):
        frame = read.to_dataframe()

        assert frame.schema == FRAME_SCHEMA  # hidden_reasons included
        assert frame.equals(element_frame(read.document))
        assert frame.height == len(read.document.elements)

    def test_the_arrow_table_is_the_frame_of_the_fixed_arrow_schema(self, read):
        table = read.to_arrow_table()

        assert table.schema == ARROW_SCHEMA
        assert table.equals(element_arrow(read.document))

    def test_the_dicts_are_the_frames_rows(self, read):
        assert read.to_dicts() == element_frame(read.document).to_dicts()

    def test_the_frame_is_built_once_and_each_caller_gets_its_own(self, made, monkeypatch):
        calls = calls_of(monkeypatch, "element_frame")
        reader = PDFReader(made.path, **OPTIONS)

        first = reader.to_dataframe()
        first.drop_in_place("text")
        rows = reader.to_dicts()
        rows[0]["text"] = "changed"
        reader.to_arrow_table()
        reader.get_sample()

        assert "text" in reader.to_dataframe().columns
        assert reader.to_dicts()[0]["text"] != "changed"
        assert len(calls) == 1

    @pytest.mark.parametrize("n_rows,expected", [(None, 20), (1, 1), (3, 3), (10_000, None)])
    def test_the_sample_is_the_frames_first_rows(self, read, n_rows, expected):
        frame = element_frame(read.document)

        sample = read.get_sample() if n_rows is None else read.get_sample(n_rows)

        assert sample.equals(frame.head(expected if expected is not None else frame.height))

    @pytest.mark.parametrize(
        "n_rows,error,message",
        [
            (0, ValueError, "n_rows must be >= 1, got 0"),
            (-5, ValueError, "n_rows must be >= 1, got -5"),
            (True, TypeError, "n_rows must be an int, got True (bool)"),
            (2.0, TypeError, "n_rows must be an int, got 2.0 (float)"),
            ("20", TypeError, "n_rows must be an int, got '20' (str)"),
        ],
    )
    def test_n_rows_is_checked_before_any_work(self, made, monkeypatch, n_rows, error, message):
        reader = PDFReader(made.path, **OPTIONS)
        refuse_work(monkeypatch)

        with pytest.raises(error, match=re.escape(message)):
            reader.get_sample(n_rows)

    def test_the_markdown_is_the_documents(self, read):
        assert read.to_markdown() == read.document.to_markdown()

    def test_a_mapping_of_the_document_has_the_same_frame(self, made, read):
        assert PDFReader(made.json).to_dataframe().equals(read.to_dataframe())

    def test_a_bundle_has_the_same_frame_its_figures_linked_to_their_files(self, made, read):
        bundled = PDFReader(made.bundle).to_dataframe()
        extracted = read.to_dataframe()

        assert bundled.drop("markdown").equals(extracted.drop("markdown"))
        changed = bundled.filter(bundled["markdown"] != extracted["markdown"])
        assert changed["kind"].to_list() == ["figure"] * len(read.figures)
        assert all(link.startswith("![](") for link in changed["markdown"].to_list())

    def test_an_empty_document_has_an_empty_frame_of_the_schema(self):
        reader = PDFReader(document_to_json(empty_document()))

        assert reader.to_dataframe().schema == FRAME_SCHEMA
        assert (reader.to_dataframe().height, reader.to_dicts(), reader.get_sample().height) == (0, [], 0)
        assert reader.to_arrow_table().num_rows == 0


def connections_of(monkeypatch) -> list:
    """Record every DuckDB connection the reader opens."""
    opened: list = []
    real = reader_module.private_connection

    def recorded():
        opened.append(real())
        return opened[-1]

    monkeypatch.setattr(reader_module, "private_connection", recorded)
    return opened


def is_closed(connection) -> bool:
    try:
        connection.execute("SELECT 1")
    except duckdb.ConnectionException:
        return True
    return False


class TestDatabase:
    """``db_table`` names the element frame in ``query_data``'s private in-memory DuckDB."""

    def test_the_table_name_is_the_doc_id_made_an_identifier(self, read):
        assert read.db_table == f"tbl_{read.metadata.doc_id}"
        assert read.db_table == f"tbl_parts_list__{read.metadata.sha256[:16]}"

    def test_a_doc_id_with_a_hyphen_or_a_leading_digit_is_a_plain_identifier(self, made, tmp_path):
        path = tmp_path / "2026-Q1 report.pdf"
        path.write_bytes(made.path.read_bytes())
        reader = PDFReader(path, **OPTIONS)

        assert reader.db_table == f"tbl_2026_Q1_report__{made.document.metadata.sha256[:16]}"
        assert re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", reader.db_table)
        assert reader.query_data(f"SELECT count(*) AS n FROM {reader.db_table}").dicts() == [
            {"n": len(made.document.elements)}
        ]

    def test_the_table_name_is_stable_across_readers_and_sources(self, made, read):
        assert {PDFReader(made.path).db_table, PDFReader(made.json).db_table, read.db_table} == {read.db_table}

    def test_a_query_reads_the_element_frame(self, read):
        result = read.query_data(f"SELECT * FROM {read.db_table}")

        assert result.dicts() == read.to_dicts()
        assert result.pl().columns == read.to_dataframe().columns
        assert result.pa().num_rows == len(read.document.elements)

    def test_the_result_has_the_csv_readers_shape(self, read):
        result = read.query_data(f"SELECT kind, count(*) AS n FROM {read.db_table} GROUP BY kind ORDER BY kind")
        counts = {"heading": len(read.headings), "paragraph": len(read.paragraphs), "table": len(read.tables)}
        counts["figure"] = len(read.figures)
        expected = [{"kind": kind, "n": n} for kind, n in sorted(counts.items())]

        assert isinstance(result.pl(), pl.DataFrame) and result.pl().to_dicts() == expected
        assert isinstance(result.pa(), pa.Table) and result.pa().to_pylist() == expected
        assert result.dicts() == expected

    def test_a_table_name_is_case_insensitive_as_duckdb_reads_it(self, read):
        assert read.query_data(f"SELECT count(*) AS n FROM {read.db_table.upper()}").dicts() == [
            {"n": len(read.document.elements)}
        ]

    def test_the_sql_runs_as_it_is_written(self, read):
        assert read.query_data("SELECT 'O''Hara; DROP' AS name").dicts() == [{"name": "O'Hara; DROP"}]
        with pytest.raises(duckdb.ParserException):
            read.query_data("SELEC 1")

    def test_spilling_to_disk_is_off(self, read):
        assert read.query_data("SELECT current_setting('temp_directory') AS t").dicts() == [{"t": ""}]

    @pytest.mark.parametrize(
        "sql,error,message",
        [
            (None, TypeError, "sql must be a str, got None (NoneType)"),
            (b"SELECT 1", TypeError, "sql must be a str, got b'SELECT 1' (bytes)"),
            ("", ValueError, "sql must be a non-blank str, got ''"),
            (" \n\t", ValueError, "sql must be a non-blank str, got ' \\n\\t'"),
        ],
    )
    def test_sql_is_checked_before_any_work(self, made, monkeypatch, sql, error, message):
        reader = PDFReader(made.path, **OPTIONS)
        opened = connections_of(monkeypatch)
        refuse_work(monkeypatch)

        with pytest.raises(error, match=re.escape(message)):
            reader.query_data(sql)
        assert opened == []

    def test_the_connection_opens_on_the_first_query_then_is_reused(self, made, monkeypatch):
        opened = connections_of(monkeypatch)
        reader = PDFReader(made.path, **OPTIONS)
        reader.document, reader.to_dataframe(), reader.db_table

        assert opened == []
        reader.query_data("CREATE TABLE kept AS SELECT 7 AS n")
        assert reader.query_data("SELECT n FROM kept").dicts() == [{"n": 7}]
        assert len(opened) == 1

    def test_each_reader_has_its_own_database(self, made):
        first, second = PDFReader(made.json), PDFReader(made.json)
        first.query_data("CREATE TABLE only_first AS SELECT 1 AS n")

        with pytest.raises(duckdb.CatalogException):
            second.query_data("SELECT n FROM only_first")

    def test_a_result_is_read_whole_and_outlives_its_reader(self, made, monkeypatch):
        opened = connections_of(monkeypatch)
        result = PDFReader(made.json).query_data("SELECT 1 AS n")
        gc.collect()

        assert is_closed(opened[0])
        assert (result.dicts(), result.pl().to_dicts(), result.pa().to_pylist()) == ([{"n": 1}],) * 3

    def test_a_statement_without_rows_is_an_empty_result(self):
        class NoRows:
            def sql(self, sql):
                return None  # DuckDB 1.x's relation for a statement that returns nothing

        result = _database.query_result(NoRows(), "SET threads=1")

        assert (result.dicts(), result.pl().shape, result.pa().num_columns) == ([], (0, 0), 0)

    def test_a_result_holds_an_arrow_table(self):
        with pytest.raises(TypeError, match="table must be a pyarrow.Table, got DataFrame"):
            _database.QueryResult(pl.DataFrame({"n": [1]}))

    def test_a_connection_whose_frame_cannot_be_registered_is_closed_not_kept(self, made, monkeypatch):
        class Refusing:
            closed = False

            def register(self, name, table):
                raise duckdb.InvalidInputException("refused")

            def close(self):
                self.closed = True

        refusing = Refusing()
        monkeypatch.setattr(reader_module, "private_connection", lambda: refusing)
        reader = PDFReader(made.json)

        with pytest.raises(duckdb.InvalidInputException, match="refused"):
            reader.query_data("SELECT 1")
        assert refusing.closed

        monkeypatch.undo()
        assert reader.query_data("SELECT 2 AS n").dicts() == [{"n": 2}]  # the next query opens its own

    def test_concurrent_queries_each_read_the_frame_once_extracted(self, made, monkeypatch):
        calls = calls_of(monkeypatch, "extract_document")
        reader = PDFReader(made.path, **OPTIONS)
        sql = "SELECT count(*) AS n FROM {}"

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: reader.query_data(sql.format(reader.db_table)).dicts(), range(16)))

        assert results == [[{"n": len(made.document.elements)}]] * 16
        assert len(calls) == 1


class TestLifecycle:
    """The connection is released automatically when the reader goes; close() and ``with`` release it early."""

    def test_a_reader_never_queried_opens_no_connection(self, made, monkeypatch):
        opened = connections_of(monkeypatch)
        reader = PDFReader(made.path, **OPTIONS)
        reader.to_dicts()

        del reader
        gc.collect()

        assert opened == []

    def test_the_connection_is_closed_when_the_reader_is_collected(self, made, monkeypatch):
        opened = connections_of(monkeypatch)
        reader = PDFReader(made.path, **OPTIONS)
        reader.query_data(f"SELECT count(*) FROM {reader.db_table}")
        alive = weakref.ref(reader)

        assert not is_closed(opened[0])
        del reader
        gc.collect()

        assert alive() is None
        assert is_closed(opened[0])

    def test_close_releases_the_connection_early_and_the_reader_stays_usable(self, made, monkeypatch):
        opened = connections_of(monkeypatch)
        reader = PDFReader(made.json)
        reader.query_data("SELECT 1")

        reader.close()
        reader.close()  # idempotent

        assert is_closed(opened[0])
        assert reader.query_data(f"SELECT count(*) AS n FROM {reader.db_table}").dicts() == [
            {"n": len(made.document.elements)}
        ]
        assert len(opened) == 2 and not is_closed(opened[1])

    def test_with_releases_the_connection_at_its_end(self, made, monkeypatch):
        opened = connections_of(monkeypatch)

        with PDFReader(made.json) as reader:
            assert isinstance(reader, PDFReader)
            reader.query_data("SELECT 1")
            assert not is_closed(opened[0])

        assert is_closed(opened[0])

    def test_with_does_not_swallow_an_error(self, made, monkeypatch):
        opened = connections_of(monkeypatch)

        with pytest.raises(RuntimeError, match="the caller's"):
            with PDFReader(made.json) as reader:
                reader.query_data("SELECT 1")
                raise RuntimeError("the caller's")

        assert is_closed(opened[0])

    def test_close_before_any_query_opens_nothing(self, made, monkeypatch):
        opened = connections_of(monkeypatch)

        PDFReader(made.json).close()

        assert opened == []
