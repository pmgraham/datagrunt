"""Tests for the v5 document JSON codec (#371, A7a): ``datagrunt.pdf.codec``.

Documents are hand-built (``pdf_tests.documents``) to hold every element
type and every field, ``None`` values and lone surrogates included. A
corrupted JSON value is one key of a document's JSON changed by its path
(:func:`changed`).
"""

import copy
import dataclasses
import json
from pathlib import Path

import pytest

from datagrunt.pdf import codec
from datagrunt.pdf.codec import (
    FORMAT,
    VERSION,
    contained_path,
    document_from_json,
    document_from_json_bytes,
    document_to_json,
    document_to_json_bytes,
)
from datagrunt.pdf.elements import (
    BulletList,
    DocumentMetadata,
    Figure,
    Heading,
    KeyValue,
    Page,
    PageImage,
    PageQuality,
    Paragraph,
    PDFDocument,
    Section,
    Table,
)
from pdf_tests.documents import empty_document, rich_document, surrogate_document

# Fields that are not data: loaders (bytes come from files or the PDF) and the derived index.
NOT_DATA = {"_loader", "_raw_loader", "_band_loader", "_index"}
# JSON keys that are not fields: an element's type.
NOT_FIELDS = {"kind"}


def encoded(document=None) -> dict:
    """A fresh JSON value of ``document`` (rich_document by default), as json.loads gives it."""
    return json.loads(json.dumps(document_to_json(document or rich_document())))


def changed(path: tuple, value=None, *, delete: bool = False, document=None) -> dict:
    """The JSON of ``document`` with the key or index at ``path`` set to ``value`` (or deleted)."""
    data = encoded(document)
    node = data
    for step in path[:-1]:
        node = node[step]
    if delete:
        del node[path[-1]]
    else:
        node[path[-1]] = value
    return data


def data_fields(cls: type) -> set[str]:
    return {field.name for field in dataclasses.fields(cls)} - NOT_DATA


class TestRoundTrip:
    def test_rich_document_round_trips(self):
        document = rich_document()
        assert document_from_json(document_to_json(document)) == document

    def test_round_trips_through_json_text(self):
        document = rich_document()
        assert document_from_json(json.loads(json.dumps(document_to_json(document)))) == document

    def test_round_trips_through_bytes(self):
        document = rich_document()
        assert document_from_json_bytes(document_to_json_bytes(document)) == document

    def test_empty_document_round_trips(self):
        document = empty_document()
        assert document_from_json_bytes(document_to_json_bytes(document)) == document

    def test_lone_surrogates_round_trip_exactly(self):
        document = surrogate_document()
        raw = document_to_json_bytes(document)
        raw.decode("ascii")  # ensure_ascii: every surrogate is a \u escape
        loaded = document_from_json_bytes(raw)
        assert loaded == document
        assert loaded.metadata.title == "t\ud800"
        assert loaded.pages[0].elements[0].text == "Bad \ud800 heading"

    def test_rich_document_holds_every_element_type_and_none(self):
        document = rich_document()
        kinds = {type(element) for element in document.elements}
        assert kinds == {Heading, Paragraph, KeyValue, Table, BulletList, Figure}
        assert document.metadata.author is None and document.pages[1].quality.valid_char_ratio is None
        assert any(page.page_image is not None for page in document.pages)

    def test_every_value_survives(self):
        document = rich_document()
        loaded = document_from_json(document_to_json(document))
        for mine, theirs in zip(document.elements, loaded.elements):
            assert type(mine) is type(theirs)
            for name in data_fields(type(mine)) - {"image_path"}:
                assert getattr(mine, name) == getattr(theirs, name), (mine.element_id, name)
        assert loaded.sections == document.sections and loaded.header_context == document.header_context
        assert [page.warnings for page in loaded.pages] == [page.warnings for page in document.pages]

    def test_floats_and_ints_keep_their_types(self):
        loaded = document_from_json(encoded())
        assert isinstance(loaded.pages[0].width_pt, float) and isinstance(loaded.pages[2].page_image.dpi, float)
        assert loaded.pages[1].height_pt == 842.25
        assert loaded.pages[0].elements[6].column_centres == (100.0, 200.5)

    def test_column_intervals_are_pairs_in_the_json_and_tuples_back(self):
        data = encoded()

        assert data["pages"][0]["elements"][6]["column_intervals"] == [[60.0, 140.0], [180.0, 221.0]]
        assert document_from_json(data).pages[0].elements[6].column_intervals == ((60.0, 140.0), (180.0, 221.0))

    def test_encoding_is_deterministic(self):
        assert document_to_json_bytes(rich_document()) == document_to_json_bytes(rich_document())

    def test_top_level_names_format_and_version(self):
        data = document_to_json(rich_document())
        assert (data["format"], data["version"]) == ("datagrunt.pdf.document", 1) == (FORMAT, VERSION)

    def test_sections_and_header_context_are_element_references(self):
        data = document_to_json(rich_document())
        assert data["header_context"] == ["p0001_e000", "p0001_e001"]
        assert data["sections"][1]["heading"] == "p0001_e002"
        assert data["sections"][0] == {
            "heading": None,
            "scope_path": [],
            "page_span": [1, 1],
            "elements": ["p0001_e000", "p0001_e001"],
        }


class TestEveryField:
    """A field added to an A1 type must be added to the codec: these fail until it is."""

    @pytest.mark.parametrize(
        ("cls", "path"),
        [
            (PDFDocument, ()),
            (DocumentMetadata, ("metadata",)),
            (Page, ("pages", 0)),
            (PageQuality, ("pages", 0, "quality")),
            (PageImage, ("pages", 2, "page_image")),
            (Section, ("sections", 1)),
        ],
    )
    def test_json_keys_are_the_types_fields(self, cls, path):
        node = encoded()
        for step in path:
            node = node[step]
        extra = {"format", "version"} if cls is PDFDocument else set()
        assert set(node) == data_fields(cls) | extra

    def test_element_keys_are_each_types_fields(self):
        data = encoded()
        elements = [element for page in data["pages"] for element in page["elements"]]
        by_kind = {element["kind"]: element for element in elements}
        types = {"heading": Heading, "paragraph": Paragraph, "key_value": KeyValue}
        types |= {"table": Table, "list": BulletList, "figure": Figure}
        assert set(by_kind) == set(types)
        for kind, cls in types.items():
            assert set(by_kind[kind]) - NOT_FIELDS == data_fields(cls), kind
        assert set(by_kind["heading"]["bbox"]) == {"x0", "y0", "x1", "y1"}


class TestPaths:
    def with_paths(self) -> PDFDocument:
        document = rich_document()
        pages = list(document.pages)
        first = pages[0]
        figure = dataclasses.replace(first.elements[8], image_path=Path("images/a.png"))
        pages[0] = dataclasses.replace(first, elements=(*first.elements[:8], figure))
        image = dataclasses.replace(
            pages[2].page_image, image_path=Path("page_images/page_0003.png"), band_paths=(Path("b/1.png"),)
        )
        pages[2] = dataclasses.replace(pages[2], page_image=image)
        return dataclasses.replace(document, pages=tuple(pages))

    def test_paths_are_relative_strings_in_the_json(self):
        data = document_to_json(self.with_paths())
        assert data["pages"][0]["elements"][8]["image_path"] == "images/a.png"
        assert data["pages"][1]["elements"][3]["image_path"] is None
        image = data["pages"][2]["page_image"]
        assert (image["image_path"], image["band_paths"]) == ("page_images/page_0003.png", ["b/1.png"])

    def test_paths_resolve_against_base_dir(self, tmp_path):
        loaded = document_from_json(document_to_json(self.with_paths()), base_dir=tmp_path)
        assert loaded.pages[0].elements[8].image_path == tmp_path / "images" / "a.png"
        assert loaded.pages[2].page_image.image_path == tmp_path / "page_images" / "page_0003.png"
        assert loaded.pages[2].page_image.band_paths == (tmp_path / "b" / "1.png",)

    def test_paths_without_base_dir_are_as_written(self):
        loaded = document_from_json(document_to_json(self.with_paths()))
        assert loaded.pages[0].elements[8].image_path == Path("images/a.png")

    def test_bytes_are_not_in_the_json(self):
        calls = []
        document = rich_document(figure_loader=lambda: calls.append(1) or b"png")
        raw = document_to_json_bytes(document)
        assert calls == [] and b"png" not in raw.replace(b".png", b"")

    @pytest.mark.parametrize("bad", ["../a.png", "/abs.png", "a/../../b.png", "a\\b.png", "", "a//b.png", "./a.png"])
    def test_a_path_escaping_base_dir_is_refused(self, bad, tmp_path):
        data = changed(("pages", 0, "elements", 8, "image_path"), bad)
        with pytest.raises(ValueError, match="image_path"):
            document_from_json(data, base_dir=tmp_path)

    @pytest.mark.parametrize(
        ("bad", "part"),
        [
            ("images/C:evil.png", "C:evil.png"),
            ("C:/x", "C:"),
            ("a/b:stream", "b:stream"),
            ("images/CON", "CON"),
            ("images/con.png", "con.png"),
            ("images/LPT1.txt", "LPT1.txt"),
            ("images/a.", "a."),
            ("images/a ", "a "),
            ("images/a\x01.png", "a\x01.png"),
            ("images\\a.png", "images\\a.png"),
            ("images/a\x00.png", "a\x00.png"),
        ],
    )
    def test_a_path_part_windows_or_a_parser_would_read_otherwise_is_refused(self, bad, part, tmp_path):
        data = changed(("pages", 0, "elements", 8, "image_path"), bad)
        with pytest.raises(ValueError, match="image_path") as info:
            document_from_json(data, base_dir=tmp_path)
        assert repr(bad) in str(info.value) and repr(part) in str(info.value)

    def test_a_path_through_a_symlinked_directory_out_of_base_dir_is_refused(self, tmp_path):
        root, outside = tmp_path / "bundle", tmp_path / "outside"
        root.mkdir()
        outside.mkdir()
        (root / "images").symlink_to(outside, target_is_directory=True)
        data = changed(("pages", 0, "elements", 8, "image_path"), "images/a.png")
        with pytest.raises(ValueError, match="resolves outside"):
            document_from_json(data, base_dir=root)


class TestContainedPath:
    """The one check of every path a bundle names (codec and bundle loader alike)."""

    @pytest.mark.parametrize(
        ("bad", "part"),
        [
            ("images/C:evil.png", "C:evil.png"),
            ("C:/x", "C:"),
            ("a/b:stream", "b:stream"),
            ("images/CON", "CON"),
            ("images/con.png", "con.png"),
            ("images/LPT1.txt", "LPT1.txt"),
            ("images/aux.tar.gz", "aux.tar.gz"),
            ("PRN/a.png", "PRN"),
            ("images/nul", "nul"),
            ("images/COM1", "COM1"),
            ("images/com9.png", "com9.png"),
            ("images/lpt9", "lpt9"),
            ("images/a.", "a."),
            ("images/a ", "a "),
            ("images/a\x01.png", "a\x01.png"),
            ("images/a\x1f", "a\x1f"),
            ("images\\a.png", "images\\a.png"),
            ("images/a\x00.png", "a\x00.png"),
            ("../a.png", ".."),
            ("a/../../b.png", ".."),
            ("./a.png", "."),
            ("/abs.png", ""),
            ("", ""),
            ("a//b.png", ""),
            ("images/", ""),
        ],
    )
    def test_a_bad_part_is_refused_naming_the_path_and_the_part(self, bad, part, tmp_path):
        with pytest.raises(ValueError, match="where") as info:
            contained_path("where", bad, tmp_path)
        assert repr(bad) in str(info.value) and repr(part) in str(info.value)

    @pytest.mark.parametrize(
        "good",
        [
            "images/a.png",
            "json/document.json",
            "page_images/bands/page_0001_band_01.png",
            "a/b/c/d.e.f",
            "images/console.png",
            "images/COM10.png",
            "images/LPT.png",
            "images/.hidden",
            "images/a b.png",
        ],
    )
    def test_a_plain_nested_path_is_joined_to_the_root(self, good, tmp_path):
        assert contained_path("where", good, tmp_path) == tmp_path.joinpath(*good.split("/"))

    def test_a_symlinked_directory_out_of_the_root_is_refused(self, tmp_path):
        root, outside = tmp_path / "bundle", tmp_path / "outside"
        root.mkdir()
        outside.mkdir()
        (root / "images").symlink_to(outside, target_is_directory=True)
        with pytest.raises(ValueError, match="'images/a.png' resolves outside"):
            contained_path("where", "images/a.png", root)

    def test_a_symlinked_file_out_of_the_root_is_refused(self, tmp_path):
        root = tmp_path / "bundle"
        root.mkdir()
        (tmp_path / "secret").write_bytes(b"x")
        (root / "a.png").symlink_to(tmp_path / "secret")
        with pytest.raises(ValueError, match="resolves outside"):
            contained_path("where", "a.png", root)

    def test_a_symlink_inside_the_root_is_accepted(self, tmp_path):
        (tmp_path / "real").mkdir()
        (tmp_path / "images").symlink_to(tmp_path / "real", target_is_directory=True)
        assert contained_path("where", "images/a.png", tmp_path) == tmp_path / "images" / "a.png"

    def test_the_root_must_exist(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            contained_path("where", "a.png", tmp_path / "missing")

    def test_a_non_str_is_refused(self, tmp_path):
        with pytest.raises(TypeError, match="where must be a str, got int"):
            contained_path("where", 3, tmp_path)


class TestVersion:
    @pytest.mark.parametrize("version", [0, 2, 99])
    def test_unknown_version_is_refused(self, version):
        with pytest.raises(ValueError, match=f"version {version} is not supported"):
            document_from_json(changed(("version",), version))

    @pytest.mark.parametrize("version", [True, "1", 1.0, None])
    def test_version_must_be_an_int(self, version):
        with pytest.raises(TypeError, match="version must be an int"):
            document_from_json(changed(("version",), version))

    @pytest.mark.parametrize("name", ["datagrunt.pdf.bundle", "", None])
    def test_unknown_format_is_refused(self, name):
        with pytest.raises(ValueError, match="format"):
            document_from_json(changed(("format",), name))


class TestUnknownAndMissingKeys:
    PLACES = [
        (),
        ("metadata",),
        ("pages", 0),
        ("pages", 0, "quality"),
        ("pages", 2, "page_image"),
        ("pages", 0, "elements", 0),
        ("pages", 0, "elements", 8),
        ("pages", 0, "elements", 0, "bbox"),
        ("sections", 0),
    ]

    @pytest.mark.parametrize("place", PLACES)
    def test_unknown_key_is_refused(self, place):
        with pytest.raises(ValueError, match=r"unknown key\(s\) \['surprise'\]"):
            document_from_json(changed((*place, "surprise"), 1))

    @pytest.mark.parametrize("place", PLACES)
    def test_missing_key_is_refused(self, place):
        node = encoded()
        for step in place:
            node = node[step]
        key = sorted(set(node) - {"kind"})[0]
        with pytest.raises(ValueError, match=rf"missing key\(s\) \['{key}'\]"):
            document_from_json(changed((*place, key), delete=True))

    def test_top_level_must_be_an_object(self):
        with pytest.raises(TypeError, match="document must be a JSON object, got list"):
            document_from_json([])


class TestBadValues:
    @pytest.mark.parametrize(
        ("path", "value", "error", "message"),
        [
            (("pages", 0, "elements", 2, "level"), 7, ValueError, "level must be <= 6"),
            (("pages", 0, "elements", 2, "level"), True, TypeError, "level must be an int"),
            (("pages", 0, "page_number"), "1", TypeError, "page_number must be an int"),
            (("pages", 0, "elements", 0, "bbox", "x0"), 600.0, ValueError, "x0 must be <= x1"),
            (("pages", 0, "elements", 0, "bbox", "y1"), "90", TypeError, "y1 must be an int or float"),
            (("pages", 0, "elements", 8, "sha256"), "ABC", ValueError, "sha256 must be 64 lowercase hex"),
            (("pages", 0, "elements", 7, "ordered"), 1, TypeError, "ordered must be a bool"),
            (("pages", 0, "status"), "bogus", ValueError, "status must be one of"),
            (("pages", 0, "elements", 3, "reading_order"), 4, ValueError, "encodes index 3"),
            (("pages", 0, "quality", "valid_char_ratio"), 1.5, ValueError, "valid_char_ratio must be <= 1.0"),
            (("pages", 2, "page_image", "dpi"), 0, ValueError, "dpi must be > 0"),
            (("pages", 2, "page_image", "contrast_normalized"), "yes", TypeError, "contrast_normalized must be a bool"),
            (("metadata", "page_count"), 3, ValueError, "pages has 4 page"),
            (("metadata", "doc_id"), "other__0000000000000000", ValueError, "doc_id must be"),
            (("pages", 0, "elements", 6, "column_centres"), [1.0], ValueError, "column_centres must have 2"),
            (("pages", 0, "elements", 6, "column_intervals"), [[60.0, 140.0]], ValueError, "intervals must have 2"),
            (("pages", 0, "elements", 6, "column_intervals", 0), [140.0, 60.0], ValueError, "must have start <= end"),
            (("pages", 0, "elements", 6, "column_intervals", 0), [60.0], ValueError, r"\[0\] must have 2 values"),
            (("pages", 0, "elements", 6, "column_intervals"), [], ValueError, "set exactly when column_centres are"),
            (("pages", 1, "elements", 1, "continued_from_id"), "p0001_e000", ValueError, "must name a table"),
        ],
    )
    def test_bad_value_is_refused_by_a1(self, path, value, error, message):
        with pytest.raises(error, match=message):
            document_from_json(changed(path, value))

    @pytest.mark.parametrize(
        ("path", "value", "message"),
        [
            (("pages", 0, "elements", 2, "scope_path"), "Intro", "scope_path must be a JSON array, got str"),
            (("pages", 0, "elements", 6, "rows"), "ab", "rows must be a JSON array, got str"),
            (("pages", 0, "elements", 6, "rows", 0), "ab", r"rows\[0\] must be a JSON array, got str"),
            (("pages", 0, "elements", 6, "column_intervals"), "ab", "column_intervals must be a JSON array, got str"),
            (
                ("pages", 0, "elements", 6, "column_intervals", 0),
                "ab",
                r"column_intervals\[0\] must be a JSON array, got str",
            ),
            (("pages", 0, "warnings"), None, "warnings must be a JSON array, got NoneType"),
            (("pages",), {}, "pages must be a JSON array, got dict"),
            (("pages", 0, "elements", 0, "bbox"), [1, 2, 3, 4], "bbox must be a JSON object, got list"),
            (("pages", 0, "quality"), [], "quality must be a JSON object or null, got list"),
            (("sections", 0, "page_span"), "11", "page_span must be a JSON array, got str"),
            (("pages", 0, "elements", 8, "image_path"), 5, "image_path must be a str or null, got int"),
        ],
    )
    def test_a_wrong_container_is_refused_not_coerced(self, path, value, message):
        with pytest.raises(TypeError, match=message):
            document_from_json(changed(path, value))

    @pytest.mark.parametrize(
        ("path", "value", "where"),
        [
            (("pages", 0, "elements", 2, "level"), 7, r"pages\[0\]\.elements\[2\]: level must be <= 6"),
            (("pages", 2, "page_image", "dpi"), 0, r"pages\[2\]: dpi must be > 0"),
            (("metadata", "sha256"), "x", "metadata: sha256 must be 64"),
            (("sections", 0, "page_span"), [2, 1], r"sections\[0\]: page_span start 2 must be <= end 1"),
            (("metadata", "page_count"), 3, "document: pages has 4 page"),
        ],
    )
    def test_an_a1_error_names_where_it_is(self, path, value, where):
        with pytest.raises((TypeError, ValueError), match=where):
            document_from_json(changed(path, value))

    def test_an_element_must_be_an_object_with_a_kind(self):
        with pytest.raises(TypeError, match=r"pages\[0\]\.elements\[0\] must be a JSON object, got list"):
            document_from_json(changed(("pages", 0, "elements", 0), []))
        with pytest.raises(ValueError, match=r"pages\[0\]\.elements\[0\] has missing key\(s\) \['kind'\]"):
            document_from_json(changed(("pages", 0, "elements", 0, "kind"), delete=True))

    def test_a_band_path_must_be_a_str(self):
        with pytest.raises(TypeError, match=r"band_paths\[0\] must be a str, got int"):
            document_from_json(changed(("pages", 2, "page_image", "band_paths"), [5]))

    def test_unknown_kind_is_refused(self):
        with pytest.raises(ValueError, match="kind must be one of"):
            document_from_json(changed(("pages", 0, "elements", 0, "kind"), "chart"))

    @pytest.mark.parametrize(
        "path",
        [("header_context", 0), ("sections", 1, "heading"), ("sections", 1, "elements", 0)],
    )
    def test_a_reference_to_no_element_is_refused(self, path):
        with pytest.raises(ValueError, match="'p0009_e000' names no element of the document"):
            document_from_json(changed(path, "p0009_e000"))

    def test_a_reference_must_be_a_str(self):
        with pytest.raises(TypeError, match=r"header_context\[0\] must be an element id"):
            document_from_json(changed(("header_context", 0), 0))

    def test_a_section_heading_reference_must_name_a_heading(self):
        with pytest.raises(TypeError, match="heading must be a Heading or None, got Paragraph"):
            document_from_json(changed(("sections", 1, "heading"), "p0001_e003"))

    def test_rejects_a_non_document(self):
        with pytest.raises(TypeError, match="document must be a PDFDocument, got dict"):
            document_to_json({})
        with pytest.raises(TypeError, match="document must be a PDFDocument, got str"):
            document_to_json_bytes("x")

    def test_base_dir_type(self):
        with pytest.raises(TypeError, match="base_dir must be a str, os.PathLike or None, got int"):
            document_from_json(encoded(), base_dir=3)

    def test_decoding_does_not_mutate_its_input(self):
        data = encoded()
        before = copy.deepcopy(data)
        document_from_json(data)
        assert data == before


class TestBytes:
    def test_over_the_bound_is_refused_unparsed(self, monkeypatch):
        raw = document_to_json_bytes(rich_document())
        monkeypatch.setattr(codec, "_MAX_JSON_BYTES", len(raw) - 1)
        with pytest.raises(ValueError, match=f"is {len(raw)} bytes, over the {len(raw) - 1}-byte bound"):
            document_from_json_bytes(raw)

    def test_the_bound_is_the_asset_limit(self):
        assert codec._MAX_JSON_BYTES == 1 << 30

    def test_at_the_bound_is_read(self, monkeypatch):
        raw = document_to_json_bytes(rich_document())
        monkeypatch.setattr(codec, "_MAX_JSON_BYTES", len(raw))
        assert document_from_json_bytes(raw) == rich_document()

    @pytest.mark.parametrize(
        ("raw", "message"),
        [
            (b'{"format": NaN}', "NaN is not JSON"),
            (b'{"format": 1, "format": 2}', "duplicate key 'format'"),
            (b"{", "not valid JSON"),
            (b"[" * 200_000 + b"]" * 200_000, "nested too deeply"),
            (b"\xff\xfe\xfd", "not valid JSON"),
        ],
    )
    def test_malformed_json_is_refused(self, raw, message):
        with pytest.raises(ValueError, match=message):
            document_from_json_bytes(raw)

    def test_raw_must_be_bytes(self):
        with pytest.raises(TypeError, match="raw must be bytes, got str"):
            document_from_json_bytes("{}")

    def test_bytes_are_ascii_json_of_the_value(self):
        raw = document_to_json_bytes(rich_document())
        assert json.loads(raw.decode("ascii")) == encoded()
