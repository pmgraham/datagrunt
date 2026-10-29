"""Tests for the v5 ``PDFDocument`` contract: invariants, derived collections, Markdown."""

import copy
import dataclasses
import hashlib
import inspect
import pickle
import re

import pytest

from datagrunt.pdf import (
    BBox,
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
from datagrunt.pdf.elements import make_element_id

SOURCE_SHA = hashlib.sha256(b"source-pdf").hexdigest()
FIGURE_SHA = hashlib.sha256(b"figure-bytes").hexdigest()
BOX = BBox(x0=72, y0=72, x1=540, y1=90)
QUALITY = PageQuality(
    valid_char_ratio=0.98,
    char_count=1200,
    vector_line_count=14,
    hidden_spans_flagged=0,
    rotation_deg=0,
    deskew_angle_deg=0.0,
    ocr_mean_confidence=None,
)


def _metadata(page_count):
    doc_id = DocumentMetadata.make_doc_id("report.pdf", SOURCE_SHA)
    return DocumentMetadata(doc_id=doc_id, source_name="report.pdf", sha256=SOURCE_SHA, page_count=page_count)


def _ids(page, index, scope_path=()):
    return {
        "element_id": make_element_id(page, index),
        "page_number": page,
        "reading_order": index,
        "bbox": BOX,
        "scope_path": scope_path,
    }


def _paragraph(page, index, text=None, scope_path=()):
    return Paragraph(**_ids(page, index, scope_path), text=text or f"Paragraph {page}.{index}.")


def _heading(page, index, text, level=1):
    # A heading's own scope_path ends with its text.
    return Heading(**_ids(page, index, (text,)), level=level, text=text)


def _table(page, index, continued_from_id=None):
    return Table(
        **_ids(page, index),
        headers=("k",),
        rows=((f"{page}.{index}",),),
        extraction_method="lattice",
        continued_from_id=continued_from_id,
    )


def _one_of_each(page):
    """One element of every DocumentElement type, in reading order on ``page``."""
    return (
        _heading(page, 0, f"Heading {page}"),
        Paragraph(**_ids(page, 1), text=f"Body {page}."),
        KeyValue(**_ids(page, 2), key="Invoice", value=str(page)),
        _table(page, 3),
        BulletList(**_ids(page, 4), items=("x",), ordered=True),
        Figure(**_ids(page, 5), sha256=FIGURE_SHA, width_px=8, height_px=8),
    )


def _page(page_number, *, elements=(), status="ok", route="digital", integrity_verified=False, **overrides):
    if status == "error":
        overrides.setdefault("error", "pdfium: bad xref")
    return Page(
        page_number=page_number,
        width_pt=612,
        height_pt=792,
        status=status,
        route=route,
        quality=None if status == "error" else QUALITY,  # an error page measured nothing
        elements=elements,
        integrity_verified=integrity_verified,
        **overrides,
    )


# Values derived from the pages: read-only properties, never dataclass fields.
DERIVED = (
    "elements",
    "headings",
    "paragraphs",
    "key_values",
    "tables",
    "lists",
    "figures",
    "page_images",
    "degraded_pages",
    "integrity_verified",
)


def _document(*pages, sections=()):
    return PDFDocument(metadata=_metadata(len(pages)), pages=pages, sections=sections)


def _two_page_document(**kwargs):
    return _document(_page(1, elements=(_paragraph(1, 0),)), _page(2, elements=(_paragraph(2, 0),)), **kwargs)


class TestDocumentFields:
    def test_valid_document(self):
        doc = _two_page_document()
        assert [page.page_number for page in doc.pages] == [1, 2]
        assert doc.metadata.page_count == 2
        assert doc.sections == ()

    def test_zero_page_document(self):
        doc = PDFDocument(metadata=_metadata(0), pages=())
        assert (doc.pages, doc.elements, doc.degraded_pages) == ((), (), ())

    def test_metadata_must_be_document_metadata(self):
        with pytest.raises(TypeError, match="metadata must be a DocumentMetadata, got dict"):
            PDFDocument(metadata={"page_count": 0}, pages=())

    @pytest.mark.parametrize(
        ("bad", "match"), [([], "pages must be a tuple"), ((_paragraph(1, 0),), r"pages\[0\] must be a Page")]
    )
    def test_pages_must_be_tuple_of_pages(self, bad, match):
        with pytest.raises(TypeError, match=match):
            PDFDocument(metadata=_metadata(len(bad)), pages=bad)

    @pytest.mark.parametrize(
        ("bad", "match"), [([], "sections must be a tuple"), ((_page(1),), r"sections\[0\] must be a Section")]
    )
    def test_sections_must_be_tuple_of_sections(self, bad, match):
        with pytest.raises(TypeError, match=match):
            PDFDocument(metadata=_metadata(0), pages=(), sections=bad)

    def test_is_frozen_slotted_and_keyword_only(self):
        doc = _two_page_document()
        with pytest.raises(dataclasses.FrozenInstanceError):
            doc.pages = ()
        assert not hasattr(doc, "__dict__")
        # Every init field supplied positionally: only kw_only can make this a TypeError.
        positional = [getattr(doc, f.name) for f in dataclasses.fields(doc) if f.init]
        with pytest.raises(TypeError, match="positional argument"):
            PDFDocument(*positional)

    @pytest.mark.parametrize("derived", [*DERIVED, "_index"])
    def test_derived_fields_are_not_constructor_arguments(self, derived):
        with pytest.raises(TypeError, match=f"unexpected keyword argument '{derived}'"):
            PDFDocument(metadata=_metadata(0), pages=(), **{derived: ()})

    @pytest.mark.parametrize("derived", DERIVED)
    def test_derived_values_are_read_only_properties_not_fields(self, derived):
        assert isinstance(inspect.getattr_static(PDFDocument, derived), property)
        assert derived not in {f.name for f in dataclasses.fields(PDFDocument)}
        # Frozen + slotted dataclasses reject assigning a non-field name with
        # TypeError (a CPython quirk; BBox.width behaves the same), a field with
        # FrozenInstanceError (an AttributeError): either way it never sticks.
        with pytest.raises((AttributeError, TypeError)):
            setattr(_two_page_document(), derived, ())

    def test_one_private_index_field_holds_the_derived_values(self):
        hidden = [f for f in dataclasses.fields(PDFDocument) if not f.init]
        assert [f.name for f in hidden] == ["_index"]
        assert (hidden[0].repr, hidden[0].compare) == (False, False)
        assert [f.name for f in dataclasses.fields(PDFDocument) if f.init] == ["metadata", "pages", "sections"]

    def test_replace_revalidates(self):
        with pytest.raises(ValueError, match="metadata.page_count is 5"):
            dataclasses.replace(_two_page_document(), metadata=_metadata(5))

    def test_replace_recomputes_derived_fields(self):
        doc = _two_page_document()
        error_page = _page(2, status="error")
        replaced = dataclasses.replace(doc, pages=(doc.pages[0], error_page))
        assert replaced.degraded_pages == (2,)
        assert replaced.elements == (_paragraph(1, 0),)

    def test_equal_and_hashable(self):
        assert _two_page_document() == _two_page_document()
        assert hash(_two_page_document()) == hash(_two_page_document())

    def test_derived_fields_not_in_repr(self):
        # Page reprs nest their own elements= / integrity_verified=, so check the outer shape.
        text = repr(_two_page_document())
        assert text.startswith("PDFDocument(metadata=DocumentMetadata(")
        assert text.endswith(", sections=())")
        assert "headings=" not in text
        assert "degraded_pages=" not in text

    @pytest.mark.parametrize(
        "clone", [lambda doc: pickle.loads(pickle.dumps(doc)), copy.deepcopy], ids=["pickle", "deepcopy"]
    )
    def test_round_trip_keeps_derived_fields(self, clone):
        def build():
            return _document(
                _page(1, elements=_one_of_each(1), integrity_verified=True),
                _page(2, status="ocr_fallback", route="vision", elements=(_paragraph(2, 0),)),
                _page(3, status="error"),
            )

        restored, fresh = clone(build()), build()
        assert restored == fresh
        for derived in DERIVED:
            assert getattr(restored, derived) == getattr(fresh, derived), derived
        # An error page (page 3) makes the document unverified.
        assert (restored.degraded_pages, restored.integrity_verified) == ((2, 3), False)


class TestDocumentPageInvariants:
    @pytest.mark.parametrize(("page_count", "pages"), [(3, 2), (1, 2), (0, 1)])
    def test_page_count_mismatch_rejected(self, page_count, pages):
        page_tuple = tuple(_page(n) for n in range(1, pages + 1))
        with pytest.raises(ValueError, match=rf"pages has {pages} page\(s\), but metadata.page_count is {page_count}"):
            PDFDocument(metadata=_metadata(page_count), pages=page_tuple)

    @pytest.mark.parametrize(
        ("numbers", "match"),
        [
            ((1, 3), r"pages\[1\] has page_number 3, expected 2"),  # gap
            ((2, 1), r"pages\[0\] has page_number 2, expected 1"),  # out of order
            ((1, 1), r"pages\[1\] has page_number 1, expected 2"),  # repeated
            ((2,), r"pages\[0\] has page_number 2, expected 1"),  # does not start at 1
        ],
    )
    def test_pages_must_be_numbered_one_to_n_in_order(self, numbers, match):
        with pytest.raises(ValueError, match=match):
            _document(*(_page(n) for n in numbers))


class TestDocumentElementIds:
    def test_same_index_on_different_pages_is_distinct(self):
        doc = _two_page_document()
        assert [e.element_id for e in doc.elements] == ["p0001_e000", "p0002_e000"]

    def test_duplicate_element_ids_across_pages_rejected(self):
        # Unreachable through validated Pages (an id encodes its page, and pages
        # are numbered 1..N), so a page whose own validation was bypassed stands
        # in for corrupted input: the document-wide check must still catch it.
        first = _page(1, elements=(_paragraph(1, 0),))
        second = _page(2)
        object.__setattr__(second, "elements", first.elements)
        with pytest.raises(ValueError, match="duplicate element_id 'p0001_e000' across pages"):
            _document(first, second)


class TestDocumentContinuations:
    def test_continuation_from_earlier_page(self):
        doc = _document(
            _page(1, elements=(_table(1, 0),)),
            _page(2, elements=(_table(2, 0, continued_from_id="p0001_e000"),)),
            _page(3, elements=(_paragraph(3, 0), _table(3, 1, continued_from_id="p0002_e000"))),
        )
        assert [t.continued_from_id for t in doc.tables] == [None, "p0001_e000", "p0002_e000"]

    def test_continuation_may_skip_a_page(self):
        doc = _document(
            _page(1, elements=(_table(1, 0),)),
            _page(2),
            _page(3, elements=(_table(3, 0, continued_from_id="p0001_e000"),)),
        )
        assert doc.tables[1].continued_from_id == "p0001_e000"

    def test_chain_of_continuations_accepted(self):
        doc = _document(
            _page(1, elements=(_table(1, 0),)),
            _page(2, elements=(_table(2, 0, continued_from_id="p0001_e000"),)),
            _page(3, elements=(_table(3, 0, continued_from_id="p0002_e000"),)),
        )
        assert len(doc.tables) == 3

    @pytest.mark.parametrize(
        ("pages", "second"),
        [
            # both continuations on the same later page
            (
                (
                    _page(1, elements=(_table(1, 0),)),
                    _page(2, elements=(_table(2, 0, "p0001_e000"), _table(2, 1, "p0001_e000"))),
                ),
                "p0002_e001",
            ),
            # the second continuation on a page after the first
            (
                (
                    _page(1, elements=(_table(1, 0),)),
                    _page(2, elements=(_table(2, 0, "p0001_e000"),)),
                    _page(3, elements=(_paragraph(3, 0), _table(3, 1, "p0001_e000"))),
                ),
                "p0003_e001",
            ),
        ],
        ids=["same-page", "later-page"],
    )
    def test_a_table_continued_twice_rejected(self, pages, second):
        # A split table has one next part: a second continuation would fork it.
        with pytest.raises(
            ValueError, match=rf"^table {second}: table 'p0001_e000' is already continued by p0002_e000$"
        ):
            _document(*pages)

    @pytest.mark.parametrize(
        ("pages", "described"),
        [
            # dangling: no element has that id
            ((_page(1, elements=(_table(1, 0),)), _page(2, elements=(_table(2, 0, "p0001_e009"),))), "no such element"),
            # forward: names a table on a later page
            (
                (_page(1, elements=(_table(1, 0, "p0002_e000"),)), _page(2, elements=(_table(2, 0),))),
                "a Table on page 2",
            ),
            # same page: not an earlier page
            ((_page(1, elements=(_table(1, 0), _table(1, 1, "p0001_e000"))), _page(2)), "a Table on page 1"),
            # names an element that is not a table
            ((_page(1, elements=(_paragraph(1, 0),)), _page(2, elements=(_table(2, 0, "p0001_e000"),))), "a Paragraph"),
        ],
    )
    def test_bad_continuation_rejected(self, pages, described):
        with pytest.raises(ValueError, match=f"must name a table on an earlier page, got {described}"):
            _document(*pages)


class TestDocumentSections:
    def test_sections_made_of_document_elements(self):
        heading = _heading(1, 0, "Intro")
        pages = (
            _page(1, elements=(heading, _paragraph(1, 1, scope_path=("Intro",)))),
            _page(2, elements=(_paragraph(2, 0),)),
        )
        section = Section(heading=heading, scope_path=("Intro",), page_span=(1, 2), elements=pages[0].elements)
        doc = _document(*pages, sections=(section,))
        assert doc.sections == (section,)

    def test_section_element_not_in_document_rejected(self):
        stray = _paragraph(1, 7)
        section = Section(heading=None, scope_path=(), page_span=(1, 1), elements=(stray,))
        with pytest.raises(ValueError, match=r"sections\[1\] element 'p0001_e007' is not in the document"):
            _two_page_document(sections=(Section(heading=None, scope_path=(), page_span=(1, 1), elements=()), section))

    def test_section_element_differing_from_document_element_rejected(self):
        altered = _paragraph(2, 0, text="Not what page 2 says.")
        section = Section(heading=None, scope_path=(), page_span=(2, 2), elements=(altered,))
        with pytest.raises(ValueError, match=r"sections\[0\] element 'p0002_e000' differs from the document's element"):
            _two_page_document(sections=(section,))

    def test_section_heading_not_in_document_rejected(self):
        stray = _heading(1, 9, "Intro")
        section = Section(heading=stray, scope_path=("Intro",), page_span=(1, 1), elements=())
        with pytest.raises(ValueError, match=r"sections\[0\] heading 'p0001_e009' is not in the document"):
            _two_page_document(sections=(section,))

    def test_section_heading_differing_from_document_element_rejected(self):
        heading = _heading(1, 0, "Intro")
        altered = _heading(1, 0, "Intro", level=2)
        section = Section(heading=altered, scope_path=("Intro",), page_span=(1, 1), elements=())
        with pytest.raises(ValueError, match=r"sections\[0\] heading 'p0001_e000' differs from the document's element"):
            _document(_page(1, elements=(heading,)), sections=(section,))

    def test_section_page_span_past_last_page_rejected(self):
        section = Section(heading=None, scope_path=(), page_span=(1, 3), elements=())
        with pytest.raises(ValueError, match=r"sections\[0\] page_span \(1, 3\) ends after the document's last page 2"):
            _two_page_document(sections=(section,))

    def test_section_page_span_ending_on_last_page_accepted(self):
        section = Section(heading=None, scope_path=(), page_span=(2, 2), elements=())
        assert _two_page_document(sections=(section,)).sections == (section,)

    def test_section_in_zero_page_document_rejected(self):
        section = Section(heading=None, scope_path=(), page_span=(1, 1), elements=())
        with pytest.raises(ValueError, match="ends after the document's last page 0"):
            PDFDocument(metadata=_metadata(0), pages=(), sections=(section,))

    def test_equal_copy_of_a_document_element_accepted(self):
        copy = _paragraph(2, 0)  # equal to, but not the same object as, the page's element
        section = Section(heading=None, scope_path=(), page_span=(2, 2), elements=(copy,))
        assert _two_page_document(sections=(section,)).sections[0].elements == (copy,)


# Two pages, two flat sections: "Intro" runs from p1 e000 to p2 e000, "Next" from p2 e001.
INTRO = ("Intro",)
NEXT = ("Next",)
H_INTRO, P11, P20 = _heading(1, 0, "Intro"), _paragraph(1, 1, scope_path=INTRO), _paragraph(2, 0, scope_path=INTRO)
H_NEXT, P22 = _heading(2, 1, "Next"), _paragraph(2, 2, scope_path=NEXT)
S_INTRO = Section(heading=H_INTRO, scope_path=INTRO, page_span=(1, 2), elements=(H_INTRO, P11, P20))
S_NEXT = Section(heading=H_NEXT, scope_path=NEXT, page_span=(2, 2), elements=(H_NEXT, P22))


def _sectioned(*sections):
    return _document(_page(1, elements=(H_INTRO, P11)), _page(2, elements=(P20, H_NEXT, P22)), sections=sections)


class TestDocumentSectionLayout:
    def test_flat_sections_in_document_order(self):
        assert _sectioned(S_INTRO, S_NEXT).sections == (S_INTRO, S_NEXT)

    def test_sections_out_of_document_order_rejected(self):
        with pytest.raises(
            ValueError,
            match=re.escape(
                "sections[1] starts at page 1 reading_order 0, not after sections[0] ends at page 2 reading_order 2"
            ),
        ):
            _sectioned(S_NEXT, S_INTRO)

    def test_interleaved_sections_rejected(self):
        # Starts are in order ((1, 0) then (1, 1)), but the first section runs past the second's start.
        first = Section(heading=H_INTRO, scope_path=INTRO, page_span=(1, 2), elements=(H_INTRO, P20))
        second = Section(heading=None, scope_path=INTRO, page_span=(1, 1), elements=(P11,))
        with pytest.raises(
            ValueError, match=r"sections\[1\] starts at page 1 reading_order 1, not after sections\[0\] ends"
        ):
            _sectioned(first, second)

    def test_empty_sections_take_no_place_in_the_order(self):
        empty = Section(heading=None, scope_path=(), page_span=(2, 2), elements=())
        assert _sectioned(empty, S_INTRO, empty, S_NEXT, empty).sections == (empty, S_INTRO, empty, S_NEXT, empty)

    def test_element_in_two_sections_rejected(self):
        again = Section(heading=None, scope_path=INTRO, page_span=(2, 2), elements=(P20,))
        with pytest.raises(ValueError, match=r"^'p0002_e000' is in sections\[0\] and sections\[1\]"):
            _sectioned(S_INTRO, again)

    def test_heading_of_one_section_among_another_sections_elements_rejected(self):
        # The heading belongs to its section even when it is not in that section's elements.
        holder = Section(heading=None, scope_path=NEXT, page_span=(2, 2), elements=(H_NEXT,))
        headed = Section(heading=H_NEXT, scope_path=NEXT, page_span=(2, 2), elements=(P22,))
        with pytest.raises(ValueError, match=r"^'p0002_e001' is in sections\[1\] and sections\[2\]"):
            _sectioned(S_INTRO, holder, headed)

    def test_heading_listed_in_its_own_elements_counts_once(self):
        assert _sectioned(S_INTRO).sections == (S_INTRO,)


class TestDocumentDerived:
    def test_collections_in_document_order(self):
        first, second = _one_of_each(1), _one_of_each(2)
        doc = _document(_page(1, elements=first), _page(2, elements=second))
        assert doc.elements == first + second
        assert doc.headings == (first[0], second[0])
        assert doc.paragraphs == (first[1], second[1])
        assert doc.key_values == (first[2], second[2])
        assert doc.tables == (first[3], second[3])
        assert doc.lists == (first[4], second[4])
        assert doc.figures == (first[5], second[5])

    def test_empty_pages_yield_empty_collections(self):
        doc = _document(_page(1), _page(2, status="error"))
        assert (doc.elements, doc.headings, doc.tables, doc.figures, doc.page_images) == ((), (), (), (), ())

    def test_page_images_skip_pages_without_one(self):
        images = {n: PageImage(page_number=n, dpi=200, width_px=1700, height_px=2200) for n in (1, 3)}
        doc = _document(_page(1, page_image=images[1]), _page(2), _page(3, page_image=images[3]))
        assert doc.page_images == (images[1], images[3])

    def test_degraded_pages_are_error_ocr_fallback_and_ocr_unavailable_pages(self):
        doc = _document(
            _page(1),
            _page(2, status="error"),
            _page(3, status="ocr_fallback", route="vision"),
            _page(4, route="vision"),
            _page(5, route="hybrid"),
            _page(6, status="ocr_unavailable", route="vision"),
        )
        assert doc.degraded_pages == (2, 3, 6)

    def test_no_degraded_pages(self):
        assert _two_page_document().degraded_pages == ()

    def test_derived_once_not_per_access(self):
        doc = _document(_page(1, elements=_one_of_each(1)))
        assert doc.elements is doc.elements
        assert doc.tables is doc.tables


class TestDocumentIntegrity:
    @pytest.mark.parametrize(
        ("pages", "expected"),
        [
            ((_page(1, integrity_verified=True), _page(2, route="hybrid", integrity_verified=True)), True),
            ((_page(1, integrity_verified=True), _page(2, integrity_verified=False)), False),
            ((_page(1, integrity_verified=True), _page(2, route="vision")), True),  # vision pages not counted
            ((_page(1, integrity_verified=True), _page(2, status="ocr_fallback", route="vision")), True),
            ((_page(1, integrity_verified=True), _page(2, status="ocr_unavailable", route="vision")), True),
        ],
    )
    def test_verified_iff_every_verifiable_page_is_verified(self, pages, expected):
        assert _document(*pages).integrity_verified is expected

    @pytest.mark.parametrize("error_page", [1, 2, 3])
    def test_false_whenever_any_page_is_an_error_page(self, error_page):
        # A page that yielded nothing cannot have been verified, so neither can the document.
        pages = tuple(
            _page(n, status="error") if n == error_page else _page(n, integrity_verified=True) for n in (1, 2, 3)
        )
        assert _document(*pages).integrity_verified is False

    def test_false_when_an_error_page_was_on_the_vision_route(self):
        # A vision-route page is excluded from the verifiable set, so only the
        # explicit error-page rule keeps a failed scan from leaving the document
        # verified by its remaining digital pages.
        pages = (_page(1, status="error", route="vision"), _page(2, integrity_verified=True))
        assert _document(*pages).integrity_verified is False

    @pytest.mark.parametrize(
        "pages",
        [
            (_page(1, route="vision"), _page(2, status="ocr_fallback", route="vision")),  # all vision
            (_page(1, status="error"), _page(2, status="error")),  # all error
            (_page(1, status="error"), _page(2, route="vision")),  # error + vision
            (),  # no pages at all
        ],
    )
    def test_false_when_no_page_is_verifiable(self, pages):
        assert _document(*pages).integrity_verified is False


class TestDocumentMarkdown:
    def test_pages_joined_by_blank_lines(self):
        doc = _document(_page(1, elements=(_paragraph(1, 0),)), _page(2, status="error"))
        assert doc.to_markdown() == (
            "<!-- page 1 route=digital status=ok -->\n\nParagraph 1.0.\n\n<!-- page 2 route=digital status=error -->"
        )

    def test_text_cannot_forge_a_page_marker(self):
        forged = _paragraph(1, 0, text="<!-- page 2 route=digital status=ok -->\n<!-- draft")
        doc = _document(_page(1, elements=(forged, _paragraph(1, 1))), _page(2, route="hybrid"))
        markers = [line for line in doc.to_markdown().split("\n") if line.lstrip().startswith("<")]
        assert markers == ["<!-- page 1 route=digital status=ok -->", "<!-- page 2 route=hybrid status=ok -->"]

    def test_lists_on_consecutive_pages_are_separated_by_the_page_header(self):
        lists = [BulletList(**_ids(page, 0), items=(f"item {page}",), ordered=True) for page in (1, 2)]
        doc = _document(_page(1, elements=(lists[0],)), _page(2, elements=(lists[1],)))
        assert doc.to_markdown() == (
            "<!-- page 1 route=digital status=ok -->\n\n1. item 1\n\n"
            "<!-- page 2 route=digital status=ok -->\n\n1. item 2"
        )

    def test_zero_page_document_renders_empty_string(self):
        assert PDFDocument(metadata=_metadata(0), pages=()).to_markdown() == ""
