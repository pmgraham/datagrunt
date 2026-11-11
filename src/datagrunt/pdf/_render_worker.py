"""The render worker (#369 A5a, #400): the guard and the draw, in a worker process whose memory is capped.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. Only a
render worker imports this module (the launcher's key ``"render"``,
:mod:`datagrunt.pdf._process`); its caller's side, the request, the replies
and the module notes are :mod:`datagrunt.pdf.render`'s. The worker caps its
own memory (:func:`~datagrunt.pdf._memory_cap.apply_memory_cap`) before it
opens the PDF; reaching the cap (a ``MemoryError`` under a backstop, or the
watchdog) ends it with :data:`~datagrunt.pdf._memory_cap.CAP_EXIT`.
"""

import ctypes
import mmap
import os
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import IO, NamedTuple

import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c

from datagrunt.pdf import pdfium_engine as _eng
from datagrunt.pdf._memory_cap import CAP_EXIT, apply_memory_cap
from datagrunt.pdf._options import MEMORY_CAP_BYTES
from datagrunt.pdf._pdfium_images import read_filters
from datagrunt.pdf._process import send_message as _send
from datagrunt.pdf._validate import int_at_least, positive_float, strict_bool
from datagrunt.pdf.pdfium_engine import (
    _PDFIUM_LOCK,
    DocumentHandle,
    PageGeometry,
    PdfiumFailure,
    WalkBoundHit,
    _checked_password,
    _checked_path,
    image_refusal,
    loaded_page,
    open_document,
)
from datagrunt.pdf.render import (
    _BATCH_FIELDS,
    _FAILED,
    _PIXEL_SIZE,
    _PIXELS,
    _REFUSED,
    _REQUEST_FIELDS,
    BGR_CHANNELS,
    GRAY_CHANNELS,
    RenderedPage,
    RenderFailed,
    RenderRefused,
    _BatchRequest,
    _checked_pages,
    _Request,
    render_size,
)

# The objects (forms' contents included) the guard walks before it refuses a
# page: ~250 times the corpus's busiest page (3,977). The walk costs ~0.4 us
# an object here, so the bound is about half a second of guard.
_MAX_GUARD_OBJECTS = 1_000_000

_WHITE = 0xFFFFFFFF
# Gray output and nothing else: no FPDF_ANNOT (annotations are not drawn), no LCD text, no printing mode.
_RENDER_FLAGS = pdfium_c.FPDF_GRAYSCALE


class _Format(NamedTuple):
    """How a page is drawn: bytes a pixel, pdfium's bitmap format and render flags, and the format's name."""

    channels: int
    bitmap: int
    flags: int
    name: str


_GRAY = _Format(GRAY_CHANNELS, pdfium_c.FPDFBitmap_Gray, _RENDER_FLAGS, "gray")
# Colour, and nothing else: no flag (no annotations, no LCD text, no printing mode).
_BGR = _Format(BGR_CHANNELS, pdfium_c.FPDFBitmap_BGR, 0, "BGR")


@contextmanager
def _guarded_page(
    source: str | os.PathLike[str], page_index: int, password: str | None
) -> Iterator[tuple[pdfium.PdfPage, PageGeometry]]:
    """The loaded page at ``page_index`` with its geometry, once the image guard passes it.

    The guard and the render that follows share this one load of the page.
    The document and page are closed on leaving.

    Raises:
        RenderRefused: If :func:`~datagrunt.pdf.pdfium_engine.image_refusal`
            refuses the page; nothing is yielded.
        FileNotFoundError, PermissionError, PDFEncryptedError, PDFCorruptError,
        IndexError: As :func:`~datagrunt.pdf.pdfium_engine.open_document` and
            :func:`~datagrunt.pdf.pdfium_engine.loaded_page` raise them.
    """
    with open_document(source, password=password) as handle, _guarded(handle, page_index) as loaded:
        yield loaded


@contextmanager
def _guarded_with_objects(
    handle: DocumentHandle, page_index: int
) -> Iterator[tuple[pdfium.PdfPage, PageGeometry, list[tuple[pdfium_c.FPDF_PAGEOBJECT, int]]]]:
    """Load ``page_index``, walk its objects once for the guard, and yield ``(page, geometry, objects)``."""
    with loaded_page(handle, page_index) as (page, geometry):
        try:
            objects = _eng._page_objects(page.raw, max_objects=_MAX_GUARD_OBJECTS)
        except WalkBoundHit as exc:
            raise RenderRefused(f"page {geometry.page_number}: {exc}; the page is not rendered") from exc
        except PdfiumFailure as exc:
            raise RenderRefused(f"page {geometry.page_number}: pdfium cannot {exc}; the page is not rendered") from exc
        refusal = image_refusal(page, geometry, max_objects=_MAX_GUARD_OBJECTS, objects=objects)
        if refusal is not None:
            raise RenderRefused(f"page {geometry.page_number}: {refusal}; the page is not rendered")
        yield page, geometry, objects


@contextmanager
def _guarded(handle: DocumentHandle, page_index: int) -> Iterator[tuple[pdfium.PdfPage, PageGeometry]]:
    """The page at ``page_index`` of the open document, loaded, once the image guard passes it; closed on leaving."""
    with _guarded_with_objects(handle, page_index) as (page, geometry, _objects):
        yield page, geometry


class _Bitmap(NamedTuple):
    """A page drawn in this process: its size, its pixels (unpadded rows) in anonymous memory, and bytes a pixel."""

    width: int
    height: int
    pixels: mmap.mmap
    channels: int = 1


def _image_items_of(
    objects: Sequence[tuple[pdfium_c.FPDF_PAGEOBJECT, int]],
) -> list[tuple[int, pdfium_c.FPDF_PAGEOBJECT]]:
    """Extract ``(walk_index, image_obj)`` before any text removal so destroyed text pointers are never touched."""
    return [
        (index, item)
        for index, (item, _form) in enumerate(objects)
        if pdfium_c.FPDFPageObj_GetType(item) == pdfium_c.FPDF_PAGEOBJ_IMAGE
    ]


def _render_guarded(source: str | os.PathLike[str], page_index: int, dpi: int, password: str | None) -> _Bitmap:
    """Guard the page, then draw it at ``dpi`` from the same load and single object walk, in this process."""
    with (
        open_document(source, password=password) as handle,
        _guarded_with_objects(handle, page_index) as (
            page,
            geometry,
            objects,
        ),
    ):
        return _draw(page, geometry, dpi, image_items=_image_items_of(objects))


def _draw(
    page: pdfium.PdfPage,
    geometry: PageGeometry,
    dpi: int,
    *,
    colour: bool = False,
    annotations: bool = False,
    image_items: Sequence[tuple[int, pdfium_c.FPDF_PAGEOBJECT]] | None = None,
) -> _Bitmap:
    """Draw the loaded page at ``dpi`` on white, in 8-bit gray (24-bit BGR if ``colour``)."""
    drawn = _BGR if colour else _GRAY
    flags = drawn.flags | (pdfium_c.FPDF_ANNOT if annotations else 0)
    width, height = render_size(geometry.width_pt, geometry.height_pt, dpi)
    if width < 1 or height < 1:
        message = f"page {geometry.page_number} renders {width} x {height} px at {dpi} DPI; the page is not rendered"
        raise RenderRefused(message)
    stride = width * drawn.channels  # unpadded rows
    pixels = _blank(stride * height)
    buffer = (ctypes.c_ubyte * (stride * height)).from_buffer(pixels)
    bitmap = pdfium_c.FPDFBitmap_CreateEx(width, height, drawn.bitmap, buffer, stride)
    if not bitmap:
        raise RenderFailed(f"page {geometry.page_number}: pdfium cannot make a {width} x {height} {drawn.name} bitmap")
    try:
        if not pdfium_c.FPDFBitmap_FillRect(bitmap, 0, 0, width, height, _WHITE):
            raise RenderFailed(f"page {geometry.page_number}: pdfium cannot fill a {width} x {height} bitmap")
        _decode_jbig2_first(page, geometry.page_number, image_items=image_items)
        # Rotation 0 draws the page as displayed (its /Rotate applied), filling the bitmap.
        pdfium_c.FPDF_RenderPageBitmap(bitmap, page.raw, 0, 0, width, height, 0, flags)
    finally:
        pdfium_c.FPDFBitmap_Destroy(bitmap)
    del buffer  # releases the mapping's export, so it can be sent and closed
    return _Bitmap(width, height, pixels, drawn.channels)


def _decode_jbig2_first(
    page: pdfium.PdfPage,
    page_number: int,
    *,
    image_items: Sequence[tuple[int, pdfium_c.FPDF_PAGEOBJECT]] | None = None,
) -> int:
    """Decode each JBIG2 image the guard's walk reaches on its own, then free it; how many."""
    candidates = (
        _image_items_of(_eng._page_objects(page.raw, max_objects=_MAX_GUARD_OBJECTS))
        if image_items is None
        else image_items
    )
    decoded = 0
    for index, item in candidates:
        if "JBIG2Decode" not in read_filters(item, f"object {index}"):
            continue
        bitmap = pdfium_c.FPDFImageObj_GetBitmap(item)
        if not bitmap:
            raise RenderFailed(
                f"page {page_number}: pdfium cannot decode JBIG2 image object {index} in the render worker; "
                "the page is not rendered"
            )
        pdfium_c.FPDFBitmap_Destroy(bitmap)
        decoded += 1
    return decoded


def _render_loaded(handle: DocumentHandle, page_index: int, dpi: int, *, colour: bool, without_text: bool) -> _Bitmap:
    """Guard the page of the open document, remove its text if asked, then draw it from the same single walk."""
    with _guarded_with_objects(handle, page_index) as (page, geometry, objects):
        images = _image_items_of(objects)
        if without_text:
            _remove_text(page, geometry.page_number, objects=objects)
        return _draw(page, geometry, dpi, colour=colour, image_items=images)


def _remove_text(
    page: pdfium.PdfPage,
    page_number: int,
    objects: Sequence[tuple[pdfium_c.FPDF_PAGEOBJECT, int]] | None = None,
) -> int:
    """Remove every text object from the loaded page, forms' included; how many. In this load only: nothing is saved."""
    walked = _eng._page_objects(page.raw, max_objects=_MAX_GUARD_OBJECTS) if objects is None else objects
    objects = walked
    removed = 0
    for index, (item, form) in enumerate(objects):  # bounded: the walk's objects
        if pdfium_c.FPDFPageObj_GetType(item) != pdfium_c.FPDF_PAGEOBJ_TEXT:
            continue
        if form < 0:
            done = pdfium_c.FPDFPage_RemoveObject(page.raw, item)
        else:
            done = pdfium_c.FPDFFormObj_RemoveObject(objects[form][0], item)
        if not done:
            raise RenderFailed(
                f"page {page_number}: pdfium cannot remove text object {index}; the page is not rendered"
            )
        pdfium_c.FPDFPageObj_Destroy(item)
        removed += 1
    return removed


def _blank(size: int) -> mmap.mmap:
    """``size`` bytes of anonymous memory, mapped but untouched: pdfium writes them with the GIL released.

    A ``bytearray`` would be zeroed here, holding the GIL, out of the macOS
    watchdog's sight.
    """
    try:
        return mmap.mmap(-1, size)
    except OSError as exc:
        # Linux refuses a mapping past RLIMIT_AS, and Windows one past the Job Object's commit limit.
        raise MemoryError(f"cannot map {size} bytes for the bitmap: {exc}") from exc


def _worker_main(message: object, replies: IO[bytes]) -> None:
    """The render worker, run by _BOOTSTRAP: one request in, one reply out on ``replies``, as raw bytes.

    A request holding ``pages`` is a batch (:func:`_batch_main`). It caps its
    own memory before it opens the PDF. Reaching the cap (a MemoryError)
    ends it with :data:`~datagrunt.pdf._memory_cap.CAP_EXIT` and no reply; a refusal and any other error
    are sent back as messages, never swallowed.
    """
    if isinstance(message, dict) and "pages" in message:
        _batch_main(message, replies)
        return
    try:
        request = _request_from(message)
        apply_memory_cap(request.memory_cap_bytes, request.timeout_s)
        bitmap = _render_guarded(request.source, request.page_index, request.dpi, request.password)
    except MemoryError:
        os._exit(CAP_EXIT)
    except RenderRefused as exc:
        _send(replies, _REFUSED + str(exc).encode("utf-8", errors="backslashreplace"))
        return
    except Exception as exc:  # every other failure reaches the parent, as a RenderFailed
        _send(replies, _FAILED + f"{type(exc).__name__}: {exc}".encode("utf-8", errors="backslashreplace"))
        return
    _send(replies, _PIXELS + _PIXEL_SIZE.pack(bitmap.width, bitmap.height))
    _send(replies, bitmap.pixels)


def _batch_main(message: dict[str, object], replies: IO[bytes]) -> None:
    """The batch worker: a batch in; each page's reply (its pixels after it) out on ``replies``, in order.

    It caps its own memory once, before it opens the PDF, then opens it once.
    Reaching the cap ends it with :data:`~datagrunt.pdf._memory_cap.CAP_EXIT`: the parent fails the pages
    not yet answered. A batch it cannot read is one failure; a PDF it cannot
    open fails each page with the reason. Nothing is swallowed.
    """
    try:
        request = _batch_request_from(message)
    except Exception as exc:  # sent, never swallowed: the parent fails the batch's pages
        _send(replies, _failure(exc))
        return
    sent = 0
    try:
        apply_memory_cap(request.memory_cap_bytes, request.timeout_s)
        with open_document(request.source, password=request.password) as handle:
            for index, dpi in request.pages:  # bounded: at most MAX_BATCH_PAGES
                _send_page(replies, handle, index, dpi, request)
                sent += 1
    except MemoryError:
        os._exit(CAP_EXIT)
    except Exception as exc:  # no page left can be drawn (the PDF cannot be opened): each fails with it
        for _ in request.pages[sent:]:
            _send(replies, _failure(exc))


def _send_page(replies: IO[bytes], handle: DocumentHandle, index: int, dpi: int, request: _BatchRequest) -> None:
    """Draw one page of the batch and send its reply; a refusal or failure is its reply, and the batch goes on.

    A MemoryError (the cap) is raised: it ends the worker.
    """
    try:
        bitmap = _render_loaded(handle, index, dpi, colour=request.colour, without_text=request.without_text)
    except MemoryError:
        raise
    except RenderRefused as exc:
        _send(replies, _REFUSED + str(exc).encode("utf-8", errors="backslashreplace"))
        return
    except Exception as exc:  # this page's failure, sent to the parent
        _send(replies, _failure(exc))
        return
    with bitmap.pixels:  # unmapped once sent: a batch holds one page's pixels at a time
        _send(replies, _PIXELS + _PIXEL_SIZE.pack(bitmap.width, bitmap.height))
        _send(replies, bitmap.pixels)


def _failure(exc: Exception) -> bytes:
    """A failed reply naming ``exc``: its type and message."""
    return _FAILED + f"{type(exc).__name__}: {exc}".encode("utf-8", errors="backslashreplace")


def _request_from(message: object) -> _Request:
    """The request a worker reads (JSON from its parent), its fields checked as render_page checks them."""
    if not isinstance(message, dict):
        raise TypeError(f"the worker's request must be a dict, got {type(message).__name__}")
    if set(message) != set(_REQUEST_FIELDS):
        raise ValueError(f"the worker's request must hold exactly {_REQUEST_FIELDS}, got {sorted(message)}")
    return _Request(
        source=str(_checked_path(message["source"], "source")),
        page_index=int_at_least("page_index", message["page_index"], 0),
        dpi=int_at_least("dpi", message["dpi"], 1),
        password=_checked_password(message["password"]),
        memory_cap_bytes=int_at_least("memory_cap_bytes", message["memory_cap_bytes"], 1),
        timeout_s=positive_float("timeout_s", message["timeout_s"]),
    )


def _batch_request_from(message: dict[str, object]) -> _BatchRequest:
    """The batch a worker reads (JSON from its parent), its fields checked as render_pages checks them."""
    if set(message) != set(_BATCH_FIELDS):
        raise ValueError(f"the batch request must hold exactly {_BATCH_FIELDS}, got {sorted(message)}")
    return _BatchRequest(
        source=str(_checked_path(message["source"], "source")),
        pages=_checked_pages(message["pages"], list),
        password=_checked_password(message["password"]),
        memory_cap_bytes=int_at_least("memory_cap_bytes", message["memory_cap_bytes"], 1),
        timeout_s=positive_float("timeout_s", message["timeout_s"]),
        colour=strict_bool("colour", message["colour"]),
        without_text=strict_bool("without_text", message["without_text"]),
    )


def _check_pixel_cap(geometry: PageGeometry, dpi: int, channels: int, memory_cap_bytes: int) -> None:
    width, height = render_size(geometry.width_pt, geometry.height_pt, dpi)
    if width >= 1 and height >= 1 and width * height * channels > memory_cap_bytes:
        raise MemoryError(f"bitmap needs {width * height * channels} bytes, over {memory_cap_bytes}-byte cap")


def draw_page_in_process(
    source: str | os.PathLike[str],
    page_index: int,
    *,
    dpi: int,
    password: str | None = None,
    memory_cap_bytes: int = MEMORY_CAP_BYTES,
    colour: bool = False,
    annotations: bool = True,
) -> RenderedPage:
    """Guard and draw ``page_index`` of ``source`` in this process under ``_PDFIUM_LOCK``."""
    checked_source = str(_checked_path(source, "source"))
    int_at_least("page_index", page_index, 0)
    int_at_least("dpi", dpi, 1)
    checked_password = _checked_password(password)
    int_at_least("memory_cap_bytes", memory_cap_bytes, 1)
    strict_bool("colour", colour)
    strict_bool("annotations", annotations)
    where = f"{checked_source}: page {page_index + 1}"
    channels = BGR_CHANNELS if colour else GRAY_CHANNELS
    try:
        with (
            _PDFIUM_LOCK,
            open_document(checked_source, password=checked_password) as handle,
            _guarded_with_objects(handle, page_index) as (page, geometry, objects),
        ):
            _check_pixel_cap(geometry, dpi, channels, memory_cap_bytes)
            bitmap = _draw(
                page, geometry, dpi, colour=colour, annotations=annotations, image_items=_image_items_of(objects)
            )
    except RenderRefused as exc:
        raise RenderRefused(f"{checked_source}: {exc}") from exc
    except Exception as exc:
        raise RenderFailed(f"{where}: the render worker failed: {type(exc).__name__}: {exc}") from exc
    with bitmap.pixels:
        raw = bytes(bitmap.pixels)
    return RenderedPage(pixels=raw, px_width=bitmap.width, px_height=bitmap.height, channels=bitmap.channels)


def draw_loaded_in_process(
    handle: DocumentHandle,
    source: str | os.PathLike[str],
    page_index: int,
    *,
    dpi: int,
    memory_cap_bytes: int = MEMORY_CAP_BYTES,
    colour: bool = False,
    without_text: bool = False,
    annotations: bool = False,
) -> RenderedPage:
    """Guard and draw ``page_index`` of an already-open ``handle`` in this process under ``_PDFIUM_LOCK``."""
    if not isinstance(handle, DocumentHandle):
        raise TypeError(f"handle must be a DocumentHandle, got {type(handle).__name__}")
    checked_source = str(_checked_path(source, "source"))
    int_at_least("page_index", page_index, 0)
    int_at_least("dpi", dpi, 1)
    int_at_least("memory_cap_bytes", memory_cap_bytes, 1)
    strict_bool("colour", colour)
    strict_bool("without_text", without_text)
    strict_bool("annotations", annotations)
    where = f"{checked_source}: page {page_index + 1}"
    channels = BGR_CHANNELS if colour else GRAY_CHANNELS
    try:
        with _PDFIUM_LOCK, _guarded_with_objects(handle, page_index) as (page, geometry, objects):
            _check_pixel_cap(geometry, dpi, channels, memory_cap_bytes)
            images = _image_items_of(objects)
            if without_text:
                _remove_text(page, geometry.page_number, objects=objects)
            bitmap = _draw(page, geometry, dpi, colour=colour, annotations=annotations, image_items=images)
    except RenderRefused as exc:
        raise RenderRefused(f"{checked_source}: {exc}") from exc
    except Exception as exc:
        raise RenderFailed(f"{where}: the render worker failed: {type(exc).__name__}: {exc}") from exc
    with bitmap.pixels:
        raw = bytes(bitmap.pixels)
    return RenderedPage(pixels=raw, px_width=bitmap.width, px_height=bitmap.height, channels=bitmap.channels)


def draw_open_page_in_process(
    page: pdfium.PdfPage,
    geometry: PageGeometry,
    objects: Sequence[tuple[pdfium_c.FPDF_PAGEOBJECT, int]],
    image_items: Sequence[tuple[int, pdfium_c.FPDF_PAGEOBJECT]],
    first_render_refusal: str | None,
    source: str | os.PathLike[str],
    *,
    dpi: int,
    memory_cap_bytes: int = MEMORY_CAP_BYTES,
    colour: bool = False,
    without_text: bool = False,
    annotations: bool = False,
) -> RenderedPage:
    """Draw an already-open ``page`` reusing its pre-walked ``objects`` and ``first_render_refusal``."""
    if not isinstance(page, pdfium.PdfPage) or not isinstance(geometry, PageGeometry):
        raise TypeError("page must be a PdfPage and geometry a PageGeometry")
    checked_source = str(_checked_path(source, "source"))
    int_at_least("dpi", dpi, 1)
    int_at_least("memory_cap_bytes", memory_cap_bytes, 1)
    strict_bool("colour", colour)
    strict_bool("without_text", without_text)
    strict_bool("annotations", annotations)
    where = f"{checked_source}: page {geometry.page_number}"
    channels = BGR_CHANNELS if colour else GRAY_CHANNELS
    try:
        with _PDFIUM_LOCK:
            if len(objects) > _MAX_GUARD_OBJECTS:
                bound_msg = f"the page holds {len(objects)} objects, over the walk's {_MAX_GUARD_OBJECTS}-object bound"
                raise RenderRefused(f"page {geometry.page_number}: {bound_msg}; the page is not rendered")
            if first_render_refusal is not None:
                raise RenderRefused(f"page {geometry.page_number}: {first_render_refusal}; the page is not rendered")
            _check_pixel_cap(geometry, dpi, channels, memory_cap_bytes)
            if without_text:
                _remove_text(page, geometry.page_number, objects=objects)
            bitmap = _draw(page, geometry, dpi, colour=colour, annotations=annotations, image_items=image_items)
    except RenderRefused as exc:
        raise RenderRefused(f"{checked_source}: {exc}") from exc
    except Exception as exc:
        raise RenderFailed(f"{where}: the render worker failed: {type(exc).__name__}: {exc}") from exc
    with bitmap.pixels:
        raw = bytes(bitmap.pixels)
    return RenderedPage(pixels=raw, px_width=bitmap.width, px_height=bitmap.height, channels=bitmap.channels)
