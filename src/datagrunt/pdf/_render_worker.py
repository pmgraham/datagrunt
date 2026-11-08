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
import json
import mmap
import os
import sys
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import IO, NamedTuple

import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c

from datagrunt.pdf._memory_cap import CAP_EXIT, Lifetime, apply_memory_cap
from datagrunt.pdf._pdfium_common import PdfiumFailure, WalkBoundHit
from datagrunt.pdf._pdfium_images import read_filters
from datagrunt.pdf._process import MAX_REQUEST_BYTES, read_message
from datagrunt.pdf._process import send_message as _send
from datagrunt.pdf._validate import int_at_least, positive_float, strict_bool
from datagrunt.pdf.pdfium_engine import (
    DocumentHandle,
    PageGeometry,
    _checked_password,
    _checked_path,
    _page_objects,
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
_IDLE_LIFETIME_S = 3600.0

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
def _guarded(handle: DocumentHandle, page_index: int) -> Iterator[tuple[pdfium.PdfPage, PageGeometry]]:
    """The page at ``page_index`` of the open document, loaded, once the image guard passes it; closed on leaving.

    Raises:
        RenderRefused: If the guard refuses the page; nothing is yielded.
        IndexError, PDFCorruptError: As
            :func:`~datagrunt.pdf.pdfium_engine.loaded_page` raises them.
    """
    with loaded_page(handle, page_index) as (page, geometry):
        refusal = image_refusal(page, geometry, max_objects=_MAX_GUARD_OBJECTS)
        if refusal is not None:
            raise RenderRefused(f"page {geometry.page_number}: {refusal}; the page is not rendered")
        yield page, geometry


@contextmanager
def _guarded_with_objects(
    handle: DocumentHandle, page_index: int
) -> Iterator[tuple[pdfium.PdfPage, PageGeometry, list[tuple[pdfium_c.FPDF_PAGEOBJECT, int]]]]:
    """Like :func:`_guarded`, also yielding the walked object list so callers walk the page once."""
    with loaded_page(handle, page_index) as (page, geometry):
        objects: list[tuple[pdfium_c.FPDF_PAGEOBJECT, int]] | None = None
        try:
            objects = _page_objects(page.raw, max_objects=_MAX_GUARD_OBJECTS)
        except (WalkBoundHit, PdfiumFailure):
            objects = None
        refusal = image_refusal(page, geometry, max_objects=_MAX_GUARD_OBJECTS, objects=objects)
        if refusal is not None:
            raise RenderRefused(f"page {geometry.page_number}: {refusal}; the page is not rendered")
        yield page, geometry, [] if objects is None else objects


class _Bitmap(NamedTuple):
    """A page drawn in this process: its size, its pixels (unpadded rows) in anonymous memory, and bytes a pixel."""

    width: int
    height: int
    pixels: mmap.mmap
    channels: int = 1


def _render_guarded(source: str | os.PathLike[str], page_index: int, dpi: int, password: str | None) -> _Bitmap:
    """Guard the page, then draw it at ``dpi`` from the same load, in this process.

    The worker calls this under its memory cap; never call it outside one.

    Raises:
        RenderRefused: If the guard refuses the page, or it draws under one
            pixel across at ``dpi``; nothing is drawn.
        RenderFailed: If pdfium cannot make or fill the bitmap.
        MemoryError: If the bitmap cannot be mapped (a memory cap).
    """
    with _guarded_page(source, page_index, password) as (page, geometry):
        return _draw(page, geometry, dpi)


def _draw(
    page: pdfium.PdfPage,
    geometry: PageGeometry,
    dpi: int,
    *,
    colour: bool = False,
    objects: list[tuple[pdfium_c.FPDF_PAGEOBJECT, int]] | None = None,
) -> _Bitmap:
    """Draw the loaded page at ``dpi`` on white, in 8-bit gray (24-bit BGR if ``colour``), without its annotations."""
    drawn = _BGR if colour else _GRAY
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
        _decode_jbig2_first(page, geometry.page_number, objects=objects)
        # Rotation 0 draws the page as displayed (its /Rotate applied), filling the bitmap.
        pdfium_c.FPDF_RenderPageBitmap(bitmap, page.raw, 0, 0, width, height, 0, drawn.flags)
    finally:
        pdfium_c.FPDFBitmap_Destroy(bitmap)
    del buffer  # releases the mapping's export, so it can be sent and closed
    return _Bitmap(width, height, pixels, drawn.channels)


def _decode_jbig2_first(
    page: pdfium.PdfPage,
    page_number: int,
    *,
    objects: list[tuple[pdfium_c.FPDF_PAGEOBJECT, int]] | None = None,
) -> int:
    """Decode each JBIG2 image the guard's walk reaches on its own, then free it; how many.

    Walks :func:`~datagrunt.pdf.pdfium_engine._page_objects` within
    :data:`_MAX_GUARD_OBJECTS` (forms included), as the guard did. The
    worker calls this under its memory cap, with the page's bitmap mapped.

    Raises:
        RenderFailed: If pdfium cannot decode one (named by its walk index).
        PdfiumFailure: If an image's filters cannot be read.
    """
    walked = _page_objects(page.raw, max_objects=_MAX_GUARD_OBJECTS) if objects is None else objects
    decoded = 0
    get_type = pdfium_c.FPDFPageObj_GetType
    image_kind = pdfium_c.FPDF_PAGEOBJ_IMAGE
    for index, (item, _form) in enumerate(walked):
        if get_type(item) != image_kind:
            continue
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
    """Guard the page of the open document, remove its text if asked, then draw it from the same load.

    The batch worker calls this under its memory cap; never call it outside one.

    Raises:
        RenderRefused: If the guard refuses the page, or it draws under one pixel across at ``dpi``.
        RenderFailed: If pdfium cannot remove a text object, or make or fill the bitmap.
        MemoryError: If the bitmap cannot be mapped (a memory cap).
    """
    with _guarded_with_objects(handle, page_index) as (page, geometry, objects):
        draw_objects = objects
        if without_text:
            get_type = pdfium_c.FPDFPageObj_GetType
            text_kind = pdfium_c.FPDF_PAGEOBJ_TEXT
            remaining = [pair for pair in objects if get_type(pair[0]) != text_kind]
            if len(remaining) != len(objects):
                _remove_text(page, geometry.page_number, objects=objects)
                draw_objects = remaining
        return _draw(page, geometry, dpi, colour=colour, objects=draw_objects)


def _remove_text(
    page: pdfium.PdfPage,
    page_number: int,
    *,
    objects: list[tuple[pdfium_c.FPDF_PAGEOBJECT, int]] | None = None,
) -> int:
    """Remove every text object from the loaded page, forms' included; how many. In this load only: nothing is saved.

    Walks the guard's walk (:func:`~datagrunt.pdf.pdfium_engine._page_objects`
    within :data:`_MAX_GUARD_OBJECTS`), so it is bounded by the page the guard
    passed. Each removed object is destroyed (pdfium hands it to the caller).

    Raises:
        RenderFailed: If pdfium cannot remove a text object (named by its walk index).
    """
    walked = _page_objects(page.raw, max_objects=_MAX_GUARD_OBJECTS) if objects is None else objects
    removed = 0
    get_type = pdfium_c.FPDFPageObj_GetType
    text_kind = pdfium_c.FPDF_PAGEOBJ_TEXT
    for index, (item, form) in enumerate(walked):  # bounded: the walk's objects
        if get_type(item) != text_kind:
            continue
        if form < 0:
            done = pdfium_c.FPDFPage_RemoveObject(page.raw, item)
        else:
            done = pdfium_c.FPDFFormObj_RemoveObject(walked[form][0], item)
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


def _file_id(path: str) -> tuple[int, int, int, int] | None:
    """The file's ``(st_dev, st_ino, st_size, st_mtime_ns)``, or None if ``os.stat`` fails."""
    try:
        st = os.stat(path)
    except OSError:
        return None
    return (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns)


def _pool_worker_main(replies: IO[bytes]) -> None:
    """A persistent render worker for :class:`~datagrunt.pdf.render.RenderWorkerPool`.

    Loops over length-prefixed JSON requests on stdin, caching the open
    :class:`~datagrunt.pdf.pdfium_engine.DocumentHandle` across consecutive
    requests while the file on disk has the same ``os.stat`` identity.
    """
    lifetime: Lifetime | None = None
    cached_key: tuple[str, str | None, tuple[int, int, int, int]] | None = None
    cached_cm: AbstractContextManager[DocumentHandle] | None = None
    cached_handle: DocumentHandle | None = None

    def get_handle(source: str, password: str | None) -> DocumentHandle:
        nonlocal cached_key, cached_cm, cached_handle
        fid = _file_id(source)
        key = (source, password, fid) if fid is not None else None
        if cached_handle is not None and key is not None and cached_key == key:
            return cached_handle
        if cached_cm is not None:
            cached_cm.__exit__(None, None, None)
            cached_cm = None
            cached_handle = None
            cached_key = None
        cm = open_document(source, password=password)
        handle = cm.__enter__()
        fid_after = _file_id(source)
        if fid_after is not None:
            cached_cm = cm
            cached_handle = handle
            cached_key = (source, password, fid_after)
        return handle

    try:
        while True:
            if lifetime is not None:
                lifetime.extend(_IDLE_LIFETIME_S)
            raw = read_message(sys.stdin.buffer, MAX_REQUEST_BYTES)
            if raw is None:
                return
            try:
                message = json.loads(raw)
            except Exception as exc:
                _send(replies, _failure(exc))
                continue
            if isinstance(message, dict) and "pages" in message:
                try:
                    batch_req = _batch_request_from(message)
                except Exception as exc:
                    _send(replies, _failure(exc))
                    continue
                sent = 0
                try:
                    if lifetime is None:
                        lifetime = apply_memory_cap(batch_req.memory_cap_bytes, batch_req.timeout_s)
                    else:
                        lifetime.extend(batch_req.timeout_s)
                    handle = get_handle(batch_req.source, batch_req.password)
                    for index, dpi in batch_req.pages:
                        _send_page(replies, handle, index, dpi, batch_req)
                        sent += 1
                except MemoryError:
                    os._exit(CAP_EXIT)
                except Exception as exc:
                    for _ in batch_req.pages[sent:]:
                        _send(replies, _failure(exc))
            else:
                try:
                    req = _request_from(message)
                    if lifetime is None:
                        lifetime = apply_memory_cap(req.memory_cap_bytes, req.timeout_s)
                    else:
                        lifetime.extend(req.timeout_s)
                    handle = get_handle(req.source, req.password)
                    bitmap = _render_loaded(handle, req.page_index, req.dpi, colour=False, without_text=False)
                except MemoryError:
                    os._exit(CAP_EXIT)
                except RenderRefused as exc:
                    _send(replies, _REFUSED + str(exc).encode("utf-8", errors="backslashreplace"))
                    continue
                except Exception as exc:
                    _send(replies, _failure(exc))
                    continue
                with bitmap.pixels:
                    _send(replies, _PIXELS + _PIXEL_SIZE.pack(bitmap.width, bitmap.height))
                    _send(replies, bitmap.pixels)
    finally:
        if cached_cm is not None:
            cached_cm.__exit__(None, None, None)


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
