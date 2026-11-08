"""The figure worker (#369 A5a): a batch of one PDF's figure reads, in a worker process whose memory is capped.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. Only a
figure worker imports this module (the launcher's key ``"figures"``,
:mod:`datagrunt.pdf._process`); its caller's side is
:mod:`datagrunt.pdf._loaders`. The worker caps its own memory
(:func:`~datagrunt.pdf._memory_cap.apply_memory_cap`) before it opens the
PDF, then reads each figure in order with the engine's
:class:`~datagrunt.pdf.pdfium_engine.ImageLoader` (which reopens the PDF
and verifies the image before it returns anything) and replies for each:
the bytes (``B``, then the bytes as a second message), or the error the
read raised (``E`` and the exception, :func:`~datagrunt.pdf._process.error_reply`).
Reaching the cap (a ``MemoryError`` under a backstop, or the watchdog) ends
it with :data:`~datagrunt.pdf._memory_cap.CAP_EXIT`: its caller fails the
figure it was reading, saying why.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any

from datagrunt.pdf._image_facts import RawImage
from datagrunt.pdf._memory_cap import CAP_EXIT, apply_memory_cap
from datagrunt.pdf._pdfium_common import PdfiumFailure, WalkBoundHit
from datagrunt.pdf._pdfium_images import decoded_png
from datagrunt.pdf._process import error_reply, send_message
from datagrunt.pdf._validate import checked_password, checked_path, choice_str, int_at_least, positive_float
from datagrunt.pdf.pdfium_engine import (
    MAX_PAGE_OBJECTS,
    DocumentHandle,
    ImageLoader,
    _load_page,
    _page_objects,
    open_document,
)

__all__: list[str] = []

_BYTES, _ERROR = b"B", b"E"
_REQUEST_KEYS = ("figures", "kind", "memory_cap_bytes", "password", "source", "timeout_s")
_KINDS = ("png", "raw")


@dataclass(frozen=True, slots=True, kw_only=True)
class _Request:
    """A figure worker's request, checked: one PDF's figures (page index, image facts), its kind, its cap."""

    source: str
    password: str | None = field(repr=False)
    kind: str
    memory_cap_bytes: int
    timeout_s: float
    figures: tuple[tuple[int, RawImage], ...]


def _worker_main(message: object, replies: IO[bytes]) -> None:
    """The figure worker, run by the launcher's bootstrap (key ``"figures"``): a batch in, a reply a figure out.

    A request it cannot read, or a cap it cannot set, is the error of every
    figure it holds (or one error, when it cannot be read at all): nothing
    is swallowed, and nothing is read uncapped.
    """
    try:
        request = _request_from(message)
    except Exception as exc:  # sent, never swallowed: the caller fails the figure it waits for
        send_message(replies, _ERROR + error_reply(exc))
        return
    try:
        apply_memory_cap(request.memory_cap_bytes, request.timeout_s)
    except Exception as exc:  # no figure is read uncapped: each fails with why
        for _ in request.figures:
            send_message(replies, _ERROR + error_reply(exc))
        return
    try:
        handle_cm = open_document(request.source, password=request.password)
        handle = handle_cm.__enter__()
    except MemoryError:
        os._exit(CAP_EXIT)
    except Exception as exc:
        for _ in request.figures:
            send_message(replies, _ERROR + error_reply(exc))
        return
    try:
        _serve_batch(replies, request, handle)
    finally:
        handle_cm.__exit__(None, None, None)


def _serve_batch(replies: IO[bytes], request: _Request, handle: DocumentHandle) -> None:
    """Read each figure in ``request.figures`` sharing ``handle`` and caching the current page load."""
    source_path = Path(request.source)
    is_png = request.kind == "png"
    current_page_index: int | None = None
    current_page: Any = None
    current_page_number: int = 0
    current_objects: list[tuple[Any, int]] | None = None
    try:
        for page_index, image in request.figures:  # bounded: the batch's figures
            try:
                loader = ImageLoader(source=source_path, page_index=page_index, image=image, password=request.password)
                if page_index >= handle.page_count:
                    raise loader._changed(f"it has {handle.page_count} page(s)")
                if current_page_index != page_index or current_page is None:
                    if current_page is not None:
                        current_page.close()
                        current_page = None
                        current_page_index = None
                        current_objects = None
                    current_page, current_page_number = _load_page(handle, page_index)
                    current_page_index = page_index
                    try:
                        current_objects = _page_objects(current_page.raw, max_objects=MAX_PAGE_OBJECTS)
                    except (WalkBoundHit, PdfiumFailure):
                        current_objects = None
                if is_png:
                    data = loader._read_from_page(
                        current_page.raw,
                        current_page_number,
                        current_objects,
                        lambda img, _raw, facts, label: decoded_png(img, facts, label),
                    )
                else:
                    data = loader._read_from_page(
                        current_page.raw,
                        current_page_number,
                        current_objects,
                        lambda _img, raw, _facts, _label: raw,
                    )
            except MemoryError:
                os._exit(CAP_EXIT)
            except Exception as exc:  # this figure's error, sent to the caller, who raises it as it was
                send_message(replies, _ERROR + error_reply(exc))
                continue
            send_message(replies, _BYTES)
            send_message(replies, data)
    finally:
        if current_page is not None:
            current_page.close()


def _request_from(message: object) -> _Request:
    """The request a worker reads (JSON from its caller), each field checked."""
    if not isinstance(message, dict) or set(message) != set(_REQUEST_KEYS):
        found = sorted(message) if isinstance(message, dict) else type(message).__name__
        raise ValueError(f"the figure worker's request must hold exactly {_REQUEST_KEYS}, got {found}")
    figures = message["figures"]
    if not isinstance(figures, list) or not figures:
        raise ValueError("figures must be a non-empty list of [page index, image] pairs")
    return _Request(
        source=str(checked_path(message["source"], "source")),
        password=checked_password(message["password"]),
        kind=choice_str("kind", message["kind"], _KINDS),
        memory_cap_bytes=int_at_least("memory_cap_bytes", message["memory_cap_bytes"], 1),
        timeout_s=positive_float("timeout_s", message["timeout_s"]),
        figures=tuple(_figure_from(position, item) for position, item in enumerate(figures)),
    )


def _figure_from(position: int, item: object) -> tuple[int, RawImage]:
    """One ``[page index, image facts]`` pair of the request, as RawImage checks it (JSON's lists made tuples)."""
    if not isinstance(item, list) or len(item) != 2 or not isinstance(item[1], dict):
        raise ValueError(f"figures[{position}] must be a [page index, image] pair")
    facts: dict[str, Any] = {key: tuple(value) if isinstance(value, list) else value for key, value in item[1].items()}
    return int_at_least(f"figures[{position}] page index", item[0], 0), RawImage(**facts)
