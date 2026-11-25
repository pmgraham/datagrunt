"""Strict JSON IPC codec for ``Page`` replies between page workers and ``pipeline`` (zero pickle).

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``.
Reuses :mod:`datagrunt.pdf.codec`'s strict page schema and serializes the three
allowlisted lazy loader types (:class:`PageDraw`, :class:`PageRender`,
:class:`FigureRead`) as validated JSON dictionaries without ever transmitting
PDF passwords over worker stdout.
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from datagrunt.pdf._image_facts import RawImage
from datagrunt.pdf._source import SourceIdentity
from datagrunt.pdf.codec import VERSION, Json, _object, _optional_object, _page, _page_json, parse_json
from datagrunt.pdf.elements import DocumentElement, Figure, Page, PageImage

if TYPE_CHECKING:
    from datagrunt.pdf._loaders import FigureRead, PageDraw, PageRender

__all__ = ["decode_page_reply", "encode_page_reply"]

_ENVELOPE_KEYS = ("page", "page_image_loader", "figure_loaders")
_IDENTITY_KEYS = ("device", "inode", "size", "mtime_ns")
_PAGE_DRAW_KEYS = (
    "kind",
    "source",
    "page_index",
    "dpi",
    "width_pt",
    "height_pt",
    "memory_cap_bytes",
    "timeout_s",
    "identity",
    "sha256",
    "sandbox",
)
_PAGE_RENDER_KEYS = (
    "kind",
    "source",
    "page_index",
    "dpi",
    "rotation",
    "memory_cap_bytes",
    "timeout_s",
    "identity",
    "sha256",
    "sandbox",
)
_FIGURE_READ_KEYS = ("source", "page_index", "image", "memory_cap_bytes", "sandbox")
_RAW_IMAGE_KEYS = (
    "bbox",
    "px_width",
    "px_height",
    "declared_px",
    "object_index",
    "raw_sha256",
    "filters",
    "bits_per_pixel",
    "colorspace",
    "icc_sha256",
)


def _identity_to_json(identity: SourceIdentity | None) -> Json | None:
    if identity is None:
        return None
    return {
        "device": identity.device,
        "inode": identity.inode,
        "size": identity.size,
        "mtime_ns": identity.mtime_ns,
    }


def _identity_from_json(name: str, value: object) -> SourceIdentity | None:
    data = _optional_object(name, value, _IDENTITY_KEYS)
    if data is None:
        return None
    return SourceIdentity(
        device=data["device"],
        inode=data["inode"],
        size=data["size"],
        mtime_ns=data["mtime_ns"],
    )


def _raw_image_to_json(image: RawImage) -> Json:
    return {
        "bbox": list(image.bbox),
        "px_width": image.px_width,
        "px_height": image.px_height,
        "declared_px": list(image.declared_px),
        "object_index": image.object_index,
        "raw_sha256": image.raw_sha256,
        "filters": list(image.filters),
        "bits_per_pixel": image.bits_per_pixel,
        "colorspace": image.colorspace,
        "icc_sha256": image.icc_sha256,
    }


def _raw_image_from_json(name: str, value: object) -> RawImage:
    data = _object(name, value, _RAW_IMAGE_KEYS)
    bbox = data["bbox"]
    declared = data["declared_px"]
    filters = data["filters"]
    if not isinstance(bbox, list) or len(bbox) != 4:
        raise ValueError(f"{name}.bbox must be a 4-item JSON array")
    if not isinstance(declared, list) or len(declared) != 2:
        raise ValueError(f"{name}.declared_px must be a 2-item JSON array")
    if not isinstance(filters, list):
        raise TypeError(f"{name}.filters must be a JSON array, got {type(filters).__name__}")
    return RawImage(
        bbox=(bbox[0], bbox[1], bbox[2], bbox[3]),
        px_width=data["px_width"],
        px_height=data["px_height"],
        declared_px=(declared[0], declared[1]),
        object_index=data["object_index"],
        raw_sha256=data["raw_sha256"],
        filters=tuple(filters),
        bits_per_pixel=data["bits_per_pixel"],
        colorspace=data["colorspace"],
        icc_sha256=data["icc_sha256"],
    )


def _page_image_loader_to_json(image: PageImage | None) -> Json | None:
    if image is None or (image._loader is None and image._band_loader is None):
        return None
    from datagrunt.pdf._loaders import PageDraw, PageRender

    owner: object = getattr(image._loader, "__self__", None)
    if isinstance(owner, PageDraw) and getattr(image._loader, "__func__", None) is PageDraw.png_bytes:
        if image._band_loader is not None:
            raise ValueError("PageDraw image cannot carry a band loader")
        return {
            "kind": "page_draw",
            "source": str(owner.source),
            "page_index": owner.page_index,
            "dpi": owner.dpi,
            "width_pt": owner.width_pt,
            "height_pt": owner.height_pt,
            "memory_cap_bytes": owner.memory_cap_bytes,
            "timeout_s": owner.timeout_s,
            "identity": _identity_to_json(owner.identity),
            "sha256": owner.sha256,
            "sandbox": owner.sandbox,
        }
    if isinstance(owner, PageRender) and getattr(image._loader, "__func__", None) is PageRender.png_bytes:
        band_owner: object = getattr(image._band_loader, "__self__", None)
        if band_owner is not owner or getattr(image._band_loader, "__func__", None) is not PageRender.band_png_bytes:
            raise ValueError("PageRender image must bind its own band_png_bytes loader")
        return {
            "kind": "page_render",
            "source": str(owner.source),
            "page_index": owner.page_index,
            "dpi": owner.dpi,
            "rotation": owner.rotation,
            "memory_cap_bytes": owner.memory_cap_bytes,
            "timeout_s": owner.timeout_s,
            "identity": _identity_to_json(owner.identity),
            "sha256": owner.sha256,
            "sandbox": owner.sandbox,
        }
    raise TypeError(f"unsupported page_image loader type: {type(owner).__name__}")


def _figure_loaders_to_json(elements: tuple[DocumentElement, ...]) -> dict[str, Json]:
    loaders: dict[str, Json] = {}
    for element in elements:
        if not isinstance(element, Figure):
            continue
        if element._loader is None and element._raw_loader is None:
            continue
        from datagrunt.pdf._loaders import FigureRead

        owner: object = getattr(element._loader, "__self__", None)
        raw_owner: object = getattr(element._raw_loader, "__self__", None)
        if (
            not isinstance(owner, FigureRead)
            or getattr(element._loader, "__func__", None) is not FigureRead.png_bytes
            or raw_owner is not owner
            or getattr(element._raw_loader, "__func__", None) is not FigureRead.raw_bytes
        ):
            raise TypeError(f"unsupported figure loader on {element.element_id}: {type(owner).__name__}")
        loaders[element.element_id] = {
            "source": str(owner.source),
            "page_index": owner.page_index,
            "image": _raw_image_to_json(owner.image),
            "memory_cap_bytes": owner.memory_cap_bytes,
            "sandbox": owner.sandbox,
        }
    return loaders


def _check_expected_source(actual: Path, expected_source: Path | None, label: str) -> None:
    if expected_source is not None and actual != expected_source.absolute():
        raise ValueError(f"{label} source {actual} does not match expected source {expected_source.absolute()}")


def _page_image_loader_from_json(
    value: object, *, expected_source: Path | None, password: str | None
) -> PageDraw | PageRender | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise TypeError(f"page_image_loader must be a JSON object or null, got {type(value).__name__}")
    kind = value.get("kind")
    from datagrunt.pdf._loaders import PageDraw, PageRender

    if kind == "page_draw":
        data = _object("page_image_loader", value, _PAGE_DRAW_KEYS)
        loader: PageDraw | PageRender = PageDraw(
            source=data["source"],
            page_index=data["page_index"],
            dpi=data["dpi"],
            width_pt=data["width_pt"],
            height_pt=data["height_pt"],
            password=password,
            memory_cap_bytes=data["memory_cap_bytes"],
            timeout_s=data["timeout_s"],
            identity=_identity_from_json("page_image_loader.identity", data["identity"]),
            sha256=data["sha256"],
            sandbox=data["sandbox"],
        )
        _check_expected_source(loader.source, expected_source, "page_image_loader")
        return loader
    if kind == "page_render":
        data = _object("page_image_loader", value, _PAGE_RENDER_KEYS)
        loader = PageRender(
            source=data["source"],
            page_index=data["page_index"],
            dpi=data["dpi"],
            rotation=data["rotation"],
            password=password,
            memory_cap_bytes=data["memory_cap_bytes"],
            timeout_s=data["timeout_s"],
            identity=_identity_from_json("page_image_loader.identity", data["identity"]),
            sha256=data["sha256"],
            sandbox=data["sandbox"],
        )
        _check_expected_source(loader.source, expected_source, "page_image_loader")
        return loader
    raise ValueError(f"page_image_loader.kind must be 'page_draw' or 'page_render', got {kind!r}")


def _figure_loader_from_json(
    name: str, value: object, *, expected_source: Path | None, password: str | None
) -> FigureRead:
    from datagrunt.pdf._loaders import FigureRead

    data = _object(name, value, _FIGURE_READ_KEYS)
    loader = FigureRead(
        source=data["source"],
        page_index=data["page_index"],
        image=_raw_image_from_json(f"{name}.image", data["image"]),
        password=password,
        memory_cap_bytes=data["memory_cap_bytes"],
        sandbox=data["sandbox"],
    )
    _check_expected_source(loader.source, expected_source, name)
    return loader


def encode_page_reply(page: Page) -> bytes:
    """Serialize ``page`` and its allowlisted lazy loaders as compact ASCII JSON (zero pickle, no password)."""
    if not isinstance(page, Page):
        raise TypeError(f"page must be a Page, got {type(page).__name__}")
    envelope: dict[str, Any] = {
        "page": _page_json(page),
        "page_image_loader": _page_image_loader_to_json(page.page_image),
        "figure_loaders": _figure_loaders_to_json(page.elements),
    }
    return json.dumps(envelope, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("ascii")


def decode_page_reply(
    body: bytes,
    *,
    expected_source: Path | None = None,
    password: str | None = None,
) -> Page:
    """Deserialize and validate a worker ``Page`` JSON reply, re-binding ``password`` from the parent."""
    if not isinstance(body, bytes):
        raise TypeError(f"body must be bytes, got {type(body).__name__}")
    raw = parse_json(body, "page reply")
    top = _object("page reply", raw, _ENVELOPE_KEYS)
    page = _page("page", top["page"], root=None, version=VERSION)

    img_loader = _page_image_loader_from_json(
        top["page_image_loader"], expected_source=expected_source, password=password
    )
    page_image = page.page_image
    if img_loader is not None:
        from datagrunt.pdf._loaders import PageRender

        if page_image is None:
            raise ValueError("page_image_loader provided for a page without page_image")
        if isinstance(img_loader, PageRender):
            page_image = dataclasses.replace(
                page_image, _loader=img_loader.png_bytes, _band_loader=img_loader.band_png_bytes
            )
        else:
            page_image = dataclasses.replace(page_image, _loader=img_loader.png_bytes)

    raw_fig_loaders = top["figure_loaders"]
    if not isinstance(raw_fig_loaders, Mapping):
        raise TypeError(f"figure_loaders must be a JSON object, got {type(raw_fig_loaders).__name__}")
    figure_ids = {el.element_id for el in page.elements if isinstance(el, Figure)}
    unknown_ids = sorted(key for key in raw_fig_loaders if key not in figure_ids)
    if unknown_ids:
        raise ValueError(f"figure_loaders names unknown or non-figure element_id(s): {unknown_ids}")

    bound_figures: dict[str, FigureRead] = {
        el_id: _figure_loader_from_json(
            f"figure_loaders[{el_id!r}]", spec, expected_source=expected_source, password=password
        )
        for el_id, spec in raw_fig_loaders.items()
    }
    elements = tuple(
        dataclasses.replace(
            el,
            _loader=bound_figures[el.element_id].png_bytes,
            _raw_loader=bound_figures[el.element_id].raw_bytes,
        )
        if isinstance(el, Figure) and el.element_id in bound_figures
        else el
        for el in page.elements
    )
    if page_image is page.page_image and elements == page.elements:
        return page
    return dataclasses.replace(page, page_image=page_image, elements=elements)
