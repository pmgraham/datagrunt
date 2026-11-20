"""Result assembly, telemetry and Polars/Arrow export for rationalization (plan §3, §4.10).

:class:`RationalizedResult` holds the complete output of a ``rationalize()`` run: the folded
:class:`~datagrunt.rationalization.folding.GroundedRecord` items in document order, the clean ``records``
(``status in ("consensus", "single_model")``), the ``needs_review`` items, the ``quarantined`` responses/records,
the ``failed_units``, the ``empty_units``, the ``skipped_units`` and the run's :class:`RationalizeTelemetry`.

:func:`polars_schema` maps a :class:`~datagrunt.rationalization.schema.ModelSpec` (or :class:`PreparedSchema` or
``BaseModel`` subclass) to a deterministic Polars column-to-dtype mapping:

- ``str``, ``str`` ``Enum``, ``str`` ``Literal`` -> ``pl.String``
- ``int``, ``int`` ``Enum``, ``int`` ``Literal`` -> ``pl.Int64`` (values outside ``[INT64_MIN, INT64_MAX]`` raise
  :class:`~datagrunt.rationalization.RationalizationError` naming the field path before building the frame)
- ``float`` -> ``pl.Float64``
- ``bool``, ``bool`` ``Literal`` -> ``pl.Boolean``
- ``Decimal`` -> ``pl.Decimal(precision=38, scale=9)`` (values with more than 9 fractional digits or more than 29
  integer digits raise :class:`~datagrunt.rationalization.RationalizationError` naming the field path)
- ``date`` -> ``pl.Date``
- ``AwareDatetime`` -> ``pl.Datetime("us", "UTC")`` (converted to UTC instant for the frame without mutating
  ``records``)
- ``NaiveDatetime`` -> ``pl.Datetime("us")``
- ``list[X]`` -> ``pl.List(dtype_of(X))``
- ``BaseModel`` -> ``pl.Struct({field.name: dtype_of(field) for field in child.fields})``

Bounds: every walk is bounded by :data:`~datagrunt.rationalization.schema.MAX_DEPTH` and linear in the serialized
records and child items. The module imports only the standard library, polars, pyarrow, pydantic, the PDF element
model and its sibling modules: nothing from ``datagrunt.core``, and no import inside a function (invariant 9).
"""

import contextlib
import copy
import json
import os
import secrets
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Generic, TypeVar

import polars as pl
import pyarrow as pa
from pydantic import BaseModel

from datagrunt.pdf.elements import HIDDEN_REASONS
from datagrunt.rationalization.consensus import QuarantinedItem, _check_strings, _check_text, _check_tuple_of
from datagrunt.rationalization.envelope import ESCAPE_CATEGORIES
from datagrunt.rationalization.exceptions import RationalizationError
from datagrunt.rationalization.folding import _CLEAN_STATUSES, GroundedRecord, ModelOutput
from datagrunt.rationalization.runner import FailedUnit, ModelUsage
from datagrunt.rationalization.schema import MAX_DEPTH, FieldSpec, ModelSpec, PreparedSchema, ScalarSpec, describe
from datagrunt.rationalization.units import SkippedUnit

DECIMAL_PRECISION = 38
"""Polars ``Decimal`` total precision for ``Decimal`` schema fields."""

DECIMAL_SCALE = 9
"""Polars ``Decimal`` fractional scale for ``Decimal`` schema fields."""

INT64_MIN = -(1 << 63)
"""Smallest signed 64-bit integer admitted by ``pl.Int64``."""

INT64_MAX = (1 << 63) - 1
"""Largest signed 64-bit integer admitted by ``pl.Int64``."""

_DECIMAL_MAX_INT_DIGITS = DECIMAL_PRECISION - DECIMAL_SCALE
_COUNTER_FIELDS: tuple[str, ...] = (
    "document_count",
    "unit_count",
    "skipped_unit_count",
    "empty_unit_count",
    "failed_unit_count",
    "oversize_unit_count",
    "cache_hits",
    "cache_misses",
    "cache_corrupt",
    "cache_write_failures",
    "quarantined_count",
)

T = TypeVar("T", bound=BaseModel)

__all__ = [
    "DECIMAL_PRECISION",
    "DECIMAL_SCALE",
    "INT64_MAX",
    "INT64_MIN",
    "RationalizeTelemetry",
    "RationalizedResult",
    "polars_schema",
]


# ---------------------------------------------------------------------------------------------------------------
# Telemetry
# ---------------------------------------------------------------------------------------------------------------


def _check_non_negative_int(name: str, value: object) -> int:
    """Require an exact ``int`` (not ``bool``) >= 0."""
    if type(value) is not int:
        raise TypeError(f"{name} must be an int, got {type(value).__name__}")
    if value < 0:
        raise ValueError(f"{name} must be >= 0, got {value}")
    return value


def _check_reason_counts(name: str, value: object, allowed_keys: Sequence[str]) -> Mapping[str, int]:
    """Validate and wrap a mapping of known string keys to non-negative ints."""
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a Mapping, got {type(value).__name__}")
    allowed = set(allowed_keys)
    copied: dict[str, int] = {}
    for key, count in value.items():
        if not isinstance(key, str):
            raise TypeError(f"{name} keys must be str, got {type(key).__name__}")
        if key not in allowed:
            raise ValueError(f"{name} has unknown key {key!r}; allowed: {tuple(allowed_keys)}")
        copied[key] = _check_non_negative_int(f"{name}[{key!r}]", count)
    return MappingProxyType(copied)


def _check_usage_by_model(value: object) -> Mapping[str, ModelUsage]:
    """Validate and wrap a mapping of model ids to :class:`ModelUsage`."""
    if not isinstance(value, Mapping):
        raise TypeError(f"usage_by_model must be a Mapping, got {type(value).__name__}")
    copied: dict[str, ModelUsage] = {}
    for model_id, usage in value.items():
        _check_text("usage_by_model key", model_id)
        if not isinstance(usage, ModelUsage):
            raise TypeError(f"usage_by_model[{model_id!r}] must be a ModelUsage, got {type(usage).__name__}")
        copied[model_id] = usage
    return MappingProxyType(copied)


def _check_models_tuple(models: object, usage_by_model: Mapping[str, ModelUsage]) -> None:
    """Validate ``models`` as 3-tuples of non-blank strings aligned with ``usage_by_model``."""
    if not isinstance(models, tuple):
        raise TypeError(f"models must be a tuple, got {type(models).__name__}")
    for idx, entry in enumerate(models):
        if not isinstance(entry, tuple) or len(entry) != 3:
            raise TypeError(f"models[{idx}] must be a (model_id, model_family, provider_kind) 3-tuple")
        for part_idx, part in enumerate(entry):
            _check_text(f"models[{idx}][{part_idx}]", part)
    model_ids = tuple(entry[0] for entry in models)
    if model_ids != tuple(usage_by_model):
        raise ValueError("models and usage_by_model must name the same model_ids in the same order")


@dataclass(frozen=True, slots=True)
class RationalizeTelemetry:
    """Run-wide counters, per-reason exclusion counts and per-model usage (plan §3).

    Args:
        document_count: Documents processed.
        unit_count: Units packed and scheduled.
        skipped_unit_count: Units skipped before model execution: ``len(RationalizedResult.skipped_units)``.
        empty_unit_count: Units where every complete response returned zero records.
        failed_unit_count: ``(unit, model)`` requests that ended without a response.
        oversize_unit_count: Units flagged ``oversize``.
        cache_hits: Distinct requests answered from cache.
        cache_misses: Distinct requests sent to a provider.
        cache_corrupt: Distinct requests whose file-cache entry was corrupt.
        cache_write_failures: Responses a cache failed to store.
        quarantined_count: Quarantined responses or records.
        hidden_excluded_by_reason: Hidden elements excluded per :data:`~datagrunt.pdf.elements.HIDDEN_REASONS`.
        escaped_by_category: R8 code points escaped per :data:`~datagrunt.rationalization.envelope.ESCAPE_CATEGORIES`.
        usage_by_model: Per-model :class:`~datagrunt.rationalization.runner.ModelUsage` in provider order.
        models: ``(model_id, model_family, provider_kind)`` tuples in provider order.

    Raises:
        TypeError: If any counter or mapping has the wrong type.
        ValueError: If any counter is negative, a mapping key is unknown, or ``models`` disagrees with
            ``usage_by_model``.
    """

    document_count: int
    unit_count: int
    skipped_unit_count: int
    empty_unit_count: int
    failed_unit_count: int
    oversize_unit_count: int
    cache_hits: int
    cache_misses: int
    cache_corrupt: int
    cache_write_failures: int
    quarantined_count: int
    hidden_excluded_by_reason: Mapping[str, int]
    escaped_by_category: Mapping[str, int]
    usage_by_model: Mapping[str, ModelUsage]
    models: tuple[tuple[str, str, str], ...]

    def __post_init__(self) -> None:
        for name in _COUNTER_FIELDS:
            _check_non_negative_int(name, getattr(self, name))
        object.__setattr__(
            self,
            "hidden_excluded_by_reason",
            _check_reason_counts("hidden_excluded_by_reason", self.hidden_excluded_by_reason, HIDDEN_REASONS),
        )
        object.__setattr__(
            self,
            "escaped_by_category",
            _check_reason_counts("escaped_by_category", self.escaped_by_category, ESCAPE_CATEGORIES),
        )
        object.__setattr__(self, "usage_by_model", _check_usage_by_model(self.usage_by_model))
        _check_models_tuple(self.models, self.usage_by_model)

    def to_dict(self) -> dict[str, Any]:
        """Return a pure JSON-serializable dict of all counters, reason maps, models and per-model usage."""
        return {
            "document_count": self.document_count,
            "unit_count": self.unit_count,
            "skipped_unit_count": self.skipped_unit_count,
            "empty_unit_count": self.empty_unit_count,
            "failed_unit_count": self.failed_unit_count,
            "oversize_unit_count": self.oversize_unit_count,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "cache_corrupt": self.cache_corrupt,
            "cache_write_failures": self.cache_write_failures,
            "quarantined_count": self.quarantined_count,
            "hidden_excluded_by_reason": dict(self.hidden_excluded_by_reason),
            "escaped_by_category": dict(self.escaped_by_category),
            "models": [
                {"model_id": entry[0], "model_family": entry[1], "provider_kind": entry[2]} for entry in self.models
            ],
            "usage_by_model": {
                model_id: {
                    "requests": usage.requests,
                    "retries": usage.retries,
                    "input_tokens": usage.input_tokens,
                    "output_tokens": usage.output_tokens,
                    "latency_ms": usage.latency_ms,
                }
                for model_id, usage in self.usage_by_model.items()
            },
        }


# ---------------------------------------------------------------------------------------------------------------
# Polars schema mapping
# ---------------------------------------------------------------------------------------------------------------


def _resolve_model_spec(spec: object) -> ModelSpec:
    """Resolve a :class:`ModelSpec`, :class:`PreparedSchema`, or ``BaseModel`` subclass to a :class:`ModelSpec`."""
    if isinstance(spec, ModelSpec):
        return spec
    if isinstance(spec, PreparedSchema):
        return spec.spec
    if isinstance(spec, type) and issubclass(spec, BaseModel) and spec is not BaseModel:
        return describe(spec)
    raise TypeError(f"spec must be a ModelSpec, PreparedSchema, or BaseModel subclass, got {type(spec).__name__}")


def _scalar_dtype(scalar: ScalarSpec) -> pl.DataType:
    """Map one :class:`ScalarSpec` to its deterministic Polars ``DataType`` (plan §4.10)."""
    base = scalar.base
    if base is str:
        return pl.String()
    if base is int:
        return pl.Int64()
    if base is float:
        return pl.Float64()
    if base is bool:
        return pl.Boolean()
    if base is Decimal:
        return pl.Decimal(precision=DECIMAL_PRECISION, scale=DECIMAL_SCALE)
    if base is date:
        return pl.Date()
    if base is datetime:
        return pl.Datetime("us", "UTC") if scalar.tz == "aware" else pl.Datetime("us")
    raise RationalizationError(f"unsupported scalar base {base!r}")


def _struct_dtype(spec: ModelSpec, depth: int) -> pl.Struct:
    """Map a nested :class:`ModelSpec` to a ``pl.Struct``."""
    if depth > MAX_DEPTH:
        raise RationalizationError(f"{spec.model.__name__}: nesting is deeper than {MAX_DEPTH} levels")
    return pl.Struct({field_spec.name: _field_dtype(field_spec, depth) for field_spec in spec.fields})


def _field_dtype(field_spec: FieldSpec, depth: int) -> pl.DataType:
    """Map one :class:`FieldSpec` to its deterministic Polars ``DataType``."""
    if depth > MAX_DEPTH:
        raise RationalizationError(f"{field_spec.name}: nesting is deeper than {MAX_DEPTH} levels")
    if field_spec.kind == "scalar" and field_spec.scalar is not None:
        return _scalar_dtype(field_spec.scalar)
    if field_spec.kind == "scalar_list" and field_spec.scalar is not None:
        return pl.List(_scalar_dtype(field_spec.scalar))
    if field_spec.kind == "model" and field_spec.child is not None:
        return _struct_dtype(field_spec.child, depth + 1)
    if field_spec.kind == "model_list" and field_spec.child is not None:
        return pl.List(_struct_dtype(field_spec.child, depth + 1))
    raise RationalizationError(f"invalid FieldSpec {field_spec.name!r}")


def polars_schema(spec: ModelSpec | PreparedSchema | type[BaseModel]) -> dict[str, pl.DataType]:
    """Return the deterministic Polars schema dict for ``spec`` (plan §4.10).

    Args:
        spec: A :class:`ModelSpec`, :class:`PreparedSchema`, or ``BaseModel`` subclass.

    Returns:
        Mapping of field attribute names in declaration order to Polars ``DataType`` instances.

    Raises:
        TypeError: If ``spec`` is not a :class:`ModelSpec`, :class:`PreparedSchema`, or ``BaseModel`` subclass.
        RationalizationError: If ``spec`` is a ``BaseModel`` class that fails schema checks or exceeds
            :data:`~datagrunt.rationalization.schema.MAX_DEPTH`.
    """
    model_spec = _resolve_model_spec(spec)
    return {field_spec.name: _field_dtype(field_spec, 1) for field_spec in model_spec.fields}


# ---------------------------------------------------------------------------------------------------------------
# RationalizedResult
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RationalizedResult(Generic[T]):
    """Complete result of a ``rationalize()`` call (plan §3, §4.10).

    Args:
        items: All folded :class:`~datagrunt.rationalization.folding.GroundedRecord` items in document order.
        records: Clean validated records (``status in ("consensus", "single_model")``).
        needs_review: Items whose status is not in ``("consensus", "single_model")``.
        quarantined: Quarantined responses and records.
        failed_units: ``(unit, model)`` requests that ended without a response.
        empty_units: Unit ids where every complete response had zero valid and zero quarantined records.
        skipped_units: Units skipped before model execution, in document order, each naming its ``reason`` and
            ``page_numbers`` (:class:`~datagrunt.rationalization.units.SkippedUnit`): a page error, an all-hidden or
            no-text group, or an ``"image_only"`` page, whose content lives only in its page image: none of its
            text was sent and no model read its image, so a record it alone holds is not in ``items``.
        telemetry: Run-wide counters and per-model usage.
        schema: Optional schema (:class:`ModelSpec`, :class:`PreparedSchema`, or ``BaseModel`` subclass) used to
            derive the Polars/Arrow schema even when ``items`` has no records.

    Raises:
        TypeError: If any field has the wrong type.
        ValueError: If ``records`` or ``needs_review`` disagrees with ``items``, or ``empty_units`` holds a blank id.
    """

    items: tuple[GroundedRecord[T], ...]
    records: tuple[T, ...] = field(repr=False)
    needs_review: tuple[GroundedRecord[T], ...] = field(repr=False)
    quarantined: tuple[QuarantinedItem, ...] = field(repr=False)
    failed_units: tuple[FailedUnit, ...] = field(repr=False)
    empty_units: tuple[str, ...]
    skipped_units: tuple[SkippedUnit, ...]
    telemetry: RationalizeTelemetry
    schema: type[T] | ModelSpec | PreparedSchema | None = field(default=None, repr=False, compare=False)
    _spec: ModelSpec | None = field(init=False, repr=False, compare=False, default=None)

    def __post_init__(self) -> None:
        _check_tuple_of("items", self.items, GroundedRecord)
        _check_tuple_of("records", self.records, BaseModel)
        _check_tuple_of("needs_review", self.needs_review, GroundedRecord)
        _check_tuple_of("quarantined", self.quarantined, QuarantinedItem)
        _check_tuple_of("failed_units", self.failed_units, FailedUnit)
        _check_strings("empty_units", self.empty_units)
        if not all(unit_id.strip() for unit_id in self.empty_units):
            raise ValueError("empty_units must hold non-blank unit ids")
        _check_tuple_of("skipped_units", self.skipped_units, SkippedUnit)
        if not isinstance(self.telemetry, RationalizeTelemetry):
            raise TypeError(f"telemetry must be a RationalizeTelemetry, got {type(self.telemetry).__name__}")

        expected_records = tuple(
            item.record for item in self.items if item.status in _CLEAN_STATUSES and item.record is not None
        )
        if self.records != expected_records:
            raise ValueError("records must equal the records of consensus and single_model items in items")
        expected_review = tuple(item for item in self.items if item.status not in _CLEAN_STATUSES)
        if self.needs_review != expected_review:
            raise ValueError("needs_review must equal the items whose status is not consensus or single_model")

        spec = self._resolve_optional_spec()
        object.__setattr__(self, "_spec", spec)
        if spec is not None:
            for item in self.items:
                if item.record is not None and not isinstance(item.record, spec.model):
                    raise TypeError(
                        f"item record must be an instance of {spec.model.__name__}, got {type(item.record).__name__}"
                    )

    def _resolve_optional_spec(self) -> ModelSpec | None:
        """Resolve the :class:`ModelSpec` once from ``schema`` or the first non-None record."""
        if self.schema is not None:
            return _resolve_model_spec(self.schema)
        for item in self.items:
            if item.record is not None:
                return describe(type(item.record))
        return None

    def _optional_spec(self) -> ModelSpec | None:
        """Return the cached :class:`ModelSpec` if ``schema`` or a non-None record is present, else ``None``."""
        return self._spec

    def _require_spec(self) -> ModelSpec:
        """Return the cached :class:`ModelSpec`, raising :class:`RationalizationError` if unavailable."""
        if self._spec is None:
            raise RationalizationError("cannot build a DataFrame from an empty RationalizedResult without schema")
        return self._spec

    @property
    def grounded_items(self) -> tuple[GroundedRecord[T], ...]:
        """Items in the grounded-union tier (:attr:`GroundedRecord.is_grounded_union`), in document order."""
        return tuple(item for item in self.items if item.is_grounded_union)

    @property
    def grounded_records(self) -> tuple[T, ...]:
        """Validated records in the grounded-union tier, in document order."""
        return tuple(item.record for item in self.items if item.is_grounded_union and item.record is not None)

    def to_dataframe(
        self,
        flatten: str | None = None,
        *,
        include_disputed: bool = False,
        include_grounded_union: bool = False,
        include_model_outputs: bool | None = None,
        by_model: bool = False,
    ) -> pl.DataFrame:
        """Convert the result's records to a Polars ``DataFrame`` with deterministic dtypes (plan §4.10).

        Args:
            flatten: Optional dotted path of ``model_list`` or ``scalar_list`` fields to left-outer explode.
                Sibling list fields at each exploded level are dropped to prevent Cartesian fan-out.
            include_disputed: When ``False`` (default), includes ``self.records`` (unless ``include_grounded_union``
                is ``True``). When ``True``, includes every item in ``self.items`` (requiring ``item.record`` unless
                ``by_model=True``) and appends ``_status`` and ``_doc_id`` string columns.
            include_grounded_union: When ``True`` (and ``include_disputed`` is ``False``), includes
                :attr:`grounded_items`, appends ``_status`` and ``_doc_id``, and defaults ``include_model_outputs``
                to ``True`` when ``by_model`` is ``False``.
            include_model_outputs: When ``True`` (default when ``include_grounded_union=True`` and ``not by_model``),
                appends ``_supporting_models`` and typed ``_model_outputs`` columns showing each model's votes,
                quotes, cited element ids, and grounding status. Pass ``False`` to omit them.
            by_model: When ``True``, explodes selected items into one typed row per ``(item, model_id, vote)`` with
                ``_model_id``, ``_model_family``, ``_vote_index``, ``_status``, ``_doc_id``, ``_is_grounded``,
                ``_grounding_quotes``, and ``_element_ids``.

        Returns:
            A :class:`polars.DataFrame`.

        Raises:
            TypeError: If ``flatten`` or any boolean option has the wrong type.
            RationalizationError: If ``flatten`` is not a valid list path, or any ``int`` or ``Decimal`` value
                exceeds ``pl.Int64`` / ``pl.Decimal(38, 9)`` bounds.
        """
        inc_disp, inc_gu, want_mo, flat_bm = _resolve_export_flags(
            include_disputed=include_disputed,
            include_grounded_union=include_grounded_union,
            include_model_outputs=include_model_outputs,
            by_model=by_model,
        )
        if flatten is not None and not isinstance(flatten, str):
            raise TypeError(f"flatten must be a str or None, got {type(flatten).__name__}")

        spec = self._require_spec()
        parts = _validate_flatten_path(spec, flatten) if flatten is not None else ()
        try:
            if flat_bm:
                selected = self._select_items(
                    include_disputed=inc_disp,
                    include_grounded_union=inc_gu,
                    require_record=False,
                )
                return _build_by_model_dataframe(spec, selected, parts, include_model_outputs=want_mo)
            selected = self._select_items(
                include_disputed=inc_disp,
                include_grounded_union=inc_gu,
                require_record=True,
            )
            return _build_merged_dataframe(
                spec,
                selected,
                parts,
                with_status_doc=inc_disp or inc_gu,
                with_model_outputs=want_mo,
            )
        except _FieldBoundsError as err:
            raise err.to_error() from None

    def to_dicts(
        self,
        *,
        include_disputed: bool = False,
        include_grounded_union: bool = False,
        include_model_outputs: bool | None = None,
        by_model: bool = False,
    ) -> list[dict[str, Any]]:
        """Convert the result's records to Python dicts (plan §4.10).

        Args:
            include_disputed: Include disputed/review items (requiring ``record`` unless ``by_model=True``).
            include_grounded_union: Include :attr:`grounded_items` (defaults ``include_model_outputs=True`` when
                ``not by_model``).
            include_model_outputs: Append ``_supporting_models`` and ``_model_outputs`` dicts per row (each model's
                ``votes`` are attribute-keyed JSON-mode dicts matching :attr:`GroundedRecord.model_votes`).
            by_model: Emit one attribute-keyed JSON-mode dict per ``(item, supporting model_id, vote)`` with
                ``_model_id``, ``_model_family``, ``_vote_index``, ``_status``, ``_doc_id``, ``_is_grounded``,
                ``_grounding_quotes``, and ``_element_ids``. Merged rows (``by_model=False``) use
                ``record.model_dump(mode="python")``.

        Returns:
            List of record dicts.
        """
        inc_disp, inc_gu, want_mo, flat_bm = _resolve_export_flags(
            include_disputed=include_disputed,
            include_grounded_union=include_grounded_union,
            include_model_outputs=include_model_outputs,
            by_model=by_model,
        )
        if flat_bm:
            selected = self._select_items(
                include_disputed=inc_disp,
                include_grounded_union=inc_gu,
                require_record=False,
            )
            return _build_by_model_dicts(selected, include_model_outputs=want_mo)
        if not inc_disp and not inc_gu and not want_mo:
            return [record.model_dump(mode="python") for record in self.records]
        selected = self._select_items(
            include_disputed=inc_disp,
            include_grounded_union=inc_gu,
            require_record=True,
        )
        return _build_merged_dicts(
            selected,
            with_status_doc=inc_disp or inc_gu,
            with_model_outputs=want_mo,
        )

    def to_arrow_table(
        self,
        flatten: str | None = None,
        *,
        include_disputed: bool = False,
        include_grounded_union: bool = False,
        include_model_outputs: bool | None = None,
        by_model: bool = False,
    ) -> pa.Table:
        """Convert the result's records to a PyArrow ``Table`` via :meth:`to_dataframe` (plan §4.10)."""
        return self.to_dataframe(
            flatten=flatten,
            include_disputed=include_disputed,
            include_grounded_union=include_grounded_union,
            include_model_outputs=include_model_outputs,
            by_model=by_model,
        ).to_arrow()

    def to_jsonl(
        self,
        path: str | os.PathLike[str],
        *,
        include_provenance: bool = True,
        include_disputed: bool = True,
        include_grounded_union: bool = False,
        by_model: bool = False,
    ) -> int:
        """Write selected items as atomic UTF-8 JSONL (provenance-by-default) and return the row count."""
        target_path = _checked_jsonl_options(
            path,
            include_provenance=include_provenance,
            include_disputed=include_disputed,
            include_grounded_union=include_grounded_union,
            by_model=by_model,
        )
        if by_model:
            selected_bm = self._select_items(
                include_disputed=include_disputed,
                include_grounded_union=include_grounded_union,
                require_record=False,
            )
            encoded = _encode_by_model_jsonl(selected_bm, include_provenance=include_provenance)
            return _write_jsonl_atomic(target_path, encoded)
        selected = self._select_jsonl_items(
            include_provenance=include_provenance,
            include_disputed=include_disputed,
            include_grounded_union=include_grounded_union,
        )
        with_meta = include_disputed or include_grounded_union
        encoded = [
            _serialize_jsonl_item(item, include_provenance=include_provenance, with_meta=with_meta) for item in selected
        ]
        return _write_jsonl_atomic(target_path, encoded)

    def _select_jsonl_items(
        self,
        *,
        include_provenance: bool,
        include_disputed: bool,
        include_grounded_union: bool,
    ) -> Sequence[GroundedRecord[T]]:
        """Select :class:`GroundedRecord` items for :meth:`to_jsonl` according to export tier flags."""
        if include_disputed:
            if include_provenance:
                return self.items
            return tuple(item for item in self.items if item.record is not None)
        if include_grounded_union:
            return tuple(item for item in self.grounded_items if item.record is not None)
        return tuple(item for item in self.items if item.status in _CLEAN_STATUSES and item.record is not None)

    def _select_items(
        self,
        *,
        include_disputed: bool,
        include_grounded_union: bool,
        require_record: bool,
    ) -> tuple[GroundedRecord[T], ...]:
        """Select :class:`GroundedRecord` items matching tier flags and ``require_record``."""
        if include_disputed:
            source: Sequence[GroundedRecord[T]] = self.items
        elif include_grounded_union:
            source = self.grounded_items
        else:
            source = tuple(item for item in self.items if item.status in _CLEAN_STATUSES)
        if not require_record:
            return tuple(source)
        return tuple(item for item in source if item.record is not None)


# ---------------------------------------------------------------------------------------------------------------
# Export flag validation, dicts, and JSONL provenance helpers
# ---------------------------------------------------------------------------------------------------------------


def _require_strict_bool(name: str, value: object) -> bool:
    """Require an exact ``bool`` value."""
    if type(value) is not bool:
        raise TypeError(f"{name} must be a bool, got {type(value).__name__}")
    return value


def _resolve_export_flags(
    *,
    include_disputed: object,
    include_grounded_union: object,
    include_model_outputs: object,
    by_model: object,
) -> tuple[bool, bool, bool, bool]:
    """Validate and resolve tier and per-model export flags."""
    inc_disp = _require_strict_bool("include_disputed", include_disputed)
    inc_gu = _require_strict_bool("include_grounded_union", include_grounded_union)
    flat_bm = _require_strict_bool("by_model", by_model)
    if include_model_outputs is not None and type(include_model_outputs) is not bool:
        raise TypeError(f"include_model_outputs must be a bool or None, got {type(include_model_outputs).__name__}")
    if flat_bm:
        want_mo = bool(include_model_outputs)
    else:
        want_mo = inc_gu if include_model_outputs is None else bool(include_model_outputs)
    return inc_disp, inc_gu, want_mo, flat_bm


def _build_merged_dicts(
    items: Sequence[GroundedRecord[Any]],
    *,
    with_status_doc: bool,
    with_model_outputs: bool,
) -> list[dict[str, Any]]:
    """Convert selected ``GroundedRecord`` items with non-None ``record`` into Python dicts."""
    out: list[dict[str, Any]] = []
    for item in items:
        if item.record is None:
            continue
        row = item.record.model_dump(mode="python")
        if with_status_doc:
            row["_status"] = item.status
            row["_doc_id"] = item.doc_id
        if with_model_outputs:
            row["_supporting_models"] = list(item.supporting_models)
            row["_model_outputs"] = [mo.to_dict() for mo in item.model_outputs]
        out.append(row)
    return out


def _build_by_model_dicts(
    items: Sequence[GroundedRecord[Any]],
    *,
    include_model_outputs: bool,
) -> list[dict[str, Any]]:
    """Explode selected items into one defensive dict per ``(item, supporting model_id, vote)``."""
    out: list[dict[str, Any]] = []
    for item in items:
        all_outputs = [mo.to_dict() for mo in item.model_outputs] if include_model_outputs else None
        supporting = list(item.supporting_models) if include_model_outputs else None
        for mo in item.model_outputs:
            for vote_idx, vote in enumerate(mo.votes):
                row: dict[str, Any] = {k: copy.deepcopy(v) for k, v in vote.items()}
                row["_model_id"] = mo.model_id
                row["_model_family"] = mo.model_family
                row["_vote_index"] = vote_idx
                row["_status"] = item.status
                row["_doc_id"] = item.doc_id
                row["_is_grounded"] = mo.is_grounded
                row["_grounding_quotes"] = list(mo.grounding_quotes)
                row["_element_ids"] = list(mo.element_ids)
                if all_outputs is not None and supporting is not None:
                    row["_supporting_models"] = list(supporting)
                    row["_model_outputs"] = copy.deepcopy(all_outputs)
                out.append(row)
    return out


def _checked_jsonl_options(
    path: object,
    *,
    include_provenance: object,
    include_disputed: object,
    include_grounded_union: object,
    by_model: object = False,
) -> Path:
    """Validate :meth:`RationalizedResult.to_jsonl` path and strict boolean flags."""
    for flag_name, flag_val in (
        ("include_provenance", include_provenance),
        ("include_disputed", include_disputed),
        ("include_grounded_union", include_grounded_union),
        ("by_model", by_model),
    ):
        _require_strict_bool(flag_name, flag_val)
    if not isinstance(path, (str, os.PathLike)):
        raise TypeError(f"path must be a str or PathLike[str], got {type(path).__name__}")
    raw_path = os.fspath(path)
    if not isinstance(raw_path, str):
        raise TypeError(f"path must be a str or PathLike[str], got {type(raw_path).__name__}")
    if not raw_path or not raw_path.strip() or "\x00" in raw_path:
        raise ValueError("path must be a non-empty path without NUL bytes")
    return Path(raw_path)


def _sources_dicts(item: GroundedRecord[Any]) -> list[dict[str, Any]]:
    """Serialize resolved :class:`ElementSource` provenance entries on ``item``."""
    return [
        {
            "doc_id": s.doc_id,
            "unit_id": s.unit_id,
            "element_id": s.element_id,
            "page_number": s.page_number,
            "bbox": {"x0": s.bbox.x0, "y0": s.bbox.y0, "x1": s.bbox.x1, "y1": s.bbox.y1},
            "scope_path": list(s.scope_path),
        }
        for s in item.sources
    ]


def _provenance_fields(item: GroundedRecord[Any]) -> dict[str, Any]:
    """Build the first-class provenance dictionary for a :class:`GroundedRecord` JSONL row."""
    if not isinstance(item, GroundedRecord):
        raise TypeError(f"item must be a GroundedRecord, got {type(item).__name__}")
    return {
        "_doc_id": item.doc_id,
        "_source_name": item.source_name,
        "_status": item.status,
        "_is_grounded": item.is_grounded,
        "_is_grounded_union": item.is_grounded_union,
        "_unit_ids": list(item.unit_ids),
        "_supporting_models": list(item.supporting_models),
        "_model_outputs": [mo.to_dict() for mo in item.model_outputs],
        "_grounding_quotes": list(item.grounding_quotes),
        "_disputed_fields": list(item.disputed_fields),
        "_notes": list(item.notes),
        "_grounding_reasons": [
            {
                "reason": r.reason,
                "model_id": r.model_id,
                "index": r.index,
                "key_field": r.key_field,
                "text": r.text,
            }
            for r in item.grounding_reasons
        ],
        "_sources": _sources_dicts(item),
    }


def _encode_jsonl_row(row: Mapping[str, Any]) -> bytes:
    """Encode ``row`` as a strict UTF-8 JSONL line ending in ``\\n``."""
    try:
        return (json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise RationalizationError(f"JSONL row cannot be serialized as strict UTF-8 JSON: {exc}") from exc


def _serialize_jsonl_item(
    item: GroundedRecord[Any],
    *,
    include_provenance: bool,
    with_meta: bool,
) -> bytes:
    """Serialize one :class:`GroundedRecord` into a single UTF-8 JSONL line ending in ``\\n``."""
    if not isinstance(item, GroundedRecord):
        raise TypeError(f"item must be a GroundedRecord, got {type(item).__name__}")
    if not isinstance(include_provenance, bool) or not isinstance(with_meta, bool):
        raise TypeError("include_provenance and with_meta must be bools")
    row: dict[str, Any] = item.record.model_dump(mode="json") if item.record is not None else {}
    if include_provenance:
        row.update(_provenance_fields(item))
    elif with_meta:
        row["_status"] = item.status
        row["_doc_id"] = item.doc_id
    return _encode_jsonl_row(row)


def _by_model_jsonl_row(
    item: GroundedRecord[Any],
    mo: ModelOutput,
    vote_idx: int,
    vote: Mapping[str, Any],
    *,
    include_provenance: bool,
) -> dict[str, Any]:
    """Assemble one JSONL row dict for a single ``(item, model_id, vote)`` entry."""
    row: dict[str, Any] = dict(vote)
    row["_model_id"] = mo.model_id
    row["_model_family"] = mo.model_family
    row["_vote_index"] = vote_idx
    row["_status"] = item.status
    row["_doc_id"] = item.doc_id
    row["_is_grounded"] = mo.is_grounded
    row["_grounding_quotes"] = list(mo.grounding_quotes)
    row["_element_ids"] = list(mo.element_ids)
    if include_provenance:
        row["_source_name"] = item.source_name
        row["_is_grounded_union"] = item.is_grounded_union
        row["_unit_ids"] = list(item.unit_ids)
        row["_disputed_fields"] = list(item.disputed_fields)
        row["_notes"] = list(item.notes)
        row["_grounding_reasons"] = [
            {"reason": r.reason, "model_id": r.model_id, "index": r.index, "key_field": r.key_field, "text": r.text}
            for r in mo.grounding_reasons
        ]
        row["_sources"] = _sources_dicts(item)
    return row


def _encode_by_model_jsonl(
    items: Sequence[GroundedRecord[Any]],
    *,
    include_provenance: bool,
) -> list[bytes]:
    """Encode selected items as per-model JSONL lines in item -> provider -> vote order."""
    encoded: list[bytes] = []
    for item in items:
        for mo in item.model_outputs:
            for vote_idx, vote in enumerate(mo.votes):
                row = _by_model_jsonl_row(item, mo, vote_idx, vote, include_provenance=include_provenance)
                encoded.append(_encode_jsonl_row(row))
    return encoded


def _unlink_quietly(path: Path) -> None:
    """Best-effort unlink of a staging file during error cleanup."""
    with contextlib.suppress(OSError):
        path.unlink()


def _write_jsonl_atomic(target_path: Path, encoded_rows: Sequence[bytes]) -> int:
    """Atomically write UTF-8 JSONL ``encoded_rows`` to ``target_path`` via a sibling staging file."""
    if not isinstance(target_path, Path) or not isinstance(encoded_rows, Sequence):
        raise TypeError("_write_jsonl_atomic requires a Path and a Sequence of bytes")
    staging = target_path.parent / f".{target_path.name}.tmp_{secrets.token_hex(16)}"
    created = False
    try:
        fd = os.open(staging, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
        created = True
        with os.fdopen(fd, "wb") as handle:
            handle.writelines(encoded_rows)
        os.replace(staging, target_path)
    except OSError as exc:
        if created:
            _unlink_quietly(staging)
        raise RationalizationError(f"failed to write JSONL to {target_path}: {type(exc).__name__}") from exc
    except BaseException:
        if created:
            _unlink_quietly(staging)
        raise
    return len(encoded_rows)


# ---------------------------------------------------------------------------------------------------------------
# Columnar serialization and lazy bounds checks
# ---------------------------------------------------------------------------------------------------------------


class _FieldBoundsError(Exception):
    """Internal bounds failure that accumulates field/index segments only while unwinding an error."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason
        self.parts: list[str] = []

    def add_index(self, idx: int) -> None:
        self.parts.append(f"[{idx}]")

    def add_field(self, name: str) -> None:
        self.parts.append(name)

    def to_error(self) -> RationalizationError:
        path = ""
        for part in reversed(self.parts):
            path = f"{path}{part}" if not path or part.startswith("[") else f"{path}.{part}"
        return RationalizationError(f"{path}: {self.reason}" if path else self.reason)


def _check_decimal_bounds(value: Decimal) -> Decimal:
    """Verify ``value`` fits ``pl.Decimal(38, 9)`` (at most 29 integer digits and 9 fractional digits)."""
    if not value.is_finite():
        raise _FieldBoundsError("Decimal value is not finite")
    _, digits, exponent = value.as_tuple()
    if not isinstance(exponent, int):
        raise _FieldBoundsError("Decimal value is not finite")
    frac_digits = max(0, -exponent)
    if frac_digits > DECIMAL_SCALE:
        raise _FieldBoundsError(f"Decimal value has {frac_digits} fractional digits, exceeding scale {DECIMAL_SCALE}")
    int_digits = max(0, len(digits) + exponent) if value != 0 else 0
    if int_digits > _DECIMAL_MAX_INT_DIGITS:
        raise _FieldBoundsError(
            f"Decimal value has {int_digits} integer digits, exceeding {_DECIMAL_MAX_INT_DIGITS} "
            f"(Decimal({DECIMAL_PRECISION}, {DECIMAL_SCALE}))"
        )
    return value


def _is_passthrough_scalar(scalar: ScalarSpec) -> bool:
    """Whether validated values of ``scalar`` need neither Enum unwrapping, bounds checks, nor UTC conversion."""
    if scalar.enum is not None:
        return False
    base = scalar.base
    return base is str or base is bool or base is date or (base is datetime and scalar.tz == "naive")


def _serialize_scalar(value: Any, scalar: ScalarSpec) -> Any:
    """Convert one scalar value for Polars frame construction without mutating the source record."""
    if value is None:
        return None
    raw = value.value if scalar.enum is not None and isinstance(value, Enum) else value
    base = scalar.base
    if base is int:
        if type(raw) is not int or not INT64_MIN <= raw <= INT64_MAX:
            raise _FieldBoundsError(f"int value is outside Int64 range [{INT64_MIN}, {INT64_MAX}]")
        return raw
    if base is Decimal:
        dec = Decimal(raw) if type(raw) is int else raw
        return _check_decimal_bounds(dec)
    if base is datetime and scalar.tz == "aware":
        return raw.astimezone(timezone.utc)
    if base is float and type(raw) is int:
        return float(raw)
    return raw


def _serialize_vote_temporal(raw: Any, scalar: ScalarSpec) -> Any:
    """Convert a JSON-mode ``date`` or ``datetime`` vote value into a typed Python temporal object."""
    if scalar.base is date:
        if isinstance(raw, date) and not isinstance(raw, datetime):
            return raw
        if isinstance(raw, str):
            try:
                return date.fromisoformat(raw)
            except ValueError:
                raise _FieldBoundsError("vote value is not an ISO-8601 date string") from None
        raise _FieldBoundsError(
            f"cannot serialize {type(raw).__name__} vote value as Date (custom serializer or non-ISO JSON format)"
        )
    if isinstance(raw, datetime):
        dt_val = raw
    elif isinstance(raw, str):
        try:
            dt_val = datetime.fromisoformat(raw)
        except ValueError:
            raise _FieldBoundsError("vote value is not an ISO-8601 datetime string") from None
    else:
        raise _FieldBoundsError(
            f"cannot serialize {type(raw).__name__} vote value as Datetime (custom serializer or non-ISO JSON format)"
        )
    if scalar.tz == "aware":
        if dt_val.tzinfo is None:
            raise _FieldBoundsError("aware Datetime vote value is missing timezone offset")
        return dt_val.astimezone(timezone.utc)
    return dt_val


def _serialize_vote_scalar(value: Any, scalar: ScalarSpec) -> Any:
    """Convert one JSON-mode vote scalar into its deterministic Polars value with bounds and type checks."""
    if value is None:
        return None
    raw = value.value if scalar.enum is not None and isinstance(value, Enum) else value
    base = scalar.base
    if base is str:
        if not isinstance(raw, str):
            raise _FieldBoundsError(f"cannot serialize {type(raw).__name__} vote value as String")
        return raw
    if base is bool:
        if type(raw) is not bool:
            raise _FieldBoundsError(f"cannot serialize {type(raw).__name__} vote value as Boolean")
        return raw
    if base is int:
        if type(raw) is not int:
            raise _FieldBoundsError(f"cannot serialize {type(raw).__name__} vote value as Int64")
        if not INT64_MIN <= raw <= INT64_MAX:
            raise _FieldBoundsError(f"int value is outside Int64 range [{INT64_MIN}, {INT64_MAX}]")
        return raw
    if base is float:
        if type(raw) is int:
            return float(raw)
        if type(raw) is float:
            return raw
        raise _FieldBoundsError(f"cannot serialize {type(raw).__name__} vote value as Float64")
    if base is Decimal:
        if isinstance(raw, Decimal):
            dec = raw
        elif isinstance(raw, (str, int, float)) and type(raw) is not bool:
            try:
                dec = Decimal(str(raw))
            except (InvalidOperation, ValueError):
                raise _FieldBoundsError("vote value cannot be parsed as Decimal") from None
        else:
            raise _FieldBoundsError(f"cannot serialize {type(raw).__name__} vote value as Decimal")
        return _check_decimal_bounds(dec)
    if base is date or base is datetime:
        return _serialize_vote_temporal(raw, scalar)
    raise RationalizationError(f"unsupported scalar base {base!r}")


def _serialize_scalar_list(items: Sequence[Any], scalar: ScalarSpec, *, from_vote: bool = False) -> list[Any]:
    """Serialize a ``scalar_list`` sequence, recording the failing element index only if bounds fail."""
    if not from_vote and _is_passthrough_scalar(scalar):
        return list(items)
    conv = _serialize_vote_scalar if from_vote else _serialize_scalar
    out: list[Any] = []
    for idx, item in enumerate(items):
        try:
            out.append(conv(item, scalar))
        except _FieldBoundsError as err:
            err.add_index(idx)
            raise
    return out


def _serialize_model_list(
    items: Sequence[BaseModel | Mapping[str, Any]],
    child_spec: ModelSpec,
    depth: int,
) -> list[dict[str, Any]]:
    """Serialize a ``model_list`` sequence into Polars Struct dicts, recording the index only on error."""
    out: list[dict[str, Any]] = []
    for idx, item in enumerate(items):
        try:
            out.append(_serialize_source(item, child_spec, depth))
        except _FieldBoundsError as err:
            err.add_index(idx)
            raise
    return out


def _serialize_field(instance: BaseModel, field_spec: FieldSpec, depth: int) -> Any:
    """Serialize one field of ``instance`` for Polars frame construction without eager path formatting."""
    return _serialize_source_field(instance, field_spec, depth)


def _serialize_source_field(
    source: BaseModel | Mapping[str, Any],
    field_spec: FieldSpec,
    depth: int,
) -> Any:
    """Serialize one field from either a validated ``BaseModel`` or an attribute-keyed ``model_votes`` dict."""
    if depth > MAX_DEPTH:
        raise RationalizationError(f"{field_spec.name}: nesting is deeper than {MAX_DEPTH} levels")
    if isinstance(source, Mapping):
        from_vote = True
        value = source.get(field_spec.name)
    else:
        from_vote = False
        value = getattr(source, field_spec.name)
    if value is None:
        return None
    try:
        if field_spec.kind == "scalar" and field_spec.scalar is not None:
            if from_vote:
                return _serialize_vote_scalar(value, field_spec.scalar)
            return value if _is_passthrough_scalar(field_spec.scalar) else _serialize_scalar(value, field_spec.scalar)
        if field_spec.kind == "scalar_list" and field_spec.scalar is not None:
            return _serialize_scalar_list(value, field_spec.scalar, from_vote=from_vote)
        if field_spec.kind == "model" and field_spec.child is not None:
            return _serialize_source(value, field_spec.child, depth + 1)
        if field_spec.kind == "model_list" and field_spec.child is not None:
            return _serialize_model_list(value, field_spec.child, depth + 1)
    except _FieldBoundsError as err:
        err.add_field(field_spec.name)
        raise
    raise RationalizationError(f"invalid FieldSpec {field_spec.name!r}")


def _serialize_source(
    source: BaseModel | Mapping[str, Any],
    spec: ModelSpec,
    depth: int,
) -> dict[str, Any]:
    """Serialize ``source`` (``BaseModel`` or JSON-mode vote dict) into a Python Struct dict."""
    if depth > MAX_DEPTH:
        raise RationalizationError(f"{spec.model.__name__}: nesting is deeper than {MAX_DEPTH} levels")
    return {field_spec.name: _serialize_source_field(source, field_spec, depth) for field_spec in spec.fields}


def _serialize_model(instance: BaseModel, spec: ModelSpec, depth: int) -> dict[str, Any]:
    """Serialize ``instance`` of ``spec`` into a Python Struct dict matching :func:`polars_schema`."""
    return _serialize_source(instance, spec, depth)


def _model_output_dtype(spec: ModelSpec) -> pl.DataType:
    """Return the deterministic Polars ``List(Struct(...))`` dtype for ``_model_outputs``."""
    return pl.List(
        pl.Struct(
            {
                "model_id": pl.String(),
                "model_family": pl.String(),
                "supported": pl.Boolean(),
                "is_grounded": pl.Boolean(),
                "votes": pl.List(_struct_dtype(spec, 1)),
                "grounding_quotes": pl.List(pl.String()),
                "element_ids": pl.List(pl.String()),
            }
        )
    )


def _serialize_item_model_outputs(item: GroundedRecord[Any], spec: ModelSpec) -> list[dict[str, Any]]:
    """Serialize ``item.model_outputs`` into Polars-typed Struct dicts with bounds validation."""
    out: list[dict[str, Any]] = []
    for mo_idx, mo in enumerate(item.model_outputs):
        typed_votes: list[dict[str, Any]] = []
        for vote_idx, vote in enumerate(mo.votes):
            try:
                typed_votes.append(_serialize_source(vote, spec, 1))
            except _FieldBoundsError as err:
                err.add_index(vote_idx)
                err.add_field("votes")
                err.add_index(mo_idx)
                err.add_field("_model_outputs")
                raise
        out.append(
            {
                "model_id": mo.model_id,
                "model_family": mo.model_family,
                "supported": mo.supported,
                "is_grounded": mo.is_grounded,
                "votes": typed_votes,
                "grounding_quotes": list(mo.grounding_quotes),
                "element_ids": list(mo.element_ids),
            }
        )
    return out


def _serialize_column(records: Sequence[BaseModel], field_spec: FieldSpec) -> list[Any]:
    """Build one unflattened column list across ``records`` directly."""
    if not records:
        return []
    name = field_spec.name
    if field_spec.kind == "scalar" and field_spec.scalar is not None:
        scalar = field_spec.scalar
        if _is_passthrough_scalar(scalar):
            return [getattr(rec, name) for rec in records]
        try:
            return [_serialize_scalar(getattr(rec, name), scalar) for rec in records]
        except _FieldBoundsError as err:
            err.add_field(name)
            raise
    if field_spec.kind == "scalar_list" and field_spec.scalar is not None and _is_passthrough_scalar(field_spec.scalar):
        return [getattr(rec, name) for rec in records]
    return [_serialize_field(rec, field_spec, 1) for rec in records]


def _build_unflattened_columns(records: Sequence[BaseModel], spec: ModelSpec) -> dict[str, list[Any]]:
    """Build columnar ``dict[str, list[Any]]`` buffers directly for unflattened :meth:`to_dataframe`."""
    return {field_spec.name: _serialize_column(records, field_spec) for field_spec in spec.fields}


def _append_merged_meta_schema(
    schema: dict[str, pl.DataType],
    spec: ModelSpec,
    *,
    with_status_doc: bool,
    with_model_outputs: bool,
) -> None:
    """Append optional ``_status``, ``_doc_id``, ``_supporting_models``, and ``_model_outputs`` dtypes."""
    if with_status_doc:
        schema["_status"] = pl.String()
        schema["_doc_id"] = pl.String()
    if with_model_outputs:
        schema["_supporting_models"] = pl.List(pl.String())
        schema["_model_outputs"] = _model_output_dtype(spec)


def _build_merged_dataframe(
    spec: ModelSpec,
    items: Sequence[GroundedRecord[Any]],
    parts: tuple[str, ...],
    *,
    with_status_doc: bool,
    with_model_outputs: bool,
) -> pl.DataFrame:
    """Build an unflattened or flattened merged-record ``DataFrame`` with optional metadata columns."""
    records = [item.record for item in items if item.record is not None]
    if not parts:
        schema = polars_schema(spec)
        _append_merged_meta_schema(schema, spec, with_status_doc=with_status_doc, with_model_outputs=with_model_outputs)
        columns = _build_unflattened_columns(records, spec)
        if with_status_doc:
            columns["_status"] = [item.status for item in items]
            columns["_doc_id"] = [item.doc_id for item in items]
        if with_model_outputs:
            columns["_supporting_models"] = [list(item.supporting_models) for item in items]
            columns["_model_outputs"] = [_serialize_item_model_outputs(item, spec) for item in items]
        return pl.DataFrame(columns, schema=schema)

    flat_schema = _flat_schema_for_level(spec, parts, "", 1)
    _append_merged_meta_schema(
        flat_schema, spec, with_status_doc=with_status_doc, with_model_outputs=with_model_outputs
    )
    flat_columns: dict[str, list[Any]] = {name: [] for name in flat_schema}
    for item in items:
        emitted = _append_flat_level(item.record, spec, parts, "", 1, flat_columns)
        if with_status_doc:
            flat_columns["_status"].extend([item.status] * emitted)
            flat_columns["_doc_id"].extend([item.doc_id] * emitted)
        if with_model_outputs:
            flat_columns["_supporting_models"].extend([list(item.supporting_models)] * emitted)
            flat_columns["_model_outputs"].extend([_serialize_item_model_outputs(item, spec)] * emitted)
    return pl.DataFrame(flat_columns, schema=flat_schema)


def _by_model_meta_schema(spec: ModelSpec, *, include_model_outputs: bool) -> dict[str, pl.DataType]:
    """Return the per-model audit metadata column schema appended when ``by_model=True``."""
    meta: dict[str, pl.DataType] = {
        "_model_id": pl.String(),
        "_model_family": pl.String(),
        "_vote_index": pl.Int64(),
        "_status": pl.String(),
        "_doc_id": pl.String(),
        "_is_grounded": pl.Boolean(),
        "_grounding_quotes": pl.List(pl.String()),
        "_element_ids": pl.List(pl.String()),
    }
    if include_model_outputs:
        meta["_supporting_models"] = pl.List(pl.String())
        meta["_model_outputs"] = _model_output_dtype(spec)
    return meta


def _extend_by_model_meta(
    columns: dict[str, list[Any]],
    item: GroundedRecord[Any],
    mo: ModelOutput,
    vote_idx: int,
    emitted: int,
    *,
    supporting: list[str] | None,
    serialized_mo: list[dict[str, Any]] | None,
) -> None:
    """Append ``emitted`` copies of per-model audit metadata columns into ``columns``."""
    columns["_model_id"].extend([mo.model_id] * emitted)
    columns["_model_family"].extend([mo.model_family] * emitted)
    columns["_vote_index"].extend([vote_idx] * emitted)
    columns["_status"].extend([item.status] * emitted)
    columns["_doc_id"].extend([item.doc_id] * emitted)
    columns["_is_grounded"].extend([mo.is_grounded] * emitted)
    columns["_grounding_quotes"].extend([list(mo.grounding_quotes)] * emitted)
    columns["_element_ids"].extend([list(mo.element_ids)] * emitted)
    if supporting is not None and serialized_mo is not None:
        columns["_supporting_models"].extend([supporting] * emitted)
        columns["_model_outputs"].extend([serialized_mo] * emitted)


def _build_by_model_dataframe(
    spec: ModelSpec,
    items: Sequence[GroundedRecord[Any]],
    parts: tuple[str, ...],
    *,
    include_model_outputs: bool,
) -> pl.DataFrame:
    """Build a flat per-model ``DataFrame`` (one or more rows per ``(item, supporting model_id, vote)``)."""
    base_schema = _flat_schema_for_level(spec, parts, "", 1) if parts else polars_schema(spec)
    schema = {**base_schema, **_by_model_meta_schema(spec, include_model_outputs=include_model_outputs)}
    columns: dict[str, list[Any]] = {name: [] for name in schema}
    for item in items:
        supporting = list(item.supporting_models) if include_model_outputs else None
        serialized_mo = _serialize_item_model_outputs(item, spec) if include_model_outputs else None
        for mo in item.model_outputs:
            for vote_idx, vote in enumerate(mo.votes):
                try:
                    if parts:
                        emitted = _append_flat_level(vote, spec, parts, "", 1, columns)
                    else:
                        for field_spec in spec.fields:
                            columns[field_spec.name].append(_serialize_source_field(vote, field_spec, 1))
                        emitted = 1
                except _FieldBoundsError as err:
                    err.add_index(vote_idx)
                    err.add_field(f"model[{mo.model_id}]")
                    raise
                _extend_by_model_meta(
                    columns,
                    item,
                    mo,
                    vote_idx,
                    emitted,
                    supporting=supporting,
                    serialized_mo=serialized_mo,
                )
    return pl.DataFrame(columns, schema=schema)


# ---------------------------------------------------------------------------------------------------------------
# Flatten helpers
# ---------------------------------------------------------------------------------------------------------------


def _validate_flatten_path(spec: ModelSpec, flatten: str) -> tuple[str, ...]:
    """Validate a dotted ``flatten`` path against ``spec`` and return its segments."""
    if not flatten or not flatten.strip():
        raise RationalizationError("flatten path must be non-blank")
    parts = tuple(flatten.split("."))
    if any(not part or not part.strip() or part != part.strip() for part in parts):
        raise RationalizationError(f"invalid flatten path {flatten!r}")
    if len(parts) > MAX_DEPTH:
        raise RationalizationError(f"flatten path {flatten!r} exceeds maximum depth {MAX_DEPTH}")

    current = spec
    for idx, part in enumerate(parts):
        field_spec = current.fields_by_name.get(part)
        if field_spec is None:
            raise RationalizationError(f"flatten path {flatten!r}: {current.model.__name__} has no field {part!r}")
        is_last = idx == len(parts) - 1
        if not is_last:
            if field_spec.kind != "model_list" or field_spec.child is None:
                raise RationalizationError(
                    f"flatten path {flatten!r}: intermediate field {part!r} must be a list of models, "
                    f"got {field_spec.kind}"
                )
            current = field_spec.child
        elif field_spec.kind not in ("model_list", "scalar_list"):
            raise RationalizationError(
                f"flatten path {flatten!r}: field {part!r} must be a list field, got {field_spec.kind}"
            )
    return parts


def _flat_schema_for_level(spec: ModelSpec, parts: tuple[str, ...], prefix: str, depth: int) -> dict[str, pl.DataType]:
    """Build the Polars schema for ``spec`` flattened along ``parts``, dropping sibling lists at each level."""
    if depth > MAX_DEPTH:
        raise RationalizationError(f"{prefix or spec.model.__name__}: nesting is deeper than {MAX_DEPTH} levels")
    target = parts[0]
    rest = parts[1:]
    out: dict[str, pl.DataType] = {}
    for field_spec in spec.fields:
        col_name = f"{prefix}{field_spec.name}"
        if field_spec.name != target:
            if field_spec.kind in ("scalar_list", "model_list"):
                continue
            out[col_name] = _field_dtype(field_spec, depth)
        elif rest:
            if field_spec.child is None:
                raise RationalizationError(f"missing child spec on {col_name}")
            out.update(_flat_schema_for_level(field_spec.child, rest, f"{col_name}.", depth + 1))
        elif field_spec.kind == "scalar_list" and field_spec.scalar is not None:
            out[col_name] = _scalar_dtype(field_spec.scalar)
        elif field_spec.kind == "model_list" and field_spec.child is not None:
            for child_field in field_spec.child.fields:
                out[f"{col_name}.{child_field.name}"] = _field_dtype(child_field, depth + 1)
    return out


def _append_child_model_fields(
    elem: BaseModel | Mapping[str, Any],
    child_spec: ModelSpec,
    col_prefix: str,
    depth: int,
    columns: dict[str, list[Any]],
) -> None:
    """Serialize and append each field of one terminal child model ``elem`` into ``columns``."""
    for child_field in child_spec.fields:
        columns[f"{col_prefix}.{child_field.name}"].append(_serialize_source_field(elem, child_field, depth))


def _append_flat_terminal_models(
    raw_items: Sequence[BaseModel | Mapping[str, Any]],
    child_spec: ModelSpec,
    target_name: str,
    col_prefix: str,
    depth: int,
    columns: dict[str, list[Any]],
) -> int:
    """Append terminal exploded ``model_list`` rows (or a single ``None`` row when empty) into ``columns``."""
    if depth > MAX_DEPTH:
        raise RationalizationError(f"{col_prefix}: nesting is deeper than {MAX_DEPTH} levels")
    if not raw_items:
        for child_field in child_spec.fields:
            columns[f"{col_prefix}.{child_field.name}"].append(None)
        return 1
    for idx, elem in enumerate(raw_items):
        try:
            _append_child_model_fields(elem, child_spec, col_prefix, depth, columns)
        except _FieldBoundsError as err:
            err.add_index(idx)
            err.add_field(target_name)
            raise
    return len(raw_items)


def _append_flat_intermediate(
    raw_items: Sequence[BaseModel | Mapping[str, Any]],
    child_spec: ModelSpec,
    target_name: str,
    rest: tuple[str, ...],
    col_prefix: str,
    depth: int,
    columns: dict[str, list[Any]],
) -> int:
    """Recurse into an intermediate ``model_list`` along ``rest``, left-outer exploding ``()`` as ``(None,)``."""
    if not raw_items:
        return _append_flat_level(None, child_spec, rest, f"{col_prefix}.", depth + 1, columns)
    total = 0
    for idx, elem in enumerate(raw_items):
        try:
            total += _append_flat_level(elem, child_spec, rest, f"{col_prefix}.", depth + 1, columns)
        except _FieldBoundsError as err:
            err.add_index(idx)
            err.add_field(target_name)
            raise
    return total


def _append_flat_target(
    source: BaseModel | Mapping[str, Any] | None,
    target_spec: FieldSpec,
    rest: tuple[str, ...],
    col_prefix: str,
    depth: int,
    columns: dict[str, list[Any]],
) -> int:
    """Explode ``target_spec`` on ``source`` into ``columns`` and return the number of emitted rows."""
    if source is None:
        from_vote = False
        raw_items: Sequence[Any] = ()
    elif isinstance(source, Mapping):
        from_vote = True
        raw_items = source.get(target_spec.name) or ()
    else:
        from_vote = False
        raw_items = getattr(source, target_spec.name) or ()
    if rest:
        if target_spec.child is None:
            raise RationalizationError(f"missing child spec on {col_prefix}")
        return _append_flat_intermediate(
            raw_items,
            target_spec.child,
            target_spec.name,
            rest,
            col_prefix,
            depth,
            columns,
        )
    if target_spec.kind == "scalar_list" and target_spec.scalar is not None:
        if not raw_items:
            columns[col_prefix].append(None)
            return 1
        try:
            serialized = _serialize_scalar_list(raw_items, target_spec.scalar, from_vote=from_vote)
        except _FieldBoundsError as err:
            err.add_field(target_spec.name)
            raise
        columns[col_prefix].extend(serialized)
        return len(serialized)
    if target_spec.child is None:
        raise RationalizationError(f"missing child spec on {col_prefix}")
    return _append_flat_terminal_models(
        raw_items,
        target_spec.child,
        target_spec.name,
        col_prefix,
        depth + 1,
        columns,
    )


def _append_flat_level(
    source: BaseModel | Mapping[str, Any] | None,
    spec: ModelSpec,
    parts: tuple[str, ...],
    prefix: str,
    depth: int,
    columns: dict[str, list[Any]],
) -> int:
    """Left-outer explode ``source`` along ``parts`` into ``columns``, skipping sibling list fields."""
    if depth > MAX_DEPTH:
        raise RationalizationError(f"{prefix or spec.model.__name__}: nesting is deeper than {MAX_DEPTH} levels")
    target = parts[0]
    target_spec = spec.fields_by_name.get(target)
    if target_spec is None:
        raise RationalizationError(f"missing target field {target!r}")
    kept: list[tuple[str, Any]] = []
    for field_spec in spec.fields:
        if field_spec.name != target and field_spec.kind not in ("scalar_list", "model_list"):
            val = None if source is None else _serialize_source_field(source, field_spec, depth)
            kept.append((f"{prefix}{field_spec.name}", val))
    count = _append_flat_target(source, target_spec, parts[1:], f"{prefix}{target}", depth, columns)
    for col_name, val in kept:
        if count == 1:
            columns[col_name].append(val)
        else:
            columns[col_name].extend([val] * count)
    return count
