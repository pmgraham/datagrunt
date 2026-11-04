"""datagrunt's schema-driven extraction over v5 PDF elements (5.1, #360).

Its purpose is to read records of a caller's schema from the visible elements of a :class:`~datagrunt.pdf.PDFDocument`.
This package exports the schema marker :data:`Key`, which marks the fields that identify an entity
(:mod:`datagrunt.rationalization.markers`), and :class:`RationalizationError`, raised on an invalid schema (its
``Key`` use included) or parameter (:mod:`datagrunt.rationalization.exceptions`). ``datagrunt`` re-exports ``Key``.

``Key`` and ``RationalizationError`` need only datagrunt's core dependencies: importing this package imports
neither pydantic nor httpx, the packages of the [rationalize] extra.
"""

from datagrunt.rationalization.exceptions import RationalizationError
from datagrunt.rationalization.markers import Key

__all__ = ["Key", "RationalizationError"]
