"""The error ``datagrunt.rationalization`` raises (spec §2.1).

:class:`RationalizationError` derives from :class:`~datagrunt.pdf.exceptions.DatagruntPDFError`, so one
``except DatagruntPDFError`` clause catches it with every error of the PDF subsystem whose documents it reads.
"""

from datagrunt.pdf.exceptions import DatagruntPDFError


class RationalizationError(DatagruntPDFError):
    """Rationalization cannot go on as asked, so it stops loudly rather than continue silently.

    Raised on an invalid schema (its ``Key`` use included), invalid parameters, or an unrecoverable provider
    configuration error, before any work starts or in place of silently continuing. The message names the field,
    parameter or provider at fault. Building a marker wrongly (``Key("")``) raises ``ValueError`` or
    ``TypeError`` instead, at the call (:mod:`datagrunt.rationalization.markers`).
    """
