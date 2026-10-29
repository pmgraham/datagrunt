"""Exception hierarchy for the v5 PDF document model.

Every error raised by ``datagrunt.pdf`` derives from :class:`DatagruntPDFError`
so callers can catch the whole family with one ``except`` clause.
"""


class DatagruntPDFError(Exception):
    """Base class for every error raised by ``datagrunt.pdf``."""


class PDFEncryptedError(DatagruntPDFError, ValueError):
    """The PDF is encrypted and cannot be opened without a password.

    Also a ``ValueError``: v4 raised ``ValueError`` for encrypted input, so
    existing ``except ValueError`` handlers keep working unchanged.
    """


class PDFCorruptError(DatagruntPDFError):
    """The PDF (or one of its pages) is structurally unreadable.

    Args:
        message: Human-readable description of the corruption.
        page_number: 1-based page the corruption was found on, or ``None``
            when it affects the document as a whole.

    Raises:
        ValueError: If ``page_number`` is not ``None`` and is a bool, a
            non-int, or less than 1.
    """

    def __init__(self, message: str, *, page_number: int | None = None) -> None:
        _check_page_number(page_number)
        super().__init__(message)
        self.page_number = page_number


def _check_page_number(page_number: object) -> None:
    """Reject a page number that is not ``None`` or an int >= 1."""
    if page_number is None:
        return
    # bool is a subclass of int; reject it explicitly so True cannot pass as page 1.
    if isinstance(page_number, bool) or not isinstance(page_number, int):
        raise ValueError(f"page_number must be an int >= 1 or None, got {page_number!r}")
    if page_number < 1:
        raise ValueError(f"page_number must be >= 1, got {page_number}")
