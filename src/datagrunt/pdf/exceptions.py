"""Exception hierarchy for the v5 PDF document model.

Every error raised by ``datagrunt.pdf`` derives from :class:`DatagruntPDFError`
so callers can catch the whole family with one ``except`` clause.
"""

from datagrunt.pdf._validate import int_at_least


class DatagruntPDFError(Exception):
    """Base class for every error raised by ``datagrunt.pdf``."""


class PDFEncryptedError(DatagruntPDFError, ValueError):
    """The PDF is encrypted and cannot be opened without a password.

    Also a ``ValueError``: v4 raised ``ValueError`` for encrypted input, so
    existing ``except ValueError`` handlers keep working unchanged.
    """


class PDFCorruptError(DatagruntPDFError, RuntimeError):
    """The PDF (or one of its pages) is structurally unreadable.

    Also a ``RuntimeError``: v4's corrupt-PDF errors were
    ``PdfiumError(RuntimeError)``, so existing ``except RuntimeError`` handlers
    keep working unchanged.

    Args:
        message: Human-readable description of the corruption.
        page_number: 1-based page the corruption was found on, or ``None``
            when it affects the document as a whole.

    Raises:
        TypeError: If ``page_number`` is not ``None`` and is a bool or a non-int.
        ValueError: If ``page_number`` is an int less than 1.
    """

    def __init__(self, message: str, *, page_number: int | None = None) -> None:
        if page_number is not None:
            int_at_least("page_number", page_number, 1)
        super().__init__(message)
        self.page_number = page_number


class PDFSourceChangedError(DatagruntPDFError):
    """The source PDF changed while it was extracted: its pages and its recorded SHA-256 could disagree.

    Raised whatever ``strict`` says: the whole document is inconsistent, not
    one page, so no document is returned. The message names what changed:
    the file's inode (it was replaced), its size or its modification time.
    """
