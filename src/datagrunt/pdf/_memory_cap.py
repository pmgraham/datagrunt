"""The memory cap every v5 PDF worker sets on itself before it opens a PDF (#369 A5a, #435).

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. Imports no
pdfium: the render, page and figure workers each call
:func:`apply_memory_cap` once their request is checked, before pdfium sees
the PDF. pdfium decodes compressed data while it merely opens a document
(object streams) and loads a page (inline images), before any check can
run, so only a worker whose memory is capped may hand it a PDF.

The cap is the worker's peak RSS over its peak once started (the
interpreter and datagrunt's imports), on every platform:

- A watchdog thread in the worker ends it (:data:`CAP_EXIT`) once its peak
  RSS passes its starting peak plus the cap: VmHWM on Linux, ``ru_maxrss``
  on macOS, ``PeakWorkingSetSize`` (``GetProcessMemoryInfo``) on Windows.
  pdfium's ctypes calls release the GIL, so it runs while pdfium allocates;
  it polls every 5 ms, and its overshoot is what is allocated in one poll,
  or in one allocation made holding the GIL (at most an image stream the
  guard copies: the 1 GiB asset limit). It reads the peak, so a spike
  between two polls still ends the worker. A reading that fails ends the
  worker (:data:`WATCH_FAILED_EXIT`), and with no first reading the cap
  raises: the worker never reads a PDF unwatched.
- Linux and Windows keep a hard limit as a backstop, at the cap plus
  :data:`BACKSTOP_MARGIN_BYTES` (4 GiB), so memory stays bounded should the
  watchdog not see an allocation: ``RLIMIT_AS`` at the address space mapped
  then (Linux; past it a new mapping fails), a Job Object's per-process
  commit limit at the commit then (Windows). The watchdog starts first, so
  its thread's stack and allocator arena (72 MiB of address space, measured
  on Linux) are in the backstop's baseline. macOS, which does not enforce
  ``RLIMIT_AS``, has the watchdog alone. No test runs on Windows: its
  watchdog's reading and its Job Object are untested.
- Anywhere else the cap raises: no worker reads a PDF uncapped.

A hard limit alone fails quietly: an allocation past it fails inside
pdfium, which then draws the page without that image, and without an error.
The watchdog fails the worker instead, so the backstop sits above it. The
margin, from measurements on Linux (arm64, pypdfium2 5.14, pdfium 156; 125
pages: the corpus, an image at each of the guard's bounds, large JBIG2 images):

- A worker's peak RSS and its address space grow alike while it draws,
  within 3 MiB, though it holds ~840 MiB of address space beyond its RSS
  once started (its imports' libraries, thread stacks and allocator arenas,
  all in the baseline). So for allocations that grow gradually, the cap is
  reached in RSS first, and the watchdog ends the worker long before the
  margin is used.
- The margin is set by single allocations, mapped whole before they are
  touched. For an image the guard sizes, the largest is a filter's output
  ahead of the image codec, at most 4 times the 1 GiB asset limit in one
  allocation (measured: 1,200 MiB from a 300 MiB stream). The others measured
  are smaller: a raw stream loaded whole (674 MiB; at most the asset
  limit), a decoded bitmap (253 MiB at 88.4 M pixels; at most
  89,478,485 x 4 bytes, 341 MiB), JBIG2's buffers (249 MiB), a progressive
  JPEG's coefficients (169 MiB), JPEG 2000's components (84 MiB each).

The residual: one allocation larger than the room left under the backstop
is still refused, and that image is drawn blank, unflagged. While the
watchdog has not ended the worker, that room is the margin, less a few MiB
(the 3 MiB above, and one poll's growth). For an image the guard sizes,
that takes an allocation of nearly 4 GiB: by the bounds above, none is
larger. An image the guard does not size (in a pattern, a Type3 glyph
procedure or a soft mask) has no bound but pdfium's own, so an allocation
larger than the margin is not excluded for it.

Linux limits are inherited: a page worker's render workers and tesseract
calls start under its ``RLIMIT_AS``. A render worker lowers its own limit
again (never above the one it inherits); tesseract runs within it.

The watchdog also ends its worker at the worker's deadline
(:data:`DEADLINE_EXIT`), a :class:`Lifetime` the worker may move later once
it knows its work (a page worker learns its pages after it opens the
document). The caller's own timer, which starts first and is shorter, kills
a worker before: the deadline ends a worker whose caller is gone.
"""

import ctypes
import os
import sys
import threading
import time

from datagrunt.pdf._validate import positive_float
from datagrunt.pdf.elements import _MAX_ASSET_BYTES

if sys.platform != "win32":
    import resource

__all__ = [
    "BACKSTOP_MARGIN_BYTES",
    "CAPPED_PLATFORMS",
    "CAP_EXIT",
    "DEADLINE_EXIT",
    "WATCHDOG",
    "WATCH_FAILED_EXIT",
    "Lifetime",
    "apply_memory_cap",
    "ending",
]

# A worker's exit codes: it reached its memory cap; its watchdog outlived its
# own deadline (the caller's timer, which starts first, kills it before);
# its watchdog could not read its peak RSS (the worker never reads unwatched).
CAP_EXIT = 86
DEADLINE_EXIT = 87
WATCH_FAILED_EXIT = 88

_WATCH_INTERVAL_S = 0.005
_WATCH_START_S = 5.0  # the watchdog's first reading is taken within this, or the cap raises
_MAX_PROC_FILE_BYTES = 1 << 16  # /proc/self/status is ~1.5 KB
CAPPED_PLATFORMS = ("linux", "darwin", "win32")

# The backstop's margin over the cap (Linux's RLIMIT_AS, Windows' commit limit; see the module notes): a filter's
# output ahead of the image codec is at most 4 times the asset limit in one allocation.
BACKSTOP_MARGIN_BYTES = 4 * _MAX_ASSET_BYTES
WATCHDOG = "datagrunt-pdf-watchdog"  # the watchdog thread's name


class Lifetime:
    """When a worker's watchdog ends it: a monotonic time the worker may move later, never earlier.

    The watchdog thread reads it every poll; the worker's own thread moves
    it. A float attribute is read and written whole, so no lock is needed.
    """

    __slots__ = ("_ends",)

    def __init__(self, seconds: float) -> None:
        self._ends = time.monotonic() + positive_float("seconds", seconds)

    def ends(self) -> float:
        """The monotonic time the watchdog ends its worker at."""
        return self._ends

    def extend(self, seconds: float) -> None:
        """Move the end to ``seconds`` from now, if that is later than it is."""
        self._ends = max(self._ends, time.monotonic() + positive_float("seconds", seconds))


def apply_memory_cap(cap_bytes: int, timeout_s: float) -> Lifetime:
    """Cap this worker's peak RSS at ``cap_bytes`` over its peak now; call it in a worker only.

    The watchdog starts first, on every platform, its deadline ``timeout_s``
    from now (the :class:`Lifetime` returned, which the worker may move
    later); then Linux and Windows set their backstop at ``cap_bytes`` plus
    :data:`BACKSTOP_MARGIN_BYTES` over what the worker holds then (see the
    module notes).

    Raises:
        OSError: On a platform with no cap here, or when the peak RSS cannot
            be read (or the watchdog takes no first reading): nothing is read
            uncapped.
        RuntimeError: If the watchdog's thread cannot start.
    """
    if sys.platform not in CAPPED_PLATFORMS:
        raise OSError(f"no memory cap is implemented for platform {sys.platform!r}; the PDF is not read")
    lifetime = Lifetime(timeout_s)
    _watch_peak_rss(_peak_rss_bytes() + cap_bytes, lifetime)
    backstop = cap_bytes + BACKSTOP_MARGIN_BYTES
    if sys.platform == "linux":
        _cap_address_space(backstop)
    elif sys.platform == "win32":
        _cap_commit(backstop)
    return lifetime


def ending(exitcode: int | None, worker: str, cap_bytes: int) -> str:
    """Why ``worker`` (``"the page worker"``, say) ended without a whole reply, by its exit code (None: unreaped)."""
    if exitcode is None:
        return f"{worker} ended without a reply and could not be reaped"
    if exitcode == CAP_EXIT:
        return f"{worker} reached its {cap_bytes}-byte memory cap and was ended"
    if exitcode == WATCH_FAILED_EXIT:
        return f"{worker}'s memory watchdog could not read its peak RSS and ended it"
    if exitcode == DEADLINE_EXIT:
        return f"{worker} outlived its own deadline and was ended"
    if exitcode < 0:
        return f"{worker} was killed by signal {-exitcode}"
    return f"{worker} exited with code {exitcode} without a reply"


def _cap_address_space(backstop_bytes: int) -> None:
    """Linux: limit this process's address space (RLIMIT_AS, soft and hard) to what it maps now plus ``backstop_bytes``.

    Past it the kernel refuses every new mapping: malloc returns NULL, Python
    raises MemoryError, mmap fails. Lowering the hard limit too means nothing
    in the worker can lift it again.
    """
    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    limit = _address_space_limit(_mapped_bytes(), backstop_bytes, soft, hard)
    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))


def _address_space_limit(mapped: int, cap_bytes: int, soft: int, hard: int) -> int:
    """``mapped + cap_bytes``, never above a limit already set (``RLIM_INFINITY``: none)."""
    limits = [mapped + cap_bytes] + [limit for limit in (soft, hard) if limit != resource.RLIM_INFINITY]
    return min(limits)


def _mapped_bytes() -> int:
    """Linux: this process's mapped address space (VmSize) in bytes, from /proc/self/statm."""
    with open("/proc/self/statm", "rb") as statm:
        pages = int(statm.read(_MAX_PROC_FILE_BYTES).split()[0])
    return pages * os.sysconf("SC_PAGE_SIZE")


def _peak_rss_bytes() -> int:
    """This process's peak resident set size in bytes (Linux, macOS and Windows).

    Linux reads VmHWM: its ru_maxrss survives fork + exec, so a spawned
    worker would read its parent's peak. macOS's ru_maxrss is bytes, reset
    at exec. Windows reads PeakWorkingSetSize (``GetProcessMemoryInfo``).

    Raises:
        OSError: If the peak cannot be read, or on another platform.
    """
    if sys.platform == "linux":
        with open("/proc/self/status", "rb") as status:
            for line in status.read(_MAX_PROC_FILE_BYTES).splitlines():
                if line.startswith(b"VmHWM:"):
                    return int(line.split()[1]) * 1024
        raise OSError("no VmHWM line in /proc/self/status")
    if sys.platform == "darwin":
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "win32":
        return _memory_counters().PeakWorkingSetSize
    raise OSError(f"no peak RSS reader for platform {sys.platform!r}")


def _watch_peak_rss(limit_bytes: int, lifetime: Lifetime) -> None:
    """End this process once its peak RSS passes ``limit_bytes``, or at ``lifetime``'s end; return once it has read it.

    A daemon thread polls every 5 ms and exits the process with
    :data:`CAP_EXIT` (past the limit), :data:`DEADLINE_EXIT`, or
    :data:`WATCH_FAILED_EXIT` (a reading failed). It reads the peak, not the
    current RSS, so a spike between two polls still ends the process. This
    returns once the thread has taken its first reading, which maps the
    thread's stack and allocator arena.

    Raises:
        RuntimeError: If the thread cannot start.
        OSError: If the thread takes no reading within ``_WATCH_START_S``.
    """
    watching = threading.Event()

    def watch() -> None:
        while time.monotonic() < lifetime.ends():  # bounded: by the lifetime, which only its worker moves
            if _watched_peak() > limit_bytes:
                os._exit(CAP_EXIT)
            watching.set()
            time.sleep(_WATCH_INTERVAL_S)
        os._exit(DEADLINE_EXIT)

    threading.Thread(target=watch, name=WATCHDOG, daemon=True).start()
    if not watching.wait(_WATCH_START_S):
        raise OSError(f"the memory watchdog took no reading within {_WATCH_START_S} s; the PDF is not read")


def _watched_peak() -> int:
    """The watchdog's reading of this process's peak RSS; a reading that fails ends the process.

    The failure is written to stderr (its tail ends the caller's error), then
    the process exits with :data:`WATCH_FAILED_EXIT`: never swallowed.
    """
    try:
        return _peak_rss_bytes()
    except Exception as exc:  # the worker never reads unwatched: it ends, saying why
        try:
            why = f"the memory watchdog cannot read this process's peak RSS: {type(exc).__name__}: {exc}\n"
            os.write(2, why.encode("utf-8", errors="backslashreplace"))
        finally:
            os._exit(WATCH_FAILED_EXIT)


class _BasicLimits(ctypes.Structure):
    """JOBOBJECT_BASIC_LIMIT_INFORMATION (winnt.h)."""

    _fields_ = (
        ("PerProcessUserTimeLimit", ctypes.c_int64),
        ("PerJobUserTimeLimit", ctypes.c_int64),
        ("LimitFlags", ctypes.c_uint32),
        ("MinimumWorkingSetSize", ctypes.c_size_t),
        ("MaximumWorkingSetSize", ctypes.c_size_t),
        ("ActiveProcessLimit", ctypes.c_uint32),
        ("Affinity", ctypes.c_size_t),
        ("PriorityClass", ctypes.c_uint32),
        ("SchedulingClass", ctypes.c_uint32),
    )


class _ExtendedLimits(ctypes.Structure):
    """JOBOBJECT_EXTENDED_LIMIT_INFORMATION (winnt.h), its IO_COUNTERS as six 64-bit counts."""

    _fields_ = (
        ("BasicLimitInformation", _BasicLimits),
        ("IoInfo", ctypes.c_uint64 * 6),
        ("ProcessMemoryLimit", ctypes.c_size_t),
        ("JobMemoryLimit", ctypes.c_size_t),
        ("PeakProcessMemoryUsed", ctypes.c_size_t),
        ("PeakJobMemoryUsed", ctypes.c_size_t),
    )


class _MemoryCounters(ctypes.Structure):
    """PROCESS_MEMORY_COUNTERS_EX (psapi.h)."""

    _fields_ = (
        ("cb", ctypes.c_uint32),
        ("PageFaultCount", ctypes.c_uint32),
        ("PeakWorkingSetSize", ctypes.c_size_t),
        ("WorkingSetSize", ctypes.c_size_t),
        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
        ("PagefileUsage", ctypes.c_size_t),
        ("PeakPagefileUsage", ctypes.c_size_t),
        ("PrivateUsage", ctypes.c_size_t),
    )


_JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9  # JOBOBJECTINFOCLASS
_JOB_OBJECT_LIMIT_PROCESS_MEMORY = 0x100
_SIZE_MAX = (1 << (8 * ctypes.sizeof(ctypes.c_size_t))) - 1


def _memory_counters() -> _MemoryCounters:
    """Windows: this process's memory counters (``GetProcessMemoryInfo``). Untested: no test runs on Windows.

    Raises:
        OSError: Off Windows, or if the call fails (its WinError).
    """
    if sys.platform != "win32":
        raise OSError("GetProcessMemoryInfo reads a Windows process only")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.GetCurrentProcess.restype = ctypes.c_void_p
    kernel32.K32GetProcessMemoryInfo.argtypes = (ctypes.c_void_p, ctypes.c_void_p, ctypes.c_uint32)
    counters = _MemoryCounters(cb=ctypes.sizeof(_MemoryCounters))
    if not kernel32.K32GetProcessMemoryInfo(kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb):
        raise ctypes.WinError(ctypes.get_last_error())
    return counters


def _commit_limit(private: int, extra: int, size_max: int) -> int:
    """``private + extra`` bytes, at most ``size_max``: a 32-bit size_t field never holds a wrapped (lower) limit."""
    return min(private + extra, size_max)


def _cap_commit(backstop_bytes: int) -> None:
    """Windows: put this process in a new Job Object limiting its commit to what it commits now plus ``backstop_bytes``.

    Past it the kernel refuses every commit (VirtualAlloc fails, Python
    raises MemoryError), as RLIMIT_AS does on Linux. Jobs nest (Windows 8
    and later), so this works inside a CI runner's or terminal's job. No
    test runs on Windows: this is untested.
    """
    if sys.platform != "win32":
        raise OSError("a Job Object caps a Windows process only")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.GetCurrentProcess.restype = ctypes.c_void_p
    kernel32.CreateJobObjectW.restype = ctypes.c_void_p
    kernel32.CreateJobObjectW.argtypes = (ctypes.c_void_p, ctypes.c_wchar_p)
    kernel32.SetInformationJobObject.argtypes = (ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint32)
    kernel32.AssignProcessToJobObject.argtypes = (ctypes.c_void_p, ctypes.c_void_p)
    process = kernel32.GetCurrentProcess()
    limits = _ExtendedLimits()
    limits.BasicLimitInformation.LimitFlags = _JOB_OBJECT_LIMIT_PROCESS_MEMORY
    limits.ProcessMemoryLimit = _commit_limit(_memory_counters().PrivateUsage, backstop_bytes, _SIZE_MAX)
    job = kernel32.CreateJobObjectW(None, None)
    if not job:
        raise ctypes.WinError(ctypes.get_last_error())
    information = ctypes.byref(limits)
    if not kernel32.SetInformationJobObject(
        job, _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION, information, ctypes.sizeof(limits)
    ):
        raise ctypes.WinError(ctypes.get_last_error())
    if not kernel32.AssignProcessToJobObject(job, process):
        raise ctypes.WinError(ctypes.get_last_error())
