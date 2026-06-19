"""Tests for the _ThreadLocalDocSession mixin."""

import threading

from datagrunt.core.pdf_io.extraction._doc_session import _ThreadLocalDocSession

# ---------------------------------------------------------------------------
# Fake subclasses for testing
# ---------------------------------------------------------------------------


class _FakeResource:
    """Minimal resource with a close() method."""

    def __init__(self, name="resource"):
        self.name = name
        self.closed = False

    def close(self):
        self.closed = True


class _EagerFake(_ThreadLocalDocSession):
    """Eager (non-lazy) fake subclass."""

    def __init__(self):
        self.filepath = "fake.pdf"
        self._local = threading.local()
        self._open_count = 0

    def _open_resource(self):
        self._open_count += 1
        return _FakeResource(f"resource-{self._open_count}")


class _LazyFake(_ThreadLocalDocSession):
    """Lazy fake subclass (mirrors PdfPlumberTableExtractor behaviour)."""

    _lazy_open = True

    def __init__(self):
        self.filepath = "fake.pdf"
        self._local = threading.local()
        self._open_count = 0

    def _open_resource(self):
        self._open_count += 1
        return _FakeResource(f"lazy-{self._open_count}")


class _SoftFailFake(_ThreadLocalDocSession):
    """Fake that always returns None from _open_resource (soft open-failure)."""

    def __init__(self):
        self.filepath = "bad.pdf"
        self._local = threading.local()

    def _open_resource(self):
        return None


# ---------------------------------------------------------------------------
# Test 1 — Nested ``with`` opens once / closes once (depth counting)
# ---------------------------------------------------------------------------


def test_nested_with_opens_once_closes_once():
    fake = _EagerFake()
    with fake:
        assert fake._local.depth == 1
        first_doc = fake._local.doc
        assert first_doc is not None
        with fake:
            assert fake._local.depth == 2
            # Same handle reused
            assert fake._local.doc is first_doc
        # After inner exit, depth back to 1, handle still open
        assert fake._local.depth == 1
        assert not first_doc.closed
    # After outermost exit, handle closed
    assert fake._local.depth == 0
    assert first_doc.closed
    # Opened exactly once
    assert fake._open_count == 1


# ---------------------------------------------------------------------------
# Test 2 — Outside-scope ``_get_doc()`` returns (resource, True)
# ---------------------------------------------------------------------------


def test_outside_scope_get_doc_returns_transient():
    fake = _EagerFake()
    resource, should_close = fake._get_doc()
    assert resource is not None
    assert should_close is True
    # Opened once (transient)
    assert fake._open_count == 1
    # Manually close transient handle as caller should
    resource.close()
    assert resource.closed


# ---------------------------------------------------------------------------
# Test 3 — Lazy subclass does NOT open on ``__enter__`` but opens on first ``_get_doc()``
# ---------------------------------------------------------------------------


def test_lazy_does_not_open_on_enter_opens_on_get_doc():
    fake = _LazyFake()
    with fake:
        # __enter__ must NOT have opened the resource
        assert fake._open_count == 0
        assert getattr(fake._local, "doc", None) is None

        # First _get_doc() inside scope opens and caches
        doc, should_close = fake._get_doc()
        assert doc is not None
        assert should_close is False
        assert fake._open_count == 1
        assert fake._local.doc is doc

        # Second _get_doc() reuses the cached handle
        doc2, should_close2 = fake._get_doc()
        assert doc2 is doc
        assert should_close2 is False
        assert fake._open_count == 1  # still only one open

    # After scope exits, handle closed
    assert doc.closed


# ---------------------------------------------------------------------------
# Test 4 — Soft-fail: ``_open_resource`` returns None → ``_get_doc()`` returns (None, False)
# ---------------------------------------------------------------------------


def test_soft_fail_returns_none_false():
    fake = _SoftFailFake()
    resource, should_close = fake._get_doc()
    assert resource is None
    assert should_close is False


def test_soft_fail_inside_scope():
    fake = _SoftFailFake()
    with fake:
        resource, should_close = fake._get_doc()
        assert resource is None
        assert should_close is False
    # Exiting cleanly (no crash) even though no handle was opened
    assert fake._local.depth == 0


# ---------------------------------------------------------------------------
# Test 5 — Per-thread independence: two threads each get their own handle
# ---------------------------------------------------------------------------


def test_per_thread_independence():
    fake = _EagerFake()
    # Keep strong references to the docs opened per thread so CPython does not
    # reuse the same memory address before we compare them.
    docs = {}
    barrier = threading.Barrier(2)

    def thread_fn(thread_id):
        with fake:
            docs[thread_id] = fake._local.doc
            barrier.wait()  # hold both threads open at the same time

    t1 = threading.Thread(target=thread_fn, args=(1,))
    t2 = threading.Thread(target=thread_fn, args=(2,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Both threads opened the resource (total 2 opens)
    assert fake._open_count == 2
    # Each thread got a distinct resource object
    assert docs[1] is not docs[2]
