"""Shared thread-local, depth-counted document session for PDF backends."""


class _ThreadLocalDocSession:
    """Mixin providing a reused, per-thread document handle.

    The outermost ``with`` opens the resource once per thread; nested ``with``
    blocks reuse it; it is closed when the outermost scope exits. Outside any
    scope, ``_get_doc()`` returns a transient resource the caller must close.

    Subclasses MUST set ``self.filepath`` and ``self._local`` (a
    ``threading.local()``) in their own ``__init__`` and implement
    ``_open_resource()``. Override ``_close_resource()`` if closing differs, and
    set ``_lazy_open = True`` to defer opening until first use (table extractor).
    ``_open_resource()`` may return ``None`` to signal a soft open-failure.
    """

    _lazy_open = False

    def _open_resource(self):
        raise NotImplementedError

    def _close_resource(self, resource):
        resource.close()

    def __enter__(self):
        if not hasattr(self._local, "depth"):
            self._local.depth = 0
            self._local.doc = None
        if self._local.depth == 0 and not self._lazy_open:
            self._local.doc = self._open_resource()
        self._local.depth += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._local.depth -= 1
        if self._local.depth == 0:
            doc = getattr(self._local, "doc", None)
            if doc:
                self._close_resource(doc)
            self._local.doc = None

    def _get_doc(self):
        """Return ``(resource, should_close)``. Cached in-scope (False); lazy
        opens-and-caches on first in-scope use; transient outside scope (True);
        ``(None, False)`` on a soft open-failure."""
        doc = getattr(self._local, "doc", None)
        if doc:
            return doc, False
        resource = self._open_resource()
        if resource is None:
            return None, False
        if getattr(self._local, "depth", 0) > 0:
            self._local.doc = resource
            return resource, False
        return resource, True
