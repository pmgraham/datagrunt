"""Tests for ``datagrunt.rationalization.cache``: the request key and the response stores (plan §4.4, R9, R10).

The key is the SHA-256 of the canonical JSON of the request's parts (the envelope's nonce-free components among
them), so it ignores the nonce, changes with every part, and is the same in every process. ``MemoryCache``
deduplicates within a run, bounded at 100,000 entries. ``FileCache`` keeps one JSON file per entry at
``<cache_dir>/<key[:2]>/<key>.json``, written atomically (0o700 directories, 0o600 files), read without following a
symlink or blocking on a FIFO, at most 8 MiB, and checked whole: a bad entry is a corrupt miss (never an error), a
tampered directory or misplaced ``cache_dir`` an error. It writes only under ``cache_dir``, and never on ``get``.
"""

import ast
import contextlib
import copy
import dataclasses
import errno
import fnmatch
import json
import os
import re
import signal
import stat
import subprocess
import sys
import textwrap
import threading
import time
import tracemalloc
from pathlib import Path
from typing import get_args

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from datagrunt.rationalization import RationalizationError
from datagrunt.rationalization import cache as cache_module
from datagrunt.rationalization._canonical import canonical_json, sha256_hex
from datagrunt.rationalization.cache import (
    CACHE_FORMAT,
    CORRUPT_REASONS,
    ENTRY_MAX_BYTES,
    MEMORY_MAX_ENTRIES,
    CacheLookup,
    CacheMeta,
    CorruptReason,
    FileCache,
    MemoryCache,
    request_key,
)
from datagrunt.rationalization.envelope import ENVELOPE_FORMAT, Envelope
from datagrunt.rationalization.wire import ProviderResponse

TIMEOUT_S = 120
POSIX = sys.platform != "win32"
needs_posix = pytest.mark.skipif(not POSIX, reason="POSIX permissions, symlinks and FIFOs")
MIB = 1024 * 1024

PAYLOAD = (
    '<document>doc</document>\n<scope></scope>\n<elements>\n<el id="p0001_e000" kind="paragraph" page="1">\n'
    "Body\n</el>\n</elements>"
)
ENVELOPE = Envelope(unit_id="doc:p0001_e000-p0001_e000", prompt="Extract parts.", context_json="null", payload=PAYLOAD)
KEY_ARGS = {
    "provider_kind": "anthropic",
    "model_id": "anthropic:claude-x",
    "generation_params": {"temperature": 0.0, "max_output_tokens": 8192, "seed": None},
    "envelope": ENVELOPE,
    "provider_schema": {"type": "object", "properties": {"records": {"type": "array"}}, "required": ["records"]},
    "schema_version": "1.0",
}
NONCES = ("0" * 32, "f" * 32)


def key_of(name: str) -> str:
    return sha256_hex(f"key:{name}")


def make_response(text: str = "body", **changes) -> ProviderResponse:
    fields = {
        "content": {"records": [{"record": {"code": text}, "grounding_quotes": [text], "source_element_ids": []}]},
        "raw_text": f'{{"records": [{text!r}]}}',
        "finish": "complete",
        "input_tokens": 10,
        "output_tokens": 5,
    }
    return ProviderResponse(**{**fields, **changes})


META_FIELDS = {
    "model_id": "anthropic:claude-x",
    "unit_id": "doc:p0001_e000-p0001_e000",
    "doc_id": "doc",
    "schema_name": "Order",
    "schema_version": "1.0",
    "schema_sha256": sha256_hex("schema"),
    "prompt_sha256": sha256_hex("prompt"),
    "latency_ms": 12.5,
    "created_at": "2026-11-04T10:00:00.123456+00:00",
}


def make_meta(**changes) -> CacheMeta:
    return CacheMeta(**{**META_FIELDS, **changes})


def entry_path(root: Path, key: str) -> Path:
    return root / key[:2] / f"{key}.json"


def entry_text(entry_key: str, stored: ProviderResponse | None = None, **changes) -> str:
    """The text ``FileCache.put`` writes for ``entry_key`` (``changes`` replace top-level fields)."""
    entry = {
        "format": CACHE_FORMAT,
        "key": entry_key,
        "response": (stored or make_response()).to_json(),
        "meta": make_meta().to_json(),
    }
    return canonical_json({**entry, **changes})


def plant(root: Path, key: str) -> Path:
    """The entry path of ``key``, its sub-directory made (0o700), nothing at the path itself."""
    path = entry_path(root, key)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    return path


def tree(root: Path) -> list[tuple[str, int, int, int]]:
    """Every path under ``root`` (``root`` too) with its lstat mode, size and mtime."""
    entries = []
    for path in [root, *sorted(root.rglob("*"))]:
        status = path.lstat()
        entries.append((str(path.relative_to(root)), status.st_mode, status.st_size, status.st_mtime_ns))
    return entries


def temp_files(root: Path) -> list[str]:
    """Every hidden file under ``root``: the cache's names are ``<xx>`` and ``<key>.json``, a temp file ``.*.tmp``."""
    return sorted(str(path) for path in root.rglob(".*"))


def mode(path: Path) -> int:
    return stat.S_IMODE(path.lstat().st_mode)


@contextlib.contextmanager
def deadline(seconds: float):
    """Raise ``TimeoutError`` in the block after ``seconds`` (POSIX ``SIGALRM``), so a blocking call fails."""

    def expire(signum, frame):
        raise TimeoutError(f"blocked for more than {seconds} s")

    previous = signal.signal(signal.SIGALRM, expire)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


@pytest.fixture
def root(tmp_path) -> Path:
    return tmp_path / "cache"


@pytest.fixture(params=["dir_fd", "path"])
def cache(root, request, monkeypatch) -> FileCache:
    """A cache under ``root``, reading and writing through the checked sub-directory's descriptor (POSIX), then
    by path (the fallback where the OS has no ``dir_fd``, as on Windows)."""
    if request.param == "path":
        monkeypatch.setattr(cache_module, "_DIR_FD", False)
    elif not cache_module._DIR_FD:
        pytest.skip("this OS has no dir_fd")
    return FileCache(root)


def bound(cache: FileCache) -> bool:
    """Whether this run of a ``cache`` test reads and writes through descriptors (``_DIR_FD``)."""
    return cache_module._DIR_FD


# ---------------------------------------------------------------------------------------------------------------
# request_key
# ---------------------------------------------------------------------------------------------------------------


class TestRequestKey:
    def test_the_sha256_of_the_canonical_request(self):
        expected = sha256_hex(
            canonical_json(
                {
                    "format": CACHE_FORMAT,
                    "provider_kind": KEY_ARGS["provider_kind"],
                    "model_id": KEY_ARGS["model_id"],
                    "generation_params": KEY_ARGS["generation_params"],
                    "envelope": ENVELOPE.key_material(),
                    "provider_schema": KEY_ARGS["provider_schema"],
                    "schema_version": KEY_ARGS["schema_version"],
                }
            )
        )

        assert CACHE_FORMAT == "datagrunt.rationalize.cache/1"
        assert request_key(**KEY_ARGS) == expected
        assert len(expected) == 64 and set(expected) <= set("0123456789abcdef")

    def test_ignores_the_nonce(self):
        before = request_key(**KEY_ARGS)
        first = ENVELOPE.render(NONCES[0])
        second = ENVELOPE.render(NONCES[1])
        rebuilt = Envelope(
            unit_id=ENVELOPE.unit_id, prompt=ENVELOPE.prompt, context_json=ENVELOPE.context_json, payload=PAYLOAD
        )

        assert first != second
        assert request_key(**KEY_ARGS) == before
        assert request_key(**{**KEY_ARGS, "envelope": rebuilt}) == before
        assert all(nonce not in canonical_json(ENVELOPE.key_material()) for nonce in NONCES)

    def test_changes_with_each_argument(self):
        variants = {
            "provider_kind": "openai-compatible",
            "model_id": "anthropic:claude-y",
            "generation_params": {**KEY_ARGS["generation_params"], "temperature": 0.5},
            "provider_schema": {**KEY_ARGS["provider_schema"], "required": []},
            "schema_version": "1.1",
            "envelope": dataclasses.replace(ENVELOPE, prompt="Extract other parts."),
        }
        keys = [request_key(**KEY_ARGS)] + [
            request_key(**{**KEY_ARGS, name: value}) for name, value in variants.items()
        ]

        assert len(set(keys)) == len(keys) == 7

    @pytest.mark.parametrize(
        "params",
        [
            {"temperature": 0.0, "max_output_tokens": 8192, "seed": 0},
            {"temperature": 0.0, "max_output_tokens": 4096, "seed": None},
            {"temperature": None, "max_output_tokens": 8192, "seed": None},
            {"temperature": 0.0, "max_output_tokens": 8192},
        ],
        ids=["seed", "max-output-tokens", "temperature-none", "seed-absent"],
    )
    def test_changes_with_each_generation_parameter(self, params):
        assert request_key(**{**KEY_ARGS, "generation_params": params}) != request_key(**KEY_ARGS)

    @pytest.mark.parametrize(
        ("name", "value"),
        [
            ("envelope_format", "datagrunt.rationalize/2"),
            ("unit_id", "doc:p0001_e000-p0001_e001"),
            ("system_rules", "other rules"),
            ("directive", ("a", "b", "c", "d", "e")),
            ("user_layout", ("<a_", ">\n", "\n</a_", ">\n<b_", ">\n", "\n</b_", ">")),
            ("header_element_ids", ("p0001_e009",)),
            ("prompt", "Extract parts!"),
            ("context_json", '{"a":1}'),
            ("payload", PAYLOAD + "\n"),
        ],
    )
    def test_changes_with_each_envelope_component(self, name, value):
        changed = dataclasses.replace(ENVELOPE)
        object.__setattr__(changed, name, value)

        assert request_key(**{**KEY_ARGS, "envelope": changed}) != request_key(**KEY_ARGS)

    def test_every_key_material_field_is_covered(self):
        # The test above changes one Envelope field per key_material() entry: none is left out.
        assert set(ENVELOPE.key_material()) == {
            "format",
            "unit_id",
            "system_rules",
            "directive",
            "user_layout",
            "header_element_ids",
            "prompt",
            "context",
            "payload",
        }
        assert ENVELOPE.key_material()["format"] == ENVELOPE_FORMAT

    def test_changes_with_the_cache_format(self, monkeypatch):
        before = request_key(**KEY_ARGS)
        monkeypatch.setattr(cache_module, "CACHE_FORMAT", "datagrunt.rationalize.cache/2")

        assert request_key(**KEY_ARGS) != before

    def test_insertion_order_is_irrelevant(self):
        reordered = {
            **KEY_ARGS,
            "generation_params": dict(reversed(list(KEY_ARGS["generation_params"].items()))),
            "provider_schema": {
                "required": ["records"],
                "properties": {"records": {"type": "array"}},
                "type": "object",
            },
        }

        assert request_key(**reordered) == request_key(**KEY_ARGS)

    @pytest.mark.parametrize("seed", ["0", "1", "4242"])
    def test_stable_across_processes_and_hash_seeds(self, seed, tmp_path):
        # Not -I, so the interpreter honours PYTHONHASHSEED; run from an empty directory, so nothing local
        # shadows an import.
        code = textwrap.dedent(
            f"""
            from datagrunt.rationalization.cache import request_key
            from datagrunt.rationalization.envelope import Envelope

            envelope = Envelope(
                unit_id={ENVELOPE.unit_id!r}, prompt={ENVELOPE.prompt!r}, context_json="null", payload={PAYLOAD!r}
            )
            print(request_key(
                provider_kind={KEY_ARGS["provider_kind"]!r},
                model_id={KEY_ARGS["model_id"]!r},
                generation_params={{"seed": None, "max_output_tokens": 8192, "temperature": 0.0}},
                envelope=envelope,
                provider_schema={KEY_ARGS["provider_schema"]!r},
                schema_version={KEY_ARGS["schema_version"]!r},
            ))
            """
        )
        completed = subprocess.run(
            [sys.executable, "-c", code],
            cwd=tmp_path,
            env={**os.environ, "PYTHONHASHSEED": seed},
            capture_output=True,
            text=True,
            timeout=TIMEOUT_S,
            check=False,
        )

        assert completed.returncode == 0, completed.stderr
        assert completed.stdout.strip().splitlines()[-1] == request_key(**KEY_ARGS)

    @pytest.mark.parametrize("name", ["provider_kind", "model_id", "schema_version"])
    def test_each_text_argument_is_a_non_blank_str(self, name):
        with pytest.raises(TypeError, match=name):
            request_key(**{**KEY_ARGS, name: None})
        with pytest.raises(TypeError, match=name):
            request_key(**{**KEY_ARGS, name: b"x"})
        with pytest.raises(ValueError, match=name):
            request_key(**{**KEY_ARGS, name: "  "})
        with pytest.raises(ValueError, match=name):
            request_key(**{**KEY_ARGS, name: "a\ud800"})

    @pytest.mark.parametrize("name", ["generation_params", "provider_schema"])
    def test_each_json_argument_is_a_json_object(self, name):
        for value in (None, [1], "{}"):
            with pytest.raises(TypeError, match=name):
                request_key(**{**KEY_ARGS, name: value})
        with pytest.raises(RationalizationError, match=rf"^{name} is not canonical JSON"):
            request_key(**{**KEY_ARGS, name: {"temperature": float("nan")}})

    @pytest.mark.parametrize("name", ["generation_params", "provider_schema"])
    @pytest.mark.parametrize(
        ("value", "folds_into"),
        [
            ({1: "x"}, {"1": "x"}),
            ({"a": {2: "y"}}, {"a": {"2": "y"}}),
            ({"a": (1, 2)}, {"a": [1, 2]}),
            ({True: "z"}, {"true": "z"}),
            ({"a": [(1,)]}, {"a": [[1]]}),
        ],
        ids=["int-key", "nested-int-key", "tuple", "bool-key", "nested-tuple"],
    )
    def test_a_json_argument_must_be_exactly_json_so_keys_never_collide(self, name, value, folds_into):
        # Canonical JSON writes {1: "x"} as {"1": "x"} and a tuple as a list: accepting both would give two different
        # requests one key. The key fails closed instead.
        assert request_key(**{**KEY_ARGS, name: folds_into})
        with pytest.raises(RationalizationError, match=rf"^{name} is not exactly JSON"):
            request_key(**{**KEY_ARGS, name: value})

    def test_the_envelope_must_be_an_envelope(self):
        with pytest.raises(TypeError, match="envelope"):
            request_key(**{**KEY_ARGS, "envelope": ENVELOPE.key_material()})

    def test_arguments_are_keyword_only(self):
        with pytest.raises(TypeError):
            request_key(*KEY_ARGS.values())


# ---------------------------------------------------------------------------------------------------------------
# CacheLookup and CacheMeta
# ---------------------------------------------------------------------------------------------------------------


class TestCacheLookup:
    def test_hit_miss_and_corrupt(self):
        response = make_response()
        hit, miss, corrupt = (
            CacheLookup(response),
            CacheLookup(None),
            CacheLookup(None, corrupt=True, reason="not_json"),
        )

        assert (hit.response, hit.corrupt, hit.reason, hit.hit) == (response, False, None, True)
        assert (miss.response, miss.corrupt, miss.reason, miss.hit) == (None, False, None, False)
        assert (corrupt.response, corrupt.corrupt, corrupt.reason, corrupt.hit) == (None, True, "not_json", False)

    def test_frozen(self):
        with pytest.raises(dataclasses.FrozenInstanceError):
            CacheLookup(None).corrupt = True

    def test_the_reasons_are_a_closed_set_of_codes(self):
        assert CORRUPT_REASONS == get_args(CorruptReason)
        assert all(reason.replace("_", "").isalpha() and reason.islower() for reason in CORRUPT_REASONS)

    @pytest.mark.parametrize(
        ("args", "kwargs", "error"),
        [
            ((make_response(),), {"corrupt": True, "reason": "not_json"}, ValueError),
            ((None,), {"corrupt": True}, ValueError),
            ((None,), {"corrupt": True, "reason": "a document's text"}, ValueError),
            ((None,), {"reason": "not_json"}, ValueError),
            ((None,), {"corrupt": 1, "reason": "not_json"}, TypeError),
            (({"content": None},), {}, TypeError),
        ],
        ids=["corrupt-with-response", "corrupt-without-reason", "free-text-reason", "reason-on-miss", "int", "dict"],
    )
    def test_inconsistent_lookups_are_refused(self, args, kwargs, error):
        with pytest.raises(error):
            CacheLookup(*args, **kwargs)


class TestCacheMeta:
    def test_round_trip(self):
        meta = make_meta()

        assert list(meta.to_json()) == list(META_FIELDS)
        assert meta.to_json() == META_FIELDS
        assert CacheMeta.from_json(json.loads(canonical_json(meta.to_json()))) == meta

    def test_keyword_only_and_frozen(self):
        with pytest.raises(TypeError):
            CacheMeta(*META_FIELDS.values())
        with pytest.raises(dataclasses.FrozenInstanceError):
            make_meta().doc_id = "other"

    @pytest.mark.parametrize("name", ["model_id", "unit_id", "doc_id", "schema_name", "schema_version"])
    def test_each_text_is_a_non_blank_str(self, name):
        with pytest.raises(TypeError, match=name):
            make_meta(**{name: None})
        with pytest.raises(ValueError, match=name):
            make_meta(**{name: " "})
        with pytest.raises(ValueError, match=name):
            make_meta(**{name: "\ud800"})

    @pytest.mark.parametrize("name", ["schema_sha256", "prompt_sha256"])
    def test_each_hash_is_64_lowercase_hex(self, name):
        for value in ("A" * 64, "a" * 63, "a" * 65, "the prompt itself", "g" * 64):
            with pytest.raises(ValueError, match=name):
                make_meta(**{name: value})
        with pytest.raises(TypeError, match=name):
            make_meta(**{name: None})

    def test_latency(self):
        assert make_meta(latency_ms=0).latency_ms == 0.0 and type(make_meta(latency_ms=3).latency_ms) is float
        for value in (True, "1", None):
            with pytest.raises(TypeError, match="latency_ms"):
                make_meta(latency_ms=value)
        for value in (-0.5, float("nan"), float("inf"), 10**400, -(10**400), 10**4000):
            with pytest.raises(ValueError, match="latency_ms"):
                make_meta(latency_ms=value)

    def test_created_at_is_utc_iso_8601(self):
        assert make_meta(created_at="2026-11-04T10:00:00Z").created_at == "2026-11-04T10:00:00Z"
        for value in (
            "2026-11-04T10:00:00",
            "2026-11-04T10:00:00+01:00",
            "yesterday",
            "",
            "99999-01-01T00:00:00+00:00",
            "2026-11-04T10:00:00+99:00",
            "2026-11-04T23:59:60+00:00",
        ):
            with pytest.raises(ValueError, match="created_at"):
                make_meta(created_at=value)
        with pytest.raises(TypeError, match="created_at"):
            make_meta(created_at=1_700_000_000)

    def test_from_json_needs_exactly_the_fields(self):
        with pytest.raises(TypeError, match="CacheMeta"):
            CacheMeta.from_json([1])
        with pytest.raises(ValueError, match="prompt"):
            CacheMeta.from_json({**META_FIELDS, "prompt": "Extract parts."})
        with pytest.raises(ValueError, match="doc_id"):
            CacheMeta.from_json({name: value for name, value in META_FIELDS.items() if name != "doc_id"})


# ---------------------------------------------------------------------------------------------------------------
# MemoryCache
# ---------------------------------------------------------------------------------------------------------------


class TestMemoryCache:
    def test_get_after_put_round_trips(self):
        memory = MemoryCache()
        response = make_response()

        assert memory.get(key_of("a")) == CacheLookup(None)
        assert memory.put(key_of("a"), response, make_meta()) is True
        assert memory.get(key_of("a")) == CacheLookup(response)
        assert len(memory) == 1

    def test_each_get_is_a_fresh_copy(self):
        memory = MemoryCache()
        memory.put(key_of("a"), make_response(), make_meta())
        first = memory.get(key_of("a")).response
        first.content["records"].clear()

        assert memory.get(key_of("a")).response == make_response()
        assert memory.get(key_of("a")).response is not memory.get(key_of("a")).response

    def test_a_put_overwrites(self):
        memory = MemoryCache()
        memory.put(key_of("a"), make_response("one"), make_meta())
        memory.put(key_of("a"), make_response("two"), make_meta())

        assert memory.get(key_of("a")).response == make_response("two")
        assert len(memory) == 1

    def test_refuses_past_its_bound_but_still_overwrites(self):
        memory = MemoryCache(max_entries=3)
        for name in "abc":
            assert memory.put(key_of(name), make_response(name), make_meta()) is True

        assert memory.put(key_of("d"), make_response("d"), make_meta()) is False
        assert memory.get(key_of("d")) == CacheLookup(None)
        assert memory.put(key_of("a"), make_response("again"), make_meta()) is True
        assert memory.get(key_of("a")).response == make_response("again")
        assert len(memory) == 3

    def test_the_default_bound_is_100_000(self):
        memory = MemoryCache()
        response, meta = make_response(), make_meta()

        assert MEMORY_MAX_ENTRIES == 100_000 and memory.max_entries == MEMORY_MAX_ENTRIES
        assert all(memory.put(f"{index:064x}", response, meta) for index in range(MEMORY_MAX_ENTRIES))
        assert memory.put(key_of("one more"), response, meta) is False
        assert len(memory) == MEMORY_MAX_ENTRIES

    @pytest.mark.parametrize(
        ("value", "error"),
        [(0, ValueError), (MEMORY_MAX_ENTRIES + 1, ValueError), (True, TypeError), (10.0, TypeError)],
    )
    def test_the_bound_is_checked(self, value, error):
        with pytest.raises(error, match="max_entries"):
            MemoryCache(max_entries=value)

    def test_arguments_are_checked(self):
        memory = MemoryCache()

        with pytest.raises(ValueError, match="key"):
            memory.get("A" * 64)
        with pytest.raises(TypeError, match="key"):
            memory.get(None)
        with pytest.raises(TypeError, match="response"):
            memory.put(key_of("a"), make_response().to_json(), make_meta())
        with pytest.raises(TypeError, match="meta"):
            memory.put(key_of("a"), make_response(), META_FIELDS)
        assert len(memory) == 0

    def test_the_bound_check_and_the_insert_are_one_step(self):
        # Two puts of new keys into the last free place: without the lock both could pass the check, then insert.
        class SlowDict(dict):
            def __len__(self):
                size = super().__len__()
                time.sleep(0.05)  # widens the window between the bound check and the insert
                return size

        memory = MemoryCache(max_entries=1)
        memory._entries = SlowDict()
        barrier = threading.Barrier(2, timeout=TIMEOUT_S)
        results: list[bool] = []

        def put(name: str) -> None:
            barrier.wait()
            results.append(memory.put(key_of(name), make_response(name), make_meta()))

        workers = [threading.Thread(target=put, args=(name,)) for name in "ab"]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(TIMEOUT_S)

        assert sorted(results) == [False, True]
        assert len(memory) == 1

    def test_sixteen_threads(self):
        memory = MemoryCache()
        errors = run_threads(memory, threads=16, keys=50)

        assert errors == []
        check_shared_and_own(memory, threads=16, keys=50)


def thread_keys(index: int, keys: int) -> list[str]:
    """Half the keys every writer shares, half its own."""
    return [key_of(f"shared-{i}") for i in range(keys // 2)] + [key_of(f"own-{index}-{i}") for i in range(keys // 2)]


def run_threads(store, *, threads: int, keys: int) -> list[str]:
    """``threads`` writers started together, each putting its keys and reading a shared one after each put."""
    barrier = threading.Barrier(threads, timeout=TIMEOUT_S)
    errors: list[str] = []

    def write(index: int) -> None:
        try:
            barrier.wait()
            for key in thread_keys(index, keys):
                response = make_response(f"{key}:{index}", content={"key": key, "writer": index})
                assert store.put(key, response, make_meta())
                lookup = store.get(key_of("shared-0"))
                assert not lookup.corrupt, lookup.reason
        except Exception as error:  # reported, then asserted empty by the caller
            errors.append(f"{index}: {error!r}")

    workers = [threading.Thread(target=write, args=(index,)) for index in range(threads)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(TIMEOUT_S)
    assert not any(worker.is_alive() for worker in workers), "a writer did not finish"
    return errors


def check_shared_and_own(store, *, threads: int, keys: int) -> None:
    """Every key holds a whole response of a writer that wrote it."""
    for index in range(threads):
        for position, key in enumerate(thread_keys(index, keys)):
            lookup = store.get(key)
            assert lookup.hit, (key, lookup)
            assert lookup.response.content["key"] == key
            writer = lookup.response.content["writer"]
            assert writer == index if position >= keys // 2 else 0 <= writer < threads


# ---------------------------------------------------------------------------------------------------------------
# FileCache: cache_dir and layout
# ---------------------------------------------------------------------------------------------------------------


class TestFileCacheDirectory:
    def test_creates_a_missing_cache_dir_with_its_parents(self, tmp_path):
        root = tmp_path / "a" / "b" / "cache"
        built = FileCache(root)

        assert root.is_dir() and built.cache_dir == root
        if POSIX:
            assert mode(root) == 0o700

    def test_a_relative_cache_dir_is_made_absolute(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        built = FileCache("relative")
        monkeypatch.chdir(tmp_path.parent)

        assert built.cache_dir == tmp_path / "relative"
        assert built.put(key_of("a"), make_response(), make_meta())
        assert entry_path(tmp_path / "relative", key_of("a")).is_file()

    @needs_posix
    def test_an_existing_cache_dir_is_never_repermissioned(self, root):
        root.mkdir()
        root.chmod(0o755)
        built = FileCache(root)
        built.put(key_of("a"), make_response(), make_meta())

        assert mode(root) == 0o755
        assert mode(entry_path(root, key_of("a")).parent) == 0o700

    @needs_posix
    def test_a_directory_made_meanwhile_is_left_as_it_is(self, root):
        # The race: another thread or process makes the directory between the check and this mkdir.
        root.mkdir()
        root.chmod(0o755)
        cache_module._make_directory(root, parents=False)

        assert mode(root) == 0o755

    def test_a_cache_dir_that_is_a_file_raises(self, root):
        root.write_text("x", encoding="utf-8")

        with pytest.raises(RationalizationError, match="not a directory"):
            FileCache(root)

    def test_a_cache_dir_under_a_file_raises(self, tmp_path):
        (tmp_path / "file").write_text("x", encoding="utf-8")

        with pytest.raises(RationalizationError, match="cache_dir") as error:
            FileCache(tmp_path / "file" / "cache")

        assert isinstance(error.value.__cause__, OSError)

    @needs_posix
    def test_a_symlink_to_a_directory_is_a_cache_dir(self, tmp_path):
        (tmp_path / "real").mkdir()
        (tmp_path / "link").symlink_to(tmp_path / "real")
        built = FileCache(tmp_path / "link")

        assert built.put(key_of("a"), make_response(), make_meta())
        assert entry_path(tmp_path / "real", key_of("a")).is_file()

    @needs_posix
    def test_a_dangling_symlink_raises(self, tmp_path):
        (tmp_path / "link").symlink_to(tmp_path / "missing")

        with pytest.raises(RationalizationError, match="not a directory"):
            FileCache(tmp_path / "link")

    @pytest.mark.parametrize(
        ("value", "error"),
        [(None, TypeError), (3, TypeError), (b"x", TypeError), ("", ValueError), ("a\x00b", ValueError)],
    )
    def test_the_argument_is_checked(self, value, error):
        with pytest.raises(error, match="cache_dir"):
            FileCache(value)

    def test_the_layout(self, cache, root):
        key = key_of("a")
        response, meta = make_response(), make_meta()

        assert cache.put(key, response, meta) is True
        assert sorted(str(path.relative_to(root)) for path in root.rglob("*")) == [key[:2], f"{key[:2]}/{key}.json"]
        assert json.loads(entry_path(root, key).read_text(encoding="utf-8")) == {
            "format": CACHE_FORMAT,
            "key": key,
            "response": response.to_json(),
            "meta": meta.to_json(),
        }
        assert entry_path(root, key).read_text(encoding="utf-8") == entry_text(key, response)

    def test_get_after_put_round_trips(self, cache):
        response = make_response(content={"x": [0.1, -0.0, 10**30, "é‮"]}, finish="truncated", input_tokens=None)
        cache.put(key_of("a"), response, make_meta())

        assert cache.get(key_of("a")) == CacheLookup(response)
        assert FileCache(cache.cache_dir).get(key_of("a")) == CacheLookup(response)

    def test_a_put_overwrites(self, cache):
        cache.put(key_of("a"), make_response("one"), make_meta())
        cache.put(key_of("a"), make_response("two"), make_meta())

        assert cache.get(key_of("a")).response == make_response("two")

    def test_a_missing_entry_is_a_miss_and_get_creates_nothing(self, cache, root):
        cache.put(key_of("a"), make_response(), make_meta())
        sibling = next(key_of(f"b{index}") for index in range(1000) if key_of(f"b{index}")[:2] == key_of("a")[:2])
        before = tree(root)

        assert cache.get(key_of("missing")) == CacheLookup(None)
        assert cache.get(sibling) == CacheLookup(None)
        assert tree(root) == before

    def test_keys_are_checked(self, cache, root):
        for key in ("A" * 64, "a" * 63, "../" + "a" * 61, "a" * 65, "g" * 64, "a" * 63 + "\n"):
            with pytest.raises(ValueError, match="key"):
                cache.get(key)
            with pytest.raises(ValueError, match="key"):
                cache.put(key, make_response(), make_meta())
        with pytest.raises(TypeError, match="key"):
            cache.get(None)
        assert list(root.iterdir()) == []

    def test_put_checks_its_values(self, cache, root):
        with pytest.raises(TypeError, match="response"):
            cache.put(key_of("a"), None, make_meta())
        with pytest.raises(TypeError, match="meta"):
            cache.put(key_of("a"), make_response(), None)
        assert list(root.iterdir()) == []


# ---------------------------------------------------------------------------------------------------------------
# FileCache: bounds
# ---------------------------------------------------------------------------------------------------------------


def sized_response(key: str, size: int) -> ProviderResponse:
    """A response whose entry for ``key`` is exactly ``size`` bytes (ASCII padding in ``raw_text``)."""
    base = len(entry_text(key, make_response(raw_text="")).encode("utf-8"))
    return make_response(raw_text="x" * (size - base))


class TestFileCacheBounds:
    def test_an_entry_of_exactly_8_mib_is_written_and_read(self, cache, root):
        key = key_of("big")
        response = sized_response(key, ENTRY_MAX_BYTES)

        assert ENTRY_MAX_BYTES == 8 * MIB
        assert cache.put(key, response, make_meta()) is True
        assert entry_path(root, key).stat().st_size == ENTRY_MAX_BYTES
        assert cache.get(key) == CacheLookup(response)

    def test_an_entry_past_8_mib_is_not_written(self, cache, root):
        key = key_of("big")

        assert cache.put(key, sized_response(key, ENTRY_MAX_BYTES + 1), make_meta()) is False
        assert list(root.iterdir()) == []

    def test_a_file_past_8_mib_is_corrupt(self, cache, root):
        key = key_of("big")
        entry_path(root, key).parent.mkdir()
        entry_path(root, key).write_bytes(b" " * (ENTRY_MAX_BYTES + 1))

        assert cache.get(key) == CacheLookup(None, corrupt=True, reason="too_large")

    def test_a_huge_file_is_read_no_further_than_the_bound(self, cache, root):
        key = key_of("huge")
        entry_path(root, key).parent.mkdir()
        with entry_path(root, key).open("wb") as stream:
            stream.truncate(16 * ENTRY_MAX_BYTES)  # sparse: cheap to make, 128 MiB to read whole
        tracemalloc.start()
        try:
            lookup = cache.get(key)
            peak = tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()

        assert lookup == CacheLookup(None, corrupt=True, reason="too_large")
        assert peak < 3 * ENTRY_MAX_BYTES


# ---------------------------------------------------------------------------------------------------------------
# FileCache: corrupt entries
# ---------------------------------------------------------------------------------------------------------------


TEXT_CORRUPTIONS = {
    "truncated": (lambda key: entry_text(key)[:-7].encode(), "not_json"),
    "empty": (lambda key: b"", "not_json"),
    "not-utf8": (lambda key: b'{"format": "\xff"}', "not_json"),
    "utf16": (lambda key: entry_text(key).encode("utf-16"), "not_json"),
    "nan": (lambda key: entry_text(key).replace('"input_tokens":10', '"input_tokens":NaN').encode(), "not_json"),
    "duplicate-field": (lambda key: entry_text(key)[:-1].encode() + f',"key":"{key}"}}'.encode(), "not_json"),
    "too-deep": (lambda key: b"[" * 200_000 + b"]" * 200_000, "not_json"),
    "array": (lambda key: f"[{entry_text(key)}]".encode(), "not_an_entry"),
    "missing-field": (
        lambda key: canonical_json(
            {"format": CACHE_FORMAT, "key": key, "response": make_response().to_json()}
        ).encode(),
        "not_an_entry",
    ),
    "extra-field": (lambda key: entry_text(key)[:-1].encode() + b',"zz":1}', "not_an_entry"),
    "wrong-format": (lambda key: entry_text(key, format="datagrunt.rationalize.cache/0").encode(), "wrong_format"),
    "no-format": (
        lambda key: canonical_json({"key": key, "response": {}, "meta": {}}).encode(),
        "wrong_format",
    ),
    "wrong-key": (lambda key: entry_text(key_of("other")).encode(), "wrong_key"),
    "key-not-str": (lambda key: entry_text(key, key=7).encode(), "wrong_key"),
    "invalid-meta": (lambda key: entry_text(key, meta={**META_FIELDS, "prompt_sha256": "x"}).encode(), "invalid_meta"),
    "invalid-response": (
        lambda key: entry_text(key, response={**make_response().to_json(), "finish": "length"}).encode(),
        "invalid_response",
    ),
    "response-not-object": (lambda key: entry_text(key, response=[1]).encode(), "invalid_response"),
    "response-bad-content": (
        lambda key: entry_text(key, response={**make_response().to_json(), "content": [1]}).encode(),
        "invalid_response",
    ),
}


class TestFileCacheCorruptEntries:
    @pytest.mark.parametrize("name", list(TEXT_CORRUPTIONS))
    def test_a_bad_file_is_a_corrupt_miss_then_put_overwrites_it(self, name, cache, root):
        key = key_of("a")
        make_data, reason = TEXT_CORRUPTIONS[name]
        plant(root, key).write_bytes(make_data(key))

        assert cache.get(key) == CacheLookup(None, corrupt=True, reason=reason)
        assert cache.put(key, make_response(), make_meta()) is True
        assert cache.get(key) == CacheLookup(make_response())

    def test_every_reason_is_reachable(self):
        planted = {"symlink", "not_regular_file", "unreadable", "too_large"}

        assert {reason for _, reason in TEXT_CORRUPTIONS.values()} | planted == set(CORRUPT_REASONS)

    @needs_posix
    def test_a_symlink_to_a_valid_entry_of_the_same_key_is_corrupt(self, cache, root, tmp_path):
        key = key_of("a")
        outside = tmp_path / "outside.json"
        outside.write_text(entry_text(key), encoding="utf-8")
        plant(root, key).symlink_to(outside)

        assert cache.get(key) == CacheLookup(None, corrupt=True, reason="symlink")
        assert cache.put(key, make_response("new"), make_meta()) is True
        assert cache.get(key) == CacheLookup(make_response("new"))
        assert not entry_path(root, key).is_symlink()
        assert outside.read_text(encoding="utf-8") == entry_text(key)

    @needs_posix
    def test_a_symlink_to_another_valid_entry_is_corrupt(self, cache, root):
        cache.put(key_of("b"), make_response("b"), make_meta())
        plant(root, key_of("a")).symlink_to(entry_path(root, key_of("b")))

        assert cache.get(key_of("a")) == CacheLookup(None, corrupt=True, reason="symlink")

    @needs_posix
    def test_a_dangling_symlink_is_corrupt(self, cache, root, tmp_path):
        plant(root, key_of("a")).symlink_to(tmp_path / "nowhere.json")

        assert cache.get(key_of("a")) == CacheLookup(None, corrupt=True, reason="symlink")

    @needs_posix
    def test_without_o_nofollow_a_symlink_is_still_refused(self, cache, root, tmp_path, monkeypatch):
        # Where the OS has no O_NOFOLLOW (Windows, which has no dir_fd either), the entry path is checked with lstat
        # before it is opened.
        key = key_of("a")
        outside = tmp_path / "outside.json"
        outside.write_text(entry_text(key), encoding="utf-8")
        plant(root, key).symlink_to(outside)
        monkeypatch.setattr(cache_module, "_O_NOFOLLOW", 0)
        monkeypatch.setattr(cache_module, "_DIR_FD", False)

        assert cache.get(key) == CacheLookup(None, corrupt=True, reason="symlink")

    @pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="POSIX FIFOs")
    def test_a_fifo_is_corrupt_and_never_blocks(self, cache, root):
        key = key_of("a")
        os.mkfifo(plant(root, key))

        with deadline(5.0):
            lookup = cache.get(key)

        assert lookup == CacheLookup(None, corrupt=True, reason="not_regular_file")
        assert cache.put(key, make_response(), make_meta()) is True
        assert cache.get(key) == CacheLookup(make_response())

    def test_a_directory_at_the_entry_path_is_corrupt_and_put_raises(self, cache, root):
        key = key_of("a")
        plant(root, key).mkdir()
        (entry_path(root, key) / "inside").write_text("kept", encoding="utf-8")

        assert cache.get(key) == CacheLookup(None, corrupt=True, reason="not_regular_file")
        with pytest.raises(RationalizationError, match="cannot write"):
            cache.put(key, make_response(), make_meta())
        assert (entry_path(root, key) / "inside").read_text(encoding="utf-8") == "kept"
        assert temp_files(root) == []

    @needs_posix
    @pytest.mark.skipif(POSIX and os.geteuid() == 0, reason="root reads any file")
    def test_an_unreadable_file_is_corrupt(self, cache, root):
        key = key_of("a")
        path = plant(root, key)
        path.write_text(entry_text(key), encoding="utf-8")
        path.chmod(0)

        assert cache.get(key) == CacheLookup(None, corrupt=True, reason="unreadable")
        assert cache.put(key, make_response("new"), make_meta()) is True
        assert cache.get(key) == CacheLookup(make_response("new"))

    def test_a_read_error_after_open_is_corrupt(self, cache, root, monkeypatch):
        key = key_of("a")
        cache.put(key, make_response(), make_meta())

        def failing_read(descriptor):
            raise OSError(errno.EIO, "I/O error")

        monkeypatch.setattr(cache_module, "_read_descriptor", failing_read)

        assert cache.get(key) == CacheLookup(None, corrupt=True, reason="unreadable")

    def test_get_closes_what_it_opens(self, cache, root, monkeypatch):
        key = key_of("a")
        cache.put(key, make_response(), make_meta())
        plant(root, key_of("dir")).mkdir()
        opened: list[int] = []
        closed: list[int] = []
        real_open, real_close = os.open, os.close

        def tracking_open(*args, **kwargs):
            descriptor = real_open(*args, **kwargs)
            opened.append(descriptor)
            return descriptor

        def tracking_close(descriptor):
            closed.append(descriptor)
            real_close(descriptor)

        monkeypatch.setattr(cache_module.os, "open", tracking_open)
        monkeypatch.setattr(cache_module.os, "close", tracking_close)
        cache.get(key)
        cache.get(key_of("dir"))

        assert len(opened) == (4 if bound(cache) else 2)  # with dir_fd, each get opens the sub-directory too
        assert sorted(opened) == sorted(closed)


class TestFileCacheTamperedDirectories:
    @needs_posix
    def test_a_sub_directory_replaced_by_a_symlink_raises(self, cache, root, tmp_path):
        key = key_of("a")
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        (elsewhere / f"{key}.json").write_text(entry_text(key), encoding="utf-8")
        root.joinpath(key[:2]).symlink_to(elsewhere, target_is_directory=True)

        with pytest.raises(RationalizationError, match="is a symlink: the cache was tampered with"):
            cache.get(key)
        with pytest.raises(RationalizationError, match="is a symlink: the cache was tampered with"):
            cache.put(key, make_response("new"), make_meta())
        assert sorted(path.name for path in elsewhere.iterdir()) == [f"{key}.json"]
        assert (elsewhere / f"{key}.json").read_text(encoding="utf-8") == entry_text(key)

    @needs_posix
    def test_a_symlink_planted_as_the_sub_directory_is_made_still_raises(self, cache, root, tmp_path, monkeypatch):
        # The race: the sub-directory is missing at the check, and a symlink appears there before the mkdir.
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()

        def plant_symlink(path, *, parents):
            path.symlink_to(elsewhere, target_is_directory=True)

        monkeypatch.setattr(cache_module, "_make_directory", plant_symlink)

        with pytest.raises(RationalizationError, match="symlink"):
            cache.put(key_of("a"), make_response(), make_meta())
        assert list(elsewhere.iterdir()) == []

    def test_a_sub_directory_that_is_a_file_raises(self, cache, root):
        key = key_of("a")
        root.joinpath(key[:2]).write_text("x", encoding="utf-8")

        with pytest.raises(RationalizationError, match="not a directory"):
            cache.get(key)
        with pytest.raises(RationalizationError, match="not a directory"):
            cache.put(key, make_response(), make_meta())

    def test_a_cache_dir_removed_after_construction_is_a_miss_then_put_raises(self, cache, root):
        root.rmdir()

        assert cache.get(key_of("a")) == CacheLookup(None)
        with pytest.raises(RationalizationError, match="cannot write") as error:
            cache.put(key_of("a"), make_response(), make_meta())
        assert isinstance(error.value.__cause__, OSError)


# ---------------------------------------------------------------------------------------------------------------
# FileCache: atomic writes
# ---------------------------------------------------------------------------------------------------------------


class TestFileCacheWrites:
    def test_the_file_and_its_directory_are_fsynced(self, cache, root, monkeypatch):
        synced: list[int] = []
        real_fsync = os.fsync

        def tracking_fsync(descriptor):
            synced.append(os.fstat(descriptor).st_mode)
            real_fsync(descriptor)

        monkeypatch.setattr(cache_module.os, "fsync", tracking_fsync)
        cache.put(key_of("a"), make_response(), make_meta())

        assert [stat.S_ISREG(found) for found in synced][:1] == [True]
        if POSIX:
            assert [stat.S_ISDIR(found) for found in synced] == [False, True]

    @pytest.mark.parametrize("failing", ["replace", "fsync", "write"])
    def test_a_failed_write_leaves_no_temp_file_and_raises(self, failing, cache, root, monkeypatch):
        key = key_of("a")
        cache.put(key, make_response("old"), make_meta())

        def fail(*args, **kwargs):
            raise OSError(errno.ENOSPC, "No space left on device")

        descriptors: list[int] = []
        real_open = os.open

        def tracking_open(path, flags, *args, **kwargs):
            descriptor = real_open(path, flags, *args, **kwargs)
            if flags & os.O_CREAT:  # the temp file, made by os.open at the directory's descriptor or by mkstemp
                descriptors.append(descriptor)
            return descriptor

        monkeypatch.setattr(cache_module.os, "open", tracking_open)
        if failing == "write":  # the module's own name for the file object it writes through
            monkeypatch.setattr(cache_module, "open", fail, raising=False)
        else:
            monkeypatch.setattr(cache_module.os, failing, fail)

        with pytest.raises(RationalizationError, match="cannot write") as error:
            cache.put(key, make_response("new"), make_meta())
        closed = []
        for descriptor in descriptors:
            with pytest.raises(OSError) as fstat_error:
                os.fstat(descriptor)
            closed.append(fstat_error.value.errno == errno.EBADF)

        monkeypatch.undo()
        assert closed == [True]
        assert isinstance(error.value.__cause__, OSError)
        assert temp_files(root) == []
        assert cache.get(key) == CacheLookup(make_response("old"))

    def test_an_interrupt_mid_write_leaves_no_temp_file_and_propagates(self, cache, root, monkeypatch):
        def interrupt(*args, **kwargs):
            raise KeyboardInterrupt

        monkeypatch.setattr(cache_module.os, "replace", interrupt)

        with pytest.raises(KeyboardInterrupt):
            cache.put(key_of("a"), make_response(), make_meta())
        assert temp_files(root) == []
        assert not entry_path(root, key_of("a")).exists()

    @needs_posix
    @pytest.mark.parametrize(("code", "written"), [(errno.EINVAL, True), (errno.EIO, False)])
    def test_a_directory_fsync_the_filesystem_refuses_is_skipped(self, code, written, cache, root, monkeypatch):
        real_fsync = os.fsync

        def fsync(descriptor):
            if stat.S_ISDIR(os.fstat(descriptor).st_mode):
                raise OSError(code, os.strerror(code))
            real_fsync(descriptor)

        monkeypatch.setattr(cache_module.os, "fsync", fsync)

        if written:
            assert cache.put(key_of("a"), make_response(), make_meta()) is True
        else:
            with pytest.raises(RationalizationError, match="cannot write"):
                cache.put(key_of("a"), make_response(), make_meta())
        monkeypatch.undo()
        assert cache.get(key_of("a")) == CacheLookup(make_response())
        assert temp_files(root) == []

    def test_the_temp_file_is_a_hidden_tmp_beside_the_entry_that_no_json_glob_matches(self, cache, root, monkeypatch):
        replaced: list[tuple[str, str, tuple[int, int]]] = []
        real_replace = os.replace

        def tracking_replace(source, target, **kwargs):
            if "src_dir_fd" in kwargs:
                places = (os.fstat(kwargs["src_dir_fd"]).st_ino, os.fstat(kwargs["dst_dir_fd"]).st_ino)
            else:
                places = (os.lstat(Path(source).parent).st_ino, os.lstat(Path(target).parent).st_ino)
            replaced.append((Path(source).name, Path(target).name, places))
            real_replace(source, target, **kwargs)

        monkeypatch.setattr(cache_module.os, "replace", tracking_replace)
        cache.put(key_of("a"), make_response(), make_meta())
        monkeypatch.undo()
        ((source, target, places),) = replaced
        directory = entry_path(root, key_of("a")).parent.lstat().st_ino

        assert target == f"{key_of('a')}.json" and places == (directory, directory)
        assert source.startswith(".") and source.endswith(".tmp")
        assert not fnmatch.fnmatch(source, "*.json")  # DuckDB's read_json('<cache_dir>/*/*.json') never sees one

    @needs_posix
    def test_put_and_get_close_what_they_open(self, cache, root):
        cache.put(key_of("warm"), make_response(), make_meta())
        cache.get(key_of("warm"))
        before = open_descriptors()
        for name in "abc":
            cache.put(key_of(name), make_response(name), make_meta())
            cache.get(key_of(name))

        assert open_descriptors() == before

    def test_no_temp_files_remain(self, cache, root):
        for name in "abcdef":
            cache.put(key_of(name), make_response(name), make_meta())

        assert temp_files(root) == []
        assert len(list(root.rglob("*.json"))) == 6

    @needs_posix
    def test_new_directories_are_0700_and_entries_0600(self, cache, root):
        cache.put(key_of("a"), make_response(), make_meta())

        assert mode(root) == 0o700
        assert mode(entry_path(root, key_of("a")).parent) == 0o700
        assert mode(entry_path(root, key_of("a"))) == 0o600

    @needs_posix
    @pytest.mark.parametrize("umask", [0o000, 0o777])
    def test_permissions_do_not_depend_on_the_umask(self, umask, tmp_path):
        code = textwrap.dedent(
            f"""
            import json, os, stat, sys
            os.umask({umask})
            from datagrunt.rationalization._canonical import sha256_hex
            from datagrunt.rationalization.cache import CacheMeta, FileCache
            from datagrunt.rationalization.wire import ProviderResponse

            root = sys.argv[1]
            key = sha256_hex("a")
            meta = CacheMeta(**{META_FIELDS!r})
            response = ProviderResponse(
                content=None, raw_text="", finish="refused", input_tokens=None, output_tokens=None
            )
            cache = FileCache(root)
            assert cache.put(key, response, meta)
            os.umask(0o022)
            paths = [root, os.path.join(root, key[:2]), os.path.join(root, key[:2], key + ".json")]
            print(json.dumps([stat.S_IMODE(os.lstat(path).st_mode) for path in paths]))
            """
        )
        completed = subprocess.run(
            [sys.executable, "-I", "-B", "-c", code, str(tmp_path / "cache")],
            capture_output=True,
            text=True,
            timeout=TIMEOUT_S,
            check=False,
        )

        assert completed.returncode == 0, completed.stderr
        assert json.loads(completed.stdout.strip().splitlines()[-1]) == [0o700, 0o700, 0o600]


# ---------------------------------------------------------------------------------------------------------------
# FileCache: an entry's content never makes get raise
# ---------------------------------------------------------------------------------------------------------------


def entry_object(entry_key: str) -> dict:
    """The JSON object ``FileCache.put`` writes for ``entry_key``, to change and write back."""
    return json.loads(entry_text(entry_key))


def write_entry(root: Path, entry_key: str, value: object) -> None:
    """Write ``value`` as ``entry_key``'s entry, ASCII-escaped (so lone surrogates and NaN can be in the file)."""
    plant(root, entry_key).write_text(json.dumps(value, ensure_ascii=True), encoding="ascii")


CRAFTED = {
    "latency-huge-int": (("meta", "latency_ms"), 10**400, "invalid_meta"),
    "latency-huge-negative-int": (("meta", "latency_ms"), -(10**400), "invalid_meta"),
    "latency-4000-digits": (("meta", "latency_ms"), 10**4000, "invalid_meta"),
    "latency-float-overflow": (("meta", "latency_ms"), float("inf"), "not_json"),
    "created-at-year-99999": (("meta", "created_at"), "99999-01-01T00:00:00+00:00", "invalid_meta"),
    "created-at-offset-99h": (("meta", "created_at"), "2026-11-04T10:00:00+99:00", "invalid_meta"),
    "created-at-not-str": (("meta", "created_at"), 10**400, "invalid_meta"),
    "tokens-huge-int": (("response", "input_tokens"), 10**400, "invalid_response"),
    "tokens-past-int64": (("response", "output_tokens"), 2**63, "invalid_response"),
    "tokens-float": (("response", "output_tokens"), 1e308, "invalid_response"),
    "raw-text-surrogate": (("response", "raw_text"), "\ud800", "invalid_response"),
    "content-surrogate-key": (("response", "content"), {"\udfff": 1}, "invalid_response"),
    "content-huge-int": (("response", "content"), {"a": 10**4000}, None),
    "meta-text-surrogate": (("meta", "doc_id"), "a\ud800", "invalid_meta"),
    "format-surrogate": (("format",), "\ud800", "wrong_format"),
    "key-list": (("key",), [1, [2]], "wrong_key"),
}


class TestFileCacheCraftedEntries:
    @pytest.mark.parametrize(
        ("part", "reason"), [("CacheMeta", "invalid_meta"), ("ProviderResponse", "invalid_response")]
    )
    def test_an_arithmetic_error_from_a_check_is_a_corrupt_miss(self, part, reason, cache, root, monkeypatch):
        # No number check is known to raise one (latency and timeouts refuse what overflows a float); should one,
        # get still returns a corrupt miss.
        key = key_of("a")
        cache.put(key, make_response(), make_meta())

        def overflow(data):
            raise OverflowError("int too large to convert to float")

        monkeypatch.setattr(getattr(cache_module, part), "from_json", overflow)

        assert cache.get(key) == CacheLookup(None, corrupt=True, reason=reason)

    @pytest.mark.parametrize("name", list(CRAFTED))
    def test_a_crafted_value_is_a_corrupt_miss_never_an_error(self, name, cache, root):
        (where, value, reason) = CRAFTED[name]
        key = key_of("a")
        entry = entry_object(key)
        target = entry
        for step in where[:-1]:
            target = target[step]
        target[where[-1]] = value
        write_entry(root, key, entry)

        lookup = cache.get(key)

        if reason is None:  # a big int in content is JSON: the entry is sound
            assert lookup.hit and lookup.response.content == value
        else:
            assert lookup == CacheLookup(None, corrupt=True, reason=reason)


HOSTILE_VALUES = [
    10**400,
    -(10**400),
    10**4000,
    2**63,
    2**64,
    -1,
    0,
    1e308,
    -1e308,
    5e-324,
    -0.0,
    float("nan"),
    float("inf"),
    True,
    False,
    None,
    "",
    " ",
    "Z",
    "\ud800",
    "99999-01-01T00:00:00+00:00",
    "2026-11-04T10:00:00+05:00",
    "complete",
    "a" * 64,
    "A" * 64,
    CACHE_FORMAT,
    [],
    {},
    [[[]]],
    {"a": {"b": [1]}},
]
"""Values that broke, or could break, a reader: huge or non-finite numbers, wrong types, edge strings, nesting."""

JUNK_SCALARS = st.sampled_from(HOSTILE_VALUES) | st.integers() | st.floats() | st.text(max_size=20)


def nested_lists(depth: int) -> list:
    """``depth`` lists, one inside the next (built in a loop: JSON's own recursion limit differs by version)."""
    value: list = []
    for _ in range(depth - 1):
        value = [value]
    return value


JUNK = st.recursive(
    JUNK_SCALARS,
    lambda inner: st.lists(inner, max_size=3) | st.dictionaries(st.text(max_size=5), inner, max_size=3),
    max_leaves=8,
) | st.integers(min_value=1, max_value=200).map(nested_lists)
"""Junk for the fuzz below; deeper nesting is written as text (test_deep_nesting_inside_an_entry_is_a_lookup)."""


def json_paths(value: object) -> list[tuple]:
    """The paths into ``value`` down to 8 levels, its root included (iterative: junk can nest 20,000 deep)."""
    found: list[tuple] = []
    stack: list[tuple[tuple, object]] = [((), value)]
    while stack:
        prefix, item = stack.pop()
        found.append(prefix)
        if len(prefix) >= 8:
            continue
        if isinstance(item, dict):
            stack.extend(((*prefix, name), child) for name, child in item.items())
        elif isinstance(item, list):
            stack.extend(((*prefix, index), child) for index, child in enumerate(item))
    return found


def changed(value: object, path: tuple, operation: str, junk: object, name: str) -> object:
    """``value`` with the item at ``path`` replaced by ``junk``, deleted, or given an extra field ``name``.

    ``junk`` goes in as a copy, so no example changes the shared ``HOSTILE_VALUES``.
    """
    junk = copy.deepcopy(junk)
    if not path:
        return junk if operation == "replace" or not isinstance(value, dict) else {**value, name: junk}
    parent = value
    for step in path[:-1]:
        parent = parent[step]
    if operation == "replace":
        parent[path[-1]] = junk
    elif operation == "delete" and isinstance(parent, dict):
        del parent[path[-1]]
    elif isinstance(parent[path[-1]], dict):
        parent[path[-1]][name] = junk
    else:
        parent[path[-1]] = junk
    return value


def test_every_hostile_value_in_every_place_is_a_lookup(tmp_path):
    root = tmp_path / "cache"
    cache = FileCache(root)
    key = key_of("a")
    raised = []
    for path in json_paths(entry_object(key)):
        for value in HOSTILE_VALUES:
            write_entry(root, key, changed(entry_object(key), path, "replace", value, ""))
            try:
                lookup = cache.get(key)
            except Exception as error:  # collected, then asserted empty
                raised.append((path, repr(value)[:30], type(error).__name__))
                continue
            assert lookup.hit or lookup.reason in CORRUPT_REASONS

    assert raised == []


@pytest.mark.parametrize("depth", [500, 5_000, 50_000, 500_000])
def test_deep_nesting_inside_an_entry_is_a_lookup(depth, tmp_path):
    # Written as text, past what json.dumps writes on any version: the parse or the checks recurse too deep, and
    # get still returns a lookup (a hit where the interpreter's limits allow it, else a corrupt miss).
    root = tmp_path / "cache"
    cache = FileCache(root)
    key = key_of("a")
    text = entry_text(key, make_response(content={"a": []}))
    assert text.count('"a":[]') == 1
    plant(root, key).write_text(text.replace('"a":[]', '"a":' + "[" * depth + "]" * depth), encoding="utf-8")

    lookup = cache.get(key)

    assert lookup.hit or lookup.reason == "not_json" or lookup.reason == "invalid_response"


@settings(max_examples=300, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(data=st.data())
def test_no_change_to_an_entrys_values_makes_get_raise(data, tmp_path_factory):
    root = tmp_path_factory.mktemp("fuzz") / "cache"
    cache = FileCache(root)
    key = key_of("a")
    entry: object = entry_object(key)
    for _ in range(data.draw(st.integers(min_value=1, max_value=4), label="changes")):
        path = data.draw(st.sampled_from(json_paths(entry)), label="path")
        operation = data.draw(st.sampled_from(["replace", "delete", "add"]), label="operation")
        entry = changed(entry, path, operation, data.draw(JUNK, label="junk"), data.draw(st.text(max_size=5)))
    write_entry(root, key, entry)

    lookup = cache.get(key)

    assert isinstance(lookup, CacheLookup)
    assert lookup.hit or (lookup.corrupt and lookup.reason in CORRUPT_REASONS)
    assert lookup.response is None or isinstance(lookup.response, ProviderResponse)


# ---------------------------------------------------------------------------------------------------------------
# FileCache: a sub-directory swapped while it is in use
# ---------------------------------------------------------------------------------------------------------------


def swap(directory: Path, replacement: str, outside: Path) -> Path:
    """Move ``directory`` aside (to ``<name>.gone``) and put ``replacement`` at its path; return where it went.

    ``"outside"`` is a symlink to ``outside``, ``"itself"`` a symlink to the moved directory (the same inode),
    ``"directory"`` a new, empty directory.
    """
    gone = directory.with_name(f"{directory.name}.gone")
    directory.rename(gone)
    if replacement == "outside":
        directory.symlink_to(outside, target_is_directory=True)
    elif replacement == "itself":
        directory.symlink_to(gone, target_is_directory=True)
    else:
        directory.mkdir(mode=0o700)
    return gone


def open_descriptors() -> int:
    """How many descriptors this process holds (``/dev/fd`` on macOS and Linux)."""
    return len(os.listdir("/dev/fd"))


@pytest.fixture
def outside(tmp_path) -> Path:
    """A directory outside the cache, which a swap points at: nothing may ever be written in it."""
    place = tmp_path / "outside"
    place.mkdir()
    return place


@needs_posix
class TestFileCacheSwappedSubDirectory:
    @pytest.mark.parametrize("replacement", ["outside", "itself", "directory"])
    def test_swapped_between_the_check_and_the_write(self, replacement, cache, root, outside, monkeypatch):
        key = key_of("a")
        directory = entry_path(root, key).parent
        real_subdirectory = cache_module._subdirectory
        moved: list[Path] = []

        def checked_then_swapped(path):
            identity = real_subdirectory(path)
            moved.append(swap(path, replacement, outside))
            return identity

        monkeypatch.setattr(cache_module, "_subdirectory", checked_then_swapped)
        before = open_descriptors()

        with pytest.raises(RationalizationError, match="tampered"):
            cache.put(key, make_response(), make_meta())
        assert open_descriptors() == before
        assert list(outside.iterdir()) == []
        assert list(moved[0].iterdir()) == []  # nothing in the checked directory either: the write never started
        if replacement == "directory":
            assert list(directory.iterdir()) == []

    def test_swapped_after_the_entry_is_replaced(self, cache, root, outside, monkeypatch):
        key = key_of("a")
        directory = entry_path(root, key).parent
        real_replace = os.replace

        def replace_then_swap(*args, **kwargs):
            real_replace(*args, **kwargs)
            swap(directory, "outside", outside)

        monkeypatch.setattr(cache_module.os, "replace", replace_then_swap)

        with pytest.raises(RationalizationError, match="tampered"):
            cache.put(key, make_response(), make_meta())
        assert list(outside.iterdir()) == []

    def test_swapped_after_the_directory_is_opened_the_write_stays_in_it(self, cache, root, outside, monkeypatch):
        if not bound(cache):
            pytest.skip("only a dir_fd write holds the directory open")
        key = key_of("a")
        real_open_directory = cache_module._open_directory
        moved: list[Path] = []

        def opened_then_swapped(path, identity):
            descriptor = real_open_directory(path, identity)
            moved.append(swap(path, "outside", outside))
            return descriptor

        monkeypatch.setattr(cache_module, "_open_directory", opened_then_swapped)

        with pytest.raises(RationalizationError, match="tampered"):
            cache.put(key, make_response(), make_meta())
        assert list(outside.iterdir()) == []
        assert [path.name for path in moved[0].iterdir()] == [f"{key}.json"]  # in the checked inode, inside cache_dir
        assert temp_files(root) == []

    @pytest.mark.parametrize("replacement", ["outside", "itself"])
    def test_swapped_between_the_check_and_the_read(self, replacement, cache, root, outside, monkeypatch):
        key = key_of("a")
        cache.put(key, make_response("inside"), make_meta())
        (outside / f"{key}.json").write_text(entry_text(key, make_response("outside")), encoding="utf-8")
        real_status = cache_module._directory_status

        def checked_then_swapped(path):
            identity = real_status(path)
            swap(path, replacement, outside)
            return identity

        monkeypatch.setattr(cache_module, "_directory_status", checked_then_swapped)
        before = open_descriptors()

        with pytest.raises(RationalizationError, match="tampered"):
            cache.get(key)
        assert open_descriptors() == before

    def test_swapped_and_swapped_back_around_the_read_never_reads_outside(self, cache, root, outside, monkeypatch):
        # A path-based read checks the directory again after it reads, which a second swap (back) defeats: so the
        # descriptor-bound read (wherever the OS has dir_fd) refuses the symlink before it reads anything.
        if not bound(cache):
            pytest.skip("the fallback without dir_fd cannot see a swap that is undone before its check")
        key = key_of("a")
        cache.put(key, make_response("inside"), make_meta())
        (outside / f"{key}.json").write_text(entry_text(key, make_response("outside")), encoding="utf-8")
        directory = entry_path(root, key).parent
        real_status, real_read = cache_module._directory_status, cache_module._read_entry
        moved: list[Path] = []

        def checked_then_swapped(path):
            identity = real_status(path)
            moved.append(swap(path, "outside", outside))
            return identity

        def read_then_swapped_back(path, directory_fd):
            data = real_read(path, directory_fd)
            directory.unlink()
            moved[0].rename(directory)
            return data

        monkeypatch.setattr(cache_module, "_directory_status", checked_then_swapped)
        monkeypatch.setattr(cache_module, "_read_entry", read_then_swapped_back)

        with pytest.raises(RationalizationError, match="tampered"):
            cache.get(key)

    def test_removed_between_the_check_and_the_read_is_a_miss(self, cache, root, monkeypatch):
        key = key_of("a")
        directory = entry_path(root, key).parent
        directory.mkdir()
        real_status = cache_module._directory_status

        def checked_then_removed(path):
            identity = real_status(path)
            path.rmdir()
            return identity

        monkeypatch.setattr(cache_module, "_directory_status", checked_then_removed)

        assert cache.get(key) == CacheLookup(None)

    @pytest.mark.parametrize("target", ["file", "directory"])
    def test_swapped_as_it_is_made_its_chmod_never_follows(self, target, cache, root, tmp_path, monkeypatch):
        # The race: the new sub-directory is swapped for a symlink between its mkdir and the chmod that follows.
        key = key_of("a")
        victim = tmp_path / "victim"
        if target == "file":
            victim.write_text("someone's file", encoding="utf-8")
        else:
            victim.mkdir()
        victim.chmod(0o644)
        real_mkdir = Path.mkdir

        def mkdir_then_swap(path, *args, **kwargs):
            real_mkdir(path, *args, **kwargs)
            if path.name == key[:2]:
                path.rmdir()
                path.symlink_to(victim)

        monkeypatch.setattr(Path, "mkdir", mkdir_then_swap)

        with pytest.raises(RationalizationError, match="tampered"):
            cache.put(key, make_response(), make_meta())
        monkeypatch.undo()
        assert mode(victim) == 0o644
        if target == "directory":
            assert list(victim.iterdir()) == []

    def test_linux_and_macos_bind_to_descriptors(self):
        # The descriptor-bound reads and writes are what makes a swap harmless: they must not quietly fall back.
        if sys.platform in ("linux", "darwin"):
            assert cache_module._DIR_FD is True

    def test_the_temp_file_is_always_a_new_file(self, cache, root, monkeypatch):
        # O_CREAT | O_EXCL: a file (or symlink) already at the temp name is never written through, even if the
        # random name were guessed.
        if not bound(cache):
            pytest.skip("the fallback's mkstemp makes its file O_EXCL itself")
        key = key_of("a")
        planted = plant(root, key).parent / f".{'0' * 32}.tmp"
        planted.write_text("kept", encoding="utf-8")
        monkeypatch.setattr(cache_module.secrets, "token_hex", lambda size: "0" * 32)

        with pytest.raises(RationalizationError, match="cannot write"):
            cache.put(key, make_response(), make_meta())
        assert planted.read_text(encoding="utf-8") == "kept"
        assert not entry_path(root, key).exists()

    def test_gone_at_the_check_and_back_at_the_write_raises(self, cache, root, monkeypatch):
        # Missing at the check after mkdir, there again at the write: the write never runs against no identity.
        key = key_of("a")
        entry_path(root, key).parent.mkdir()
        monkeypatch.setattr(cache_module, "_directory_status", lambda path: None)
        monkeypatch.setattr(cache_module, "_make_directory", lambda path, *, parents: None)

        with pytest.raises(RationalizationError, match="cannot write") as error:
            cache.put(key, make_response(), make_meta())
        assert isinstance(error.value.__cause__, FileNotFoundError)
        assert list(entry_path(root, key).parent.iterdir()) == []

    def test_removed_as_it_is_made_raises(self, cache, root, monkeypatch):
        monkeypatch.setattr(cache_module, "_make_directory", lambda path, *, parents: None)

        with pytest.raises(RationalizationError, match="cannot write") as error:
            cache.put(key_of("a"), make_response(), make_meta())
        assert isinstance(error.value.__cause__, FileNotFoundError)

    @pytest.mark.skipif(POSIX and os.geteuid() == 0, reason="root opens any directory")
    def test_a_sub_directory_that_cannot_be_opened_raises(self, cache, root):
        if not bound(cache):
            pytest.skip("only a dir_fd read opens the sub-directory itself")
        key = key_of("a")
        cache.put(key, make_response(), make_meta())
        directory = entry_path(root, key).parent
        directory.chmod(0)
        try:
            with pytest.raises(RationalizationError, match="cannot be opened"):
                cache.get(key)
        finally:
            directory.chmod(0o700)


# ---------------------------------------------------------------------------------------------------------------
# FileCache: concurrency
# ---------------------------------------------------------------------------------------------------------------

WRITER = textwrap.dedent(
    """
    import json, os, sys, time
    from datagrunt.rationalization._canonical import sha256_hex
    from datagrunt.rationalization.cache import CacheMeta, FileCache
    from datagrunt.rationalization.wire import ProviderResponse

    root, sync, index, keys, meta_fields = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), sys.argv[5]
    meta = CacheMeta(**json.loads(meta_fields))
    names = [f"shared-{i}" for i in range(keys // 2)] + [f"own-{index}-{i}" for i in range(keys // 2)]
    open(os.path.join(sync, f"ready-{index}"), "w").close()
    give_up = time.monotonic() + 60
    while not os.path.exists(os.path.join(sync, "go")):
        if time.monotonic() > give_up:
            sys.exit("no go signal")
        time.sleep(0.002)
    cache = FileCache(root)
    for name in names:
        key = sha256_hex(f"key:{name}")
        response = ProviderResponse(
            content={"key": key, "writer": index}, raw_text=f"{key}:{index}", finish="complete",
            input_tokens=1, output_tokens=1,
        )
        assert cache.put(key, response, meta)
        assert not cache.get(sha256_hex("key:shared-0")).corrupt
    print("done", index)
    """
)


class TestFileCacheConcurrency:
    def test_eight_processes_fifty_keys_half_shared(self, tmp_path):
        root, sync = tmp_path / "cache", tmp_path / "sync"
        sync.mkdir()
        processes = [
            subprocess.Popen(
                [
                    sys.executable,
                    "-I",
                    "-B",
                    "-c",
                    WRITER,
                    str(root),
                    str(sync),
                    str(index),
                    "50",
                    json.dumps(META_FIELDS),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            for index in range(8)
        ]
        try:
            give_up = time.monotonic() + TIMEOUT_S
            while len(list(sync.glob("ready-*"))) < 8 and time.monotonic() < give_up:
                assert all(process.poll() is None for process in processes), "a writer ended before the start"
                time.sleep(0.01)
            (sync / "go").touch()
            results = [process.communicate(timeout=TIMEOUT_S) for process in processes]
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                    process.communicate()

        assert [process.returncode for process in processes] == [0] * 8, [stderr for _, stderr in results]
        assert temp_files(root) == []
        files = sorted(root.rglob("*.json"))
        assert len(files) == 25 + 8 * 25
        assert all(path.parent.name == path.stem[:2] for path in files)
        check_shared_and_own(FileCache(root), threads=8, keys=50)

    def test_sixteen_threads(self, cache, root):
        errors = run_threads(cache, threads=16, keys=50)

        assert errors == []
        assert temp_files(root) == []
        assert len(list(root.rglob("*.json"))) == 25 + 16 * 25
        check_shared_and_own(cache, threads=16, keys=50)


# ---------------------------------------------------------------------------------------------------------------
# FileCache: what it writes, and where (an audit hook in a fresh process)
# ---------------------------------------------------------------------------------------------------------------

GUARD = textwrap.dedent(
    """
    import json, os, stat, sys
    from datagrunt.rationalization._canonical import sha256_hex
    from datagrunt.rationalization.cache import CacheMeta, FileCache
    from datagrunt.rationalization.wire import ProviderResponse

    WRITE_FLAGS = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND | getattr(os, "O_EXCL", 0)
    PATH_EVENTS = {
        "os.mkdir", "os.rename", "os.remove", "os.rmdir", "os.truncate", "os.link", "os.symlink", "os.mkfifo",
        "os.mknod", "os.chmod", "os.chown", "os.utime", "os.chflags", "os.lchflags", "os.setxattr",
        "os.removexattr", "tempfile.mkstemp", "tempfile.mkdtemp", "shutil.copyfile", "shutil.copymode",
        "shutil.copystat", "shutil.copytree", "shutil.move", "shutil.rmtree",
    }
    recording = []
    seen = []

    def hook(event, args):
        if not recording:
            return
        if event == "open":
            path, mode, flags = args
            if isinstance(path, int):
                return
            if (isinstance(mode, str) and any(c in mode for c in "wax+")) or (flags or 0) & WRITE_FLAGS:
                seen.append([event, os.fsdecode(path)])
        elif event in PATH_EVENTS:
            seen.append([event, *[os.fsdecode(a) for a in args if isinstance(a, (str, bytes, os.PathLike))]])

    def snapshot(root):
        found = []
        for directory, names, files in os.walk(root):
            for name in [directory, *[os.path.join(directory, n) for n in names + files]]:
                status = os.lstat(name)
                found.append([name, status.st_mode, status.st_size, status.st_mtime_ns])
        return sorted(found)

    root = sys.argv[1]
    meta = CacheMeta(**json.loads(sys.argv[2]))
    keys = [sha256_hex(f"key:{i}") for i in range(6)]
    sys.addaudithook(hook)

    recording.append(True)
    cache = FileCache(root)
    for key in keys:
        response = ProviderResponse(
            content={"k": key}, raw_text=key, finish="complete", input_tokens=1, output_tokens=2
        )
        assert cache.put(key, response, meta)
    puts = list(seen)
    recording.clear()

    planted = {
        "truncated": sha256_hex("key:truncated"),
        "fifo": sha256_hex("key:fifo"),
        "symlink": sha256_hex("key:symlink"),
        "directory": sha256_hex("key:directory"),
    }
    for kind, key in planted.items():
        os.makedirs(os.path.join(root, key[:2]), mode=0o700, exist_ok=True)
        path = os.path.join(root, key[:2], key + ".json")
        if kind == "truncated":
            with open(path, "w") as stream:
                stream.write('{"format": ')
        elif kind == "fifo":
            os.mkfifo(path)
        elif kind == "symlink":
            os.symlink(os.path.join(root, keys[0][:2], keys[0] + ".json"), path)
        else:
            os.mkdir(path)

    before = snapshot(root)
    seen.clear()
    recording.append(True)
    lookups = [FileCache(root).get(key).hit for key in keys]
    lookups += [cache.get(sha256_hex(f"key:missing-{i}")).hit for i in range(20)]
    reasons = {kind: cache.get(key).reason for kind, key in planted.items()}
    gets = list(seen)
    recording.clear()
    after = snapshot(root)
    print("GUARD " + json.dumps({"puts": puts, "gets": gets, "lookups": lookups, "reasons": reasons,
                                 "same": before == after}))
    """
)


def placed(path: str, root: Path) -> bool:
    """Whether a write the guard saw lands under ``root``: an absolute path under it, or a name the cache writes at
    its sub-directory's descriptor (a temp file or an entry; a name resolved anywhere else would show up in the
    working directory, which must stay empty)."""
    if Path(path).is_absolute():
        return Path(path) == root or root in Path(path).parents
    return re.fullmatch(r"\.[0-9a-f]{32}\.tmp|[0-9a-f]{64}\.json", path) is not None


@needs_posix
def test_file_cache_writes_only_under_cache_dir_and_never_on_get(tmp_path):
    places = {name: tmp_path / name for name in ("home", "tmp", "cwd", "parent")}
    for place in places.values():
        place.mkdir()
    root = places["parent"] / "cache"
    environment = {
        **os.environ,
        "HOME": str(places["home"]),
        **{name: str(places["tmp"]) for name in ("TMPDIR", "TMP", "TEMP")},
    }
    before = {name: tree(place) for name, place in places.items() if name != "parent"}
    completed = subprocess.run(
        [sys.executable, "-I", "-B", "-c", GUARD, str(root), json.dumps(META_FIELDS)],
        cwd=places["cwd"],
        env=environment,
        capture_output=True,
        text=True,
        timeout=TIMEOUT_S,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr[-4000:]
    (line,) = [line for line in completed.stdout.splitlines() if line.startswith("GUARD ")]
    seen = json.loads(line.removeprefix("GUARD "))
    written = [path for event in seen["puts"] for path in event[1:]]
    assert {event[0] for event in seen["puts"]} >= {"os.mkdir", "os.chmod", "open", "os.rename"}
    assert written and all(placed(path, root) for path in written), written
    assert seen["gets"] == []
    assert seen["same"] is True
    assert seen["lookups"] == [True] * 6 + [False] * 20
    assert seen["reasons"] == {
        "truncated": "not_json",
        "fifo": "not_regular_file",
        "symlink": "symlink",
        "directory": "not_regular_file",
    }
    assert {name: tree(place) for name, place in places.items() if name != "parent"} == before
    assert sorted(path.name for path in places["parent"].iterdir()) == ["cache"]


# ---------------------------------------------------------------------------------------------------------------
# The module's import discipline (plan invariant 9)
# ---------------------------------------------------------------------------------------------------------------


def test_imports_only_the_stdlib_and_its_siblings_and_nothing_inside_functions():
    tree_ = ast.parse(Path(cache_module.__file__).read_text(encoding="utf-8"))
    functions = [node for node in ast.walk(tree_) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    nested = [
        inner.lineno
        for function in functions
        for inner in ast.walk(function)
        if isinstance(inner, (ast.Import, ast.ImportFrom))
    ]
    modules = [node.module or "" for node in ast.walk(tree_) if isinstance(node, ast.ImportFrom)]
    modules += [alias.name for node in ast.walk(tree_) if isinstance(node, ast.Import) for alias in node.names]
    allowed = {
        "datagrunt.rationalization._canonical",
        "datagrunt.rationalization.envelope",
        "datagrunt.rationalization.exceptions",
        "datagrunt.rationalization.wire",
    }

    assert functions, "vacuous: the module defines no function"
    assert nested == []
    assert [m for m in modules if m not in allowed and m.split(".")[0] not in sys.stdlib_module_names] == []


def test_importing_the_module_loads_neither_pydantic_nor_httpx():
    code = (
        "import json, sys; import datagrunt.rationalization.cache; "
        "print(json.dumps(sorted(m for m in sys.modules if m.split('.')[0] in ('pydantic', 'httpx'))))"
    )
    completed = subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True, timeout=TIMEOUT_S, check=False
    )
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout.strip().splitlines()[-1]) == []
