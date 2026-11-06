"""Tests for the ``Key`` marker (``datagrunt.rationalization.markers``): a schema's entity-identity fields."""

import copy
import dataclasses
import pickle

import pytest

from datagrunt.rationalization import Key
from datagrunt.rationalization.markers import _KeyMarker

# Namespaces a caller may write, each with its normalized form: stripped (Unicode whitespace too), then lower-cased
# by str.lower (a final capital sigma becomes "ς"; ß and fullwidth letters keep their own lower case).
NAMESPACES = {
    "part": "part",
    "Part": "part",
    "  PART\t\n": "part",
    "\u3000Assembly\u3000": "assembly",
    "part-number/v2": "part-number/v2",
    "\u039f\u0394\u039f\u03a3": "\u03bf\u03b4\u03bf\u03c2",  # ΟΔΟΣ -> οδος, ending in a final sigma
    "Stra\u00dfe": "stra\u00dfe",  # ß has no other lower case
    "\uff30\uff21\uff32\uff34": "\uff50\uff41\uff52\uff54",  # fullwidth PART -> fullwidth part
    "\u0130d": "i\u0307d",  # İ lower-cases to i and a combining dot
}
BLANK_NAMESPACES = ["", " ", "\t\n", "\u3000", "\u00a0\u2003"]
NON_STR_NAMESPACES = [None, 1, 1.5, True, b"part", ["part"], Key]


class TestBareKey:
    def test_has_no_namespace(self):
        assert Key.namespace is None

    def test_is_a_marker_equal_to_a_fresh_one(self):
        assert isinstance(Key, _KeyMarker)
        assert Key == _KeyMarker()
        assert hash(Key) == hash(_KeyMarker())

    def test_repr(self):
        assert repr(Key) == "Key"

    def test_is_frozen(self):
        with pytest.raises(dataclasses.FrozenInstanceError):
            Key.namespace = "part"

    def test_naming_a_namespace_leaves_the_bare_key_unchanged(self):
        Key("part")

        assert Key.namespace is None


class TestNamespacedKey:
    @pytest.mark.parametrize(("written", "normalized"), list(NAMESPACES.items()))
    def test_namespace_is_stripped_and_lower_cased(self, written, normalized):
        marker = Key(written)

        assert marker.namespace == normalized == written.strip().lower()
        assert _KeyMarker(namespace=marker.namespace) == marker  # what Key(...) builds passes the direct check

    def test_compares_and_hashes_by_namespace(self):
        assert Key("Part") == Key("part")
        assert hash(Key("Part")) == hash(Key("part"))
        assert Key("part") != Key("assembly")
        assert Key("part") != Key
        assert {Key, _KeyMarker(), Key("Part"), Key(" part ")} == {Key, Key("part")}

    def test_repr(self):
        assert repr(Key("part")) == "Key('part')"
        assert repr(Key(" Part ")) == "Key('part')"

    @pytest.mark.parametrize("blank", BLANK_NAMESPACES)
    def test_blank_namespace_raises_value_error(self, blank):
        # Anchored: __post_init__'s message starts with the same words, so an unanchored match passes without the guard.
        with pytest.raises(ValueError, match=r"^Key namespace must be non-empty$"):
            Key(blank)

    @pytest.mark.parametrize("bad", NON_STR_NAMESPACES)
    def test_non_str_namespace_raises_type_error(self, bad):
        with pytest.raises(TypeError, match="Key namespace must be a str"):
            Key(bad)

    def test_a_namespaced_marker_cannot_take_another_namespace(self):
        with pytest.raises(TypeError, match=r"Key\('a'\) already has a namespace"):
            Key("a")("b")

    @pytest.mark.parametrize("marker", [Key, Key("part")], ids=["bare", "namespaced"])
    def test_copies_and_pickles_compare_equal(self, marker):
        # Same-process round trip of a marker built here (no untrusted bytes): schema metadata can be copied.
        for copied in (copy.copy(marker), copy.deepcopy(marker), pickle.loads(pickle.dumps(marker))):
            assert copied == marker
            assert repr(copied) == repr(marker)


class TestDirectConstruction:
    def test_accepts_none_and_a_normalized_namespace(self):
        assert _KeyMarker(namespace=None) == Key
        assert _KeyMarker(namespace="part") == Key("part")
        assert _KeyMarker("part") == Key("part")

    @pytest.mark.parametrize("bad", ["", " ", "Part", "PART", " part", "part ", "\tpart\n", "\u039f\u0394\u039f\u03a3"])
    def test_rejects_a_namespace_that_is_not_normalized(self, bad):
        with pytest.raises(ValueError, match="namespace must be non-empty, stripped and lower-case"):
            _KeyMarker(namespace=bad)

    @pytest.mark.parametrize("bad", [1, 1.5, True, b"part", ["part"]])
    def test_rejects_a_namespace_that_is_not_a_str(self, bad):
        with pytest.raises(TypeError, match="namespace must be a str or None"):
            _KeyMarker(namespace=bad)
