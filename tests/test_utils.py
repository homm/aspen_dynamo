from decimal import Decimal

from aspen_dynamo import _coerce_decimal_types, _sanitize_empty_sets


def test_coerce_decimal_types_scalar_values():
    assert _coerce_decimal_types(Decimal("2.0")) == 2
    assert _coerce_decimal_types(Decimal("2.5")) == 2.5
    assert _coerce_decimal_types("unchanged") == "unchanged"


def test_coerce_decimal_types_nested_structures():
    data = {
        "count": Decimal("3"),
        "ratio": Decimal("0.25"),
        "items": [Decimal("1.0"), {"nested": Decimal("4.5")}],
        "tags": {Decimal("2"), Decimal("3.0")},
    }

    result = _coerce_decimal_types(data)

    assert result["count"] == 3
    assert result["ratio"] == 0.25
    assert result["items"] == [1, {"nested": 4.5}]
    assert result["tags"] == {2, 3}


def test_sanitize_empty_sets_keeps_non_empty_sets():
    value = {"letters": {"a", "b"}, "numbers": {1}}
    result = _sanitize_empty_sets(value)
    assert result == value
    assert result["letters"] is value["letters"]


def test_sanitize_empty_sets_replaces_empty_sets():
    value = {"empty": set(), "nested": {"inner": set()}}
    result = _sanitize_empty_sets(value)
    assert result == {"empty": [], "nested": {"inner": []}}


def test_sanitize_empty_sets_handles_sequences():
    value = ({"items": [set(), {"more": set()}]}, set())
    result = _sanitize_empty_sets(value)
    assert result == [{"items": [[], {"more": []}]}, []]
