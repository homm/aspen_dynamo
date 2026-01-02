from decimal import Decimal

import pytest
from pydantic import BaseModel

from aspen_dynamo import DynamoDBTable


class _DummyExceptions:
    pass


class _DummyClient:
    def __init__(self):
        self.exceptions = _DummyExceptions()


class _DummyMeta:
    def __init__(self):
        self.client = _DummyClient()


class _DummyResource:
    def __init__(self):
        self.meta = _DummyMeta()

    async def Table(self, name):
        return object()


def test_primary_key_single_string():
    table = DynamoDBTable("table", "pk", resource=_DummyResource())
    assert table.primary_key == ("pk",)
    assert table.key_from_values(("value",)) == {"pk": "value"}


def test_primary_key_single_tuple():
    table = DynamoDBTable("table", ("pk",), resource=_DummyResource())
    assert table.primary_key == ("pk",)
    assert table.key_from_values(("value",)) == {"pk": "value"}


def test_primary_key_composite_tuple():
    table = DynamoDBTable("table", ("pk", "sk"), resource=_DummyResource())
    assert table.primary_key == ("pk", "sk")
    assert table.key_from_values(("value", "sort")) == {"pk": "value", "sk": "sort"}


def test_coerce_item_without_model_coerces_decimals():
    table = DynamoDBTable("table", "pk", resource=_DummyResource())
    item = {"count": Decimal("2"), "ratio": Decimal("0.5")}

    result = table.coerce_item(item)

    assert result == {"count": 2, "ratio": 0.5}


def test_coerce_item_with_model_returns_model_instance():
    class ItemModel(BaseModel):
        count: int

    table = DynamoDBTable(
        "table",
        "pk",
        resource=_DummyResource(),
        model=ItemModel,
    )

    result = table.coerce_item({"count": 3})

    assert isinstance(result, ItemModel)
    assert result.count == 3


@pytest.mark.asyncio
async def test_table_resource_is_cached():
    table = DynamoDBTable("table", "pk", resource=_DummyResource())

    first = await table.table_resource()
    second = await table.table_resource()

    assert first is second
