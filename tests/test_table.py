from decimal import Decimal
from typing import cast

import pytest
from boto3.dynamodb.conditions import Key
from pydantic import BaseModel
from types_aiobotocore_dynamodb.service_resource import DynamoDBServiceResource

from aspen_dynamo import DynamoDBTable


class MockExceptions:
    pass


class MockClient:
    def __init__(self):
        self.exceptions = MockExceptions()


class MockMeta:
    def __init__(self):
        self.client = MockClient()


class MockTable:
    def __init__(self, name):
        self.name = name
        self.responses = []
        self.calls = []

    async def query(self, **kwargs):
        self.calls.append(kwargs)
        return self.responses.pop(0)

    async def scan(self, **kwargs):
        self.calls.append(kwargs)
        return self.responses.pop(0)


class _MockResource:
    def __init__(self):
        self.meta = MockMeta()

    async def Table(self, name):
        return MockTable(name)


MockResource = cast(type[DynamoDBServiceResource], _MockResource)


def test_primary_key_single_string():
    table = DynamoDBTable("table", "pk", resource=MockResource())
    assert table.primary_key == ("pk",)
    assert table.key_from_values(("value",)) == {"pk": "value"}


def test_primary_key_single_tuple():
    table = DynamoDBTable("table", ("pk",), resource=MockResource())
    assert table.primary_key == ("pk",)
    assert table.key_from_values(("value",)) == {"pk": "value"}


def test_primary_key_composite_tuple():
    table = DynamoDBTable("table", ("pk", "sk"), resource=MockResource())
    assert table.primary_key == ("pk", "sk")
    assert table.key_from_values(("value", "sort")) == {"pk": "value", "sk": "sort"}


def test_coerce_item_without_model_coerces_decimals():
    table = DynamoDBTable("table", "pk", resource=MockResource())
    item = {"count": Decimal("2"), "ratio": Decimal("0.5")}

    result = table.coerce_item(item)

    assert result == {"count": 2, "ratio": 0.5}


def test_coerce_item_with_model_returns_model_instance():
    class ItemModel(BaseModel):
        count: int

    table = DynamoDBTable(
        "table",
        "pk",
        resource=MockResource(),
        model=ItemModel,
    )

    result = table.coerce_item({"count": 3})

    assert isinstance(result, ItemModel)
    assert result.count == 3


@pytest.mark.asyncio
async def test_table_resource_is_cached():
    table = DynamoDBTable("table", "pk", resource=MockResource())

    assert not table._table_resource
    with pytest.raises(IndexError):
        await table.query(0)
    assert table._table_resource

    second = await table.table_resource()

    assert table._table_resource is second


@pytest.mark.asyncio
async def test_query_builds_key_condition_and_returns_items():
    table = DynamoDBTable("table", ("pk", "sk"), resource=MockResource())
    table_mock = cast(MockTable, await table.table_resource())
    table_mock.responses = [
        {
            "Items": [
                {"pk": "hash-value", "sk": "a", "count": Decimal("1")},
                {"pk": "hash-value", "sk": "b", "count": Decimal("2")},
            ],
            "LastEvaluatedKey": {"pk": "next"},
        }
    ]

    items, last_key = await table.query("hash-value", Limit=5, ExclusiveStartKey=None)

    assert items == [
        {"pk": "hash-value", "sk": "a", "count": 1},
        {"pk": "hash-value", "sk": "b", "count": 2},
    ]
    assert last_key == {"pk": "next"}
    assert len(table_mock.calls) == 1
    call_kwargs = table_mock.calls[0]
    assert call_kwargs["KeyConditionExpression"] == Key("pk").eq("hash-value")
    assert call_kwargs["Limit"] == 5
    assert "ExclusiveStartKey" not in call_kwargs


@pytest.mark.asyncio
async def test_query_all_paginates_until_exhausted():
    table = DynamoDBTable("table", ("pk", "sk"), resource=MockResource())
    table_mock = cast(MockTable, await table.table_resource())
    table_mock.responses = [
        {
            "Items": [{"pk": "hash-value", "sk": "a", "count": Decimal("1")}],
            "LastEvaluatedKey": {"pk": "next"},
        },
        {
            "Items": [{"pk": "hash-value", "sk": "b", "count": Decimal("2")}],
            "LastEvaluatedKey": None,
        },
    ]

    results = []
    async for item in table.query_all("hash-value"):
        results.append(item)

    assert results == [
        {"pk": "hash-value", "sk": "a", "count": 1},
        {"pk": "hash-value", "sk": "b", "count": 2},
    ]
    assert len(table_mock.calls) == 2


@pytest.mark.asyncio
async def test_scan_returns_items_and_removes_empty_start_key():
    table = DynamoDBTable("table", "pk", resource=MockResource())
    table_mock = cast(MockTable, await table.table_resource())
    table_mock.responses = [
        {
            "Items": [{"count": Decimal("1")}, {"count": Decimal("2")}],
            "LastEvaluatedKey": {"pk": "next"},
        }
    ]

    items, last_key = await table.scan(Limit=10, ExclusiveStartKey=None)

    assert items == [{"count": 1}, {"count": 2}]
    assert last_key == {"pk": "next"}
    assert len(table_mock.calls) == 1
    call_kwargs = table_mock.calls[0]
    assert call_kwargs["Limit"] == 10
    assert "ExclusiveStartKey" not in call_kwargs


@pytest.mark.asyncio
async def test_scan_all_paginates_until_exhausted():
    table = DynamoDBTable("table", "pk", resource=MockResource())
    table_mock = cast(MockTable, await table.table_resource())
    table_mock.responses = [
        {"Items": [{"count": Decimal("1")}], "LastEvaluatedKey": {"pk": "next"}},
        {"Items": [{"count": Decimal("2")}], "LastEvaluatedKey": None},
    ]

    results = []
    async for item in table.scan_all():
        results.append(item)

    assert results == [{"count": 1}, {"count": 2}]
    assert len(table_mock.calls) == 2
