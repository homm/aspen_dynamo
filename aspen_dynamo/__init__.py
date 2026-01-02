from __future__ import annotations

from collections import abc as collections_abc
from decimal import Decimal
from typing import TYPE_CHECKING, Any, AsyncIterator, Generic, TypeVar, cast, overload

from boto3.dynamodb.conditions import ConditionBase, Key
from boto3.exceptions import Boto3Error
from pydantic import BaseModel


TItem = TypeVar("TItem")
if TYPE_CHECKING:
    from types_aiobotocore_dynamodb.client import Exceptions
    from types_aiobotocore_dynamodb.service_resource import (
        DynamoDBServiceResource, Table as _ServiceTable)
    from types_aiobotocore_dynamodb.type_defs import TableAttributeValueTypeDef

    TModel = TypeVar("TModel", bound=BaseModel)
    TItemDict = dict[str, TableAttributeValueTypeDef]

    class AspenExceptions(Exceptions):
        NoSuchKey: type["NoSuchKey"]


class NoSuchKey(Boto3Error):
    pass


def _coerce_decimal_types(value) -> Any:
    if isinstance(value, Decimal):
        value = float(value)
        return int(value) if value.is_integer() else value
    if isinstance(value, dict):
        return {k: _coerce_decimal_types(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_coerce_decimal_types(v) for v in value]
    if isinstance(value, set):
        return {_coerce_decimal_types(v) for v in value}
    return value


def _sanitize_empty_sets(value) -> Any:
    if isinstance(value, collections_abc.Set):
        return value if value else []
    if isinstance(value, collections_abc.Mapping):
        return {k: _sanitize_empty_sets(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize_empty_sets(item) for item in value]
    return value


class DynamoDBTable(Generic[TItem]):
    _table_resource: _ServiceTable | None = None

    @overload
    def __init__(
        self: "DynamoDBTable[TModel]",
        name: str, primary_key: str | tuple[str] | tuple[str, str],
        *, resource: DynamoDBServiceResource, model: type[TModel],
    ) -> None: ...

    @overload
    def __init__(
        self: "DynamoDBTable[TItemDict]",
        name: str, primary_key: str | tuple[str] | tuple[str, str],
        *, resource: DynamoDBServiceResource, model: None = None,
    ) -> None: ...

    def __init__(
        self, name: str, primary_key: str | tuple[str] | tuple[str, str],
        *, resource: DynamoDBServiceResource, model: type[BaseModel] | None = None,
    ):
        if not isinstance(primary_key, tuple):
            primary_key = (primary_key,)
        self.table_name = name
        self.primary_key = primary_key

        self.resource = resource
        self.model = model
        # Shortcut
        self.exceptions = cast("AspenExceptions", self.resource.meta.client.exceptions)
        self.exceptions.NoSuchKey = NoSuchKey

    async def table_resource(self):
        if self._table_resource is None:
            self._table_resource = await self.resource.Table(self.table_name)
        return self._table_resource

    def coerce_item(self, item: dict) -> TItem:
        if self.model:
            return cast(TItem, self.model.model_validate(item))
        return cast(TItem, _coerce_decimal_types(item))

    def key_from_values(self, key_values):
        return {name: value for name, value in zip(self.primary_key, key_values)}

    async def get_item(self, *key_values, **kwargs) -> TItem:
        table = await self.table_resource()

        resp = await table.get_item(Key=self.key_from_values(key_values), **kwargs)
        if "Item" not in resp:
            raise self.exceptions.NoSuchKey(self.table_name, key_values)
        return self.coerce_item(resp["Item"])

    async def put_item(
        self, item: dict | BaseModel, exclude_defaults=False,
        sanitize_empty_sets=True, **kwargs
    ) -> TItem | None:
        table = await self.table_resource()
        if isinstance(item, BaseModel):
            item = item.model_dump(exclude_defaults=exclude_defaults)

        if sanitize_empty_sets:
            item = cast(dict, _sanitize_empty_sets(item))

        resp = await table.put_item(Item=item, **kwargs)
        if "Attributes" in resp:
            return self.coerce_item(resp["Attributes"])

    async def update_item(self, *key_values, **kwargs) -> TItem | None:
        table = await self.table_resource()

        resp = await table.update_item(Key=self.key_from_values(key_values), **kwargs)
        if "Attributes" in resp:
            return self.coerce_item(resp["Attributes"])

    async def delete_item(self, *key_values, **kwargs) -> TItem | None:
        table = await self.table_resource()

        resp = await table.delete_item(Key=self.key_from_values(key_values), **kwargs)
        if "Attributes" in resp:
            return self.coerce_item(resp["Attributes"])

    async def query(self, hash_key, **kwargs) -> tuple[list[TItem], dict | None]:
        table = await self.table_resource()

        if not isinstance(hash_key, ConditionBase):
            hash_key = Key(self.primary_key[0]).eq(hash_key)
        if kwargs.get("ExclusiveStartKey") is None:
            kwargs.pop("ExclusiveStartKey", None)
        resp = await table.query(KeyConditionExpression=hash_key, **kwargs)

        items = [self.coerce_item(item) for item in resp.get("Items", [])]
        return items, resp.get("LastEvaluatedKey", None)

    async def query_all(self, hash_key, **kwargs) -> AsyncIterator[TItem]:
        assert "ExclusiveStartKey" not in kwargs
        kwargs.setdefault("Limit", 50)

        last_key, first_run = None, True
        while last_key or first_run:
            first_run = False
            items, last_key = await self.query(
                hash_key, ExclusiveStartKey=last_key, **kwargs)
            for item in items:
                yield item

    async def scan(self, **kwargs) -> tuple[list[TItem], dict | None]:
        table = await self.table_resource()

        if kwargs.get("ExclusiveStartKey") is None:
            kwargs.pop("ExclusiveStartKey", None)
        resp = await table.scan(**kwargs)

        items = [self.coerce_item(item) for item in resp.get("Items", [])]
        return items, resp.get("LastEvaluatedKey", None)

    async def scan_all(self, **kwargs) -> AsyncIterator[TItem]:
        assert "ExclusiveStartKey" not in kwargs
        kwargs.setdefault("Limit", 50)

        last_key, first_run = None, True
        while last_key or first_run:
            first_run = False
            items, last_key = await self.scan(
                ExclusiveStartKey=last_key, **kwargs)
            for item in items:
                yield item
