from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, cast

from boto3.dynamodb.conditions import Key
from boto3.exceptions import Boto3Error
from pydantic import BaseModel


if TYPE_CHECKING:
    from types_aiobotocore_dynamodb.client import Exceptions
    from types_aiobotocore_dynamodb.service_resource import (
        DynamoDBServiceResource, Table as _ServiceTable)

    class ExceptionsProxy(Exceptions):
        NoSuchKey: type["NoSuchKey"]


class NoSuchKey(Boto3Error):
    pass


def _coerce_decimal_types(item: dict) -> None:
    for k, v in item.items():
        if isinstance(v, Decimal):
            v = float(v)
            item[k] = int(v) if v.is_integer() else v
        elif isinstance(v, dict):
            _coerce_decimal_types(v)


class DynamoDBTable:
    _table_resource: _ServiceTable | None = None

    def __init__(
        self,
        name: str,
        primary_key: str | tuple[str] | tuple[str, str],
        *,
        resource: DynamoDBServiceResource,
        model: type[BaseModel] | None = None,
    ):
        if not isinstance(primary_key, tuple):
            primary_key = (primary_key,)
        self.table_name = name
        self.primary_key = primary_key

        self.resource = resource
        self.model = model
        # Shortcut
        self.exceptions = cast("ExceptionsProxy", self.resource.meta.client.exceptions)
        self.exceptions.NoSuchKey = NoSuchKey

    async def table_resource(self):
        if self._table_resource is None:
            self._table_resource = await self.resource.Table(self.table_name)
        return self._table_resource

    def coerce_item(self, item: dict) -> dict | BaseModel:
        if self.model:
            return self.model.model_validate(item)
        _coerce_decimal_types(item)
        return item

    async def get_item(self, *key_values, **kwargs):
        table = await self.table_resource()

        key = {name: value for name, value in zip(self.primary_key, key_values)}
        resp = await table.get_item(Key=key, **kwargs)
        if "Item" not in resp:
            raise self.exceptions.NoSuchKey(self.table_name, key_values)
        return self.coerce_item(resp["Item"])

    async def query(self, hash_key, **kwargs):
        table = await self.table_resource()

        hash_key = Key(self.primary_key[0]).eq(hash_key)
        if kwargs.get("ExclusiveStartKey") is None:
            kwargs.pop("ExclusiveStartKey", None)
        resp = await table.query(KeyConditionExpression=hash_key, **kwargs)

        items = [self.coerce_item(item) for item in resp.get("Items", [])]
        return items, resp.get("LastEvaluatedKey", None)

    async def query_all(self, *args, **kwargs):
        assert "ExclusiveStartKey" not in kwargs

        last_key, first_run = None, True
        while last_key or first_run:
            first_run = False
            items, last_key = await self.query(
                *args, ExclusiveStartKey=last_key, **kwargs
            )
            for item in items:
                yield item
