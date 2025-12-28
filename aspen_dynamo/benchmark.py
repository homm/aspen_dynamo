import argparse
import asyncio
import ssl
import time
from typing import Any, Awaitable, cast

import certifi
import uvloop
from aioboto3 import Session
from aiobotocore.credentials import AioCredentials
from aiodynamo.client import Client
from aiodynamo.credentials import Key, StaticCredentials
from aiodynamo.http.aiohttp import AIOHTTP
from aiodynamo.http.httpx import HTTPX
from aiohttp import ClientSession, TCPConnector
from httpx import AsyncClient as HttpXClient

from aspen_dynamo import DynamoDBTable


boto_session = Session()


async def _get_aiodynamo_cred():
    boto_cred = await cast(Awaitable[AioCredentials], boto_session.get_credentials())
    frozen = await boto_cred.get_frozen_credentials()
    if frozen.access_key and frozen.secret_key:
        return StaticCredentials(
            Key(frozen.access_key, frozen.secret_key, frozen.token)
        )
    raise RuntimeError()


class Runner:
    def __init__(self, table_name: str, table_pk: str, item_id):
        self.table_name = table_name
        self.table_pk = table_pk
        self.item_id = item_id

    async def prepare(self): ...

    async def close(self): ...

    async def get_item(self) -> Any: ...


class Boto3ClientRunner(Runner):
    async def prepare(self):
        self._wrapper = boto_session.client("dynamodb")
        self.client = await self._wrapper.__aenter__()

    async def close(self):
        await self._wrapper.__aexit__(None, None, None)

    async def get_item(self):
        return await self.client.get_item(
            TableName=self.table_name,
            Key={self.table_pk: {"N": str(self.item_id)}}
        )


class Boto3ResourceRunner(Runner):
    async def prepare(self):
        self._wrapper = boto_session.resource("dynamodb")
        resource = await self._wrapper.__aenter__()
        self.table = await resource.Table(self.table_name)

    async def close(self):
        await self._wrapper.__aexit__(None, None, None)

    async def get_item(self):
        return await self.table.get_item(Key={self.table_pk: self.item_id})


class AIODynamoHttpXRunner(Runner):
    async def prepare(self):
        cred = await _get_aiodynamo_cred()
        client = Client(HTTPX(HttpXClient()), cred, boto_session.region_name)
        self.table = client.table(self.table_name)

    async def get_item(self):
        return await self.table.get_item({self.table_pk: self.item_id})


class AIODynamoAioHttpRunner(Runner):
    async def prepare(self):
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        self._wrapper = ClientSession(connector=TCPConnector(ssl=ssl_context))
        session = await self._wrapper.__aenter__()

        cred = await _get_aiodynamo_cred()
        client = Client(AIOHTTP(session), cred, boto_session.region_name)
        self.table = client.table(self.table_name)

    async def close(self):
        await self._wrapper.__aexit__(None, None, None)

    async def get_item(self):
        return await self.table.get_item({self.table_pk: self.item_id})


class AspenDynamoRunner(Runner):
    async def prepare(self):
        self._wrapper = boto_session.resource("dynamodb")
        resource = await self._wrapper.__aenter__()
        self.table = DynamoDBTable(self.table_name, self.table_pk, resource=resource)

    async def close(self):
        await self._wrapper.__aexit__(None, None, None)

    async def get_item(self):
        return await self.table.get_item(self.item_id)


async def get_latency(runner: Runner, n=10):
    # Run 10 times and return average run time
    start = time.monotonic()
    for _ in range(n):
        await runner.get_item()
    return (time.monotonic() - start) / n


async def get_load(runner: Runner, n=20, times=5):
    # Run 20 times and return average CPU time
    start = time.process_time()
    for _ in range(times):
        await asyncio.gather(*[
            runner.get_item()
            for _ in range(n)
        ])
    return (time.process_time() - start) / n / times


async def run(table_name: str, table_pk: str, item_id):
    if item_id is None:
        runner = Boto3ResourceRunner(table_name, table_pk, None)
        await runner.prepare()
        resp = await runner.table.scan(Limit=1)
        item_id = resp["Items"][0][table_pk]
        print("Item_is is not provided. Found:", item_id)
        await runner.close()

    for runner_class in [
        Boto3ClientRunner,
        Boto3ResourceRunner,
        AIODynamoHttpXRunner,
        AIODynamoAioHttpRunner,
        AspenDynamoRunner,
    ]:
        runner = runner_class(table_name, table_pk, item_id)

        print(f"Running {runner_class.__name__}...")
        await runner.prepare()
        print("==== resp ", "=" * 40)
        print(await runner.get_item())
        print("=" * 50)

        latency = await get_latency(runner)
        print(f"Latency: {latency * 1000:.2f}ms")

        load = await get_load(runner)
        print(f"CPU time: {load * 1000:.2f}ms")

        await runner.close()
        print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark DynamoDB clients.")
    parser.add_argument("name", help="DynamoDB table name.")
    parser.add_argument("pk", help="Primary key attribute name.")
    parser.add_argument(
        "item_id", nargs="?", type=int, default=None,
        help="Item id (optional, default: 0).")
    args = parser.parse_args()
    uvloop.run(run(args.name, args.pk, args.item_id))
