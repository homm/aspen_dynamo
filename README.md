# aspen-dynamo

Async DynamoDB table helper built on `aioboto3`. It wraps a DynamoDB table
resource with small conveniences for key handling, pagination, and optional
Pydantic model validation.

## Installation

```bash
pip install aspen-dynamo
```

Optional extras:

```bash
pip install "aspen-dynamo[benchmark]"
```

## Usage

Create a `DynamoDBTable` with a boto3 DynamoDB resource:

```python
import aioboto3
from aspen_dynamo import DynamoDBTable

session = aioboto3.Session()

async with session.resource("dynamodb") as resource:
    table = DynamoDBTable("MyTable", "pk", resource=resource)
    item = await table.get_item(123)
    print(item)
```

### Partition + sort key (composite key)

Pass a tuple of attribute names for a table with a partition key and sort key:

```python
import aioboto3
from aspen_dynamo import DynamoDBTable

session = aioboto3.Session()

async with session.resource("dynamodb") as resource:
    table = DynamoDBTable("MyTable", ("pk", "sk"), resource=resource)
    item = await table.get_item(123, "v1")
    print(item)
```

### Usage with a Pydantic model

Provide a model to coerce DynamoDB items into typed objects:

```python
import aioboto3
from pydantic import BaseModel
from aspen_dynamo import DynamoDBTable

class Widget(BaseModel):
    pk: int
    name: str

session = aioboto3.Session()

async with session.resource("dynamodb") as resource:
    table = DynamoDBTable("MyTable", "pk", resource=resource, model=Widget)
    widget = await table.get_item(123)
    print(widget.name)
```

## Benchmark

The `benchmark.py` script compares `aspen-dynamo` to other async DynamoDB
clients. It uses the same `DynamoDBTable` API as above:

```bash
python -m aspen_dynamo.benchmark MyTable pk 123
```

Results from two runs:

* `uvloop` 0.19.0
* `aioboto3` 13.0.0
* `aiobotocore` 2.13.0
* `aiodynamo` 24.7
* `aiohttp` 3.9.5
* `boto3` 1.34.106
* `httpx` 0.28.1

| Test | Avg latency 1 | Avg latency 2 | CPU time 1 | CPU time 2 |
| --- | --- | --- | --- | --- |
| aioboto3 (client)     | 3.12ms | 2.67ms | 0.75ms | 0.71ms |
| aioboto3 (resource)   | 3.39ms | 2.64ms | 0.80ms | 0.78ms |
| aspen-dynamo          | 2.61ms | 3.24ms | 0.78ms | 0.79ms |
| raw request (aiohttp) | 2.43ms | 2.17ms | 0.26ms | 0.28ms |
| aiodynamo (aiohttp)   | 2.34ms | 2.15ms | 0.32ms | 0.32ms |
| raw request (httpx)   | 2.53ms | 2.54ms | 2.05ms | 1.99ms |
| aiodynamo (httpx)     | 3.02ms | 3.01ms | 2.07ms | 2.23ms |
