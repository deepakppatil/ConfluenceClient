# `confluence-client` – Throttling‑safe, search‑ready, and checkpoint‑aware Confluence client

This repository provides **two battle‑tested Confluence REST client implementations**—one async (`aiohttp`‑based) and one synchronous (SDK‑based)—both designed specifically to avoid overloading your Confluence server while still enabling large‑scale page loading, CQL search, and attachment handling. Both implementations honor Confluence's rate‑limiting behavior, including `Retry‑After`, exponential backoff with jitter, and proactive throttling based on `X‑RateLimit‑*` headers, and offer:

- safe pagination,
- CQL search with `cursor`‑based continuation,
- attachment listing and streaming download,
- filesystem‑based checkpointing and resume support,
- and structured logging for throttling and retry events.

Atlassian’s Confluence Cloud REST API documents rate‑limiting headers such as `Retry‑After`, `X‑RateLimit‑Remaining`, and `X‑RateLimit‑FillRate`, and recommends that clients respond to backpressure instead of blindly retrying or firing requests into a throttled service. This library is built around that pattern, using `aiohttp` and `asyncio` to give you fine‑grained control over concurrency, pacing, and backoff.[^1][^2]
**Status & Support**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.9+](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![Build Status](https://img.shields.io/github/actions/workflow/status/yourusername/confluence-client/tests.yml?branch=main&logo=github)](https://github.com/yourusername/confluence-client/actions)
[![PyPI version](https://img.shields.io/pypi/v/confluence-client.svg?logo=pypi)](https://pypi.org/project/confluence-client/)
[![Implementations: Async + SDK](https://img.shields.io/badge/Implementations-Async%20%2B%20SDK-green.svg)](#2-implementation-options)
[![Concurrency: aiohttp + threading](https://img.shields.io/badge/Concurrency-aiohttp%20%2B%20threading-blue.svg)](#2-implementation-options)
[![SDK compatible](https://img.shields.io/badge/SDK--compatible-atlassian--python--api-orange.svg)](https://atlassian-python-api.readthedocs.io/)
[![codecov](https://img.shields.io/codecov/c/github/yourusername/confluence-client?logo=codecov)](https://codecov.io/gh/yourusername/confluence-client)
***

## Table of contents

1. [Overview and design goals](#1-overview-and-design-goals)
2. [Implementation options](#2-implementation-options)
3. [What this client does](#3-what-this-client-does)
4. [Key components](#4-key-components)
5. [Concepts and terminology](#5-concepts-and-terminology)
6. [Installation and dependencies](#6-installation-and-dependencies)
7. [Quick start](#7-quick-start)
8. [Detailed usage](#8-detailed-usage)
    - 8.1 Initialize and connect
    - 8.2 Configuring rate‑limit behavior
    - 8.3 Spaces
    - 8.4 Pages and pagination
    - 8.5 CQL search
    - 8.6 Attachments
    - 8.7 Checkpointing and resume
    - 8.8 Logging and events
9. [Retry and throttling strategy](#9-retry-and-throttling-strategy)
10. [Error handling and observability](#10-error-handling-and-observability)
11. [Performance and tuning](#11-performance-and-tuning)
12. [Configuration reference](#12-configuration-reference)
13. [Security and authentication](#13-security-and-authentication)
14. [Extending the client](#14-extending-the-client)
15. [Testing and validation](#15-testing-and-validation)
16. [Limitations and caveats](#16-limitations-and-caveats)
17. [License and attribution](#17-license-and-attribution)

***

## 1. Overview and design goals

### Why this exists

The Confluence REST API can be very expressive, but using it naively—especially for bulk page loads, CQL search, and large attachment downloads—can quickly overwhelm a server due to:

- rate limits,
- 429 `Too Many Requests` replies,
- 5xx server errors,
- or even degraded UX if the instance is CPU‑/IO‑bound.

Atlassian’s guidance for integrations is to:

- keep concurrency low,
- honor `Retry‑After`,
- use exponential backoff with jitter,
- and adjust your request rate when you see `X‑RateLimit‑Remaining` approach zero, instead of just retrying blindly.[^2][^1]

This library:

- encapsulates those patterns into a single async client,
- exposes Confluence’s pagination and `cursor`‑based continuation mechanics,
- layers on checkpointing so a long crawl can resume from an offset,
- and adds structured logging to make retrying and throttling decisions visible instead of opaque.


### Non‑goals

This library is **not**:

- a full Confluence ORM or object model.
- a UI or dashboard for Confluence.
- an opinionated workflow engine (e.g., full sync vs. incremental).

It **is** a low‑level safe HTTP client meant to be composed into your own sync, mirror, or search‑based services.

***

## 2. Implementation options

This library provides **two implementations** to suit different application patterns:

### 2.1 Async client (`AsyncSafeConfluenceClient`)

- **Use when**: Your application uses `asyncio` and you want to handle many concurrent requests with minimal threads.
- **Concurrency model**: `asyncio.Semaphore` + cooperative multitasking
- **HTTP library**: `aiohttp` (async/await based)
- **Best for**: FastAPI, async scripts, applications that already use `asyncio`
- **Import**: `from confluence_client.client import AsyncSafeConfluenceClient`

**Example:**
```python
async with AsyncSafeConfluenceClient(
    base_url="https://your-domain.atlassian.net",
    username="user@example.com",
    token="api-token",
) as client:
    pages = await client.get_all_pages_from_space("ENG")
```

### 2.2 SDK client (`SafeConfluenceSdkClient`)

- **Use when**: Your application is synchronous or you prefer the `atlassian-python-api` SDK's convenience methods.
- **Concurrency model**: `threading.BoundedSemaphore` + thread pool
- **HTTP library**: `atlassian` SDK (wraps `requests`)
- **Best for**: Existing codebases using `atlassian` SDK, synchronous scripts, simpler integration
- **Import**: `from confluence_client.sdk_client import SafeConfluenceSdkClient`

**Example:**
```python
client = SafeConfluenceSdkClient(
    url="https://your-domain.atlassian.net",
    username="user@example.com",
    token="api-token",
)
pages = client.get_all_pages_from_space("ENG")
client.close()
```

### 2.3 Comparison

| Feature | Async | SDK |
| :-- | :-- | :-- |
| Concurrency | `asyncio.Semaphore` | `ThreadPoolExecutor` + threading |
| HTTP library | `aiohttp` | `requests` (via `atlassian`) |
| API style | `async`/`await` | Synchronous |
| Thread count | Single (event loop) | Configurable (`max_workers`) |
| Rate limiting | Proactive header‑based + min_request_interval | Proactive header‑based + min_request_interval |
| Best for | Concurrent, high-throughput workflows | Simpler, sync-compatible codebases |
| Checkpointing | ✓ Supported | ✓ Supported |
| Structured logging | ✓ JSON events | ✓ JSON events |

Both implementations support the full feature set: **pagination, CQL search, attachments, checkpointing, and structured logging**.

***

## 3. What this client does

The clients offer:


| Feature | Description |
| :-- | :-- |
| Bounded concurrency | Maximum concurrent HTTP requests controlled by `asyncio.Semaphore` (async) or `threading.BoundedSemaphore` (SDK). |
| Global pacing | A global pacing clock enforces `min_request_interval_seconds` between requests. |
| `Retry‑After` handling | `Retry‑After` and `retry‑after` headers are honored directly. |
| Exponential backoff with jitter | On 429/50x and transport errors, the client sleeps with `backoff_factor × 2^retry` plus jitter. |
| Header‑based throttling | `X‑RateLimit‑Remaining`, `X‑RateLimit‑FillRate`, and `X‑RateLimit‑Interval‑Seconds` are used to slow down proactively when tokens are low. |
| Safe pagination | `iter_all_pages_from_space` and `get_all_pages_from_space` page through `start`/`limit` with config‑driven batch size. |
| CQL search | `cql_search` and `cql_search_iter` call `/rest/api/search` with `limit` and `cursor`‑based continuation. |
| Attachment handling | `list_attachments`, `get_attachment_download_url`, and `download_attachment` implement the documented attachment endpoints. |
| Checkpointing and resume | A JSON checkpoint file records `start` offsets per crawl key, so long runs can resume from the last known offset. |
| Structured logging | Every throttle, retry, and transport failure is logged as a JSON‑style `event` entry for observability. |

All of these map cleanly to the documented Confluence Cloud REST API, including spaces, content (`/rest/api/content`), search (`/rest/api/search`), and attachments (`/rest/api/content/{id}/child/attachment` and download links).[^3][^4][^5]

***

## 3. Key components

The main class is:

```python
class AsyncSafeConfluenceClient:
    ...
```

Key configuration:

```python
@dataclass
class AsyncConfluenceRateLimitConfig:
    ...
```

Major behavioral building blocks:

1. **`__aenter__` / `__aexit__`**
    - Manages an `aiohttp.ClientSession` with configurable timeout, SSL, and auth.
2. **`_semaphore`**
    - Limits concurrent requests to `max_concurrency`.
3. **`_pace_lock` and `min_request_interval_seconds`**
    - Enforces a minimum spacing between requests, preventing “burst” traffic.
4. **`_apply_header_based_throttling`**
    - Reads `Retry‑After`, `X‑RateLimit‑Remaining`, etc., and stalls the pacing clock when needed.
5. **`_compute_retry_delay`**
    - Exponential backoff with jitter, capped at `max_backoff_seconds`.
6. **Checkpointing via `checkpoint_file`**
    - JSON‑on‑disk state tracking for `start` offsets and `done` flags.
7. **Structured logging hooks**
    - Events such as `throttle_applied`, `retry_scheduled`, `request_failed`, etc., are logged as JSON‑style messages.

***

## 4. Key components

### Async implementation

The main async class is:

```python
class AsyncSafeConfluenceClient:
    ...
```

Configuration for async uses `AsyncConfluenceRateLimitConfig` with parameters like `max_concurrency`, `page_batch_size`, etc.

### SDK implementation

The main SDK class is:

```python
class SafeConfluenceSdkClient:
    ...
```

Configuration for SDK uses `ConfluenceRateLimitConfig` with parameters like `max_workers`, `page_batch_size`, etc.

### Shared behavioral building blocks

Both implementations share these core patterns:

1. **Concurrency Control**
    - Async: `asyncio.Semaphore` limits concurrent requests
    - SDK: `threading.BoundedSemaphore` with thread pool limits concurrent threads
2. **Global Pacing Lock**
    - Enforces minimum spacing between requests via `min_request_interval_seconds`
3. **Header‑Based Throttling**
    - Proactively reads `Retry‑After`, `X‑RateLimit‑*` headers and stalls the pacing clock
4. **Exponential Backoff with Jitter**
    - Computes `backoff_factor × 2^(retry_number-1)` plus random jitter, capped at `max_backoff_seconds`
5. **Checkpointing (JSON‑based)**
    - Records `start` offsets and `done` flags per crawl key for resume support
6. **Structured Logging**
    - All throttle, retry, and error events logged as JSON for observability

***

## 5. Concepts and terminology

### 5.1 Tokens and rate limits

- `Retry‑After` / `retry‑after`
Seconds you should wait before retrying; the client will sleep this many seconds plus jitter.[^1]
- `X‑RateLimit‑Remaining`
How many tokens you have left in the current window; if below `low_token_threshold`, the client will slow down, optionally using `X‑RateLimit‑FillRate` and `X‑RateLimit‑Interval‑Seconds` to estimate when tokens refill.
- `X‑RateLimit‑FillRate` / `X‑RateLimit‑Interval‑Seconds`
Periodic refill information that can be used to compute a “token refill time” if `Retry‑After` is missing.[^1]


### 5.2 Pagination and `cursor`

Confluence uses:

- `start`/`limit` for content pagination, and
- `cursor` for search‑based continuation.

This client exposes:

- `iter_all_pages_from_space` / `get_all_pages_from_space` for `start`/`limit` sequences, and
- `cql_search` / `cql_search_iter` for `cursor`‑based search continuation.

Both honor `page_batch_size` and `cql_batch_size` settings from the config, and both support `resume` mode backed by checkpoint files.

### 5.3 Checkpointing and resume

- A **checkpoint key** is a string that identifies a particular crawl, such as `"pages:ENG:page"` or `"cql:my_query"`.
- The client stores:
    - `start` (offset)
    - `done` (flag: `True` when the sequence is complete).
- On resume, it reads the checkpoint, picks up from `start`, and then updates the checkpoint as it progresses.

Checkpointing is **file‑based** (JSON); there is no built‑in database or distributed coordination.

### 5.4 Logging and events

Every important throttling, retry, or error decision is logged as a structured entry:

```python
self.logger.info(json.dumps({"event": "throttle_applied", "sleep_seconds": 2.1, ...}))
```

This lets you:

- chart throttling events,
- alert on repeated 429s,
- or replay and debug why a particular crawl slowed down.

***

## 6. Installation and dependencies

### 6.1 Dependencies

This library supports two implementations with different dependencies:

#### Async implementation

Required:
- Python ≥ 3.9
- `aiohttp` ≥ 3.8.0

Example `requirements.txt`:
```txt
aiohttp>=3.8.0
pydantic>=2.0.0
```

#### SDK implementation

Required:
- Python ≥ 3.9
- `atlassian-python-api` (wraps `requests`)
- `pydantic` for configuration

Example `requirements.txt`:
```txt
atlassian-python-api>=3.0.0
pydantic>=2.0.0
```

#### Installing both implementations

To support both async and SDK usage in your project:

```txt
aiohttp>=3.8.0
atlassian-python-api>=3.0.0
pydantic>=2.0.0
```

### 6.2 Installing into your project

Install the package:

```bash
pip install -e .
```

Or manually integrate the modules:

```bash
# Copy both implementations
cp src/confluence_client/client.py myapp/confluence_client/
cp src/confluence_client/sdk_client.py myapp/confluence_client/
```

Then import what you need:

```python
# Async implementation
from confluence_client.client import AsyncSafeConfluenceClient, AsyncConfluenceRateLimitConfig

# SDK implementation
from confluence_client.sdk_client import SafeConfluenceSdkClient, ConfluenceRateLimitConfig
```


***

## 7. Quick start

### 7.1 Async implementation (minimal example)

```python
import asyncio
from confluence_client.client import AsyncSafeConfluenceClient, AsyncConfluenceRateLimitConfig

async def main():
    config = AsyncConfluenceRateLimitConfig(
        max_concurrency=2,
        page_batch_size=25,
    )

    async with AsyncSafeConfluenceClient(
        base_url="https://your-domain.atlassian.net",
        username="you@example.com",
        token="your-api-token",
        config=config,
    ) as client:
        # Fetch a space
        space = await client.get_space("ENG")
        print("Space:", space.get("name"))

        # Get all pages
        pages = await client.get_all_pages_from_space("ENG", limit=50)
        print(f"Pages: {len(pages)}")

        # CQL search
        hits = await client.cql_search(
            'space = "ENG" AND type = page',
            limit=20,
        )
        print(f"Search hits: {len(hits)}")

if __name__ == "__main__":
    asyncio.run(main())
```

### 7.2 SDK implementation (minimal example)

```python
from confluence_client.sdk_client import SafeConfluenceSdkClient, ConfluenceRateLimitConfig

config = ConfluenceRateLimitConfig(
    max_workers=2,
    page_batch_size=25,
)

client = SafeConfluenceSdkClient(
    url="https://your-domain.atlassian.net",
    username="you@example.com",
    token="your-api-token",
    config=config,
)

try:
    # Fetch a space
    space = client.get_space("ENG")
    print("Space:", space.get("name"))

    # Get all pages
    pages = client.get_all_pages_from_space("ENG", limit=50)
    print(f"Pages: {len(pages)}")

    # CQL search
    hits = client.cql_search(
        'space = "ENG" AND type = page',
        limit=20,
    )
    print(f"Search hits: {len(hits)}")

finally:
    client.close()
```

### 7.3 Extended async example

```python
import asyncio
import logging

from confluence_client.client import AsyncSafeConfluenceClient, AsyncConfluenceRateLimitConfig


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    config = AsyncConfluenceRateLimitConfig(
        max_concurrency=2,
        page_batch_size=25,
        cql_batch_size=25,
        max_retries=8,
        min_request_interval_seconds=0.4,
        backoff_factor=1.0,
        backoff_jitter=0.8,
        max_backoff_seconds=300,
        low_token_threshold=3,
        timeout_seconds=60,
        checkpoint_file="confluence_checkpoint.json",
    )

    async with AsyncSafeConfluenceClient(
        base_url="https://your-domain.atlassian.net",
        username="you@example.com",
        token="your-api-token",
        config=config,
    ) as client:

        # Fetch a space
        space = await client.get_space("ENG")
        print("Space name:", space.get("name"))

        # Paginate pages from a space
        pages = await client.get_all_pages_from_space(
            "ENG",
            expand=None,
            status="current",
            limit=50,
        )
        print("Pages count:", len(pages))

        # CQL search
        hits = await client.cql_search(
            'space = "ENG" AND type = page ORDER BY lastmodified DESC',
            limit=20,
        )
        print("CQL hits:", len(hits))

        # List attachments on a page
        attachments = await client.list_attachments(
            page_id="123456",
        )
        print("Attachment count:", len(attachments.get("results", [])))

        # Download an attachment
        if attachments.get("results"):
            first = attachments["results"][0]
            path = await client.download_attachment(
                page_id="123456",
                attachment_id=first["id"],
                destination="./downloads",
            )
            print("Downloaded:", path)


if __name__ == "__main__":
    asyncio.run(main())
```


***

## 8. Detailed usage

### 8.1 Initialize and connect

#### Async client

```python
from confluence_client.client import AsyncSafeConfluenceClient, AsyncConfluenceRateLimitConfig

config = AsyncConfluenceRateLimitConfig(
    max_concurrency=2,
    page_batch_size=25,
    ...
    checkpoint_file="confluence_checkpoint.json",
)

async with AsyncSafeConfluenceClient(
    base_url="https://your-domain.atlassian.net",
    username="you@example.com",
    token="your-api-token",
    config=config,
) as client:
    ...
```

#### SDK client

```python
from confluence_client.sdk_client import SafeConfluenceSdkClient, ConfluenceRateLimitConfig

config = ConfluenceRateLimitConfig(
    max_workers=2,
    page_batch_size=25,
    ...
    checkpoint_file="confluence_checkpoint.json",
)

client = SafeConfluenceSdkClient(
    url="https://your-domain.atlassian.net",
    username="you@example.com",
    token="your-api-token",
    config=config,
)

try:
    ...
finally:
    client.close()
```

**Connection notes:**

- `base_url` (async) or `url` (SDK) must not end with a trailing slash.
- You can use:
    - `username` + `token` (API token),
    - `username` + `password` (if your instance allows it),
    - or `bearer_token` for OAuth/Bearer schemes.

The async client uses `aiohttp.ClientSession` internally, which is configured with timeout and SSL verification. The SDK client wraps the `atlassian` library's `Confluence` class.


### 8.2 Configuring rate‑limit behavior

#### Async: `AsyncConfluenceRateLimitConfig`


| Field | Default | Meaning |
| :-- | :-- | :-- |
| `max_concurrency` | 2 | Maximum concurrent HTTP requests. |
| `page_batch_size` | 50 | `limit` per page fetch; capped at 100 by Confluence. |
| `cql_batch_size` | 25 | `limit` per CQL search page. |
| `attachment_batch_size` | 25 | `limit` for attachment listing. |
| `max_retries` | 8 | Total retries for a failing request. |
| `min_request_interval_seconds` | 0.25 | Minimum spacing between requests. |
| `backoff_factor` | 1.0 | Base for exponential backoff: `delay = backoff_factor × 2^retry`. |
| `backoff_jitter` | 0.5 | Random jitter added to retry delay. |
| `max_backoff_seconds` | 300 | Ceiling on retry delay. |
| `low_token_threshold` | 3 | When `X‑RateLimit‑Remaining` drops below this, trigger proactive throttling. |
| `timeout_seconds` | 60 | `aiohttp` timeout. |
| `checkpoint_file` | `confluence_checkpoint.json` | Path to checkpoint JSON file. |

- Start with `max_concurrency` (async) or `max_workers` (SDK) = 1–2 for a production Confluence instance.
- Increase only after observing that the server is stable and not hitting 429s.
- Lower `min_request_interval_seconds` only if you are sure your instance can handle it (e.g., 0.1–0.25).

#### SDK: `ConfluenceRateLimitConfig`

Same fields as above, but used with `SafeConfluenceSdkClient`. Replace `max_concurrency` with `max_workers` for thread pool sizing.


### 8.3 Spaces

#### Async: `get_space(space_key, expand=None)`

```python
space = await client.get_space("ENG", expand="description")
print(space["name"])
```

#### SDK: `get_space(space_key, expand=None)`

```python
space = client.get_space("ENG", expand="description")
print(space["name"])
```


#### Async: `list_spaces(limit=25, start=0, expand=None)`

```python
result = await client.list_spaces(limit=50)
for space in result.get("results", []):
    print(space["key"], space["name"])
```

#### SDK: `list_spaces(limit=25, start=0, expand=None)`

```python
result = client.list_spaces(limit=50)
for space in result.get("results", []):
    print(space["key"], space["name"])
```


### 8.4 Pages and pagination

#### Async: Fetch a single page

```python
page = await client.get_page_by_id(
    page_id="123456",
    expand="body.storage,version,space"
)
print(page["title"])
```

#### SDK: Fetch a single page

```python
page = client.get_page_by_id(
    page_id="123456",
    expand="body.storage,version,space"
)
print(page["title"])
```


#### Async: Paginate all pages from a space

```python
async for page in client.iter_all_pages_from_space(
    space_key="ENG",
    expand="version",
    content_type="page",
    resume=True,  # resume from checkpoint if interrupted
):
    print(page["id"], page["title"])
```

Or collect all at once:

```python
pages = await client.get_all_pages_from_space(
    "ENG",
    status="current",
    limit=100,
)
```

#### SDK: Paginate all pages from a space

```python
for page in client.iter_all_pages_from_space(
    space_key="ENG",
    expand="version",
    content_type="page",
    resume=True,
):
    print(page["id"], page["title"])
```

Or collect all at once:

```python
pages = client.get_all_pages_from_space(
    "ENG",
    status="current",
    limit=100,
)
```


#### Async: Load page details with full body

```python
pages = await client.load_page_details_for_space(
    space_key="ENG",
    page_expand="body.storage,version",
    status="current",
)
for page in pages:
    print(page["title"], "body:", page["body"]["storage"]["value"][:100])
```

#### SDK: Load page details with full body

```python
pages = client.load_page_details_for_space(
    space_key="ENG",
    page_expand="body.storage,version",
    status="current",
)
for page in pages:
    print(page["title"], "body:", page["body"]["storage"]["value"][:100])
```


### 8.5 CQL search

#### Async: Basic CQL search

#### Async: Basic CQL search

```python
hits = await client.cql_search(
    'space = "ENG" AND type = page ORDER BY lastmodified DESC',
    limit=100,
)
```

#### SDK: Basic CQL search

```python
hits = client.cql_search(
    'space = "ENG" AND type = page ORDER BY lastmodified DESC',
    limit=100,
)
```

Both use cursor-based continuation and checkpoint support.


### 8.6 Attachments

#### Async: `list_attachments(page_id, start=0, limit=None, expand=None)`

```python
attachments = await client.list_attachments(page_id="123456")
for att in attachments.get("results", []):
    print(att["title"], att["id"])
```

#### SDK: `list_attachments(page_id, start=0, limit=None, expand=None)`

```python
attachments = client.list_attachments(page_id="123456")
for att in attachments.get("results", []):
    print(att["title"], att["id"])
```


#### Async: `get_attachment_download_url(page_id, attachment_id)`

```python
url = await client.get_attachment_download_url(
    page_id="123456",
    attachment_id="att_id"
)
print("Download URL:", url)
```

#### SDK: `get_attachment_download_url(page_id, attachment_id)`

```python
url = client.get_attachment_download_url(
    page_id="123456",
    attachment_id="att_id"
)
print("Download URL:", url)
```


#### Async: `download_attachment(page_id, attachment_id, destination, filename=None, chunk_size=...)`

```python
path = await client.download_attachment(
    page_id="123456",
    attachment_id="att_id",
    destination="./downloads",
    filename="custom_name.pdf",
)
print("Downloaded to:", path)
```

#### SDK: `download_attachment(page_id, attachment_id, destination, filename=None, chunk_size=...)`

```python
path = client.download_attachment(
    page_id="123456",
    attachment_id="att_id",
    destination="./downloads",
    filename="custom_name.pdf",
)
print("Downloaded to:", path)
```


### 8.7 Checkpointing and resume

#### Async: Using checkpoints

```python
# Iterate with checkpoint support
async for page in client.iter_all_pages_from_space("ENG", resume=True):
    print(page["id"])

# On resume, the client reads the checkpoint and picks up where it left off
```

#### SDK: Using checkpoints

```python
# Iterate with checkpoint support
for page in client.iter_all_pages_from_space("ENG", resume=True):
    print(page["id"])

# On resume, the client reads the checkpoint and picks up where it left off
```

**Checkpoint methods (both implementations):**

```python
# Load checkpoint
checkpoint = client.load_checkpoint()

# Update a checkpoint key
client.update_checkpoint("pages:ENG:page", {"start": 100, "done": False})

# Clear a specific key or all checkpoints
client.clear_checkpoint("pages:ENG:page")
client.clear_checkpoint()  # Clears all
```


### 8.8 Logging and events

Both implementations log all important events (throttles, retries, errors) as structured JSON entries.

#### Async: Structured logging

```python
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("safe_confluence_async")

# Logs appear as JSON entries like:
# {"event": "throttle_applied", "sleep_seconds": 2.1, "reason": "low_remaining", ...}
```

#### SDK: Structured logging

```python
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("safe_confluence_sdk")

# Logs appear as JSON entries like:
# {"event": "throttle_applied", "sleep_seconds": 2.1, "reason": "low_remaining", ...}
```

These logs are useful for:

- Observability: Chart when the client is throttled
- Debugging: Replay why a crawl slowed down
- Alerting: Detect repeated 429s or transport errors

***

## 9. Retry and throttling strategy

This applies to **both async and SDK implementations**.

### 9.1 Retry logic

When a request fails with 429 (Too Many Requests), 500, 502, 503, or 504, the client:

1. Extracts `Retry-After` header if present
2. Computes exponential backoff: `backoff_factor × 2^(retry_number-1)` + jitter
3. Sleeps for the computed delay
4. Retries the request
5. Fails permanently after `max_retries` attempts

### 9.2 Proactive throttling

Even before hitting 429, the client watches:

- `X-RateLimit-Remaining`: If below `low_token_threshold`, the client waits
- `X-RateLimit-FillRate` / `X-RateLimit-Interval-Seconds`: Used to estimate token refill time

This **prevents** exhausting the rate limit instead of just reacting to 429s.

### 9.3 Global pacing

Every request respects `min_request_interval_seconds`, ensuring bursty traffic is smoothed into a steady stream.

***

## 10. Error handling and observability

Both implementations raise on HTTP errors, transport errors, and resource not found (404):

```python
from requests import HTTPError

try:
    page = await client.get_page_by_id("nonexistent")
except HTTPError as e:
    print("Error:", e)
```

All errors are logged as JSON events before being raised, so you can replay the sequence of events leading up to the failure.

***

## 11. Performance and tuning

### 11.1 Async  client tuning

- **`max_concurrency`**: Control how many requests are in-flight simultaneously
  - Start with 2–3, increase gradually after monitoring the instance
- **`min_request_interval_seconds`**: Globally smooth out traffic  
  - 0.25 sec is conservative; 0.1 sec is aggressive
- **`page_batch_size` / `cql_batch_size`**: How many items per API page
  - Larger batches = fewer API calls but higher latency per call
  - Confluence caps at 100 for pages, 25–50 for CQL

### 11.2 SDK client tuning

- **`max_workers`**: Number of concurrent threads in the pool
  - Start with 2–3; threads are cheaper than you think but still bounded
- **`min_request_interval_seconds`**: Same as async
- **`page_batch_size` / `cql_batch_size`**: Same as async

### 11.3 General guidance

- Monitor `X-RateLimit-Remaining` in logs to detect if you are running out of tokens
- Use checkpoints for large crawls so you can resume if interrupted
- Run pilot tests on a staging instance before running large-scale crawls

***

## 12. Configuration reference

### Async: `AsyncConfluenceRateLimitConfig`

<div align="center">⁂</div>

[^1]: https://developer.atlassian.com/cloud/confluence/rate-limiting/

[^2]: https://confluence.atlassian.com/doc/improving-instance-stability-with-rate-limiting-992679004.html

[^3]: https://developer.atlassian.com/cloud/confluence/rest/v1/api-group-search/

[^4]: https://developer.atlassian.com/cloud/confluence/rest/v1/api-group-space/

[^5]: https://developer.atlassian.com/cloud/confluence/rest/v1/api-group-content---attachments/

[^6]: https://developers.qodex.ai/atlassian-confluence/atlassian-confluence-cloud/api-content-1/search-content-by-cql

