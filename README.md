# `Confluence-client` – Throttling‑safe, search‑ready, and checkpoint‑aware Confluence  client

This repository provides an **async `aiohttp`‑based Confluence REST client** designed specifically to avoid overloading your Confluence server while still enabling large‑scale page loading, CQL search, and attachment handling. The implementation honors Confluence’s rate‑limiting behavior, including `Retry‑After`, exponential backoff with jitter, and proactive throttling based on `X‑RateLimit‑*` headers, and offers:

- safe pagination,
- CQL search with `cursor`‑based continuation,
- attachment listing and streaming download,
- filesystem‑based checkpointing and resume support,
- and structured logging for throttling and retry events.

Atlassian’s Confluence Cloud REST API documents rate‑limiting headers such as `Retry‑After`, `X‑RateLimit‑Remaining`, and `X‑RateLimit‑FillRate`, and recommends that clients respond to backpressure instead of blindly retrying or firing requests into a throttled service. This library is built around that pattern, using `aiohttp` and `asyncio` to give you fine‑grained control over concurrency, pacing, and backoff.[^1][^2]
**Status & Support**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.9+](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![Build Status](https://img.shields.io/github/actions/workflow/status/yourusername/async-confluence-client/tests.yml?branch=main&logo=github)](https://github.com/yourusername/async-confluence-client/actions)
[![PyPI version](https://img.shields.io/pypi/v/async-confluence-client.svg?logo=pypi)](https://pypi.org/project/async-confluence-client/)
[![async/await](https://img.shields.io/badge/async-await-green.svg)](https://docs.python.org/3/library/asyncio.html)
[![aiohttp](https://img.shields.io/badge/built--with-aiohttp-blue.svg)](https://docs.aiohttp.org/)
[![codecov](https://img.shields.io/codecov/c/github/yourusername/async-confluence-client?logo=codecov)](https://codecov.io/gh/yourusername/async-confluence-client)
***

## Table of contents

1. [Overview and design goals](#1-overview-and-design-goals)
2. [What this client does](#2-what-this-client-does)
3. [Key components](#3-key-components)
4. [Concepts and terminology](#4-concepts-and-terminology)
5. [Installation and dependencies](#5-installation-and-dependencies)
6. [Quick start](#6-quick-start)
7. [Detailed usage](#7-detailed-usage)
    - 7.1 Initialize and connect
    - 7.2 Configuring rate‑limit behavior
    - 7.3 Spaces
    - 7.4 Pages and pagination
    - 7.5 CQL search
    - 7.6 Attachments
    - 7.7 Checkpointing and resume
    - 7.8 Logging and events
8. [Retry and throttling strategy](#8-retry-and-throttling-strategy)
9. [Error handling and observability](#9-error-handling-and-observability)
10. [Performance and tuning](#10-performance-and-tuning)
11. [Configuration reference](#11-configuration-reference)
12. [Security and authentication](#12-security-and-authentication)
13. [Extending the client](#13-extending-the-client)
14. [Testing and validation](#14-testing-and-validation)
15. [Limitations and caveats](#15-limitations-and-caveats)
16. [License and attribution](#16-license-and-attribution)

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

## 2. What this client does

The client offers:


| Feature | Description |
| :-- | :-- |
| Bounded concurrency | Maximum concurrent HTTP requests controlled by an `asyncio.Semaphore`. |
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

## 4. Concepts and terminology

### 4.1 Tokens and rate limits

- `Retry‑After` / `retry‑after`
Seconds you should wait before retrying; the client will sleep this many seconds plus jitter.[^1]
- `X‑RateLimit‑Remaining`
How many tokens you have left in the current window; if below `low_token_threshold`, the client will slow down, optionally using `X‑RateLimit‑FillRate` and `X‑RateLimit‑Interval‑Seconds` to estimate when tokens refill.
- `X‑RateLimit‑FillRate` / `X‑RateLimit‑Interval‑Seconds`
Periodic refill information that can be used to compute a “token refill time” if `Retry‑After` is missing.[^1]


### 4.2 Pagination and `cursor`

Confluence uses:

- `start`/`limit` for content pagination, and
- `cursor` for search‑based continuation.

This client exposes:

- `iter_all_pages_from_space` / `get_all_pages_from_space` for `start`/`limit` sequences, and
- `cql_search` / `cql_search_iter` for `cursor`‑based search continuation.

Both honor `page_batch_size` and `cql_batch_size` settings from the config, and both support `resume` mode backed by checkpoint files.

### 4.3 Checkpointing and resume

- A **checkpoint key** is a string that identifies a particular crawl, such as `"pages:ENG:page"` or `"cql:my_query"`.
- The client stores:
    - `start` (offset)
    - `done` (flag: `True` when the sequence is complete).
- On resume, it reads the checkpoint, picks up from `start`, and then updates the checkpoint as it progresses.

Checkpointing is **file‑based** (JSON); there is no built‑in database or distributed coordination.

### 4.4 Logging and events

Every important throttling, retry, or error decision is logged as a structured entry:

```python
self.logger.info(json.dumps({"event": "throttle_applied", "sleep_seconds": 2.1, ...}))
```

This lets you:

- chart throttling events,
- alert on repeated 429s,
- or replay and debug why a particular crawl slowed down.

***

## 5. Installation and dependencies

### 5.1 Dependencies

This client requires:

- Python ≥ 3.8
- `aiohttp` for async HTTP

Example `requirements.txt`:

```txt
aiohttp>=3.8.0
```

There is **no dependency** on the `atlassian-python-api` SDK; this client uses raw REST calls.

### 5.2 Installing into your project

Clone or copy the module into your project, for example:

```bash
mkdir -p myapp/confluence_client
cp async_confluence_client.py myapp/confluence_client/__init__.py
```

Then in your code:

```python
from myapp.confluence_client import AsyncSafeConfluenceClient, AsyncConfluenceRateLimitConfig
```


***

## 6. Quick start

### 6.1 Minimal example

```python
import asyncio
import logging

from myapp.confluence_client import AsyncSafeConfluenceClient, AsyncConfluenceRateLimitConfig


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
        base_url="https://your-domain.atlassian.net/wiki",
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
            first = attachments["results"][^0]
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

## 7. Detailed usage

### 7.1 Initialize and connect

```python
config = AsyncConfluenceRateLimitConfig(
    max_concurrency=2,
    page_batch_size=25,
    ...
    checkpoint_file="confluence_checkpoint.json",
)

async with AsyncSafeConfluenceClient(
    base_url="https://your-domain.atlassian.net/wiki",
    username="you@example.com",
    token="your-api-token",
    config=config,
) as client:
    ...
```

- `base_url` must not end with a trailing slash.
- You can use:
    - `username` + `token` (API token),
    - `username` + `password` (if your instance allows it),
    - or `bearer_token` for OAuth/Bearer schemes.

The client uses `aiohttp.ClientSession` internally, which is configured with:

- a fixed timeout,
- optional SSL verification,
- and the chosen auth method.


### 7.2 Configuring rate‑limit behavior

`AsyncConfluenceRateLimitConfig` fields:


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

Tuning guidance:

- Start with `max_concurrency` = 1–2 for a production Confluence instance.
- Increase only after observing that the server is stable and not hitting 429s.
- Lower `min_request_interval_seconds` only if you are sure your instance and network can handle it (e.g., 0.1–0.25).


### 7.3 Spaces

#### `get_space(space_key, expand=None)`

Retrieves a single space, optionally expanded.

```python
space = await client.get_space("ENG", expand="description")
print(space["name"], space.get("description"))
```

Maps to:

```bash
GET /rest/api/space/{spaceKey}
```

with optional `expand` query parameter.[^4]

#### `list_spaces(limit=25, start=0, expand=None)`

Fetches a page of spaces:

```python
page = await client.list_spaces(limit=50)
for s in page.get("results", []):
    print(s["key"], s["name"])
```

This is the building block for scanning all spaces if needed.

***

### 7.4 Pages and pagination

#### `get_page_by_id(page_id, expand=..., status=None, version=None)`

```python
page = await client.get_page_by_id("123456", expand="body.storage,version,space")
print(page["title"])
```

Maps to:

```bash
GET /rest/api/content/{id}
```

with `expand`, `status`, and `version` as query params.

#### `iter_all_pages_from_space(..., resume=False, checkpoint_key=None)`

Asynchronous generator that pages over `start`/`limit`:

```python
async for page in client.iter_all_pages_from_space(
    "ENG",
    expand=None,
    status="current",
    content_type="page",
    resume=True,
    checkpoint_key="pages:ENG:page",
):
    ...
```

- `resume=True` reads `checkpoint_file` and resumes from the last `start` offset.
- `checkpoint_key` defaults to `"pages:{space_key}:{content_type}"`.

This is the safest way to crawl a large space, because it:

- respects `page_batch_size`,
- writes checkpoints after each page batch,
- and can be interrupted and restarted.


#### `get_all_pages_from_space(..., resume=False, checkpoint_key=None)`

Convenience method that consumes the async iterator into a list:

```python
pages = await client.get_all_pages_from_space(
    "ENG",
    expand=None,
    status="current",
    limit=1000,
)
```

- `limit` stops early if you only want a sample.
- `resume` and `checkpoint_key` behave exactly as in the iterator method.


#### `load_page_details_for_space(...)`

First retrieves page stubs, then loads full details in parallel, bounded by `max_concurrency`:

```python
details = await client.load_page_details_for_space(
    "ENG",
    page_expand="body.storage,version,space",
    status="current",
    limit=50,
)
```

- This is very useful for extracting full page bodies when you only need bodies for a subset of pages.

***

### 7.5 CQL search

Confluence Cloud exposes CQL‑based search via:

```bash
GET /rest/api/search
```

with `cql`, `limit`, and `cursor` parameters.[^6][^3]

#### `cql_search(cql, limit=None, expand=None, cqlcontext=None)`

Synchronous‑style wrapper that returns a list:

```python
hits = await client.cql_search(
    'space = "ENG" AND type = page ORDER BY lastmodified DESC',
    limit=100,
)
```

- Internally uses `cursor` to continue until the requested `limit` is

<div align="center">⁂</div>

[^1]: https://developer.atlassian.com/cloud/confluence/rate-limiting/

[^2]: https://confluence.atlassian.com/doc/improving-instance-stability-with-rate-limiting-992679004.html

[^3]: https://developer.atlassian.com/cloud/confluence/rest/v1/api-group-search/

[^4]: https://developer.atlassian.com/cloud/confluence/rest/v1/api-group-space/

[^5]: https://developer.atlassian.com/cloud/confluence/rest/v1/api-group-content---attachments/

[^6]: https://developers.qodex.ai/atlassian-confluence/atlassian-confluence-cloud/api-content-1/search-content-by-cql

