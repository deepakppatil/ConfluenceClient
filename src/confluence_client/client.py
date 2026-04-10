"""Core Confluence client implementation."""

import asyncio
import logging
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

import asyncio
import json
import logging
import random
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional, Union
from urllib.parse import quote

import aiohttp


@dataclass
class AsyncConfluenceRateLimitConfig:
    max_concurrency: int = 2
    page_batch_size: int = 50
    cql_batch_size: int = 25
    attachment_batch_size: int = 25
    max_retries: int = 8
    min_request_interval_seconds: float = 0.25
    backoff_factor: float = 1.0
    backoff_jitter: float = 0.5
    max_backoff_seconds: int = 300
    low_token_threshold: int = 3
    timeout_seconds: int = 60
    checkpoint_file: str = "confluence_checkpoint.json"


class AsyncSafeConfluenceClient:
    """
    Async Confluence REST client with:
    - bounded concurrency
    - global pacing
    - Retry-After support
    - exponential backoff with jitter
    - proactive rate-limit slowdown
    - safe pagination
    - checkpoint/resume support
    - structured logging hooks
    """

    def __init__(
        self,
        base_url: str,
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        bearer_token: Optional[str] = None,
        verify_ssl: bool = True,
        config: Optional[AsyncConfluenceRateLimitConfig] = None,
        default_headers: Optional[Dict[str, str]] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.config = config or AsyncConfluenceRateLimitConfig()
        self.verify_ssl = verify_ssl
        self.logger = logger or logging.getLogger("async_confluence")

        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(self.config.max_concurrency)
        self._pace_lock = asyncio.Lock()
        self._checkpoint_lock = asyncio.Lock()
        self._next_allowed_time = 0.0

        self._default_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if default_headers:
            self._default_headers.update(default_headers)

        self._auth = None
        if bearer_token:
            self._default_headers["Authorization"] = f"Bearer {bearer_token}"
        elif token and username:
            self._auth = aiohttp.BasicAuth(username, token)
        elif username and password:
            self._auth = aiohttp.BasicAuth(username, password)

    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)
        connector = aiohttp.TCPConnector(ssl=self.verify_ssl)
        self._session = aiohttp.ClientSession(
            timeout=timeout,
            auth=self._auth,
            headers=self._default_headers,
            connector=connector,
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._session:
            await self._session.close()

    # ---------- spaces ----------

    async def get_space(self, space_key: str, expand: Optional[str] = None) -> Dict[str, Any]:
        params = {}
        if expand:
            params["expand"] = expand
        return await self._request_json("GET", f"/rest/api/space/{quote(space_key)}", params=params)

    async def list_spaces(self, limit: int = 25, start: int = 0, expand: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {"limit": limit, "start": start}
        if expand:
            params["expand"] = expand
        return await self._request_json("GET", "/rest/api/space", params=params)

    # ---------- content/pages ----------

    async def get_page_by_id(
        self,
        page_id: Union[str, int],
        expand: str = "history,space,version",
        status: Optional[str] = None,
        version: Optional[int] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"expand": expand}
        if status is not None:
            params["status"] = status
        if version is not None:
            params["version"] = version
        return await self._request_json("GET", f"/rest/api/content/{quote(str(page_id))}", params=params)

    async def iter_all_pages_from_space(
        self,
        space_key: str,
        *,
        expand: Optional[str] = None,
        status: Optional[str] = None,
        content_type: str = "page",
        resume: bool = False,
        checkpoint_key: Optional[str] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        state_key = checkpoint_key or f"pages:{space_key}:{content_type}"
        checkpoint = await self.load_checkpoint() if resume else {}
        start = int(checkpoint.get(state_key, {}).get("start", 0))

        batch_size = min(self.config.page_batch_size, 100)

        while True:
            params: Dict[str, Any] = {
                "spaceKey": space_key,
                "type": content_type,
                "start": start,
                "limit": batch_size,
            }
            if expand:
                params["expand"] = expand
            if status:
                params["status"] = status

            payload = await self._request_json("GET", "/rest/api/content", params=params)
            batch = payload.get("results", [])

            if not batch:
                if resume:
                    await self.update_checkpoint(state_key, {"start": start, "done": True})
                return

            for item in batch:
                yield item

            start += len(batch)

            if resume:
                await self.update_checkpoint(state_key, {"start": start, "done": False})

            if len(batch) < batch_size:
                if resume:
                    await self.update_checkpoint(state_key, {"start": start, "done": True})
                return

    async def get_all_pages_from_space(
        self,
        space_key: str,
        *,
        expand: Optional[str] = None,
        status: Optional[str] = None,
        content_type: str = "page",
        limit: Optional[int] = None,
        resume: bool = False,
        checkpoint_key: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        async for item in self.iter_all_pages_from_space(
            space_key=space_key,
            expand=expand,
            status=status,
            content_type=content_type,
            resume=resume,
            checkpoint_key=checkpoint_key,
        ):
            results.append(item)
            if limit is not None and len(results) >= limit:
                break
        return results

    async def load_page_details_for_space(
        self,
        space_key: str,
        *,
        page_expand: str = "body.storage,version,space",
        status: Optional[str] = None,
        content_type: str = "page",
        limit: Optional[int] = None,
        resume: bool = False,
        checkpoint_key: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        pages = await self.get_all_pages_from_space(
            space_key=space_key,
            expand=None,
            status=status,
            content_type=content_type,
            limit=limit,
            resume=resume,
            checkpoint_key=checkpoint_key,
        )

        tasks = [
            asyncio.create_task(
                self.get_page_by_id(
                    page_id=str(page["id"]),
                    expand=page_expand,
                    status=status,
                )
            )
            for page in pages
        ]

        results: List[Dict[str, Any]] = []
        for coro in asyncio.as_completed(tasks):
            results.append(await coro)
        return results

    # ---------- cql search ----------

    async def cql_search(
        self,
        cql: str,
        *,
        limit: Optional[int] = None,
        expand: Optional[str] = None,
        cqlcontext: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        batch_size = self.config.cql_batch_size

        while True:
            params: Dict[str, Any] = {"cql": cql, "limit": batch_size}
            if expand:
                params["expand"] = expand
            if cqlcontext:
                params["cqlcontext"] = cqlcontext
            if cursor:
                params["cursor"] = cursor

            payload = await self._request_json("GET", "/rest/api/search", params=params)
            batch = payload.get("results", [])
            results.extend(batch)

            if limit is not None and len(results) >= limit:
                return results[:limit]

            links = payload.get("_links", {})
            next_link = links.get("next")
            cursor = self._extract_cursor(next_link)

            if not batch or not cursor:
                return results

    async def cql_search_iter(
        self,
        cql: str,
        *,
        expand: Optional[str] = None,
        cqlcontext: Optional[str] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        cursor: Optional[str] = None
        batch_size = self.config.cql_batch_size

        while True:
            params: Dict[str, Any] = {"cql": cql, "limit": batch_size}
            if expand:
                params["expand"] = expand
            if cqlcontext:
                params["cqlcontext"] = cqlcontext
            if cursor:
                params["cursor"] = cursor

            payload = await self._request_json("GET", "/rest/api/search", params=params)
            batch = payload.get("results", [])

            if not batch:
                return

            for item in batch:
                yield item

            cursor = self._extract_cursor(payload.get("_links", {}).get("next"))
            if not cursor:
                return

    # ---------- attachments ----------

    async def list_attachments(
        self,
        page_id: Union[str, int],
        *,
        start: int = 0,
        limit: Optional[int] = None,
        expand: Optional[str] = None,
    ) -> Dict[str, Any]:
        effective_limit = limit or self.config.attachment_batch_size
        params: Dict[str, Any] = {"start": start, "limit": effective_limit}
        if expand:
            params["expand"] = expand

        return await self._request_json(
            "GET",
            f"/rest/api/content/{quote(str(page_id))}/child/attachment",
            params=params,
        )

    async def get_attachment_download_url(
        self,
        page_id: Union[str, int],
        attachment_id: Union[str, int],
    ) -> str:
        payload = await self._request_json(
            "GET",
            f"/rest/api/content/{quote(str(page_id))}/child/attachment/{quote(str(attachment_id))}/download",
        )

        download_link = payload.get("_links", {}).get("download") or payload.get("downloadLink")
        if not download_link:
            raise RuntimeError("Attachment download URL not found in response")

        if download_link.startswith("http://") or download_link.startswith("https://"):
            return download_link

        return f"{self.base_url}{download_link}"

    async def download_attachment(
        self,
        page_id: Union[str, int],
        attachment_id: Union[str, int],
        destination: Union[str, Path],
        *,
        filename: Optional[str] = None,
        chunk_size: int = 1024 * 128,
    ) -> Path:
        destination_path = Path(destination)
        destination_path.mkdir(parents=True, exist_ok=True)

        download_url = await self.get_attachment_download_url(page_id, attachment_id)

        metadata = await self._request_json(
            "GET",
            f"/rest/api/content/{quote(str(page_id))}/child/attachment/{quote(str(attachment_id))}",
        )
        resolved_name = filename or metadata.get("title") or f"{attachment_id}.bin"
        output_file = destination_path / resolved_name

        response = await self._request_absolute("GET", download_url)
        try:
            with output_file.open("wb") as fh:
                async for chunk in response.content.iter_chunked(chunk_size):
                    fh.write(chunk)
        finally:
            await response.release()

        return output_file

    # ---------- checkpointing ----------

    async def load_checkpoint(self) -> Dict[str, Any]:
        path = Path(self.config.checkpoint_file)
        if not path.exists():
            return {}
        async with self._checkpoint_lock:
            return json.loads(path.read_text(encoding="utf-8"))

    async def update_checkpoint(self, key: str, value: Dict[str, Any]) -> None:
        path = Path(self.config.checkpoint_file)
        async with self._checkpoint_lock:
            current = {}
            if path.exists():
                current = json.loads(path.read_text(encoding="utf-8"))
            current[key] = value
            path.write_text(json.dumps(current, indent=2, sort_keys=True), encoding="utf-8")

    async def clear_checkpoint(self, key: Optional[str] = None) -> None:
        path = Path(self.config.checkpoint_file)
        async with self._checkpoint_lock:
            if not path.exists():
                return
            if key is None:
                path.unlink(missing_ok=True)
                return
            current = json.loads(path.read_text(encoding="utf-8"))
            current.pop(key, None)
            path.write_text(json.dumps(current, indent=2, sort_keys=True), encoding="utf-8")

    # ---------- request core ----------

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        response = await self._request(method, path, params=params, json_body=json_body)
        try:
            return await response.json()
        finally:
            await response.release()

    async def _request_absolute(self, method: str, url: str) -> aiohttp.ClientResponse:
        return await self._request(method, None, absolute_url=url)

    async def _request(
        self,
        method: str,
        path: Optional[str],
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        absolute_url: Optional[str] = None,
    ) -> aiohttp.ClientResponse:
        if not self._session:
            raise RuntimeError("Client session is not initialized. Use 'async with'.")

        retries = 0
        last_delay = self.config.backoff_factor
        url = absolute_url or f"{self.base_url}{path}"

        while True:
            async with self._semaphore:
                await self._wait_for_turn()

                try:
                    response = await self._session.request(
                        method=method,
                        url=url,
                        params=params,
                        json=json_body,
                    )

                    if response.status < 400:
                        await self._apply_header_based_throttling(response.headers, url=url)
                        await self._enforce_min_spacing()
                        return response

                    if response.status in (429, 500, 502, 503, 504):
                        if retries >= self.config.max_retries:
                            text = await response.text()
                            self._log_event(
                                "request_failed_after_retries",
                                {
                                    "method": method,
                                    "url": url,
                                    "status": response.status,
                                    "retries": retries,
                                    "body": text[:1000],
                                },
                                level="error",
                            )
                            raise RuntimeError(
                                f"Confluence request failed after retries: "
                                f"{response.status} {text}"
                            )

                        delay = self._compute_retry_delay(
                            headers=response.headers,
                            retry_number=retries + 1,
                            last_delay=last_delay,
                        )
                        retries += 1
                        last_delay = delay

                        self._log_event(
                            "retry_scheduled",
                            {
                                "method": method,
                                "url": url,
                                "status": response.status,
                                "retry_number": retries,
                                "delay_seconds": round(delay, 3),
                            },
                            level="warning",
                        )

                        await response.release()
                        await asyncio.sleep(delay)
                        continue

                    text = await response.text()
                    self._log_event(
                        "request_failed",
                        {
                            "method": method,
                            "url": url,
                            "status": response.status,
                            "body": text[:1000],
                        },
                        level="error",
                    )
                    raise RuntimeError(f"Confluence request failed: {response.status} {text}")

                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    if retries >= self.config.max_retries:
                        self._log_event(
                            "transport_failed_after_retries",
                            {
                                "method": method,
                                "url": url,
                                "retries": retries,
                                "error": repr(exc),
                            },
                            level="error",
                        )
                        raise RuntimeError(f"Transport failure after retries: {exc}") from exc

                    delay = min(
                        self.config.backoff_factor * (2 ** retries) + random.uniform(0, self.config.backoff_jitter),
                        self.config.max_backoff_seconds,
                    )
                    retries += 1
                    last_delay = delay

                    self._log_event(
                        "transport_retry_scheduled",
                        {
                            "method": method,
                            "url": url,
                            "retry_number": retries,
                            "delay_seconds": round(delay, 3),
                            "error": repr(exc),
                        },
                        level="warning",
                    )
                    await asyncio.sleep(delay)

    # ---------- pacing + throttling ----------

    async def _wait_for_turn(self):
        async with self._pace_lock:
            now = time.monotonic()
            sleep_for = max(0.0, self._next_allowed_time - now)

        if sleep_for > 0:
            await asyncio.sleep(sleep_for)

    async def _enforce_min_spacing(self):
        async with self._pace_lock:
            self._next_allowed_time = max(self._next_allowed_time, time.monotonic()) + \
                                      self.config.min_request_interval_seconds

    async def _apply_header_based_throttling(self, headers, *, url: str):
        retry_after = self._get_int_header(headers, "Retry-After")
        if retry_after is None:
            retry_after = self._get_int_header(headers, "retry-after")
        if retry_after is None:
            retry_after = self._get_int_header(headers, "Beta-Retry-After")

        remaining = self._get_int_header(headers, "X-RateLimit-Remaining")
        if remaining is None:
            remaining = self._get_int_header(headers, "X-Beta-RateLimit-Remaining")

        fill_rate = self._get_int_header(headers, "X-RateLimit-FillRate")
        interval_seconds = self._get_int_header(headers, "X-RateLimit-Interval-Seconds")

        additional_sleep = 0.0
        reason = None

        if retry_after is not None and retry_after > 0:
            additional_sleep = max(additional_sleep, float(retry_after))
            reason = "retry_after"

        if remaining is not None and remaining <= self.config.low_token_threshold:
            if retry_after is not None and retry_after > 0:
                additional_sleep = max(additional_sleep, float(retry_after))
                reason = "low_remaining_with_retry_after"
            elif fill_rate and interval_seconds:
                token_refill_time = float(interval_seconds) / float(max(fill_rate, 1))
                additional_sleep = max(additional_sleep, token_refill_time)
                reason = "low_remaining_with_refill_math"
            else:
                additional_sleep = max(additional_sleep, 1.0)
                reason = "low_remaining_fallback"

        if additional_sleep > 0:
            additional_sleep += random.uniform(0, self.config.backoff_jitter)
            async with self._pace_lock:
                self._next_allowed_time = max(
                    self._next_allowed_time,
                    time.monotonic() + additional_sleep
                )

            self._log_event(
                "throttle_applied",
                {
                    "url": url,
                    "reason": reason,
                    "sleep_seconds": round(additional_sleep, 3),
                    "retry_after": retry_after,
                    "remaining": remaining,
                    "fill_rate": fill_rate,
                    "interval_seconds": interval_seconds,
                },
                level="info",
            )

    def _compute_retry_delay(self, headers, retry_number: int, last_delay: float) -> float:
        retry_after = self._get_int_header(headers, "Retry-After")
        if retry_after is None:
            retry_after = self._get_int_header(headers, "retry-after")
        if retry_after is None:
            retry_after = self._get_int_header(headers, "Beta-Retry-After")

        if retry_after is not None and retry_after > 0:
            return min(
                retry_after + random.uniform(0, self.config.backoff_jitter),
                self.config.max_backoff_seconds,
            )

        exponential = self.config.backoff_factor * (2 ** (retry_number - 1))
        delay = max(exponential, last_delay)
        delay += random.uniform(0, self.config.backoff_jitter)
        return min(delay, self.config.max_backoff_seconds)

    @staticmethod
    def _get_int_header(headers, key: str) -> Optional[int]:
        value = headers.get(key)
        if value is None:
            return None
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _extract_cursor(next_link: Optional[str]) -> Optional[str]:
        if not next_link or "cursor=" not in next_link:
            return None
        tail = next_link.split("cursor=", 1)[1]
        return tail.split("&", 1)[0]

    def _log_event(self, event_name: str, payload: Dict[str, Any], *, level: str = "info") -> None:
        record = {"event": event_name, **payload}
        message = json.dumps(record, default=str)
        if level == "warning":
            self.logger.warning(message)
        elif level == "error":
            self.logger.error(message)
        else:
            self.logger.info(message)