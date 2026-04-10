import json
import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Union
from urllib.parse import urlparse, parse_qs

from requests import HTTPError
from atlassian import Confluence


@dataclass
class ConfluenceRateLimitConfig:
    max_workers: int = 2
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


class SafeConfluenceSdkClient:
    """
    SDK-based Confluence client with:
    - raw response access via advanced_mode=True
    - bounded concurrency
    - global pacing
    - Retry-After support
    - exponential backoff with jitter
    - proactive throttling from X-RateLimit-* headers
    - safe pagination
    - CQL search
    - attachment listing and download
    - checkpoint/resume support
    - structured logging
    """

    def __init__(
        self,
        url: str,
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        api_version: str = "latest",
        cloud: bool = False,
        verify_ssl: bool = True,
        config: Optional[ConfluenceRateLimitConfig] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.config = config or ConfluenceRateLimitConfig()
        self.logger = logger or logging.getLogger("safe_confluence_sdk")

        self._pace_lock = threading.Lock()
        self._checkpoint_lock = threading.Lock()
        self._semaphore = threading.BoundedSemaphore(self.config.max_workers)
        self._next_allowed_time = 0.0

        self.confluence = Confluence(
            url=url,
            username=username,
            password=password,
            token=token,
            api_version=api_version,
            cloud=cloud,
            verify_ssl=verify_ssl,
            timeout=self.config.timeout_seconds,
            advanced_mode=True,
            backoff_and_retry=False,
        )

    # ---------- spaces ----------

    def get_space(self, space_key: str, expand: Optional[str] = None) -> Dict[str, Any]:
        params = {}
        if expand:
            params["expand"] = expand

        response = self._request_json(
            "GET",
            f"rest/api/space/{space_key}",
            params=params,
            absolute=False,
        )
        return response

    def list_spaces(self, limit: int = 25, start: int = 0, expand: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {"limit": limit, "start": start}
        if expand:
            params["expand"] = expand

        return self._request_json("GET", "rest/api/space", params=params)

    # ---------- pages ----------

    def get_page_by_id(
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

        return self._request_json(
            "GET",
            f"rest/api/content/{page_id}",
            params=params,
        )

    def iter_all_pages_from_space(
        self,
        space_key: str,
        *,
        expand: Optional[str] = None,
        status: Optional[str] = None,
        content_type: str = "page",
        resume: bool = False,
        checkpoint_key: Optional[str] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        state_key = checkpoint_key or f"pages:{space_key}:{content_type}"
        checkpoint = self.load_checkpoint() if resume else {}
        start = int(checkpoint.get(state_key, {}).get("start", 0))
        batch_size = min(self.config.page_batch_size, 100)

        while True:
            batch = self._request_json_via_sdk_method(
                lambda: self.confluence.get_all_pages_from_space(
                    space=space_key,
                    start=start,
                    limit=batch_size,
                    status=status,
                    expand=expand,
                    content_type=content_type,
                    advanced_mode=True,
                )
            )

            results = batch if isinstance(batch, list) else batch.get("results", [])

            if not results:
                if resume:
                    self.update_checkpoint(state_key, {"start": start, "done": True})
                return

            for item in results:
                yield item

            start += len(results)

            if resume:
                self.update_checkpoint(state_key, {"start": start, "done": False})

            if len(results) < batch_size:
                if resume:
                    self.update_checkpoint(state_key, {"start": start, "done": True})
                return

    def get_all_pages_from_space(
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
        for item in self.iter_all_pages_from_space(
            space_key=space_key,
            expand=expand,
            status=status,
            content_type=content_type,
            resume=resume,
            checkpoint_key=checkpoint_key,
        ):
            results.append(item)
            if limit is not None and len(results) >= limit:
                return results[:limit]
        return results

    def load_page_details_for_space(
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
        pages = self.get_all_pages_from_space(
            space_key=space_key,
            expand=None,
            status=status,
            content_type=content_type,
            limit=limit,
            resume=resume,
            checkpoint_key=checkpoint_key,
        )

        results: List[Dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = [
                executor.submit(
                    self.get_page_by_id,
                    page["id"],
                    page_expand,
                    status,
                    None,
                )
                for page in pages
            ]
            for future in as_completed(futures):
                results.append(future.result())
        return results

    # ---------- cql ----------

    def cql_search(
        self,
        cql: str,
        *,
        limit: Optional[int] = None,
        expand: Optional[str] = None,
        include_archived_spaces: Optional[bool] = None,
        excerpt: Optional[str] = None,
        resume: bool = False,
        checkpoint_key: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        state_key = checkpoint_key or f"cql:{cql}"
        checkpoint = self.load_checkpoint() if resume else {}
        start = int(checkpoint.get(state_key, {}).get("start", 0))
        batch_size = self.config.cql_batch_size

        while True:
            payload = self._request_json_via_sdk_method(
                lambda s=start: self.confluence.cql(
                    cql=cql,
                    start=s,
                    limit=batch_size,
                    expand=expand,
                    include_archived_spaces=include_archived_spaces,
                    excerpt=excerpt,
                    advanced_mode=True,
                )
            )

            batch = payload.get("results", []) if isinstance(payload, dict) else payload

            if not batch:
                if resume:
                    self.update_checkpoint(state_key, {"start": start, "done": True})
                return results

            results.extend(batch)
            start += len(batch)

            if resume:
                self.update_checkpoint(state_key, {"start": start, "done": False})

            if limit is not None and len(results) >= limit:
                return results[:limit]

            next_cursor = self._extract_cursor_from_links(payload)
            if next_cursor:
                payload = self._request_json(
                    "GET",
                    "rest/api/search",
                    params={
                        "cql": cql,
                        "cursor": next_cursor,
                        "limit": batch_size,
                        **({"expand": expand} if expand else {}),
                        **({"excerpt": excerpt} if excerpt else {}),
                        **(
                            {"includeArchivedSpaces": include_archived_spaces}
                            if include_archived_spaces is not None else {}
                        ),
                    },
                )
                batch = payload.get("results", [])
                if not batch:
                    if resume:
                        self.update_checkpoint(state_key, {"start": start, "done": True})
                    return results
                results.extend(batch)
                start += len(batch)
                if resume:
                    self.update_checkpoint(state_key, {"start": start, "done": False})
                if limit is not None and len(results) >= limit:
                    return results[:limit]
                if not self._extract_cursor_from_links(payload):
                    if resume:
                        self.update_checkpoint(state_key, {"start": start, "done": True})
                    return results
            else:
                if len(batch) < batch_size:
                    if resume:
                        self.update_checkpoint(state_key, {"start": start, "done": True})
                    return results

    # ---------- attachments ----------

    def list_attachments(
        self,
        page_id: Union[str, int],
        *,
        start: int = 0,
        limit: Optional[int] = None,
        expand: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self._request_json_via_sdk_method(
            lambda: self.confluence.get_attachments_from_content(
                page_id=page_id,
                start=start,
                limit=limit or self.config.attachment_batch_size,
                expand=expand,
                advanced_mode=True,
            )
        )

    def get_attachment_download_url(
        self,
        page_id: Union[str, int],
        attachment_id: Union[str, int],
    ) -> str:
        payload = self._request_json(
            "GET",
            f"rest/api/content/{page_id}/child/attachment/{attachment_id}/download",
        )
        download_link = payload.get("_links", {}).get("download") or payload.get("downloadLink")
        if not download_link:
            raise RuntimeError("Attachment download URL not found in response")
        if download_link.startswith("http://") or download_link.startswith("https://"):
            return download_link
        return f"{self.confluence.url.rstrip('/')}{download_link}"

    def download_attachment(
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

        metadata = self._request_json(
            "GET",
            f"rest/api/content/{page_id}/child/attachment/{attachment_id}",
        )
        resolved_name = filename or metadata.get("title") or f"{attachment_id}.bin"
        output_file = destination_path / resolved_name

        download_url = self.get_attachment_download_url(page_id, attachment_id)
        response = self._request_raw("GET", download_url, absolute=True, stream=True)

        with output_file.open("wb") as fh:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    fh.write(chunk)

        return output_file

    # ---------- checkpointing ----------

    def load_checkpoint(self) -> Dict[str, Any]:
        path = Path(self.config.checkpoint_file)
        if not path.exists():
            return {}
        with self._checkpoint_lock:
            return json.loads(path.read_text(encoding="utf-8"))

    def update_checkpoint(self, key: str, value: Dict[str, Any]) -> None:
        path = Path(self.config.checkpoint_file)
        with self._checkpoint_lock:
            current = {}
            if path.exists():
                current = json.loads(path.read_text(encoding="utf-8"))
            current[key] = value
            path.write_text(json.dumps(current, indent=2, sort_keys=True), encoding="utf-8")

    def clear_checkpoint(self, key: Optional[str] = None) -> None:
        path = Path(self.config.checkpoint_file)
        with self._checkpoint_lock:
            if not path.exists():
                return
            if key is None:
                path.unlink(missing_ok=True)
                return
            current = json.loads(path.read_text(encoding="utf-8"))
            current.pop(key, None)
            path.write_text(json.dumps(current, indent=2, sort_keys=True), encoding="utf-8")

    # ---------- internal request handling ----------

    def _request_json_via_sdk_method(self, fn):
        response = self._call_with_control(fn)
        if hasattr(response, "json"):
            return response.json()
        return response

    def _request_json(self, method: str, path: str, *, params=None, data=None, absolute=False):
        response = self._request_raw(method, path, params=params, data=data, absolute=absolute)
        return response.json()

    def _request_raw(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None,
        absolute: bool = False,
        stream: bool = False,
    ):
        return self._call_with_control(
            lambda: self.confluence.request(
                method=method,
                path=path,
                params=params,
                data=data,
                absolute=absolute,
                advanced_mode=True,
                not_json_response=stream,
            )
        )

    def _call_with_control(self, fn):
        retries = 0
        last_delay = self.config.backoff_factor

        while True:
            with self._semaphore:
                self._wait_for_turn()
                try:
                    response = fn()

                    if hasattr(response, "headers"):
                        self._apply_header_based_throttling(response.headers, url=str(getattr(response, "url", "")))
                        self._enforce_min_spacing()
                        response.raise_for_status()
                        return response

                    self._enforce_min_spacing()
                    return response

                except HTTPError as exc:
                    response = getattr(exc, "response", None)
                    if response is None:
                        raise

                    if response.status_code in (429, 500, 502, 503, 504):
                        if retries >= self.config.max_retries:
                            self._log_event(
                                "request_failed_after_retries",
                                {
                                    "status": response.status_code,
                                    "retries": retries,
                                    "url": str(getattr(response, "url", "")),
                                    "body": response.text[:1000] if hasattr(response, "text") else "",
                                },
                                level="error",
                            )
                            raise

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
                                "status": response.status_code,
                                "retry_number": retries,
                                "delay_seconds": round(delay, 3),
                                "url": str(getattr(response, "url", "")),
                            },
                            level="warning",
                        )
                        time.sleep(delay)
                        continue

                    raise

                except Exception as exc:
                    if retries >= self.config.max_retries:
                        self._log_event(
                            "transport_failed_after_retries",
                            {"retries": retries, "error": repr(exc)},
                            level="error",
                        )
                        raise

                    delay = min(
                        self.config.backoff_factor * (2 ** retries) + random.uniform(0, self.config.backoff_jitter),
                        self.config.max_backoff_seconds,
                    )
                    retries += 1
                    last_delay = delay

                    self._log_event(
                        "transport_retry_scheduled",
                        {
                            "retry_number": retries,
                            "delay_seconds": round(delay, 3),
                            "error": repr(exc),
                        },
                        level="warning",
                    )
                    time.sleep(delay)

    def _wait_for_turn(self):
        with self._pace_lock:
            now = time.monotonic()
            sleep_for = max(0.0, self._next_allowed_time - now)
        if sleep_for > 0:
            time.sleep(sleep_for)

    def _enforce_min_spacing(self):
        with self._pace_lock:
            self._next_allowed_time = max(self._next_allowed_time, time.monotonic()) + \
                                      self.config.min_request_interval_seconds

    def _apply_header_based_throttling(self, headers, *, url: str):
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
            with self._pace_lock:
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
    def _extract_cursor_from_links(payload: Dict[str, Any]) -> Optional[str]:
        next_link = payload.get("_links", {}).get("next")
        if not next_link:
            return None
        parsed = urlparse(next_link)
        qs = parse_qs(parsed.query)
        cursor = qs.get("cursor")
        return cursor[0] if cursor else None

    def _log_event(self, event_name: str, payload: Dict[str, Any], *, level: str = "info"):
        message = json.dumps({"event": event_name, **payload}, default=str)
        if level == "warning":
            self.logger.warning(message)
        elif level == "error":
            self.logger.error(message)
        else:
            self.logger.info(message)