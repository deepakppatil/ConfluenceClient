
import asyncio
import logging


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    config = AsyncConfluenceRateLimitConfig(
        max_concurrency=2,
        page_batch_size=25,
        cql_batch_size=25,
        attachment_batch_size=10,
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

        space = await client.get_space("ENG")
        print("Space name:", space.get("name"))

        pages = []
        async for page in client.iter_all_pages_from_space(
            "ENG",
            status="current",
            resume=True,
        ):
            pages.append(page)
            if len(pages) >= 50:
                break
        print("Loaded pages:", len(pages))

        search_results = await client.cql_search(
            'space = "ENG" AND type = page ORDER BY lastmodified DESC',
            limit=20,
        )
        print("CQL hits:", len(search_results))

        attachments = await client.list_attachments(page_id="123456")
        print("Attachment count:", len(attachments.get("results", [])))

        if attachments.get("results"):
            first_attachment = attachments["results"][0]
            file_path = await client.download_attachment(
                page_id="123456",
                attachment_id=first_attachment["id"],
                destination="./downloads",
            )
            print("Downloaded:", file_path)


if __name__ == "__main__":
    asyncio.run(main())