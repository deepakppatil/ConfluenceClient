import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

config = ConfluenceRateLimitConfig(
    max_workers=2,
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

client = SafeConfluenceSdkClient(
    url="https://your-domain.atlassian.net/wiki",
    username="you@example.com",
    token="your-api-token",
    cloud=True,
    config=config,
)

space = client.get_space("ENG")
print("Space:", space.get("name"))

pages = client.get_all_pages_from_space(
    "ENG",
    status="current",
    limit=50,
    resume=True,
)
print("Pages:", len(pages))

hits = client.cql_search(
    'space = "ENG" AND type = page ORDER BY lastmodified DESC',
    limit=20,
    resume=True,
)
print("CQL hits:", len(hits))

attachments = client.list_attachments(page_id="123456")
print("Attachment count:", len(attachments.get("results", [])))

if attachments.get("results"):
    first = attachments["results"][0]
    path = client.download_attachment(
        page_id="123456",
        attachment_id=first["id"],
        destination="./downloads",
    )
    print("Downloaded:", path)