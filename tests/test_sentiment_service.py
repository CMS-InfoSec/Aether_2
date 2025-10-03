import datetime as dt
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from sentiment_ingest import (
    SentimentIngestService,
    SentimentRepository,
    SocialPost,
    create_app,
)


class _StaticSentimentModel:
    def classify(self, text: str):  # pragma: no cover - deterministic for tests
        return "positive", 1.0


@pytest.fixture
def sentiment_app(tmp_path: Path) -> TestClient:
    database_url = f"sqlite:///{tmp_path/'sentiment.db'}"
    repository = SentimentRepository(database_url)
    service = SentimentIngestService(repository=repository, model=_StaticSentimentModel(), sources=[])
    app = create_app(service=service, repository=repository)
    return TestClient(app)


def test_latest_requires_admin_token(sentiment_app: TestClient) -> None:
    response = sentiment_app.get("/sentiment/latest", params={"symbol": "BTC-USD"})
    assert response.status_code == 401
    assert response.json()["detail"].lower().startswith("missing authorization")


def test_refresh_requires_admin_token(sentiment_app: TestClient) -> None:
    response = sentiment_app.post("/sentiment/refresh", json=["BTC-USD"])
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_repository_persists_across_instances(tmp_path: Path) -> None:
    database_url = f"sqlite:///{tmp_path/'sentiment_history.db'}"
    repository = SentimentRepository(database_url)
    observation = SocialPost(
        symbol="btc-usd",
        text="BTC rally continues",
        source="twitter",
        created_at=dt.datetime.now(dt.timezone.utc),
    )
    await repository.insert(observation, "positive")

    second_repo = SentimentRepository(database_url)
    latest = await second_repo.latest("BTC-USD")

    assert latest is not None
    stored_symbol, stored_label, stored_source, stored_ts = latest
    assert stored_symbol == "BTC-USD"
    assert stored_label == "positive"
    assert stored_source == "twitter"
    assert stored_ts.tzinfo is not None
