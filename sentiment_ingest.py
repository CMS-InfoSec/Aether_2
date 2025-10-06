"""Sentiment ingestion microservice for alternative data feeds.

This module provides a lightweight ingestion loop that pulls public market
sentiment signals from Twitter, Reddit, and generic news APIs.  When API
credentials are unavailable it gracefully falls back to deterministic stub data
so that downstream systems can still exercise the data flow.  All retrieved
mentions are scored with a pretrained transformer sentiment model when
available, otherwise a heuristic fallback is used.  The resulting observations
are stored in a shared SQL table named ``sentiment_scores`` and optionally pushed
into the in-memory Feast façade that powers integration tests for the wider
platform.

The module also exposes a small FastAPI application with a single endpoint that
returns the latest sentiment score for a requested symbol.  The intent is to
provide both a data pipeline and an online serving surface suitable for model
training as well as manual inspection.
"""
from __future__ import annotations

import asyncio
import dataclasses
import datetime as dt
import importlib
import importlib.util
import logging
import os
from pathlib import Path
from typing import Callable, Iterable, List, Optional, Sequence, Tuple

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.routing import APIRouter
from pydantic import BaseModel
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    func,
    insert,
    select,
)
from sqlalchemy.engine import Engine

try:  # pragma: no cover - optional dependency for HTTP clients
    import httpx
except Exception:  # pragma: no cover - keep runtime light during tests
    httpx = None  # type: ignore

from shared.spot import filter_spot_symbols, is_spot_symbol, normalize_spot_symbol

def _resolve_security_dependency() -> Callable[..., str]:
    module_names = ("services.common.security", "aether.services.common.security")
    for module_name in module_names:
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError:
            continue
        except Exception as exc:  # pragma: no cover - fail fast on unexpected import errors
            raise
        else:
            dependency = getattr(module, "require_admin_account", None)
            if dependency is not None:
                return dependency

    from pathlib import Path
    import sys

    base_dir = Path(__file__).resolve().parent
    fallback_path = base_dir / "services" / "common" / "security.py"
    if fallback_path.exists():
        spec = importlib.util.spec_from_file_location("services.common.security", fallback_path)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            sys.modules.setdefault(spec.name, module)
            spec.loader.exec_module(module)  # type: ignore[union-attr]
            dependency = getattr(module, "require_admin_account", None)
            if dependency is not None:
                return dependency

    from fastapi import Header, Request  # type: ignore

    def _missing_dependency(
        request: Request,
        authorization: Optional[str] = Header(None, alias="Authorization"),
        x_account_id: Optional[str] = Header(None, alias="X-Account-ID"),
    ) -> str:
        raise HTTPException(
            status_code=500,
            detail="Security dependency unavailable; configure services.common.security.",
        )

    return _missing_dependency


require_admin_account = _resolve_security_dependency()


LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclasses.dataclass(slots=True)
class SocialPost:
    """Container representing a market-related post or article."""

    symbol: str
    text: str
    source: str
    created_at: dt.datetime


class SentimentModel:
    """Wrapper around a HuggingFace sentiment pipeline with a safe fallback."""

    _LABEL_MAPPING = {
        "LABEL_0": "negative",
        "LABEL_1": "neutral",
        "LABEL_2": "positive",
        "NEGATIVE": "negative",
        "NEUTRAL": "neutral",
        "POSITIVE": "positive",
    }

    def __init__(self, model_name: str | None = None) -> None:
        self._model_name = model_name or "cardiffnlp/twitter-roberta-base-sentiment"
        self._pipeline = self._load_pipeline()

    def _load_pipeline(self):  # type: ignore[no-untyped-def]
        try:
            from transformers import pipeline  # type: ignore
        except Exception:  # pragma: no cover - dependency optional in tests
            LOGGER.warning("transformers not available; falling back to rule-based sentiment")
            return None

        try:
            LOGGER.info("Loading sentiment model %s", self._model_name)
            return pipeline("sentiment-analysis", model=self._model_name)
        except Exception:  # pragma: no cover - handle model download failures
            LOGGER.exception("Failed to load transformer model; falling back to rule-based sentiment")
            return None

    def classify(self, text: str) -> Tuple[str, float]:
        """Return ``(label, score)`` for ``text``.

        ``label`` is one of ``{"positive", "neutral", "negative"}`` while
        ``score`` is mapped onto ``{-1.0, 0.0, 1.0}`` for convenient numeric use.
        """

        text = text.strip()
        if not text:
            return "neutral", 0.0

        if self._pipeline is not None:
            try:
                result = self._pipeline(text, truncation=True)[0]
                raw_label = str(result.get("label", "NEUTRAL")).upper()
                label = self._LABEL_MAPPING.get(raw_label, raw_label.lower())
            except Exception:  # pragma: no cover - runtime robustness
                LOGGER.exception("Sentiment pipeline inference failed; using fallback heuristic")
                label = self._fallback_label(text)
        else:
            label = self._fallback_label(text)

        score_map = {"positive": 1.0, "neutral": 0.0, "negative": -1.0}
        return label, score_map.get(label, 0.0)

    def _fallback_label(self, text: str) -> str:
        lowered = text.lower()
        positive_keywords = {"rally", "bull", "bullish", "surge", "moon", "win"}
        negative_keywords = {"dump", "bear", "bearish", "sell", "crash", "loss"}

        if any(word in lowered for word in positive_keywords):
            return "positive"
        if any(word in lowered for word in negative_keywords):
            return "negative"
        return "neutral"


class BaseSource:
    """Abstract sentiment source."""

    name: str

    async def fetch(self, symbol: str) -> List[SocialPost]:
        raise NotImplementedError

    @staticmethod
    def _stub_posts(symbol: str, source: str, samples: Sequence[str]) -> List[SocialPost]:
        now = dt.datetime.now(tz=dt.timezone.utc)
        return [
            SocialPost(symbol=symbol.upper(), text=text.format(symbol=symbol.upper()), source=source, created_at=now)
            for text in samples
        ]


class TwitterSource(BaseSource):
    """Twitter (X) recent search client with graceful fallbacks."""

    def __init__(self, api_key: Optional[str] = None, *, max_results: int = 10) -> None:
        self.name = "twitter"
        self._api_key = api_key
        self._max_results = max_results

    async def fetch(self, symbol: str) -> List[SocialPost]:
        if not self._api_key or httpx is None:
            samples = [
                "{symbol} community feeling bullish after the latest rally",
                "Traders debate whether {symbol} can sustain the momentum",
            ]
            return self._stub_posts(symbol, self.name, samples)

        query = f"{symbol} (crypto OR stock) lang:en -is:retweet"
        url = "https://api.twitter.com/2/tweets/search/recent"
        params = {"query": query, "max_results": min(self._max_results, 100)}
        headers = {"Authorization": f"Bearer {self._api_key}"}

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(url, params=params, headers=headers)
                response.raise_for_status()
                payload = response.json()
        except Exception:
            LOGGER.exception("Twitter API call failed; using stubbed posts")
            samples = [
                "{symbol} saw mixed interest among traders today",
                "Speculation about regulatory moves keeps {symbol} investors cautious",
            ]
            return self._stub_posts(symbol, self.name, samples)

        posts: List[SocialPost] = []
        for entry in payload.get("data", [])[: self._max_results]:
            text = entry.get("text", "")
            created_at = entry.get("created_at")
            timestamp = _parse_timestamp(created_at) or dt.datetime.now(tz=dt.timezone.utc)
            posts.append(
                SocialPost(
                    symbol=symbol.upper(),
                    text=text,
                    source=self.name,
                    created_at=timestamp,
                )
            )
        return posts


class RedditSource(BaseSource):
    """Fetch Reddit submissions mentioning the target symbol."""

    def __init__(self, client_id: Optional[str] = None, client_secret: Optional[str] = None) -> None:
        self.name = "reddit"
        self._client_id = client_id
        self._client_secret = client_secret

    async def fetch(self, symbol: str) -> List[SocialPost]:
        if httpx is None:
            return self._stub(symbol)

        if not self._client_id or not self._client_secret:
            return self._stub(symbol)

        headers = {"User-Agent": "aether-sentiment/0.1"}
        query = f"{symbol} crypto"
        url = "https://www.reddit.com/search.json"
        params = {"q": query, "limit": 10, "sort": "new"}

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(url, params=params, headers=headers)
                response.raise_for_status()
                payload = response.json()
        except Exception:
            LOGGER.exception("Reddit API call failed; using stubbed posts")
            return self._stub(symbol)

        posts: List[SocialPost] = []
        for child in payload.get("data", {}).get("children", []):
            data = child.get("data", {})
            title = data.get("title", "")
            selftext = data.get("selftext", "")
            body = title if not selftext else f"{title}\n{selftext}"
            created_utc = data.get("created_utc")
            timestamp = _parse_timestamp(created_utc) or dt.datetime.now(tz=dt.timezone.utc)
            posts.append(
                SocialPost(
                    symbol=symbol.upper(),
                    text=body,
                    source=self.name,
                    created_at=timestamp,
                )
            )
        return posts

    def _stub(self, symbol: str) -> List[SocialPost]:
        samples = [
            "Retail chatter hints at a potential pump for {symbol}",
            "Skepticism remains around {symbol} fundamentals despite hype",
        ]
        return self._stub_posts(symbol, self.name, samples)


class NewsSource(BaseSource):
    """Fetch finance news headlines mentioning the target symbol."""

    def __init__(self, api_key: Optional[str] = None) -> None:
        self.name = "news"
        self._api_key = api_key

    async def fetch(self, symbol: str) -> List[SocialPost]:
        if not self._api_key or httpx is None:
            samples = [
                "Analysts publish a neutral report on {symbol} performance",
                "Market recap: {symbol} featured amid broader risk sentiment",
            ]
            return self._stub_posts(symbol, self.name, samples)

        url = "https://newsapi.org/v2/everything"
        params = {"q": symbol, "language": "en", "pageSize": 10, "sortBy": "publishedAt"}
        headers = {"Authorization": self._api_key}

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(url, params=params, headers=headers)
                response.raise_for_status()
                payload = response.json()
        except Exception:
            LOGGER.exception("News API call failed; using stubbed headlines")
            samples = [
                "Macro headwinds weigh on {symbol} outlook",
                "Institutional flows lift {symbol} prospects",
            ]
            return self._stub_posts(symbol, self.name, samples)

        posts: List[SocialPost] = []
        for article in payload.get("articles", []):
            title = article.get("title") or ""
            description = article.get("description") or ""
            text = f"{title}\n{description}".strip()
            published_at = article.get("publishedAt")
            timestamp = _parse_timestamp(published_at) or dt.datetime.now(tz=dt.timezone.utc)
            posts.append(
                SocialPost(
                    symbol=symbol.upper(),
                    text=text,
                    source=self.name,
                    created_at=timestamp,
                )
            )
        return posts


def _parse_timestamp(value: object) -> dt.datetime | None:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        try:
            return dt.datetime.fromtimestamp(float(value), tz=dt.timezone.utc)
        except Exception:
            return None

    if isinstance(value, str):
        for fmt in (dt.datetime.fromisoformat, _try_parse_iso8601):
            try:
                parsed = fmt(value)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=dt.timezone.utc)
                return parsed.astimezone(dt.timezone.utc)
            except Exception:
                continue
    return None


def _try_parse_iso8601(value: str) -> dt.datetime:
    return dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")


_METADATA = MetaData()


_SENTIMENT_ID_TYPE = BigInteger().with_variant(Integer, "sqlite")


_SENTIMENT_TABLE = Table(
    "sentiment_scores",
    _METADATA,
    Column("id", _SENTIMENT_ID_TYPE, primary_key=True, autoincrement=True),
    Column("symbol", String(64), nullable=False),
    Column("score", String(16), nullable=False),
    Column("source", String(32), nullable=False),
    Column("ts", DateTime(timezone=True), nullable=False),
    Column("ingested_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    sqlite_autoincrement=True,
)
Index("ix_sentiment_scores_symbol_ts", _SENTIMENT_TABLE.c.symbol, _SENTIMENT_TABLE.c.ts)
Index("ix_sentiment_scores_ts", _SENTIMENT_TABLE.c.ts)


def _default_database_url() -> str:
    candidates = [
        os.getenv("SENTIMENT_DATABASE_URL"),
        os.getenv("TIMESCALE_DATABASE_URL"),
        os.getenv("TIMESCALE_URI"),
        os.getenv("DATABASE_URL"),
    ]
    for candidate in candidates:
        if candidate:
            return candidate
    default_path = Path("data/sentiment/sentiment.db")
    default_path.parent.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{default_path}"


def _create_engine(url: str) -> Engine:
    options: dict[str, object] = {"future": True, "pool_pre_ping": True}
    if url.startswith("sqlite"):
        options["connect_args"] = {"check_same_thread": False}
    return create_engine(url, **options)


class SentimentRepository:
    """SQLAlchemy-backed storage for sentiment scores."""

    def __init__(self, database_url: str | None = None, *, engine: Engine | None = None) -> None:
        url = str(database_url) if database_url is not None else _default_database_url()
        self._engine: Engine = engine or _create_engine(url)
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        try:
            _METADATA.create_all(self._engine, tables=[_SENTIMENT_TABLE])
        except Exception:  # pragma: no cover - fail fast if schema creation fails
            LOGGER.exception("Failed to bootstrap sentiment schema")
            raise

    async def insert(self, observation: SocialPost, label: str) -> None:
        normalized_symbol = normalize_spot_symbol(observation.symbol)
        if not normalized_symbol or not is_spot_symbol(normalized_symbol):
            raise ValueError(
                f"SentimentRepository only accepts spot symbols (got {observation.symbol!r})"
            )

        values = {
            "symbol": normalized_symbol,
            "score": label,
            "source": observation.source,
            "ts": observation.created_at.astimezone(dt.timezone.utc),
        }
        await asyncio.to_thread(self._insert_sync, values)

    def _insert_sync(self, values: dict[str, object]) -> None:
        with self._engine.begin() as connection:
            connection.execute(insert(_SENTIMENT_TABLE).values(**values))

    async def latest(self, symbol: str) -> Optional[Tuple[str, str, str, dt.datetime]]:
        normalized_symbol = normalize_spot_symbol(symbol)
        if not normalized_symbol or not is_spot_symbol(normalized_symbol):
            raise ValueError(f"Symbol '{symbol}' is not a supported spot market")

        stmt = (
            select(
                _SENTIMENT_TABLE.c.symbol,
                _SENTIMENT_TABLE.c.score,
                _SENTIMENT_TABLE.c.source,
                _SENTIMENT_TABLE.c.ts,
            )
            .where(_SENTIMENT_TABLE.c.symbol == normalized_symbol)
            .order_by(_SENTIMENT_TABLE.c.ts.desc())
            .limit(1)
        )
        row = await asyncio.to_thread(self._fetch_one, stmt)
        if row is None:
            return None
        ts_value = row.ts
        if isinstance(ts_value, dt.datetime):
            ts = ts_value if ts_value.tzinfo else ts_value.replace(tzinfo=dt.timezone.utc)
            ts = ts.astimezone(dt.timezone.utc)
        else:
            ts = _parse_timestamp(ts_value) or dt.datetime.now(tz=dt.timezone.utc)
        return row.symbol, row.score, row.source, ts

    def _fetch_one(self, stmt):
        with self._engine.begin() as connection:
            result = connection.execute(stmt)
            row = result.first()
        return row


class FeastSentimentWriter:
    """Optional bridge that stores sentiment labels inside the RedisFeastAdapter stub."""

    def __init__(self, account_id: str = "company") -> None:
        try:
            from services.common.adapters import RedisFeastAdapter
        except Exception:  # pragma: no cover - Feast adapter optional
            LOGGER.info("RedisFeastAdapter unavailable; Feast integration disabled")
            self._adapter = None
        else:
            self._adapter = RedisFeastAdapter(account_id=account_id)

    def write(self, symbol: str, label: str, score: float) -> None:
        if self._adapter is None:
            return

        try:
            store = self._adapter._account_feature_store()  # type: ignore[attr-defined]
        except Exception:
            LOGGER.debug("Unable to access Feast store for sentiment updates")
            return

        normalized_symbol = normalize_spot_symbol(symbol)
        if not normalized_symbol or not is_spot_symbol(normalized_symbol):
            LOGGER.debug("Skipping non-spot sentiment update for symbol %s", symbol)
            return

        instrument_store = store.setdefault(normalized_symbol, {})
        sentiment_payload = instrument_store.setdefault("sentiment", {})
        sentiment_payload.update({
            "label": label,
            "score": score,
            "updated_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        })


class SentimentIngestService:
    """Coordinates fetching posts, scoring sentiment, and persisting outputs."""

    def __init__(
        self,
        *,
        repository: SentimentRepository,
        model: SentimentModel,
        sources: Iterable[BaseSource],
        feast_writer: FeastSentimentWriter | None = None,
    ) -> None:
        self._repository = repository
        self._model = model
        self._sources = list(sources)
        self._feast_writer = feast_writer

    async def ingest_symbol(self, symbol: str) -> None:
        normalized_symbol = normalize_spot_symbol(symbol)
        if not normalized_symbol or not is_spot_symbol(normalized_symbol):
            raise ValueError(f"Symbol '{symbol}' is not a supported spot market")

        tasks = [source.fetch(normalized_symbol) for source in self._sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for source, result in zip(self._sources, results):
            if isinstance(result, Exception):
                LOGGER.exception("Source %s failed for symbol %s", source.name, symbol)
                continue
            for post in result:
                post_symbol = normalize_spot_symbol(getattr(post, "symbol", "") or normalized_symbol)
                if not post_symbol or not is_spot_symbol(post_symbol):
                    LOGGER.debug(
                        "Skipping non-spot sentiment observation from %s: %s",
                        source.name,
                        getattr(post, "symbol", ""),
                    )
                    continue
                canonical_post = dataclasses.replace(post, symbol=post_symbol)
                label, score = self._model.classify(post.text)
                await self._repository.insert(canonical_post, label)
                if self._feast_writer is not None:
                    self._feast_writer.write(post_symbol, label, score)

    async def ingest_many(self, symbols: Sequence[str]) -> None:
        for symbol in symbols:
            await self.ingest_symbol(symbol)


class SentimentResponse(BaseModel):
    symbol: str
    score: str
    source: str
    ts: dt.datetime


class SentimentAPI:
    """REST API wrapper exposing sentiment lookups."""

    def __init__(self, service: SentimentIngestService, repository: SentimentRepository) -> None:
        self._service = service
        self._repository = repository
        self.router = APIRouter(prefix="/sentiment", tags=["sentiment"])
        self._register_routes()

    def _register_routes(self) -> None:
        @self.router.get("/latest", response_model=SentimentResponse)
        async def latest(
            symbol: str = Query(..., description="Symbol ticker, e.g. BTC-USD"),
            _: str = Depends(require_admin_account),
        ) -> SentimentResponse:
            normalized_symbol = normalize_spot_symbol(symbol)
            if not normalized_symbol or not is_spot_symbol(normalized_symbol):
                raise HTTPException(
                    status_code=422,
                    detail=f"Symbol '{symbol}' is not a supported spot market",
                )
            try:
                record = await self._repository.latest(normalized_symbol)
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            if record is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"No sentiment found for {normalized_symbol}",
                )
            return SentimentResponse(symbol=record[0], score=record[1], source=record[2], ts=record[3])

        @self.router.post("/refresh")
        async def refresh(symbols: List[str], _: str = Depends(require_admin_account)) -> dict[str, str]:
            normalized_symbols: List[str] = []
            for raw_symbol in symbols:
                normalized_symbol = normalize_spot_symbol(raw_symbol)
                if not normalized_symbol or not is_spot_symbol(normalized_symbol):
                    raise HTTPException(
                        status_code=422,
                        detail=f"Symbol '{raw_symbol}' is not a supported spot market",
                    )
                normalized_symbols.append(normalized_symbol)

            try:
                await self._service.ingest_many(normalized_symbols)
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            return {"status": "ok", "symbols": ",".join(normalized_symbols)}


def bootstrap_service(
    database_url: str | Path | None = None,
) -> Tuple[SentimentIngestService, SentimentRepository]:
    url: str | None
    if database_url is None:
        url = None
    elif isinstance(database_url, Path):
        url = f"sqlite:///{database_url}"
    else:
        url = str(database_url)

    repository = SentimentRepository(url)
    model = SentimentModel()

    twitter_source = TwitterSource(api_key=os.getenv("TWITTER_BEARER_TOKEN"))
    reddit_source = RedditSource(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    )
    news_source = NewsSource(api_key=os.getenv("NEWS_API_KEY"))

    feast_writer = FeastSentimentWriter(account_id=os.getenv("ACCOUNT_ID", "company"))

    service = SentimentIngestService(
        repository=repository,
        model=model,
        sources=[twitter_source, reddit_source, news_source],
        feast_writer=feast_writer,
    )
    return service, repository


def create_app(
    service: SentimentIngestService | None = None,
    repository: SentimentRepository | None = None,
) -> FastAPI:
    if service is None or repository is None:
        service, repository = bootstrap_service()

    app = FastAPI(title="Aether Sentiment Service")
    app.state.sentiment_service = service
    app.state.sentiment_repository = repository

    sentiment_api = SentimentAPI(service=service, repository=repository)
    app.include_router(sentiment_api.router)

    return app


async def run_once(symbols: Sequence[str], *, database_url: Path | str | None = None) -> None:
    service, _ = bootstrap_service(database_url=database_url)
    spot_symbols = filter_spot_symbols(symbols, logger=LOGGER)
    if not spot_symbols:
        LOGGER.warning("No spot symbols supplied for sentiment ingestion; skipping run")
        return
    await service.ingest_many(spot_symbols)


def _default_symbols() -> List[str]:
    env_value = os.getenv("SENTIMENT_SYMBOLS")
    if not env_value:
        return ["BTC-USD", "ETH-USD", "SOL-USD"]
    return [symbol.strip() for symbol in env_value.split(",") if symbol.strip()]


if __name__ == "__main__":
    symbols = _default_symbols()
    LOGGER.info("Running one-off sentiment ingestion for symbols: %s", symbols)
    asyncio.run(run_once(symbols))
