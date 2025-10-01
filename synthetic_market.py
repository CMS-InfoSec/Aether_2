"""Synthetic market data generation using GAN or agent-based simulation.

This module provides tooling for synthesising order book snapshots and trade
events based on historical Kraken data. When historical data and PyTorch are
available a lightweight Generative Adversarial Network (GAN) is trained to
model the joint distribution of key order book features. If either the data or
PyTorch is not present, the module falls back to a stochastic agent-based model
that perturbs reference prices using a mean reverting process and simulates
basic liquidity taking behaviour.

The main entry point is :func:`generate_synthetic_data` which returns a newline
delimited JSON string containing synthetic order book and trade events for a
requested number of hours. A simple command line interface is exposed:

```
python synthetic_market.py --hours 24 --out data/sim.json
```

This script is intentionally self contained so that the core functionality can
be used programmatically or via the CLI for model training and stress testing
pipelines.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

import numpy as np
import pandas as pd

try:  # pragma: no cover - handled gracefully if torch is unavailable
    import torch
    from torch import nn
    from torch.utils.data import DataLoader, TensorDataset

    TORCH_AVAILABLE = True
except Exception:  # pragma: no cover - torch is optional for this module
    torch = None  # type: ignore
    nn = object  # type: ignore
    DataLoader = object  # type: ignore
    TensorDataset = object  # type: ignore
    TORCH_AVAILABLE = False


LOGGER = logging.getLogger("synthetic_market")


DEFAULT_DATA_LOCATIONS: Sequence[Path] = (
    Path("data/ingest/kraken_orderbook.parquet"),
    Path("data/ingest/kraken_orderbook.csv"),
)


def _discover_dataset(custom_path: Optional[str]) -> Optional[Path]:
    """Return the first existing dataset path.

    Parameters
    ----------
    custom_path:
        Optional explicit path provided by the caller.

    Returns
    -------
    Optional[Path]
        Path to the dataset if one exists, otherwise ``None``.
    """

    if custom_path:
        candidate = Path(custom_path)
        if candidate.exists():
            return candidate
        LOGGER.warning("Custom dataset %s not found", candidate)

    for candidate in DEFAULT_DATA_LOCATIONS:
        if candidate.exists():
            return candidate

    return None


def _load_dataset(path: Path) -> pd.DataFrame:
    """Load historical Kraken order book data from ``path``.

    The loader supports CSV and Parquet formats. Only numeric columns are kept
    as GAN features. Timestamps (if present) are used solely for ordering the
    data.
    """

    if path.suffix == ".csv":
        df = pd.read_csv(path)
    else:
        df = pd.read_parquet(path)

    # Sort by time if a timestamp-like column exists.
    timestamp_cols = [
        col
        for col in df.columns
        if "time" in col.lower() or "timestamp" in col.lower()
    ]
    if timestamp_cols:
        df = df.sort_values(timestamp_cols[0])

    numeric_df = df.select_dtypes(include=[np.number]).copy()
    if numeric_df.empty:
        raise ValueError(
            "Dataset does not contain numeric columns required for GAN training"
        )
    numeric_df = numeric_df.dropna()

    LOGGER.info(
        "Loaded Kraken dataset with shape %s and columns %s",
        numeric_df.shape,
        list(numeric_df.columns),
    )

    return numeric_df


def _as_tensor(data: np.ndarray) -> "torch.Tensor":  # pragma: no cover - torch optional
    if not TORCH_AVAILABLE:
        raise RuntimeError("PyTorch is required for GAN functionality")
    tensor = torch.tensor(data, dtype=torch.float32)
    if torch.cuda.is_available():
        return tensor.cuda()
    return tensor


@dataclass
class GANConfig:
    """Configuration for the GAN training."""

    latent_dim: int = 32
    hidden_dim: int = 128
    batch_size: int = 256
    epochs: int = 200
    learning_rate: float = 2e-4
    betas: Sequence[float] = (0.5, 0.9)


class Generator(nn.Module):  # pragma: no cover - simple feed-forward network
    """Simple MLP generator."""

    def __init__(self, latent_dim: int, output_dim: int, hidden_dim: int) -> None:
        super().__init__()
        self.model = nn.Sequential(
            nn.Linear(latent_dim, hidden_dim),
            nn.LeakyReLU(0.2, inplace=True),
            nn.Linear(hidden_dim, hidden_dim),
            nn.LeakyReLU(0.2, inplace=True),
            nn.Linear(hidden_dim, output_dim),
        )

    def forward(self, z: "torch.Tensor") -> "torch.Tensor":
        return self.model(z)


class Discriminator(nn.Module):  # pragma: no cover - simple feed-forward network
    """Discriminator network distinguishing real from synthetic samples."""

    def __init__(self, input_dim: int, hidden_dim: int) -> None:
        super().__init__()
        self.model = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.LeakyReLU(0.2, inplace=True),
            nn.Linear(hidden_dim, hidden_dim),
            nn.LeakyReLU(0.2, inplace=True),
            nn.Linear(hidden_dim, 1),
            nn.Sigmoid(),
        )

    def forward(self, x: "torch.Tensor") -> "torch.Tensor":
        return self.model(x)


class OrderBookGAN:
    """Utility encapsulating GAN training and sampling for order book data."""

    def __init__(self, feature_dim: int, config: GANConfig) -> None:
        if not TORCH_AVAILABLE:  # pragma: no cover - handled via fallback
            raise RuntimeError("PyTorch not available for GAN training")

        self.config = config
        self.generator = Generator(config.latent_dim, feature_dim, config.hidden_dim)
        self.discriminator = Discriminator(feature_dim, config.hidden_dim)
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.generator.to(self.device)
        self.discriminator.to(self.device)

        self.g_optimizer = torch.optim.Adam(
            self.generator.parameters(),
            lr=config.learning_rate,
            betas=config.betas,
        )
        self.d_optimizer = torch.optim.Adam(
            self.discriminator.parameters(),
            lr=config.learning_rate,
            betas=config.betas,
        )
        self.criterion = nn.BCELoss()
        self.feature_mean: Optional[torch.Tensor] = None
        self.feature_std: Optional[torch.Tensor] = None

    def _normalise(self, data: "torch.Tensor") -> "torch.Tensor":
        if self.feature_mean is None or self.feature_std is None:
            raise RuntimeError("Normalization statistics not initialised")
        return (data - self.feature_mean) / (self.feature_std + 1e-6)

    def _denormalise(self, data: "torch.Tensor") -> "torch.Tensor":
        if self.feature_mean is None or self.feature_std is None:
            raise RuntimeError("Normalization statistics not initialised")
        return data * (self.feature_std + 1e-6) + self.feature_mean

    def fit(self, features: np.ndarray) -> None:
        tensor = _as_tensor(features)
        self.feature_mean = tensor.mean(dim=0, keepdim=True)
        self.feature_std = tensor.std(dim=0, keepdim=True)
        normalised = self._normalise(tensor)

        dataset = TensorDataset(normalised)
        loader = DataLoader(dataset, batch_size=self.config.batch_size, shuffle=True)

        real_label = 1.0
        fake_label = 0.0

        for epoch in range(self.config.epochs):  # pragma: no cover - training loop
            d_loss_epoch = 0.0
            g_loss_epoch = 0.0
            for (batch,) in loader:
                batch = batch.to(self.device)

                # Train discriminator with real data
                self.discriminator.zero_grad()
                real_output = self.discriminator(batch)
                real_labels = torch.full_like(real_output, real_label)
                d_loss_real = self.criterion(real_output, real_labels)

                # Train discriminator with fake data
                noise = torch.randn(batch.size(0), self.config.latent_dim, device=self.device)
                fake_samples = self.generator(noise)
                fake_output = self.discriminator(fake_samples.detach())
                fake_labels = torch.full_like(fake_output, fake_label)
                d_loss_fake = self.criterion(fake_output, fake_labels)

                d_loss = d_loss_real + d_loss_fake
                d_loss.backward()
                self.d_optimizer.step()

                # Train generator to fool discriminator
                self.generator.zero_grad()
                fake_output = self.discriminator(fake_samples)
                g_loss = self.criterion(fake_output, real_labels)
                g_loss.backward()
                self.g_optimizer.step()

                d_loss_epoch += d_loss.item()
                g_loss_epoch += g_loss.item()

            if epoch % 20 == 0 or epoch == self.config.epochs - 1:
                LOGGER.debug(
                    "Epoch %s: discriminator loss %.4f, generator loss %.4f",
                    epoch,
                    d_loss_epoch / max(len(loader), 1),
                    g_loss_epoch / max(len(loader), 1),
                )

    def sample(self, n_samples: int) -> np.ndarray:
        self.generator.eval()
        with torch.no_grad():  # pragma: no cover - inference
            noise = torch.randn(n_samples, self.config.latent_dim, device=self.device)
            generated = self.generator(noise)
            denorm = self._denormalise(generated)
            return denorm.cpu().numpy()


def _agent_based_simulation(
    hours: int,
    frequency_per_hour: int,
    base_price: float,
    columns: Sequence[str],
) -> pd.DataFrame:
    """Fallback agent-based generator.

    Simulates a mean reverting price process with stochastic spreads and volume
    to ensure downstream consumers receive plausible data even without GAN
    training resources.
    """

    n_steps = hours * frequency_per_hour
    dt = 1.0 / frequency_per_hour
    prices = [base_price]
    mean_price = base_price
    kappa = 1.5  # Mean reversion strength
    volatility = 0.02

    for _ in range(1, n_steps):
        shock = np.random.normal(0.0, volatility * math.sqrt(dt))
        drift = kappa * (mean_price - prices[-1]) * dt
        prices.append(max(0.0, prices[-1] + drift + shock))

    prices = np.array(prices)
    spread = np.random.uniform(0.5, 1.5, size=n_steps)
    bid_prices = prices - spread / 2
    ask_prices = prices + spread / 2
    bid_sizes = np.random.lognormal(mean=1.0, sigma=0.5, size=n_steps)
    ask_sizes = np.random.lognormal(mean=1.0, sigma=0.5, size=n_steps)

    base_df = pd.DataFrame(
        {
            "mid_price": prices,
            "bid_price": bid_prices,
            "ask_price": ask_prices,
            "bid_size": bid_sizes,
            "ask_size": ask_sizes,
        }
    )

    # If the historical dataset provided more columns, fill them with noise
    # using matching magnitudes.
    for extra in columns:
        if extra in base_df.columns:
            continue
        scale = np.std(base_df["mid_price"])
        base_df[extra] = np.random.normal(0, scale, size=n_steps)

    return base_df[columns]


def _train_or_fallback(
    hours: int,
    dataset_path: Optional[str],
    gan_config: GANConfig,
) -> pd.DataFrame:
    """Train a GAN if possible otherwise fall back to agent-based simulation."""

    dataset_file = _discover_dataset(dataset_path)
    frequency_per_hour = 60  # Assume one sample per minute

    if dataset_file is None:
        LOGGER.warning("No Kraken dataset found, using agent-based simulation")
        columns = [
            "mid_price",
            "bid_price",
            "ask_price",
            "bid_size",
            "ask_size",
        ]
        return _agent_based_simulation(hours, frequency_per_hour, 25000.0, columns)

    data = _load_dataset(dataset_file)
    columns = list(data.columns)

    if not TORCH_AVAILABLE:
        LOGGER.warning("PyTorch unavailable, generating data via agent-based model")
        base_price = float(data[columns[0]].mean()) if not data.empty else 25000.0
        return _agent_based_simulation(hours, frequency_per_hour, base_price, columns)

    gan = OrderBookGAN(feature_dim=data.shape[1], config=gan_config)
    gan.fit(data.values.astype(np.float32))

    n_samples = hours * frequency_per_hour
    synthetic = gan.sample(n_samples)
    synthetic_df = pd.DataFrame(synthetic, columns=columns)
    return synthetic_df


def _build_trade_event(
    book_row: Dict[str, float],
    timestamp: datetime,
) -> Dict[str, object]:
    """Generate a synthetic trade event from an order book snapshot."""

    mid_price = float(
        book_row.get(
            "mid_price",
            (book_row.get("bid_price", 0.0) + book_row.get("ask_price", 0.0)) / 2,
        )
    )
    spread = float(
        abs(book_row.get("ask_price", mid_price) - book_row.get("bid_price", mid_price))
    )
    side = np.random.choice(["buy", "sell"])
    price_noise = np.random.normal(0, max(spread / 4, 1e-3))
    price = max(0.0, mid_price + (price_noise if side == "buy" else -price_noise))
    size = float(np.random.lognormal(mean=0.0, sigma=0.5))

    trade = {
        "timestamp": timestamp.isoformat(),
        "side": side,
        "price": price,
        "size": size,
        "liquidity": np.random.choice(["maker", "taker"], p=[0.4, 0.6]),
    }
    return trade


def _book_snapshot(book_row: Dict[str, float], timestamp: datetime) -> Dict[str, object]:
    """Convert a row of synthetic features into a structured order book."""

    snapshot = {
        "timestamp": timestamp.isoformat(),
        "levels": {},
    }

    for column, value in book_row.items():
        snapshot["levels"][column] = float(value)

    mid = book_row.get("mid_price")
    if mid is None and {"bid_price", "ask_price"}.issubset(book_row.keys()):
        snapshot["levels"]["mid_price"] = float(
            (book_row["bid_price"] + book_row["ask_price"]) / 2
        )

    return snapshot


def _build_json_stream(
    rows: Iterable[Dict[str, float]],
    start_time: datetime,
    freq_minutes: float,
) -> str:
    """Create a newline delimited JSON stream from synthetic rows."""

    events: List[str] = []
    current_time = start_time
    delta = timedelta(minutes=freq_minutes)

    for row in rows:
        trade = _build_trade_event(row, current_time)
        book = _book_snapshot(row, current_time)
        payload = {
            "order_book": book,
            "trade": trade,
        }
        events.append(json.dumps(payload))
        current_time += delta

    return "\n".join(events)


def generate_synthetic_data(
    hours: int,
    dataset_path: Optional[str] = None,
    gan_config: Optional[GANConfig] = None,
) -> str:
    """Generate a JSON stream of synthetic trades and order books.

    Parameters
    ----------
    hours:
        Number of hours of synthetic data to generate.
    dataset_path:
        Optional explicit path to the Kraken order book dataset.
    gan_config:
        Optional GAN training configuration.

    Returns
    -------
    str
        Newline delimited JSON representation of synthetic market events.
    """

    if hours <= 0:
        raise ValueError("hours must be a positive integer")

    config = gan_config or GANConfig()
    frequency_per_hour = 60
    df = _train_or_fallback(hours, dataset_path, config)

    start_time = datetime.now(timezone.utc)
    rows = df.to_dict(orient="records")
    json_stream = _build_json_stream(rows, start_time, 60 / frequency_per_hour)
    return json_stream


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Synthetic Kraken market data generator")
    parser.add_argument("--hours", type=int, required=True, help="Number of hours to generate")
    parser.add_argument(
        "--out",
        type=str,
        required=True,
        help="Destination file for the JSON stream",
    )
    parser.add_argument(
        "--dataset",
        type=str,
        default=None,
        help="Optional explicit dataset path (CSV or Parquet)",
    )
    parser.add_argument(
        "--epochs",
        type=int,
        default=None,
        help="Override GAN epochs (default 200)",
    )
    parser.add_argument(
        "--latent-dim",
        type=int,
        default=None,
        help="Override GAN latent dimension (default 32)",
    )
    parser.add_argument(
        "--hidden-dim",
        type=int,
        default=None,
        help="Override GAN hidden dimension (default 128)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default=os.environ.get("SYNTHETIC_MARKET_LOG_LEVEL", "INFO"),
        help="Logging level",
    )
    return parser.parse_args(argv)


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    _configure_logging(args.log_level)

    gan_config = GANConfig()
    if args.epochs is not None:
        gan_config.epochs = args.epochs
    if args.latent_dim is not None:
        gan_config.latent_dim = args.latent_dim
    if args.hidden_dim is not None:
        gan_config.hidden_dim = args.hidden_dim

    LOGGER.info(
        "Generating %s hours of synthetic Kraken data (dataset=%s)",
        args.hours,
        args.dataset or _discover_dataset(None),
    )

    json_stream = generate_synthetic_data(
        hours=args.hours,
        dataset_path=args.dataset,
        gan_config=gan_config,
    )

    output_path = Path(args.out)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json_stream)

    LOGGER.info("Wrote synthetic data to %s", output_path.resolve())
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())

