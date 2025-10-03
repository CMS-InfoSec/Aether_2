#!/bin/sh
set -euo pipefail

PAIRS="${KRAKEN_PAIRS:-BTC/USD,ETH/USD}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
TRADE_TOPIC="${KRAKEN_TRADE_TOPIC:-md.trades}"
BOOK_TOPIC="${KRAKEN_BOOK_TOPIC:-md.book}"
BOOK_DEPTH="${KRAKEN_BOOK_DEPTH:-}"
EXTRA_ARGS="${RISK_INGESTOR_EXTRA_ARGS:-}"

set -- \
  --pairs "${PAIRS}" \
  --kafka-bootstrap "${BOOTSTRAP}" \
  --trade-topic "${TRADE_TOPIC}" \
  --book-topic "${BOOK_TOPIC}"

if [ -n "${BOOK_DEPTH}" ]; then
  set -- "$@" --book-depth "${BOOK_DEPTH}"
fi

if [ -n "${EXTRA_ARGS}" ]; then
  # shellcheck disable=SC2086
  set -- "$@" ${EXTRA_ARGS}
fi

exec python -m services.kraken_ws_ingest "$@"
