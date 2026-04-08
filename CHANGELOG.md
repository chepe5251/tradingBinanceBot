# Changelog

All notable changes to this project are documented in this file.

Versioning follows SemVer and repository tags (`vMAJOR.MINOR.PATCH`).

## [v1.0.3] - 2026-04-07

### Added
- New 15m mean-reversion short engine (`mean_reversion_short_15m`) in `strategy.py`.
- 15m short second-pass evaluation (only when 15m long returns no signal):
  - Backtest flow (`backtest/backtest.py`).
  - Runtime scan flow (`services/signal_service.py`).
- HTF mapping for `15m_short -> 1h` in `services/bootstrap_service.py`.
- Data stream snapshot invalidation tests for replace/append updates and interval due-selection tests:
  - `tests/test_data_stream_pool.py`.

### Changed
- Strategy filters/tuning refinements in `strategy.py`:
  - 15m dead-zone spread behavior and confirmation tuning.
  - 1h spread cold rejection moved to hard filter.
  - 4h BOS spread cold protection and threshold adjustments.
  - NR4 1d confirmation/risk-score thresholds aligned with latest calibration.
  - 15m short tightened with:
    - spread cap (`reject_short_spread_wide`),
    - RSI bounded range (`70..74`),
    - stronger bearish body requirement (`>= 0.65`).
- Backtest pipeline improvements in `backtest/backtest.py`:
  - Dynamic interval plan reuse + one-level higher HTF extension.
  - Larger default candle depth per timeframe.
  - Kline cache key includes indicator config signature.
  - Indicators are precomputed at fetch/cache stage and reused in simulation.
  - Symbol-grouped simulation tasks reduce IPC overhead in phase 2.
- Runtime data/scan performance improvements:
  - `data_stream.py`: fixed worker-pool usage, interval-due refresh targeting, DataFrame snapshot cache invalidation on replace/append.
  - `services/signal_service.py`: indicator-enriched DataFrame caching for main/context frames.

### CI Verification
- Local CI-equivalent checks passed:
  - `ruff check .`
  - `python -m pytest tests/ -v -m unit`
  - `python -m pytest tests/ -v -m integration`
  - `python -m pytest tests/ --cov=risk --cov=strategy --cov=sizing --cov=execution --cov=monitor_logic --cov=indicators --cov-report=term-missing -q`
- Docker build check could not be executed locally because Docker is not installed in this environment.

## [v1.0.2] - 2026-04-04

Baseline tag for this release line.

