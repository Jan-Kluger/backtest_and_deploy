"""ctrade - Research-grade backtesting engine."""

from ._ctrade import (
    Strategy,
    BacktestConfig,
    BacktestResult,
    MarketState,
    DatabaseConfig,
    ExecutionContext,
    backtest,
)

__all__ = [
    "Strategy",
    "BacktestConfig",
    "BacktestResult",
    "MarketState",
    "DatabaseConfig",
    "ExecutionContext",
    "backtest",
]
