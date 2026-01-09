"""Example usage of ctrade backtesting engine."""

from datetime import datetime
from ctrade import (
    Strategy,
    BacktestConfig,
    DatabaseConfig,
    MarketState,
    ExecutionContext,
    backtest,
)


class MyStrategy(Strategy):
    """Example strategy - BTCUSDT only for now."""

    def init(self):
        """Initialize strategy state."""
        print("Strategy initialized")

    def on_bar(self, market: MarketState, ctx: ExecutionContext):
        """Called on each market bar.
        
        Args:
            market: Current market state (BTCUSDT, asset_id=0)
            ctx: Execution context for placing orders
        """
        # TODO: Implement strategy logic
        # Example: simple moving average crossover, momentum, etc.
        pass


def main():
    # Build config
    config = BacktestConfig()
    
    # Database connection
    config.db_config = DatabaseConfig()
    config.db_config.host = "localhost"
    config.db_config.port = 5432
    config.db_config.database = "ctrade"
    config.db_config.user = "postgres"
    config.db_config.password = ""
    
    # Time range
    start = datetime(2021, 1, 1)
    end = datetime(2023, 1, 1)
    config.start_ts = int(start.timestamp())
    config.end_ts = int(end.timestamp())
    
    # Create and run strategy
    strategy = MyStrategy()
    result = backtest(strategy, config)
    
    # Results
    print(f"Backtest completed")
    print(f"Equity points: {len(result.equity)}")
    if result.equity:
        print(f"Final equity: {result.equity[-1]}")


if __name__ == "__main__":
    main()
