"""Smoke tests for ctrade module."""

import pytest


def test_import_ctrade():
    """Test that ctrade module can be imported."""
    try:
        import ctrade
        assert hasattr(ctrade, "Strategy")
        assert hasattr(ctrade, "BacktestConfig")
        assert hasattr(ctrade, "MarketState")
        assert hasattr(ctrade, "ExecutionContext")
        assert hasattr(ctrade, "backtest")
    except ImportError:
        pytest.skip("ctrade module not built yet")


def test_create_backtest_config():
    """Test creating a BacktestConfig."""
    try:
        from ctrade import BacktestConfig, DatabaseConfig

        config = BacktestConfig()
        config.db_config = DatabaseConfig()
        config.db_config.host = "localhost"
        config.db_config.port = 5432
        config.start_ts = 1000
        config.end_ts = 2000

        assert config.db_config.host == "localhost"
        assert config.db_config.port == 5432
        assert config.start_ts == 1000
        assert config.end_ts == 2000
    except ImportError:
        pytest.skip("ctrade module not built yet")


def test_market_state_fields():
    """Test MarketState has expected fields."""
    try:
        from ctrade import MarketState

        state = MarketState()
        state.asset_id = 0
        state.timestamp = 1000
        state.close = 50000.0
        state.bid = 49999.0
        state.ask = 50001.0

        assert state.asset_id == 0
        assert state.timestamp == 1000
        assert state.close == 50000.0
    except ImportError:
        pytest.skip("ctrade module not built yet")


def test_strategy_subclass():
    """Test that Strategy can be subclassed."""
    try:
        from ctrade import Strategy, MarketState, ExecutionContext

        class TestStrategy(Strategy):
            def init(self):
                self.initialized = True

            def on_bar(self, market: MarketState, ctx: ExecutionContext):
                pass

        strategy = TestStrategy()
        # Note: Can't call init() directly as it's a pure virtual
        # This test just verifies the class can be defined
        assert strategy is not None
    except ImportError:
        pytest.skip("ctrade module not built yet")

