#include "ctrade/market_data.hpp"
#include "ctrade/market_state.hpp"
#include "ctrade/config.hpp"
#include <stdexcept>

namespace ctrade {

// PostgresMarketData - streaming from TimescaleDB
// For now: single asset (BTCUSDT), will expand to multi-asset later
class PostgresMarketData : public MarketData {
public:
  explicit PostgresMarketData(const BacktestConfig& config) {
    (void)config;  // Suppress unused warning
    // TODO: Open DB connection using config.db_config
    // TODO: Query BTCUSDT data for time range [start_ts, end_ts]
    // TODO: Set up cursor for streaming rows
    throw std::runtime_error("TODO: PostgresMarketData constructor");
  }

  bool next() override {
    // TODO: Fetch next row from DB cursor
    // TODO: Populate current_state_ with OHLCV + funding data
    // TODO: Set asset_id = 0 (BTCUSDT)
    // TODO: Return false when cursor exhausted
    throw std::runtime_error("TODO: PostgresMarketData::next");
  }

  const MarketState& current() const override {
    // TODO: Return current_state_
    throw std::runtime_error("TODO: PostgresMarketData::current");
  }

  ~PostgresMarketData() override {
    // TODO: Close DB connection and cursor
  }

private:
  MarketState current_state_{};
  // TODO: Add PGconn*, PGresult*, cursor state, etc.
};

} // namespace ctrade

