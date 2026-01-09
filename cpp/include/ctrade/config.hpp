#pragma once
#include <cstdint>
#include <string>

namespace ctrade {

struct DatabaseConfig {
  std::string host;
  int port;
  std::string database;
  std::string user;
  std::string password;
};

struct BacktestConfig {
  DatabaseConfig db_config;
  int64_t start_ts;
  int64_t end_ts;
  // For now: single asset (BTCUSDT), will expand to multi-asset later
};

} // namespace ctrade
