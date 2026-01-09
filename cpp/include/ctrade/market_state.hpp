#pragma once
#include <cstdint>

namespace ctrade {

struct MarketState {
  int asset_id;  // Runtime asset identifier (0 = BTCUSDT for now)
  int64_t timestamp;

  double open;
  double high;
  double low;
  double close;
  double volume;

  double bid;
  double ask;
  double mid;

  double mark_price;
  double index_price;
  double funding_rate;
};

} // namespace ctrade
