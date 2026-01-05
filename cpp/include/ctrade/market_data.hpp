#pragma once
#include "market_state.hpp"

namespace ctrade {

struct MarketData {
  virtual bool next() = 0;
  virtual const MarketState &current() const = 0;
  virtual ~MarketData() = default;
};

} // namespace ctrade
