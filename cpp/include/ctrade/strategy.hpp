#pragma once
#include "execution_context.hpp"
#include "market_state.hpp"

namespace ctrade {

struct Strategy {
  virtual void init() = 0;

  virtual void on_bar(const MarketState &market, ExecutionContext &ctx) = 0;

  virtual ~Strategy() = default;
};

} // namespace ctrade
