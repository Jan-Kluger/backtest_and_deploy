#pragma once
#include "fill.hpp"
#include "market_state.hpp"
#include "order.hpp"
#include <vector>

namespace ctrade {

struct ExecutionEngine {
  virtual std::vector<Fill> execute(const std::vector<Order> &orders,
                                    const MarketState &market) = 0;

  virtual ~ExecutionEngine() = default;
};

} // namespace ctrade
