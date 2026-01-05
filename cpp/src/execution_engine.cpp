#include "ctrade/execution_engine.hpp"
#include <stdexcept>

namespace ctrade {

std::vector<Fill> ExecutionEngine::execute(const std::vector<Order> &,
                                           const MarketState &) {
  throw std::runtime_error("TODO: ExecutionEngine::execute");
}

} // namespace ctrade
