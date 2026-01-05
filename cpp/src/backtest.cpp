#include "ctrade/backtest.hpp"
#include <stdexcept>

namespace ctrade {

BacktestResult backtest(Strategy &, const BacktestConfig &) {
  throw std::runtime_error("TODO: backtest not implemented");
}

} // namespace ctrade
