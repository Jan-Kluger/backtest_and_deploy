#pragma once
#include <cstdint>
#include <vector>

namespace ctrade {

struct BacktestResult {
  std::vector<int64_t> timestamps;
  std::vector<double> equity;
  std::vector<double> pnl;
  std::vector<double> drawdown;
};

} // namespace ctrade
