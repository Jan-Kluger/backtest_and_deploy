#pragma once
#include <cstdint>

namespace ctrade {

struct BacktestConfig {
  int64_t start_ts;
  int64_t end_ts;
};

} // namespace ctrade
