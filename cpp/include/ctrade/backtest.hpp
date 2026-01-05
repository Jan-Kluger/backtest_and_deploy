#pragma once
#include "backtest_result.hpp"
#include "config.hpp"
#include "strategy.hpp"

namespace ctrade {

BacktestResult backtest(Strategy &strategy, const BacktestConfig &config);

}
