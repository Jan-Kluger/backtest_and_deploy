#include "ctrade/market_data.hpp"
#include <stdexcept>

namespace ctrade {

bool MarketData::next() { throw std::runtime_error("TODO: MarketData::next"); }

const MarketState &MarketData::current() const {
  throw std::runtime_error("TODO: MarketData::current");
}

} // namespace ctrade
