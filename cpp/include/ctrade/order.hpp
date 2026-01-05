#pragma once
#include <cstdint>

namespace ctrade {

enum class Side { Buy, Sell };
enum class OrderType { Market, Limit, Stop, StopLimit };

struct Order {
  int64_t id;
  Side side;
  OrderType type;
  double price;
  double size;
  int64_t timestamp;
};

} // namespace ctrade
