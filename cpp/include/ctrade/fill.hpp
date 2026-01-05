#pragma once
#include <cstdint>

namespace ctrade {

struct Fill {
  int64_t order_id;
  double price;
  double size;
  double fee;
  int64_t timestamp;
};

} // namespace ctrade
