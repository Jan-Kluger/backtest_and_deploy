#pragma once

namespace ctrade {

struct Portfolio {
  double cash = 0.0;
  double position = 0.0;
  double equity = 0.0;

  void apply_fill();
};

} // namespace ctrade
