#include <pybind11/pybind11.h>

double get_most_recent_price();

PYBIND11_MODULE(ctrade, m) {
  m.doc() = "Minimal C++ Python bindings for ctrade";

  m.def("get_most_recent_price", &get_most_recent_price,
        "Return the most recent BTCUSDT close price");
}
