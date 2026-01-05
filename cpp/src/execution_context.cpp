#include "ctrade/execution_context.hpp"
#include <stdexcept>

namespace ctrade {

struct TodoExecutionContext : ExecutionContext {
  void market_buy(double) override { throw std::runtime_error("TODO"); }
  void market_sell(double) override { throw std::runtime_error("TODO"); }
  void limit_buy(double, double) override { throw std::runtime_error("TODO"); }
  void limit_sell(double, double) override { throw std::runtime_error("TODO"); }
  void stop_buy(double, double) override { throw std::runtime_error("TODO"); }
  void stop_sell(double, double) override { throw std::runtime_error("TODO"); }
  void stop_limit_buy(double, double, double) override {
    throw std::runtime_error("TODO");
  }
  void stop_limit_sell(double, double, double) override {
    throw std::runtime_error("TODO");
  }
  void close_position() override { throw std::runtime_error("TODO"); }
  void close_long() override { throw std::runtime_error("TODO"); }
  void close_short() override { throw std::runtime_error("TODO"); }
  void close_amount(double) override { throw std::runtime_error("TODO"); }
  void cancel_order(int) override { throw std::runtime_error("TODO"); }
  void cancel_all() override { throw std::runtime_error("TODO"); }
  void set_leverage(int) override { throw std::runtime_error("TODO"); }
  void set_cross_mode() override { throw std::runtime_error("TODO"); }
  void set_isolated_mode() override { throw std::runtime_error("TODO"); }
};

} // namespace ctrade
