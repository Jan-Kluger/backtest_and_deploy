#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include "ctrade/backtest.hpp"
#include "ctrade/config.hpp"
#include "ctrade/strategy.hpp"
#include "ctrade/backtest_result.hpp"
#include "ctrade/market_state.hpp"
#include "ctrade/execution_context.hpp"

namespace py = pybind11;

// Python strategy trampoline class
class PyStrategy : public ctrade::Strategy {
public:
  using ctrade::Strategy::Strategy;

  void init() override {
    PYBIND11_OVERRIDE_PURE(void, ctrade::Strategy, init);
  }

  void on_bar(const ctrade::MarketState& market, ctrade::ExecutionContext& ctx) override {
    PYBIND11_OVERRIDE_PURE(void, ctrade::Strategy, on_bar, market, ctx);
  }
};

// Python execution context trampoline (for type exposure)
class PyExecutionContext : public ctrade::ExecutionContext {
public:
  using ctrade::ExecutionContext::ExecutionContext;

  void market_buy(double size) override {
    PYBIND11_OVERRIDE_PURE(void, ctrade::ExecutionContext, market_buy, size);
  }
  void market_sell(double size) override {
    PYBIND11_OVERRIDE_PURE(void, ctrade::ExecutionContext, market_sell, size);
  }
  void limit_buy(double size, double price) override {
    PYBIND11_OVERRIDE_PURE(void, ctrade::ExecutionContext, limit_buy, size, price);
  }
  void limit_sell(double size, double price) override {
    PYBIND11_OVERRIDE_PURE(void, ctrade::ExecutionContext, limit_sell, size, price);
  }
  void stop_buy(double size, double stop_price) override {
    PYBIND11_OVERRIDE_PURE(void, ctrade::ExecutionContext, stop_buy, size, stop_price);
  }
  void stop_sell(double size, double stop_price) override {
    PYBIND11_OVERRIDE_PURE(void, ctrade::ExecutionContext, stop_sell, size, stop_price);
  }
  void stop_limit_buy(double size, double stop_price, double limit_price) override {
    PYBIND11_OVERRIDE_PURE(void, ctrade::ExecutionContext, stop_limit_buy, size, stop_price, limit_price);
  }
  void stop_limit_sell(double size, double stop_price, double limit_price) override {
    PYBIND11_OVERRIDE_PURE(void, ctrade::ExecutionContext, stop_limit_sell, size, stop_price, limit_price);
  }
  void close_position() override {
    PYBIND11_OVERRIDE_PURE(void, ctrade::ExecutionContext, close_position);
  }
  void close_long() override {
    PYBIND11_OVERRIDE_PURE(void, ctrade::ExecutionContext, close_long);
  }
  void close_short() override {
    PYBIND11_OVERRIDE_PURE(void, ctrade::ExecutionContext, close_short);
  }
  void close_amount(double size) override {
    PYBIND11_OVERRIDE_PURE(void, ctrade::ExecutionContext, close_amount, size);
  }
  void cancel_order(int order_id) override {
    PYBIND11_OVERRIDE_PURE(void, ctrade::ExecutionContext, cancel_order, order_id);
  }
  void cancel_all() override {
    PYBIND11_OVERRIDE_PURE(void, ctrade::ExecutionContext, cancel_all);
  }
  void set_leverage(int lev) override {
    PYBIND11_OVERRIDE_PURE(void, ctrade::ExecutionContext, set_leverage, lev);
  }
  void set_cross_mode() override {
    PYBIND11_OVERRIDE_PURE(void, ctrade::ExecutionContext, set_cross_mode);
  }
  void set_isolated_mode() override {
    PYBIND11_OVERRIDE_PURE(void, ctrade::ExecutionContext, set_isolated_mode);
  }
};

PYBIND11_MODULE(_ctrade, m) {
  m.doc() = "C++ backtesting engine for ctrade";

  // DatabaseConfig
  py::class_<ctrade::DatabaseConfig>(m, "DatabaseConfig")
    .def(py::init<>())
    .def_readwrite("host", &ctrade::DatabaseConfig::host)
    .def_readwrite("port", &ctrade::DatabaseConfig::port)
    .def_readwrite("database", &ctrade::DatabaseConfig::database)
    .def_readwrite("user", &ctrade::DatabaseConfig::user)
    .def_readwrite("password", &ctrade::DatabaseConfig::password);

  // BacktestConfig
  py::class_<ctrade::BacktestConfig>(m, "BacktestConfig")
    .def(py::init<>())
    .def_readwrite("db_config", &ctrade::BacktestConfig::db_config)
    .def_readwrite("start_ts", &ctrade::BacktestConfig::start_ts)
    .def_readwrite("end_ts", &ctrade::BacktestConfig::end_ts);

  // MarketState
  py::class_<ctrade::MarketState>(m, "MarketState")
    .def(py::init<>())
    .def_readwrite("asset_id", &ctrade::MarketState::asset_id)
    .def_readwrite("timestamp", &ctrade::MarketState::timestamp)
    .def_readwrite("open", &ctrade::MarketState::open)
    .def_readwrite("high", &ctrade::MarketState::high)
    .def_readwrite("low", &ctrade::MarketState::low)
    .def_readwrite("close", &ctrade::MarketState::close)
    .def_readwrite("volume", &ctrade::MarketState::volume)
    .def_readwrite("bid", &ctrade::MarketState::bid)
    .def_readwrite("ask", &ctrade::MarketState::ask)
    .def_readwrite("mid", &ctrade::MarketState::mid)
    .def_readwrite("mark_price", &ctrade::MarketState::mark_price)
    .def_readwrite("index_price", &ctrade::MarketState::index_price)
    .def_readwrite("funding_rate", &ctrade::MarketState::funding_rate);

  // BacktestResult
  py::class_<ctrade::BacktestResult>(m, "BacktestResult")
    .def(py::init<>())
    .def_readwrite("timestamps", &ctrade::BacktestResult::timestamps)
    .def_readwrite("equity", &ctrade::BacktestResult::equity)
    .def_readwrite("pnl", &ctrade::BacktestResult::pnl)
    .def_readwrite("drawdown", &ctrade::BacktestResult::drawdown);

  // ExecutionContext (abstract base, exposed for type hints)
  py::class_<ctrade::ExecutionContext, PyExecutionContext>(m, "ExecutionContext")
    .def(py::init<>())
    .def("market_buy", &ctrade::ExecutionContext::market_buy)
    .def("market_sell", &ctrade::ExecutionContext::market_sell)
    .def("limit_buy", &ctrade::ExecutionContext::limit_buy)
    .def("limit_sell", &ctrade::ExecutionContext::limit_sell)
    .def("stop_buy", &ctrade::ExecutionContext::stop_buy)
    .def("stop_sell", &ctrade::ExecutionContext::stop_sell)
    .def("stop_limit_buy", &ctrade::ExecutionContext::stop_limit_buy)
    .def("stop_limit_sell", &ctrade::ExecutionContext::stop_limit_sell)
    .def("close_position", &ctrade::ExecutionContext::close_position)
    .def("close_long", &ctrade::ExecutionContext::close_long)
    .def("close_short", &ctrade::ExecutionContext::close_short)
    .def("close_amount", &ctrade::ExecutionContext::close_amount)
    .def("cancel_order", &ctrade::ExecutionContext::cancel_order)
    .def("cancel_all", &ctrade::ExecutionContext::cancel_all)
    .def("set_leverage", &ctrade::ExecutionContext::set_leverage)
    .def("set_cross_mode", &ctrade::ExecutionContext::set_cross_mode)
    .def("set_isolated_mode", &ctrade::ExecutionContext::set_isolated_mode);

  // Strategy (abstract base for Python strategies)
  py::class_<ctrade::Strategy, PyStrategy>(m, "Strategy")
    .def(py::init<>())
    .def("init", &ctrade::Strategy::init)
    .def("on_bar", &ctrade::Strategy::on_bar);

  // Main backtest function
  m.def("backtest", &ctrade::backtest,
        "Run backtest with strategy and config",
        py::arg("strategy"), py::arg("config"));
}
