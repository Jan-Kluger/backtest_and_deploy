#pragma once

namespace ctrade {

struct ExecutionContext {
  // --- Market orders ---
  virtual void market_buy(double size) = 0;
  virtual void market_sell(double size) = 0;

  // --- Limit orders ---
  virtual void limit_buy(double size, double price) = 0;
  virtual void limit_sell(double size, double price) = 0;

  // --- Stops ---
  virtual void stop_buy(double size, double stop_price) = 0;
  virtual void stop_sell(double size, double stop_price) = 0;

  // --- Stop-limits ---
  virtual void stop_limit_buy(double size, double stop_price,
                              double limit_price) = 0;

  virtual void stop_limit_sell(double size, double stop_price,
                               double limit_price) = 0;

  // --- Position management ---
  virtual void close_position() = 0;
  virtual void close_long() = 0;
  virtual void close_short() = 0;
  virtual void close_amount(double size) = 0;

  // --- Order management ---
  virtual void cancel_order(int order_id) = 0;
  virtual void cancel_all() = 0;

  // --- Futures controls ---
  virtual void set_leverage(int lev) = 0;
  virtual void set_cross_mode() = 0;
  virtual void set_isolated_mode() = 0;

  virtual ~ExecutionContext() = default;
};

} // namespace ctrade
