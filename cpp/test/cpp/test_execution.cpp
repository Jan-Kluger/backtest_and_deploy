#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include "ctrade/execution_engine.hpp"
#include "ctrade/market_state.hpp"
#include "ctrade/order.hpp"
#include "ctrade/fill.hpp"
#include <vector>

using Catch::Approx;

// Mock execution engine for testing
class MockExecutionEngine : public ctrade::ExecutionEngine {
public:
    std::vector<ctrade::Fill> execute(
        const std::vector<ctrade::Order>& orders,
        const ctrade::MarketState& market
    ) override {
        std::vector<ctrade::Fill> fills;

        for (const auto& order : orders) {
            if (order.type == ctrade::OrderType::Market) {
                ctrade::Fill fill;
                fill.order_id = order.id;
                fill.size = order.size;

                // Market buy fills at ask, sell fills at bid
                if (order.side == ctrade::Side::Buy) {
                    fill.price = market.ask;
                } else {
                    fill.price = market.bid;
                }

                fill.fee = fill.size * fill.price * 0.001;  // 0.1% fee
                fill.timestamp = market.timestamp;

                fills.push_back(fill);
            }
        }

        return fills;
    }
};

TEST_CASE("Market buy fills at ask price", "[execution]") {
    MockExecutionEngine engine;

    ctrade::MarketState market{};
    market.ask = 101.0;
    market.bid = 99.0;
    market.timestamp = 1000;

    ctrade::Order order{};
    order.id = 1;
    order.side = ctrade::Side::Buy;
    order.type = ctrade::OrderType::Market;
    order.size = 10.0;

    auto fills = engine.execute({order}, market);

    REQUIRE(fills.size() == 1);
    REQUIRE(fills[0].price == Approx(101.0));
    REQUIRE(fills[0].size == Approx(10.0));
}

TEST_CASE("Market sell fills at bid price", "[execution]") {
    MockExecutionEngine engine;

    ctrade::MarketState market{};
    market.ask = 101.0;
    market.bid = 99.0;
    market.timestamp = 1000;

    ctrade::Order order{};
    order.id = 1;
    order.side = ctrade::Side::Sell;
    order.type = ctrade::OrderType::Market;
    order.size = 10.0;

    auto fills = engine.execute({order}, market);

    REQUIRE(fills.size() == 1);
    REQUIRE(fills[0].price == Approx(99.0));
}

TEST_CASE("Execution calculates fees", "[execution]") {
    MockExecutionEngine engine;

    ctrade::MarketState market{};
    market.ask = 100.0;
    market.bid = 100.0;
    market.timestamp = 1000;

    ctrade::Order order{};
    order.id = 1;
    order.side = ctrade::Side::Buy;
    order.type = ctrade::OrderType::Market;
    order.size = 10.0;

    auto fills = engine.execute({order}, market);

    REQUIRE(fills.size() == 1);
    // Fee = 10 * 100 * 0.001 = 1.0
    REQUIRE(fills[0].fee == Approx(1.0));
}

TEST_CASE("Multiple orders produce multiple fills", "[execution]") {
    MockExecutionEngine engine;

    ctrade::MarketState market{};
    market.ask = 100.0;
    market.bid = 100.0;
    market.timestamp = 1000;

    ctrade::Order order1{};
    order1.id = 1;
    order1.side = ctrade::Side::Buy;
    order1.type = ctrade::OrderType::Market;
    order1.size = 10.0;

    ctrade::Order order2{};
    order2.id = 2;
    order2.side = ctrade::Side::Sell;
    order2.type = ctrade::OrderType::Market;
    order2.size = 5.0;

    auto fills = engine.execute({order1, order2}, market);

    REQUIRE(fills.size() == 2);
}

