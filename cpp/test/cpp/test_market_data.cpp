#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include "ctrade/market_data.hpp"
#include "ctrade/market_state.hpp"
#include <vector>

using Catch::Approx;

// Mock MarketData for testing (no DB dependency)
class MockMarketData : public ctrade::MarketData {
public:
    MockMarketData() : current_index_(0) {
        // Pre-populate with test data
        ctrade::MarketState s1{};
        s1.asset_id = 0;
        s1.timestamp = 1000;
        s1.open = 100.0;
        s1.high = 105.0;
        s1.low = 99.0;
        s1.close = 102.0;
        s1.volume = 1000.0;
        s1.bid = 101.5;
        s1.ask = 102.5;
        s1.mid = 102.0;
        s1.mark_price = 102.0;
        s1.index_price = 102.0;
        s1.funding_rate = 0.0001;
        states_.push_back(s1);

        ctrade::MarketState s2{};
        s2.asset_id = 0;
        s2.timestamp = 2000;
        s2.open = 102.0;
        s2.high = 108.0;
        s2.low = 101.0;
        s2.close = 106.0;
        s2.volume = 1200.0;
        s2.bid = 105.5;
        s2.ask = 106.5;
        s2.mid = 106.0;
        s2.mark_price = 106.0;
        s2.index_price = 106.0;
        s2.funding_rate = 0.0001;
        states_.push_back(s2);
    }

    bool next() override {
        if (current_index_ < states_.size() - 1) {
            current_index_++;
            return true;
        }
        return false;
    }

    const ctrade::MarketState& current() const override {
        return states_[current_index_];
    }

private:
    std::vector<ctrade::MarketState> states_;
    size_t current_index_;
};

TEST_CASE("MockMarketData streams sequentially", "[market_data]") {
    MockMarketData data;

    REQUIRE(data.current().timestamp == 1000);
    REQUIRE(data.current().close == Approx(102.0));

    REQUIRE(data.next() == true);
    REQUIRE(data.current().timestamp == 2000);
    REQUIRE(data.current().close == Approx(106.0));

    REQUIRE(data.next() == false);  // End of data
}

TEST_CASE("MarketState has correct asset_id", "[market_data]") {
    MockMarketData data;

    const auto& state = data.current();
    REQUIRE(state.asset_id == 0);  // BTCUSDT
}

TEST_CASE("MarketState provides bid/ask spread", "[market_data]") {
    MockMarketData data;

    const auto& state = data.current();
    REQUIRE(state.bid == Approx(101.5));
    REQUIRE(state.ask == Approx(102.5));
    REQUIRE(state.mid == Approx(102.0));
    REQUIRE(state.ask > state.bid);  // Spread is positive
}

TEST_CASE("MarketState includes funding rate", "[market_data]") {
    MockMarketData data;

    const auto& state = data.current();
    REQUIRE(state.funding_rate == Approx(0.0001));
}

