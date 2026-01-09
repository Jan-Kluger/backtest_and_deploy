#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include "ctrade/portfolio.hpp"
#include "ctrade/fill.hpp"

using Catch::Approx;

TEST_CASE("Portfolio initializes with zero values", "[portfolio]") {
    ctrade::Portfolio p;

    REQUIRE(p.cash == Approx(0.0));
    REQUIRE(p.position == Approx(0.0));
    REQUIRE(p.equity == Approx(0.0));
}

TEST_CASE("Portfolio apply_fill updates state", "[portfolio]") {
    ctrade::Portfolio p;
    p.cash = 10000.0;
    p.position = 0.0;

    // TODO: Once apply_fill is implemented, test:
    // - Buy fill reduces cash, increases position
    // - Sell fill increases cash, decreases position
    // - Fees are deducted
    // - Equity is updated

    // For now, just verify structure exists
    REQUIRE(p.cash == Approx(10000.0));
}

TEST_CASE("Portfolio tracks multiple fills", "[portfolio]") {
    ctrade::Portfolio p;
    p.cash = 10000.0;

    // TODO: Test sequence of fills
    // - Multiple buys accumulate position
    // - Partial sells reduce position
    // - Net PnL is tracked

    REQUIRE(p.cash == Approx(10000.0));
}

