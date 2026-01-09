# C++ unit tests using Catch2

# Test executable for portfolio
add_executable(test_portfolio
    test_portfolio.cpp
    ${CMAKE_SOURCE_DIR}/src/portfolio.cpp
)

target_include_directories(test_portfolio
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(test_portfolio
    PRIVATE
        Catch2::Catch2WithMain
)

# Test executable for execution engine
add_executable(test_execution
    test_execution.cpp
    ${CMAKE_SOURCE_DIR}/src/execution_engine.cpp
)

target_include_directories(test_execution
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(test_execution
    PRIVATE
        Catch2::Catch2WithMain
)

# Test executable for market data (mocked, no DB)
add_executable(test_market_data
    test_market_data.cpp
)

target_include_directories(test_market_data
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(test_market_data
    PRIVATE
        Catch2::Catch2WithMain
)

# Register tests with CTest
include(CTest)
include(Catch)
catch_discover_tests(test_portfolio)
catch_discover_tests(test_execution)
catch_discover_tests(test_market_data)

