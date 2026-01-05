cmake_minimum_required(VERSION 3.15)

# IMPORTANT: set compiler BEFORE project()
set(CMAKE_CXX_COMPILER clang++)

project(ctrade LANGUAGES CXX)

# ---- C++ standard & flags ----
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(-Wall -Wextra -O2)

# ---- pybind11 / Python ----
cmake_policy(SET CMP0148 NEW)
set(PYBIND11_FINDPYTHON ON)

find_package(pybind11 REQUIRED)
find_package(PostgreSQL REQUIRED)

# ---- Build private Python extension: _ctrade ----
pybind11_add_module(_ctrade
    bindings/bindings.cpp

    # Core engine skeleton (TODO implementations are fine)
    src/backtest.cpp
    src/execution_context.cpp
    src/execution_engine.cpp
    src/portfolio.cpp
    src/market_data.cpp
)

# ---- Include paths ----
target_include_directories(_ctrade
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${PostgreSQL_INCLUDE_DIRS}
)

# ---- Link libraries ----
target_link_libraries(_ctrade
    PRIVATE
        ${PostgreSQL_LIBRARIES}
)

# ---- Place the .so next to ctrade/__init__.py ----
set_target_properties(_ctrade PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/ctrade
)

