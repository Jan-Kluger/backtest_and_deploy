cmake_minimum_required(VERSION 3.15)

# IMPORTANT: set compiler BEFORE project()
set(CMAKE_CXX_COMPILER clang++)

project(ctrade LANGUAGES CXX)

# ---- C++ standard & flags ----
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(-Wall -Wextra -O2)

# ---- Generate compile_commands.json for LSP support ----
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ---- pybind11 / Python ----
cmake_policy(SET CMP0148 NEW)
set(PYBIND11_FINDPYTHON ON)

find_package(pybind11 REQUIRED)
find_package(PostgreSQL REQUIRED)

# ---- Catch2 for testing (FetchContent) ----
include(FetchContent)
FetchContent_Declare(
  Catch2
  GIT_REPOSITORY https://github.com/catchorg/Catch2.git
  GIT_TAG        v3.5.0
)
FetchContent_MakeAvailable(Catch2)

# ---- Build private Python extension: _ctrade ----
pybind11_add_module(_ctrade
    bindings/bindings.cpp

    # Core engine skeleton
    src/backtest.cpp
    src/execution_context.cpp
    src/execution_engine.cpp
    src/portfolio.cpp
    src/market_data.cpp
    src/postgres_market_data.cpp
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

# ---- Enable testing ----
enable_testing()

# ---- Add C++ test subdirectory ----
add_subdirectory(test/cpp)
