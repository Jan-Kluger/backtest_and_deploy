cmake_minimum_required(VERSION 3.15)

# IMPORTANT: set compiler BEFORE project()
set(CMAKE_CXX_COMPILER clang++)

project(ctrade LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(-Wall -Wextra -O2)

cmake_policy(SET CMP0148 NEW)
set(PYBIND11_FINDPYTHON ON)

find_package(pybind11 REQUIRED)
find_package(PostgreSQL REQUIRED)

# Build PRIVATE extension module: _ctrade
pybind11_add_module(_ctrade
    bindings/bindings.cpp
    src/ctrade.cpp
)

target_include_directories(_ctrade PRIVATE ${PostgreSQL_INCLUDE_DIRS})
target_link_libraries(_ctrade PRIVATE ${PostgreSQL_LIBRARIES})

# Ensure the .so lands in the Python package directory
set_target_properties(_ctrade PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/ctrade
)
