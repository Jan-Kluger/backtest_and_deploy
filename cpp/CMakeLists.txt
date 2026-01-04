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

pybind11_add_module(ctrade
    bindings.cpp
    ctrade.cpp
)

target_include_directories(ctrade PRIVATE ${PostgreSQL_INCLUDE_DIRS})
target_link_libraries(ctrade PRIVATE ${PostgreSQL_LIBRARIES})

