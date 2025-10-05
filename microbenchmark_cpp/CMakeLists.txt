cmake_minimum_required(VERSION 3.16)
project(io_uring_microbenchmark CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON CACHE INTERNAL "")
set(CMAKE_CXX_FLAGS "-O3")

add_executable(io_uring_microbenchmark io_uring_benchmark.cpp)

add_executable(open_microbenchmark create_benchmark.cpp)

target_link_libraries(io_uring_microbenchmark uring pthread)
target_link_libraries(open_microbenchmark uring pthread)

target_compile_options(io_uring_microbenchmark PRIVATE -g)
target_compile_options(open_microbenchmark PRIVATE -g)