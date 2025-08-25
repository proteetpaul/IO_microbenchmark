cmake_minimum_required(VERSION 3.16)
project(spdk_microbenchmark CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Always generate compile_commands.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON CACHE INTERNAL "")
set(ENV{PKG_CONFIG_PATH} "/home/proteet/git_repos/spdk/build/lib/pkgconfig:$ENV{PKG_CONFIG_PATH}")

# Where to find SPDK installation (adjust path if you installed elsewhere)
set(SPDK_DIR "~/git_repos/spdk")

# Add SPDK include directories
include_directories(
    ${SPDK_DIR}/include
    ${SPDK_DIR}/include/spdk
    /usr/include/c++/11
)

# Find libraries inside SPDK build
link_directories(${SPDK_DIR}/build/lib ${SPDK_DIR}/dpdk/build/lib)

add_executable(spdk_microbenchmark read_seq_benchmark.cpp)

# Link against SPDK libs you need
find_package(PkgConfig REQUIRED)
pkg_check_modules(SPDK_ENV REQUIRED spdk_env_dpdk)
pkg_check_modules(SPDK_NVME REQUIRED spdk_nvme)

execute_process(
    COMMAND pkg-config --libs --static spdk_syslibs
    OUTPUT_VARIABLE SPDK_SYS_LIBS
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
separate_arguments(SPDK_SYS_LIBS NATIVE_COMMAND ${SPDK_SYS_LIBS})

message(STATUS "The value of SPDK_NVME_LIBRARIES is: ${SPDK_NVME_LIBRARIES}")
# Order matters here!!
target_link_libraries(spdk_microbenchmark ${SPDK_NVME_LIBRARIES} ${SPDK_ENV_LIBRARIES} ${SPDK_SYS_LIBS})
# target_include_directories(spdk_microbenchmark PRIVATE ${SPDK_INCLUDE_DIRS})
