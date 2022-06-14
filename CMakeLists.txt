#set(Rust_TOOLCHAIN stable-aarch64-apple-darwin)
set(CMAKE_OSX_ARCHITECTURES "arm64" CACHE INTERNAL "" FORCE)

include(FetchContent)
include(ExternalProject)

cmake_minimum_required(VERSION 3.17)
project(arrow_duckdb_test)

set(CMAKE_CXX_STANDARD 17)

FetchContent_Declare(
        duckdb
        GIT_REPOSITORY https://github.com/duckdb/duckdb.git
        GIT_TAG        v0.3.4
)
FetchContent_MakeAvailable(duckdb)

include(FetchContent)

FetchContent_Declare(
        Corrosion
        GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
        GIT_TAG v0.2.1
)
FetchContent_MakeAvailable(Corrosion)
corrosion_import_crate(MANIFEST_PATH rust_arrow_client/Cargo.toml)

add_executable(arrow_duckdb_test main.cpp)
if (APPLE)
    target_link_libraries(arrow_duckdb_test PRIVATE duckdb_static rust_arrow_client "-framework Security")
else()
    target_link_libraries(arrow_duckdb_test PRIVATE duckdb_static rust_arrow_client)
endif()