include(FetchContent)
include(ExternalProject)

cmake_minimum_required(VERSION 3.17)
project(arrow_duckdb_test)

set(CMAKE_CXX_STANDARD 17)

FetchContent_Declare(
        duckdb
        GIT_REPOSITORY https://github.com/duckdb/duckdb.git
        GIT_TAG        v0.3.2
)
FetchContent_MakeAvailable(duckdb)

#ExternalProject_Add(
#        arrow_ep
#        GIT_REPOSITORY "https://github.com/apache/arrow"
#        GIT_TAG release-7.0.0
#        PREFIX "${CMAKE_BINARY_DIR}/third_party/arrow"
#        INSTALL_DIR "${CMAKE_BINARY_DIR}/third_party/arrow/install"
#        BUILD_BYPRODUCTS
#        <INSTALL_DIR>/lib64/libarrow.a
#        <INSTALL_DIR>/lib64/libarrow_bundled_dependencies.a
#        <INSTALL_DIR>/lib64/libparquet.a
#        ${CMAKE_BINARY_DIR}/third_party/arrow/src/arrow_ep-build/thrift_ep-install/lib/libthriftd.a
#        ${CMAKE_BINARY_DIR}/third_party/arrow/src/arrow_ep-build/utf8proc_ep-install/lib/libutf8proc.a
#        ${CMAKE_BINARY_DIR}/third_party/arrow/src/arrow_ep-build/zstd_ep-install/lib/libzstd.a
#        ${CMAKE_BINARY_DIR}/third_party/arrow/src/arrow_ep-build/snappy_ep/src/snappy_ep-install/lib/libsnappy.a
#        ${CMAKE_BINARY_DIR}/third_party/arrow/src/arrow_ep-build/lz4_ep-prefix/include/liblz4.a
#        CONFIGURE_COMMAND
#        ${CMAKE_COMMAND} -G${CMAKE_GENERATOR} -DCMAKE_BUILD_TYPE=Release
#        -DBUILD_WARNING_LEVEL=PRODUCTION -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
#        -DCMAKE_CXX_COMPILER_LAUNCHER=${CMAKE_CXX_COMPILER_LAUNCHER}
#        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
#        -DCMAKE_CXX_STANDARD=${CMAKE_CXX_STANDARD}
#        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
#        -DCMAKE_C_COMPILER_LAUNCHER=${CMAKE_C_COMPILER_LAUNCHER}
#        -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/third_party/arrow/install
#        -DCMAKE_MODULE_PATH=${CMAKE_MODULE_PATH}
#        -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE} -DARROW_ALTIVEC=OFF
#        -DARROW_USE_CCACHE=ON -DARROW_BOOST_USE_SHARED=OFF -DARROW_BUILD_SHARED=OFF
#        -DARROW_BUILD_STATIC=ON -DARROW_BUILD_UTILITIES=OFF -DARROW_COMPUTE=OFF
#        -DARROW_DATASET=OFF -DARROW_FLIGHT=OFF -DARROW_GFLAGS_USE_SHARED=OFF
#        -DARROW_HDFS=OFF -DARROW_IPC=ON -DARROW_JSON=ON -DARROW_CSV=OFF
#        -DARROW_JEMALLOC=OFF -DARROW_ORC=OFF -DARROW_PARQUET=ON
#        -DARROW_PROTOBUF_USE_SHARED=OFF -DARROW_USE_GLOG=OFF -DARROW_SIMD_LEVEL=NONE
#        -DARROW_RUNTIME_SIMD_LEVEL=NONE -DARROW_WITH_BROTLI=OFF -DARROW_WITH_LZ4=ON
#        -DARROW_WITH_PROTOBUF=OFF -DARROW_WITH_RAPIDJSON=OFF -DARROW_WITH_SNAPPY=ON
#        -DARROW_WITH_ZLIB=ON -DARROW_WITH_ZSTD=ON -DBOOST_SOURCE=AUTO
#        <SOURCE_DIR>/cpp
#        CMAKE_ARGS -Wno-dev
#        UPDATE_COMMAND "")
#ExternalProject_Get_Property(arrow_ep install_dir)
#set(install_arrow ${install_dir})
#include_directories(${install_arrow}/include)

include(FetchContent)

FetchContent_Declare(
        Corrosion
        GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
        GIT_TAG v0.2.1
)
FetchContent_MakeAvailable(Corrosion)
corrosion_import_crate(MANIFEST_PATH rust_arrow_client/Cargo.toml)

add_executable(arrow_duckdb_test main.cpp)
#add_dependencies(arrow_duckdb_test arrow_ep)
target_link_libraries(arrow_duckdb_test PRIVATE
        duckdb_static
#        ${install_arrow}/lib64/libarrow.a
        rust_arrow_client
#        -fsanitize=address
#        -fsanitize=undefined
        )
