cmake_minimum_required(VERSION 3.13.0)
project(faabricrpc)

# -----------------------------------------------
# This library must support being compiled on its own
# -----------------------------------------------

set(FAABRIC_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/../../include)

include_directories(${FAABRIC_INCLUDE_DIR})

set(PUBLIC_HEADERS
        ${FAABRIC_INCLUDE_DIR}/faabric/rpc/rpc.h
    )


add_library(faabricrpc rpc.cpp ${PUBLIC_HEADERS})
add_library(faabric::faabricrpc ALIAS faabricrpc)

set_target_properties(faabricrpc
    PROPERTIES PUBLIC_HEADER "${PUBLIC_HEADERS}"
    )

if(CMAKE_SYSTEM_NAME STREQUAL "Wasm")
    message(STATUS "Faabric RPC WebAssembly build")
    set(LIB_DIRECTORY ${CMAKE_SYSROOT}/lib/wasm32-wasi)

    install(TARGETS faabricrpc
        ARCHIVE DESTINATION ${LIB_DIRECTORY}
        LIBRARY DESTINATION ${LIB_DIRECTORY}
        PUBLIC_HEADER DESTINATION ${CMAKE_SYSROOT}/include/faabric/rpc
    )
else()
    message(STATUS "Faabric RPC native build")
    install(TARGETS faabricrpc
        PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_PREFIX}/include/faabric/rpc
    )
endif()

