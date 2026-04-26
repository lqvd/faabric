faabric_lib(rpc
    rpc.cpp
    RpcClientTransport.cpp
    RpcContext.cpp
    RpcContextRegistry.cpp
    RpcServer.cpp
)

target_link_libraries(rpc PRIVATE
    faabric::transport_tcp
    faabric::util
    faabric::proto
    faabric::scheduling_util
)
