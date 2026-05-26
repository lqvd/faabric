faabric_lib(rpc
    rpc.cpp
    RpcContext.cpp
    RpcContextRegistry.cpp
    RpcServer.cpp
    RpcTracker.cpp
    RpcTransportClient.cpp
)

target_link_libraries(rpc PRIVATE
    faabric::transport_tcp
    faabric::util
    faabric::proto
    faabric::scheduling_util
)
