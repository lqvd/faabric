faabric_lib(rpc
    ExternalChannel.cpp
    FaabricChannel.cpp
    rpc.cpp
    RpcContext.cpp
    RpcServer.cpp
)

target_link_libraries(rpc PRIVATE
    faabric::transport_tcp
    faabric::util
    faabric::proto
    faabric::scheduling_util
)
