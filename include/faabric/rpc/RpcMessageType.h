#pragma once

namespace faabric::rpc {

enum RpcMessageType
{
    INVOKE = 0,
    RESPONSE = 1,
    FETCH = 2,
    SHUTDOWN_SERVICE = 3,
};

}   // namespace faabric::rpc
