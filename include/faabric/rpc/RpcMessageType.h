#pragma once

namespace faabric::rpc {

enum RpcMessageType
{
    INVOKE = 0,
    RESPONSE = 1,
    FETCH = 2,
    INVOKE_FETCH = 3,
    SHUTDOWN_SERVICE = 4,
};

}   // namespace faabric::rpc
