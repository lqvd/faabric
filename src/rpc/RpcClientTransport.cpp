#include <faabric/rpc/RpcClientTransport.h>

#include <faabric/rpc/rpc.h>
#include <faabric/rpc/RpcMessageType.h>
#include <faabric/transport/common.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>

#include <stdexcept>

namespace faabric::rpc {

RpcClientTransport::RpcClientTransport(const std::string& hostIn,
                                       int asyncPortIn,
                                       int syncPortIn,
                                       int timeoutMs)
  : client(hostIn, asyncPortIn, syncPortIn, timeoutMs)
{}

void RpcClientTransport::sendRequestAsync(uint32_t requestId,
                                          const faabric::RpcRequest& reqIn)
{
    faabric::RpcRequest req = reqIn;
    req.set_requestid(requestId);
    req.set_replyhost(faabric::util::getSystemConfig().endpointHost);
    req.set_replyport(RPC_ASYNC_PORT);

    std::string buffer;
    if (!req.SerializeToString(&buffer)) {
        throw std::runtime_error("Failed to serialise RpcRequest");
    }

    client.asyncSend(faabric::rpc::RpcMessageType::INVOKE,
                     BYTES_CONST(buffer.c_str()),
                     buffer.size(),
                     requestId);

    SPDLOG_TRACE("RPC - Sent async request {}", requestId);
}

} // namespace faabric::rpc