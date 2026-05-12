#include <faabric/rpc/RpcTransportClient.h>

#include <faabric/rpc/rpc.h>
#include <faabric/rpc/RpcMessageType.h>
#include <faabric/transport/common.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>

#include <stdexcept>

namespace faabric::rpc {

RpcTransportClient::RpcTransportClient(const std::string& hostIn,
                                       int asyncPortIn,
                                       int syncPortIn,
                                       int timeoutMs)
  : asyncPort(asyncPortIn)
  , asyncEndpoint(
        hostIn == faabric::util::getSystemConfig().endpointHost
          ? AsyncSendEndpointVariant(
                std::in_place_type<faabric::transport::AsyncInternalSendMessageEndpoint>,
                RPC_INPROC_LABEL,
                timeoutMs)
          : AsyncSendEndpointVariant(
                std::in_place_type<faabric::transport::AsyncSendMessageEndpoint>,
                hostIn,
                asyncPortIn,
                timeoutMs))
{}

void RpcTransportClient::asyncSendRequest(uint32_t requestId,
                                          const faabric::RpcRequest& reqIn)
{
    faabric::RpcRequest req = reqIn;
    req.set_requestid(requestId);
    req.set_replyhost(faabric::util::getSystemConfig().endpointHost);
    req.set_replyport(asyncPort);

    std::string buffer;
    if (!req.SerializeToString(&buffer)) {
        throw std::runtime_error("Failed to serialise RpcRequest");
    }

    std::visit(
        [&](auto& endpoint) {
            endpoint.send(faabric::rpc::RpcMessageType::INVOKE,
                        BYTES_CONST(buffer.c_str()),
                        buffer.size(),
                        requestId);
        },
        asyncEndpoint);

    SPDLOG_TRACE("RPC - Sent async request {}", requestId);
}

void RpcTransportClient::asyncSendResponse(const faabric::RpcResponse& resp)
{
    std::string buffer;
    if (!resp.SerializeToString(&buffer)) {
        SPDLOG_ERROR("RPC - Failed to serialise response for response {}",
                     resp.requestid());
        return; // client will timeout
    }

    std::visit(
        [&](auto& endpoint) {
            endpoint.send(faabric::rpc::RpcMessageType::RESPONSE,
                          BYTES_CONST(buffer.c_str()),
                          buffer.size(),
                          resp.requestid());
        },
        asyncEndpoint);
}

} // namespace faabric::rpc