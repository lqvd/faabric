#include <faabric/rpc/RpcTransportClient.h>

#include <faabric/rpc/rpc.h>
#include <faabric/rpc/RpcMessageType.h>
#include <faabric/transport/common.h>
#include <faabric/util/config.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/testing.h>

#include <stdexcept>

namespace faabric::rpc {

// -----------------------------------
// Mocking
// -----------------------------------
static std::mutex mockMutex;
static std::vector<faabric::RpcRequest> mockRpcRequests;
static std::vector<faabric::RpcResponse> mockRpcResponses;

std::vector<faabric::RpcRequest> getMockRpcRequests()
{
    faabric::util::UniqueLock lock(mockMutex);
    return mockRpcRequests;
}

std::vector<faabric::RpcResponse> getMockRpcResponses()
{
    faabric::util::UniqueLock lock(mockMutex);
    return mockRpcResponses;
}

void clearMockRpcMessages()
{
    faabric::util::UniqueLock lock(mockMutex);
    mockRpcRequests.clear();
    mockRpcResponses.clear();
}

// -----------------------------------
// Rpc transport client
// -----------------------------------

RpcTransportClient::RpcTransportClient(const std::string& hostIn,
                                       int asyncPortIn,
                                       int syncPortIn,
                                       int timeoutMs)
  : asyncPort(asyncPortIn)
{
    namespace Transport = faabric::transport;
    if (faabric::util::isMockMode()) {
        asyncEndpoint.emplace<std::monostate>();
    } else if (hostIn == faabric::util::getSystemConfig().endpointHost) {
        asyncEndpoint.emplace<Transport::AsyncInternalSendMessageEndpoint>(
            RPC_INPROC_LABEL, timeoutMs);
    } else {
        asyncEndpoint.emplace<Transport::AsyncSendMessageEndpoint>(
            hostIn, asyncPortIn, timeoutMs);
    }
}

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
        using Endpoint = std::decay_t<decltype(endpoint)>;

        if constexpr (std::is_same_v<Endpoint, std::monostate>) {
            SPDLOG_TRACE("RPC mock - recording request {}", requestId);
            faabric::util::UniqueLock lock(mockMutex);
            mockRpcRequests.push_back(req);
        } else {
            endpoint.send(faabric::rpc::RpcMessageType::INVOKE,
                          BYTES_CONST(buffer.c_str()),
                          buffer.size(),
                          requestId);
        }
    },
    asyncEndpoint);

    SPDLOG_TRACE("RPC - Sent async request {}", requestId);
}

void RpcTransportClient::asyncSendResponse(const faabric::RpcResponse& resp)
{
    std::string buffer;
    if (!resp.SerializeToString(&buffer)) {
        SPDLOG_ERROR("RPC - Failed to serialise response for {}",
                     resp.requestid());
        return;
    }

    std::visit(
    [&](auto& endpoint) {
        using Endpoint = std::decay_t<decltype(endpoint)>;

        if constexpr (std::is_same_v<Endpoint, std::monostate>) {
            SPDLOG_TRACE("RPC mock - recording response {}", resp.requestid());
            faabric::util::UniqueLock lock(mockMutex);
            mockRpcResponses.push_back(resp);
        } else {
            endpoint.send(faabric::rpc::RpcMessageType::RESPONSE,
                          BYTES_CONST(buffer.c_str()),
                          buffer.size(),
                          resp.requestid());
        }
    },
    asyncEndpoint);

    SPDLOG_TRACE("RPC - Sent async response {}", resp.requestid());
}

} // namespace faabric::rpc