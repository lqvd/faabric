#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/MessageEndpoint.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

namespace faabric::rpc {

// -----------------------------------
// Mocking
// -----------------------------------
std::vector<faabric::RpcRequest> getMockRpcRequests();
std::vector<faabric::RpcResponse> getMockRpcResponses();
void clearMockRpcMessages();

class RpcTransportClient
{
  public:
    RpcTransportClient(const std::string& hostIn,
                       int asyncPortIn,
                       int syncPortIn,
                       int timeoutMs);

    void asyncSendRequest(uint32_t requestId, const faabric::RpcRequest& req);
    
    void asyncSendResponse(const faabric::RpcResponse& resp);

    void asyncSendFetch(const faabric::RpcFetchRequest& fetch);

  private:
    int asyncPort;

    using AsyncSendEndpointVariant =
        std::variant<std::monostate,
                     faabric::transport::AsyncSendMessageEndpoint,
                     faabric::transport::AsyncInternalSendMessageEndpoint>;
    AsyncSendEndpointVariant asyncEndpoint;
};

} // namespace faabric::rpc