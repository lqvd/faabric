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

class RpcTransportClient
{
  public:
    RpcTransportClient(const std::string& hostIn,
                       int asyncPortIn,
                       int syncPortIn,
                       int timeoutMs);

    void asyncSendRequest(uint32_t requestId, const faabric::RpcRequest& req);
    
    void asyncSendResponse(const faabric::RpcResponse& resp);

  private:
    int asyncPort;

    using AsyncSendEndpointVariant =
        std::variant<faabric::transport::AsyncSendMessageEndpoint,
                     faabric::transport::AsyncInternalSendMessageEndpoint>;
    AsyncSendEndpointVariant asyncEndpoint;
};

} // namespace faabric::rpc