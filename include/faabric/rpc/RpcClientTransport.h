#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/MessageEndpointClient.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

namespace faabric::rpc {

struct RpcOp
{
    bool ready = false;
    bool failed = false;
    faabric::RpcResponse response;
};

class RpcClientTransport
{
  public:
    RpcClientTransport(const std::string& hostIn,
                       int asyncPortIn,
                       int syncPortIn,
                       int timeoutMs);

    void sendRequestAsync(uint32_t requestId, const faabric::RpcRequest& req);

    // Non-consuming readiness check.
    bool testResponse(uint32_t requestId);

    // Non-consuming get.  Returns false if unknown or not ready.
    bool peekResponse(uint32_t requestId, faabric::RpcResponse& out);

    // Consuming get. Returns false if unknown or not ready.
    bool getResponse(uint32_t requestId, faabric::RpcResponse& out);

    // Optional legacy helper.
    faabric::RpcResponse waitForResponse(uint32_t requestId);

    // Called when a response message arrives.
    void onResponseReceived(const faabric::RpcResponse& resp);

    bool hasPendingRequest(uint32_t requestId);

    void eraseRequest(uint32_t requestId);

  private:
    faabric::transport::MessageEndpointClient client;

    std::atomic<uint32_t> nextRequestId{ 1 };

    std::mutex mx;
    std::condition_variable cv;
    std::unordered_map<uint32_t, RpcOp> ops;
};

} // namespace faabric::rpc