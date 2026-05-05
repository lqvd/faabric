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

class RpcClientTransport
{
  public:
    RpcClientTransport(const std::string& hostIn,
                       int asyncPortIn,
                       int syncPortIn,
                       int timeoutMs);

    void sendRequestAsync(uint32_t requestId, const faabric::RpcRequest& req);

  private:
    faabric::transport::MessageEndpointClient client;
};

} // namespace faabric::rpc