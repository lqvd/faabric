#pragma once

#include <faabric/rpc/RpcContext.h>
#include <faabric/util/concurrent_map.h>

#include <cstdint>
#include <optional>

namespace faabric::rpc {

class RpcContextRegistry
{
  public:
    void registerContext(int32_t contextId, std::shared_ptr<RpcContext> ctx);

    void registerInFlightRequest(uint32_t requestId, int32_t contextId);

    std::shared_ptr<RpcContext> getContextForRequest(uint32_t requestId);

    std::optional<int32_t> getContextIdForRequest(uint32_t requestId);

    void clearRequest(uint32_t requestId);

    void removeContext(int32_t contextId);

    void clearAllRequestsForContext(int32_t targetContextId);

    void setForwardingAddress(int32_t contextId, std::string newHost);

    std::optional<std::string> getForwardingAddress(int32_t contextId);

  private:
    faabric::util::ConcurrentMap<int32_t, std::shared_ptr<RpcContext>> contexts;
    faabric::util::ConcurrentMap<uint32_t, int32_t> requestToContextId;

    // If Wasm module migrates to Host B, we proxy the async reply to B via the
    // forwarding table.
    faabric::util::ConcurrentMap<int32_t, std::string> forwardingTable;
};

RpcContextRegistry& getRpcContextRegistry();

} // namespace faabric::rpc