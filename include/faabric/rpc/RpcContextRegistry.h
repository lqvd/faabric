#pragma once

#include <faabric/rpc/RpcContext.h>
#include <faabric/util/concurrent_map.h>

#include <cstdint>
#include <optional>

namespace faabric::rpc {

class RpcContextRegistry
{
  public:
    void registerContext(int32_t msgIdx, std::shared_ptr<RpcContext> ctx);

    void registerInFlightRequest(uint32_t requestId, int32_t msgIdx);

    std::shared_ptr<RpcContext> getContext(int32_t msgIdx);

    void removeContext(int32_t msgIdx);

    std::shared_ptr<RpcContext> getContextForRequest(uint32_t requestId);

    std::optional<int32_t> getMsgIdxForRequest(uint32_t requestId);

    void clearRequest(uint32_t requestId);

    void clearAllRequestsForContext(int32_t msgIdx);

    void setForwardingAddress(int32_t msgIdx, std::string newHost);

    std::optional<std::string> getForwardingAddress(int32_t msgIdx);

  private:
    template <typename Key, typename Value>
    using ConcurrentMap = faabric::util::ConcurrentMap<Key, Value>;

    ConcurrentMap<int32_t, std::shared_ptr<RpcContext>> msgIdxToContext;
    ConcurrentMap<uint32_t, int32_t> requestToMsgIdx;

    // If Wasm module migrates to Host B, we proxy the async reply to B via the
    // forwarding table.
    ConcurrentMap<int32_t, std::string> forwardingTable;
};

RpcContextRegistry& getRpcContextRegistry();

} // namespace faabric::rpc