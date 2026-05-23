#pragma once

#include <faabric/rpc/RpcContext.h>
#include <faabric/util/concurrent_map.h>

#include <chrono>
#include <cstdint>
#include <optional>

namespace faabric::rpc {

using namespace std::literals::chrono_literals;

static constexpr int32_t kTimeoutTtlMultiplier = 2;

struct ForwardingEntry {
    std::string host;
    std::unordered_set<uint32_t> pendingRequestIds;
    std::chrono::steady_clock::time_point expiresAt;
};

struct PendingFetch {
    std::string host;
    int port;
};

class RpcContextRegistry
{
  public:
    void registerContext(int32_t msgIdx, std::shared_ptr<RpcContext> ctx);

    void registerInFlightRequest(uint32_t requestId, int32_t msgIdx);

    std::shared_ptr<RpcContext> getContext(int32_t msgIdx);

    void removeContext(int32_t msgIdx);
    
    std::shared_ptr<RpcContext> getContextForRequest(uint32_t requestId);
    
    void clearAllRequestsForContext(int32_t msgIdx);

    std::optional<int32_t> getMsgIdxForRequest(uint32_t requestId);

    void clearRequest(uint32_t requestId);

    void setForwardingAddress(
        int32_t msgIdx,
        std::string newHost,
        std::unordered_set<uint32_t> pendingRequestIds,
        std::chrono::milliseconds ttl = 30s);

    void markForwarded(int32_t msgIdx, uint32_t requestId);

    std::optional<std::string> getForwardingAddress(int32_t msgIdx);
    
    // Cache response for migrated host to then fetch
    void cacheForwardedResponse(uint32_t requestId,
                                const faabric::RpcResponse& resp);

    std::optional<faabric::RpcResponse> consumeForwardedResponse(
        uint32_t requestId);

    void registerPendingFetch(uint32_t requestId,
                              const std::string& host,
                              int port);

    std::optional<PendingFetch> consumePendingFetch(uint32_t requestId);

    void reset();

  private:
    std::shared_mutex mx;

    std::unordered_map<int32_t, std::shared_ptr<RpcContext>> msgIdxToContext;
    std::unordered_map<uint32_t, int32_t> requestToMsgIdx;
    std::unordered_map<int32_t, ForwardingEntry> forwardingTable;
    std::unordered_map<uint32_t, faabric::RpcResponse> forwardedResponseCache;
    std::unordered_map<uint32_t, PendingFetch> pendingFetches;
};

RpcContextRegistry& getRpcContextRegistry();

} // namespace faabric::rpc