#pragma once

#include <faabric/rpc/RpcContext.h>
#include <faabric/util/concurrent_map.h>

#include <chrono>
#include <cstdint>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <shared_mutex>

namespace faabric::rpc {

using namespace std::literals::chrono_literals;

static constexpr int32_t kTimeoutTtlMultiplier = 2;

struct RpcAppMsgIds
{
    int32_t appId = 0;
    int32_t msgId = 0;

    bool operator==(const RpcAppMsgIds& other) const
    {
        return appId == other.appId && msgId == other.msgId;
    }
};

struct RpcAppMsgIdsHash
{
    size_t operator()(const RpcAppMsgIds& k) const
    {
        size_t h1 = std::hash<int32_t>{}(k.appId);
        size_t h2 = std::hash<int32_t>{}(k.msgId);
        return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
    }
};

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
    // -----------------------------------
    // context mappings
    // -----------------------------------

    void registerContext(
      int32_t appId, int32_t msgIdx, std::shared_ptr<RpcContext> ctx);

    std::shared_ptr<RpcContext> getContext(int32_t appId, int32_t msgId);

    void removeContext(int32_t appId, int32_t msgIdx);

    std::shared_ptr<RpcContext> getContextForRequest(uint32_t requestId);


    void clearAllRequestsForContext(int32_t appId, int32_t msgIdx);
    
    // -----------------------------------
    // request ID mappings
    // -----------------------------------
    
    void registerInFlightRequest(
      uint32_t requestId, int32_t appId, int32_t msgId);

    void clearRequest(uint32_t requestId);

    std::optional<RpcAppMsgIds> getAppMsgIdForRequest(uint32_t requestId);

    // -----------------------------------
    // forwarding
    // -----------------------------------

    void setForwardingAddress(
      int32_t appId,
      int32_t msgId,
      std::string newHost,
      std::unordered_set<uint32_t> pendingRequestIds,
      std::chrono::milliseconds ttl = 30s);

    void markForwarded(int32_t appId, int32_t msgIdx, uint32_t requestId);

    std::optional<std::string> getForwardingAddress(
      int32_t appId, int32_t msgIdx);
    
    // Cache response for migrated host to then fetch
    void cacheForwardedResponse(uint32_t requestId,
                                const faabric::RpcResponse& resp);

    std::optional<faabric::RpcResponse> consumeForwardedResponse(
        uint32_t requestId);

    // -----------------------------------
    // fetch
    // -----------------------------------

    void registerPendingFetch(uint32_t requestId,
                              const std::string& host,
                              int port);

    std::optional<PendingFetch> consumePendingFetch(uint32_t requestId);

    // -----------------------------------
    // reset mappings
    // -----------------------------------

    void reset();

  private:
    std::shared_mutex mx;

    std::unordered_map<
      RpcAppMsgIds, std::shared_ptr<RpcContext>, RpcAppMsgIdsHash> contextByKey;

    std::unordered_map<uint32_t, RpcAppMsgIds> requestToContextKey;

    std::unordered_map<
      RpcAppMsgIds, ForwardingEntry, RpcAppMsgIdsHash> forwardingTable;

    std::unordered_map<uint32_t, faabric::RpcResponse> forwardedResponseCache;

    std::unordered_map<uint32_t, PendingFetch> pendingFetches;
};

RpcContextRegistry& getRpcContextRegistry();

} // namespace faabric::rpc