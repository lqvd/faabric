#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace faabric {
class RpcResponse;
}

namespace faabric::rpc {

class RpcContext;

using namespace std::literals::chrono_literals;

static constexpr int32_t kTimeoutTtlMultiplier = 2;
static constexpr std::chrono::milliseconds kDefaultRpcRequestTtl = 5min;

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

struct PendingFetch
{
    std::string host;
    int port = 0;
};

class RpcContextRegistry
{
  public:
    // -----------------------------------
    // context mappings
    // -----------------------------------

    void registerContext(
      int32_t appId,
      int32_t msgId,
      std::shared_ptr<faabric::rpc::RpcContext> ctx);

    std::shared_ptr<faabric::rpc::RpcContext> getContext(
      int32_t appId,
      int32_t msgId);

    void removeContext(int32_t appId, int32_t msgId);

    std::shared_ptr<faabric::rpc::RpcContext> getContextForRequest(
      uint32_t requestId);

    void clearAllRequestsForContext(int32_t appId, int32_t msgId);

    // -----------------------------------
    // request ownership / validity
    // -----------------------------------

    void registerInFlightRequest(
      uint32_t requestId,
      int32_t appId,
      int32_t msgId,
      std::chrono::milliseconds ttl = kDefaultRpcRequestTtl);

    void refreshRequestTtl(
      uint32_t requestId,
      std::chrono::milliseconds ttl);

    bool hasRequest(uint32_t requestId);

    void clearRequest(uint32_t requestId);

    std::optional<RpcAppMsgIds> getAppMsgIdForRequest(uint32_t requestId);

    // -----------------------------------
    // fetch-first response
    // -----------------------------------

    void cacheResponse(
      uint32_t requestId,
      const faabric::RpcResponse& resp);

    std::optional<faabric::RpcResponse> consumeCachedResponse(
      uint32_t requestId);

    void registerPendingFetch(
      uint32_t requestId,
      const std::string& host,
      int port);

    std::optional<PendingFetch> consumePendingFetch(uint32_t requestId);

    // -----------------------------------
    // reset mappings
    // -----------------------------------

    void reset();

  private:
    struct InFlightRequest
    {
        RpcAppMsgIds owner;
        std::chrono::steady_clock::time_point expiresAt;
    };

    bool expireRequestIfNeeded(uint32_t requestId);

    void clearRequestLocked(uint32_t requestId);

    std::shared_mutex mx;

    std::unordered_map<RpcAppMsgIds,
                       std::shared_ptr<faabric::rpc::RpcContext>,
                       RpcAppMsgIdsHash> contextByKey;

    std::unordered_map<uint32_t, InFlightRequest> requests;

    std::unordered_map<uint32_t, faabric::RpcResponse> cachedResponses;

    std::unordered_map<uint32_t, PendingFetch> pendingFetches;
};

RpcContextRegistry& getRpcContextRegistry();

} // namespace faabric::rpc