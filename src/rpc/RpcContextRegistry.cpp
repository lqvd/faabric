#include <faabric/rpc/RpcContextRegistry.h>

#include <faabric/proto/faabric.pb.h>
#include <faabric/rpc/RpcContext.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

#include <algorithm>
#include <cstdint>
#include <optional>

namespace faabric::rpc {

RpcContextRegistry& getRpcContextRegistry()
{
    static RpcContextRegistry reg;
    return reg;
}

// -----------------------------------
// private helpers
// -----------------------------------

void RpcContextRegistry::clearRequestLocked(uint32_t requestId)
{
    requests.erase(requestId);
    cachedResponses.erase(requestId);
    pendingFetches.erase(requestId);
}

bool RpcContextRegistry::expireRequestIfNeeded(uint32_t requestId)
{
    auto it = requests.find(requestId);
    if (it == requests.end()) {
        return true;
    }

    if (std::chrono::steady_clock::now() < it->second.expiresAt) {
        return false;
    }

    SPDLOG_DEBUG("RPC - Expiring request {}", requestId);
    clearRequestLocked(requestId);
    return true;
}

// -----------------------------------
// context handling
// -----------------------------------

void RpcContextRegistry::registerContext(
  int32_t appId,
  int32_t msgId,
  std::shared_ptr<RpcContext> ctx)
{
    faabric::util::FullLock lock(mx);

    RpcAppMsgIds key{ .appId = appId, .msgId = msgId };
    contextByKey[key] = std::move(ctx);

    SPDLOG_TRACE("RPC - Registered context app={} msg={}", appId, msgId);
}

std::shared_ptr<RpcContext> RpcContextRegistry::getContext(
  int32_t appId,
  int32_t msgId)
{
    faabric::util::SharedLock lock(mx);

    RpcAppMsgIds key{ .appId = appId, .msgId = msgId };
    auto it = contextByKey.find(key);

    return it == contextByKey.end() ? nullptr : it->second;
}

void RpcContextRegistry::removeContext(int32_t appId, int32_t msgId)
{
    faabric::util::FullLock lock(mx);

    RpcAppMsgIds key{ .appId = appId, .msgId = msgId };
    contextByKey.erase(key);

    SPDLOG_TRACE("RPC - Removed context app={} msg={}", appId, msgId);
}

std::shared_ptr<RpcContext> RpcContextRegistry::getContextForRequest(
  uint32_t requestId)
{
    faabric::util::FullLock lock(mx);

    if (expireRequestIfNeeded(requestId)) {
        return nullptr;
    }

    auto reqIt = requests.find(requestId);
    auto ctxIt = contextByKey.find(reqIt->second.owner);

    return ctxIt == contextByKey.end() ? nullptr : ctxIt->second;
}

void RpcContextRegistry::clearAllRequestsForContext(
  int32_t appId,
  int32_t msgId)
{
    faabric::util::FullLock lock(mx);

    RpcAppMsgIds key{ .appId = appId, .msgId = msgId };

    for (auto it = requests.begin(); it != requests.end();) {
        if (it->second.owner == key) {
            const uint32_t requestId = it->first;
            cachedResponses.erase(requestId);
            pendingFetches.erase(requestId);
            it = requests.erase(it);
        } else {
            ++it;
        }
    }
}

// -----------------------------------
// request ownership / validity
// -----------------------------------

void RpcContextRegistry::registerInFlightRequest(
  uint32_t requestId,
  int32_t appId,
  int32_t msgId,
  std::chrono::milliseconds ttl)
{
    faabric::util::FullLock lock(mx);

    if (ttl.count() <= 0) {
        ttl = kDefaultRpcRequestTtl;
    }

    requests[requestId] = InFlightRequest{
        .owner = RpcAppMsgIds{ .appId = appId, .msgId = msgId },
        .expiresAt = std::chrono::steady_clock::now() + ttl,
    };
}

void RpcContextRegistry::refreshRequestTtl(
  uint32_t requestId,
  std::chrono::milliseconds ttl)
{
    faabric::util::FullLock lock(mx);

    auto it = requests.find(requestId);
    if (it == requests.end()) {
        return;
    }

    if (ttl.count() <= 0) {
        ttl = kDefaultRpcRequestTtl;
    }

    it->second.expiresAt = std::chrono::steady_clock::now() + ttl;
}

bool RpcContextRegistry::hasRequest(uint32_t requestId)
{
    faabric::util::FullLock lock(mx);
    return !expireRequestIfNeeded(requestId);
}

void RpcContextRegistry::clearRequest(uint32_t requestId)
{
    faabric::util::FullLock lock(mx);
    clearRequestLocked(requestId);
}

std::optional<RpcAppMsgIds> RpcContextRegistry::getAppMsgIdForRequest(
  uint32_t requestId)
{
    faabric::util::FullLock lock(mx);

    if (expireRequestIfNeeded(requestId)) {
        return std::nullopt;
    }

    return requests.at(requestId).owner;
}

// -----------------------------------
// fetch-first response
// -----------------------------------

void RpcContextRegistry::cacheResponse(
  uint32_t requestId,
  const faabric::RpcResponse& resp)
{
    faabric::util::FullLock lock(mx);

    if (expireRequestIfNeeded(requestId)) {
        SPDLOG_WARN("RPC - Not caching response for unknown/expired request {}",
                    requestId);
        return;
    }

    cachedResponses[requestId] = resp;
}

std::optional<faabric::RpcResponse>
RpcContextRegistry::consumeCachedResponse(uint32_t requestId)
{
    faabric::util::FullLock lock(mx);

    if (expireRequestIfNeeded(requestId)) {
        return std::nullopt;
    }

    auto it = cachedResponses.find(requestId);
    if (it == cachedResponses.end()) {
        return std::nullopt;
    }

    auto resp = std::move(it->second);
    cachedResponses.erase(it);
    return resp;
}

void RpcContextRegistry::registerPendingFetch(
  uint32_t requestId,
  const std::string& host,
  int port)
{
    faabric::util::FullLock lock(mx);

    if (expireRequestIfNeeded(requestId)) {
        SPDLOG_WARN("RPC - Not registering FETCH for unknown/expired request {}",
                    requestId);
        return;
    }

    pendingFetches[requestId] = PendingFetch{
        .host = host,
        .port = port,
    };
}

std::optional<PendingFetch>
RpcContextRegistry::consumePendingFetch(uint32_t requestId)
{
    faabric::util::FullLock lock(mx);

    if (expireRequestIfNeeded(requestId)) {
        return std::nullopt;
    }

    auto it = pendingFetches.find(requestId);
    if (it == pendingFetches.end()) {
        return std::nullopt;
    }

    auto fetch = std::move(it->second);
    pendingFetches.erase(it);
    return fetch;
}

// -----------------------------------
// reset mappings
// -----------------------------------

void RpcContextRegistry::reset()
{
    faabric::util::FullLock lock(mx);

    contextByKey.clear();
    requests.clear();
    cachedResponses.clear();
    pendingFetches.clear();
}

} // namespace faabric::rpc