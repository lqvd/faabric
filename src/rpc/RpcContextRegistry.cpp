#include <faabric/rpc/RpcContextRegistry.h>

#include <faabric/transport/common.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

#include <cstdint>
#include <mutex>
#include <optional>
#include <vector>

namespace faabric::rpc {

RpcContextRegistry& getRpcContextRegistry()
{
    static RpcContextRegistry reg;
    return reg;
}

// -----------------------------------
// context handling
// -----------------------------------

void RpcContextRegistry::registerContext(int32_t appId, int32_t msgId,
                                         std::shared_ptr<RpcContext> ctx)
{
    faabric::util::FullLock lock(mx);

    RpcAppMsgIds key{ .appId = appId, .msgId = msgId };
    contextByKey[key] = std::move(ctx);

    SPDLOG_TRACE("RPC - Registered context app={} msg={}", appId, msgId);
}

std::shared_ptr<RpcContext> RpcContextRegistry::getContext(
  int32_t appId, int32_t msgId)
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

void RpcContextRegistry::clearRequest(uint32_t requestId)
{
    faabric::util::FullLock lock(mx);
    requestToContextKey.erase(requestId);
}

void RpcContextRegistry::clearAllRequestsForContext(int32_t appId,
                                                    int32_t msgId)
{
    faabric::util::FullLock lock(mx);

    RpcAppMsgIds key{ .appId = appId, .msgId = msgId };

    for (auto it = requestToContextKey.begin();
         it != requestToContextKey.end();) {
        if (it->second == key) {
            it = requestToContextKey.erase(it);
        } else {
            ++it;
        }
    }
}

// -----------------------------------
// request ID mappings
// -----------------------------------

void RpcContextRegistry::registerInFlightRequest(uint32_t requestId,
                                                 int32_t appId,
                                                 int32_t msgId)
{
    faabric::util::FullLock lock(mx);

    requestToContextKey[requestId] =
      RpcAppMsgIds{ .appId = appId, .msgId = msgId };
}

std::shared_ptr<RpcContext> RpcContextRegistry::getContextForRequest(
  uint32_t requestId)
{
    faabric::util::SharedLock lock(mx);

    auto reqIt = requestToContextKey.find(requestId);
    if (reqIt == requestToContextKey.end()) {
        return nullptr;
    }

    auto ctxIt = contextByKey.find(reqIt->second);
    return ctxIt == contextByKey.end() ? nullptr : ctxIt->second;
}

std::optional<RpcAppMsgIds> RpcContextRegistry::getAppMsgIdForRequest(
  uint32_t requestId)
{
    faabric::util::SharedLock lock(mx);

    auto it = requestToContextKey.find(requestId);
    if (it == requestToContextKey.end()) {
        return std::nullopt;
    }

    return it->second;
}

// -----------------------------------
// response routing
// -----------------------------------

void RpcContextRegistry::setForwardingAddress(
  int32_t appId,
  int32_t msgId,
  std::string newHost,
  std::unordered_set<uint32_t> pendingRequestIds,
  std::chrono::milliseconds ttl)
{
    faabric::util::FullLock lock(mx);

    RpcAppMsgIds key{ .appId = appId, .msgId = msgId };

    ForwardingEntry entry{
        .host = std::move(newHost),
        .pendingRequestIds = std::move(pendingRequestIds),
        .expiresAt = std::chrono::steady_clock::now() + ttl,
    };

    forwardingTable[key] = std::move(entry);
}

void RpcContextRegistry::markForwarded(int32_t appId,
                                       int32_t msgId,
                                       uint32_t requestId)
{
    faabric::util::FullLock lock(mx);

    RpcAppMsgIds key{ .appId = appId, .msgId = msgId };

    auto it = forwardingTable.find(key);
    if (it == forwardingTable.end()) {
        SPDLOG_WARN(
          "RPC - markForwarded for app={} msg={} with no forwarding entry",
          appId,
          msgId);
        return;
    }

    auto erased = it->second.pendingRequestIds.erase(requestId);
    if (erased == 0) {
        SPDLOG_WARN(
          "RPC - Forwarded resp {} for app={} msg={} not in pending set",
          requestId,
          appId,
          msgId);
    }

    if (it->second.pendingRequestIds.empty()) {
        forwardingTable.erase(it);
    }
}

std::optional<std::string> RpcContextRegistry::getForwardingAddress(
  int32_t appId, int32_t msgId)
{
    faabric::util::FullLock lock(mx);

    RpcAppMsgIds key{ .appId = appId, .msgId = msgId };

    auto it = forwardingTable.find(key);
    if (it == forwardingTable.end()) {
        return std::nullopt;
    }

    if (std::chrono::steady_clock::now() > it->second.expiresAt) {
        SPDLOG_DEBUG("RPC - Forwarding entry for app={} msg={} expired",
                     appId,
                     msgId);
        forwardingTable.erase(it);
        return std::nullopt;
    }

    return it->second.host;
}

void RpcContextRegistry::cacheForwardedResponse(
    uint32_t requestId, const faabric::RpcResponse& resp)
{
    faabric::util::FullLock lock(mx);
    forwardedResponseCache[requestId] = resp;
}

std::optional<faabric::RpcResponse>
RpcContextRegistry::consumeForwardedResponse(uint32_t requestId)
{
    faabric::util::FullLock lock(mx);
    auto it = forwardedResponseCache.find(requestId);
    if (it == forwardedResponseCache.end()) return std::nullopt;
    auto resp = std::move(it->second);
    forwardedResponseCache.erase(it);
    return resp;
}

void RpcContextRegistry::registerPendingFetch(uint32_t requestId,
                                               const std::string& host,
                                               int port)
{
    faabric::util::FullLock lock(mx);
    pendingFetches[requestId] = { host, port };
}

std::optional<PendingFetch>
RpcContextRegistry::consumePendingFetch(uint32_t requestId)
{
    faabric::util::FullLock lock(mx);
    auto it = pendingFetches.find(requestId);
    if (it == pendingFetches.end()) return std::nullopt;
    auto fetch = std::move(it->second);
    pendingFetches.erase(it);
    return fetch;
}

void RpcContextRegistry::reset()
{
    faabric::util::FullLock lock(mx);

    contextByKey.clear();
    requestToContextKey.clear();
    forwardingTable.clear();
    forwardedResponseCache.clear();
    pendingFetches.clear();
}

} // namespace faabric::rpc