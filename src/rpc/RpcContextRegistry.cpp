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
        SPDLOG_INFO("RPC - request not found {}", requestId);
        return true;
    }

    if (std::chrono::steady_clock::now() < it->second.expiresAt) {
        return false;
    }

    SPDLOG_INFO("RPC - Expiring request {}", requestId);
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

vvoid RpcContextRegistry::registerRestoredContext(
  int32_t appId,
  int32_t msgId,
  std::shared_ptr<RpcContext> ctx,
  const faabric::RpcMigrationState& migrationCtx)
{
    if (!ctx) {
        throw std::runtime_error("Cannot register null restored RpcContext");
    }

    std::vector<faabric::RpcResponse> locallyCachedResponses;

    {
        faabric::util::FullLock lock(mx);

        RpcAppMsgIds owner{ .appId = appId, .msgId = msgId };

        contextByKey[owner] = ctx;

        const auto now = std::chrono::steady_clock::now();

        for (const auto& pendingReq : migrationCtx.pendingrequests()) {
            const uint32_t requestId = pendingReq.requestid();

            std::chrono::milliseconds ttl = kDefaultRpcRequestTtl;
            if (pendingReq.timeoutremaining() > 0) {
                ttl = std::chrono::milliseconds(pendingReq.timeoutremaining());
            }

            requests[requestId] = InFlightRequest{
                .owner = owner,
                .expiresAt = now + ttl,
            };

            pendingFetches.erase(requestId);

            auto cachedIt = cachedResponses.find(requestId);
            if (cachedIt != cachedResponses.end()) {
                locallyCachedResponses.emplace_back(std::move(cachedIt->second));
                cachedResponses.erase(cachedIt);
            }

            SPDLOG_DEBUG(
              "RPC - Restored request route req={} -> app={} msg={} ttl={}ms",
              requestId,
              appId,
              msgId,
              ttl.count());
        }

        SPDLOG_INFO(
          "RPC - Registered restored context app={} msg={} with {} pending requests",
          appId,
          msgId,
          migrationCtx.pendingrequests_size());
    }

    // Deliver outside the registry lock.
    for (const auto& resp : locallyCachedResponses) {
        SPDLOG_INFO("RPC - Delivering locally cached restored response req={}",
                    resp.requestid());
        ctx->onResponseReceived(resp);
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

    registerInFlightRequestUnlocked(requestId, appId, msgId, ttl);
}


void RpcContextRegistry::registerInFlightRequestUnlocked(
  uint32_t requestId,
  int32_t appId,
  int32_t msgId,
  std::chrono::milliseconds ttl)
{
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
    SPDLOG_INFO("Clearing request for {}", requestId);
    faabric::util::FullLock lock(mx);
    // clearRequestLocked(requestId);
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

ResponseRoute RpcContextRegistry::routeResponse(
  uint32_t requestId,
  const faabric::RpcResponse& resp)
{
    faabric::util::FullLock lock(mx);

    if (expireRequestIfNeeded(requestId)) {
        SPDLOG_INFO("RPC - Dropping response to {}", requestId);
        return { ResponseDisposition::Drop, nullptr, {} };
    }

    auto reqIt = requests.find(requestId);
    auto ctxIt = contextByKey.find(reqIt->second.owner);

    if (ctxIt != contextByKey.end()) {
        SPDLOG_INFO("RPC - Delivering locally {}", requestId);
        ctxIt->second->onResponseReceived(resp);
        return { ResponseDisposition::Local, nullptr, {} };
    }

    auto fIt = pendingFetches.find(requestId);
    if (fIt != pendingFetches.end()) {
        PendingFetch fetch = std::move(fIt->second);
        pendingFetches.erase(fIt);
        return { ResponseDisposition::Forward, nullptr, std::move(fetch) };
    }

    cachedResponses[requestId] = resp;
    return { ResponseDisposition::Cached, nullptr, {} };
}


std::shared_mutex& RpcContextRegistry::getMutex()
{
    return mx;
}

void RpcContextRegistry::reset()
{
    faabric::util::FullLock lock(mx);

    contextByKey.clear();
    requests.clear();
    cachedResponses.clear();
    pendingFetches.clear();
}

} // namespace faabric::rpc