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

void RpcContextRegistry::registerContext(int32_t msgIdx,
                                         std::shared_ptr<RpcContext> ctx)
{
    faabric::util::SharedLock lock(mx);
    msgIdxToContext[msgIdx] = std::move(ctx);
    SPDLOG_TRACE("RPC - Registered Context ID {}", msgIdx);
}

std::shared_ptr<RpcContext> RpcContextRegistry::getContext(int32_t msgId)
{
    faabric::util::SharedLock lock(mx);
    auto it = msgIdxToContext.find(msgId);
    return it == msgIdxToContext.end() ? nullptr : it->second;
}

void RpcContextRegistry::removeContext(int32_t msgIdx)
{
    faabric::util::FullLock lock(mx);
    msgIdxToContext.erase(msgIdx);
    SPDLOG_TRACE("RPC - Removed context for msg {}", msgIdx);
}

void RpcContextRegistry::clearAllRequestsForContext(int32_t msgIdx)
{
    faabric::util::FullLock lock(mx);

    for (auto it = requestToMsgIdx.begin(); it != requestToMsgIdx.end();) {
        if (it->second == msgIdx) {
            it = requestToMsgIdx.erase(it);
        } else {
            ++it;
        }
    }
}

// -----------------------------------
// request ID mappings
// -----------------------------------

void RpcContextRegistry::registerInFlightRequest(uint32_t requestId,
                                                 int32_t msgIdx)
{
    faabric::util::FullLock lock(mx);
    requestToMsgIdx[requestId] = msgIdx;
}

std::shared_ptr<RpcContext> RpcContextRegistry::getContextForRequest(
  uint32_t requestId)
{
    faabric::util::SharedLock lock(mx);
    auto reqIt = requestToMsgIdx.find(requestId);
    if (reqIt == requestToMsgIdx.end()) {
        return nullptr;
    }
    auto ctxIt = msgIdxToContext.find(reqIt->second);
    return ctxIt == msgIdxToContext.end() ? nullptr : ctxIt->second;
}

std::optional<int32_t> RpcContextRegistry::getMsgIdxForRequest(
  uint32_t requestId)
{
    faabric::util::SharedLock lock(mx);
    auto it = requestToMsgIdx.find(requestId);
    if (it == requestToMsgIdx.end()) {
        return std::nullopt;
    }
    return it->second;
}

void RpcContextRegistry::clearRequest(uint32_t requestId)
{
    faabric::util::FullLock lock(mx);
    requestToMsgIdx.erase(requestId);
}

// -----------------------------------
// response routing
// -----------------------------------

void RpcContextRegistry::setForwardingAddress(
    int32_t msgIdx,
    std::string newHost,
    std::unordered_set<uint32_t> pendingRequestIds,
    std::chrono::milliseconds ttl)
{
    faabric::util::FullLock lock(mx);
    ForwardingEntry entry{
        .host = std::move(newHost),
        .pendingRequestIds = std::move(pendingRequestIds),
        .expiresAt = std::chrono::steady_clock::now() + ttl,
    };
    forwardingTable[msgIdx] = std::move(entry);
}
void RpcContextRegistry::markForwarded(int32_t msgIdx, uint32_t requestId)
{
    faabric::util::FullLock lock(mx);
    auto it = forwardingTable.find(msgIdx);
    if (it == forwardingTable.end()) {
        SPDLOG_WARN("RPC - markForwarded for msg {} with no forwarding entry",
                    msgIdx);
        return;
    }
    auto erased = it->second.pendingRequestIds.erase(requestId);
    if (erased == 0) {
        SPDLOG_WARN("RPC - Forwarded resp {} for msg {} not in pending set",
                    requestId, msgIdx);
    }
    if (it->second.pendingRequestIds.empty()) {
        forwardingTable.erase(it);
    }
}

std::optional<std::string> RpcContextRegistry::getForwardingAddress(
    int32_t msgIdx)
{
    faabric::util::FullLock lock(mx);
    auto it = forwardingTable.find(msgIdx);
    if (it == forwardingTable.end()) {
        return std::nullopt;
    }
    if (std::chrono::steady_clock::now() > it->second.expiresAt) {
        SPDLOG_DEBUG("RPC - Forwarding entry for msg {} expired", msgIdx);
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
    msgIdxToContext.clear();
    requestToMsgIdx.clear();
    forwardingTable.clear();
    forwardedResponseCache.clear();
    pendingFetches.clear();
}

} // namespace faabric::rpc