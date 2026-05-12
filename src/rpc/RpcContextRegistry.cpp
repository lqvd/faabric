#include <faabric/rpc/RpcContextRegistry.h>
#include <faabric/util/logging.h>

#include <cstdint>
#include <optional>

namespace faabric::rpc {

RpcContextRegistry& getRpcContextRegistry()
{
    static RpcContextRegistry reg;
    return reg;
}

void RpcContextRegistry::registerContext(int32_t msgIdx,
                                         std::shared_ptr<RpcContext> ctx)
{
    msgIdxToContext.insertOrAssign(msgIdx, std::move(ctx));
    SPDLOG_TRACE("RPC - Registered Context ID {}", msgIdx);
}

std::shared_ptr<RpcContext> RpcContextRegistry::getContext(int32_t msgId)
{
    return msgIdxToContext.get(msgId).value_or(nullptr);
}

void RpcContextRegistry::removeContext(int32_t msgIdx)
{
    msgIdxToContext.erase(msgIdx);
    SPDLOG_TRACE("RPC - Removed context for msg {}", msgIdx);
}

void RpcContextRegistry::registerInFlightRequest(uint32_t requestId,
                                                 int32_t msgIdx)
{
    requestToMsgIdx.insertOrAssign(requestId, std::move(msgIdx));
}

std::shared_ptr<RpcContext> RpcContextRegistry::getContextForRequest(
  uint32_t requestId)
{
    auto msgIdOpt = requestToMsgIdx.get(requestId);
    if (!msgIdOpt.has_value()) {
        return nullptr;
    }
    return msgIdxToContext.get(msgIdOpt.value()).value_or(nullptr);
}

std::optional<int32_t> RpcContextRegistry::getMsgIdxForRequest(uint32_t requestId)
{
    return requestToMsgIdx.get(requestId);
}

void RpcContextRegistry::clearRequest(uint32_t requestId)
{
    requestToMsgIdx.erase(requestId);
}

void RpcContextRegistry::clearAllRequestsForContext(int32_t msgIdx)
{
    std::vector<uint32_t> toErase;
    requestToMsgIdx.inspectAll(
      [&toErase, msgIdx](const uint32_t& reqId, const int32_t& mappedMsgIdx) {
          if (mappedMsgIdx == msgIdx) {
              toErase.push_back(reqId);
          }
      });

    for (uint32_t reqId : toErase) {
        requestToMsgIdx.erase(reqId);
    }
}

void RpcContextRegistry::setForwardingAddress(
    int32_t msgIdx,
    std::string newHost,
    std::unordered_set<uint32_t> pendingRequestIds,
    std::chrono::milliseconds ttl)
{
    ForwardingEntry entry{
        .host = std::move(newHost),
        .pendingRequestIds = std::move(pendingRequestIds),
        .expiresAt = std::chrono::steady_clock::now() + ttl,
    };
    forwardingTable.insertOrAssign(msgIdx, std::move(entry));
}

void RpcContextRegistry::markForwarded(int32_t msgIdx, uint32_t requestId) {
    forwardingTable.mutate(msgIdx, [&](ForwardingEntry& entry) {
        auto erased = entry.pendingRequestIds.erase(requestId);
        if (erased == 0) {
            SPDLOG_WARN("RPC - Forwarded resp {} for msg {} not in pending set",
                        requestId, msgIdx);
        }
        if (entry.pendingRequestIds.empty()) {
            forwardingTable.erase(msgIdx);
        }
    });
}

std::optional<std::string> RpcContextRegistry::getForwardingAddress(
    int32_t msgIdx)
{
    if (auto entryOpt = forwardingTable.get(msgIdx)) {
        if (std::chrono::steady_clock::now() > entryOpt->expiresAt) {
            SPDLOG_DEBUG("RPC - Forwarding entry for msg {} expired", msgIdx);
            forwardingTable.erase(msgIdx);
            return {};
        }
        return entryOpt->host;
    }
    return {};
}

} // namespace faabric::rpc