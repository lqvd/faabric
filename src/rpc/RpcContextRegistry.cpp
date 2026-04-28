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

void RpcContextRegistry::setForwardingAddress(int32_t msgIdx,
                                              std::string newHost) 
{
    forwardingTable.insertOrAssign(msgIdx, std::move(newHost));
    msgIdxToContext.erase(msgIdx); 
}

std::optional<std::string> RpcContextRegistry::getForwardingAddress(
    int32_t msgIdx)
{
    return forwardingTable.get(msgIdx);
}

} // namespace faabric::rpc