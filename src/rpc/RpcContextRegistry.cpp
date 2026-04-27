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

void RpcContextRegistry::registerContext(int32_t contextId,
                                         std::shared_ptr<RpcContext> ctx)
{
    contexts.insertOrAssign(contextId, std::move(ctx));
    SPDLOG_TRACE("RPC - Registered Context ID {}", contextId);
}

void RpcContextRegistry::registerInFlightRequest(uint32_t requestId,
                                                 int32_t contextId)
{
    requestToContextId.insertOrAssign(requestId, std::move(contextId));
}

std::shared_ptr<RpcContext> RpcContextRegistry::getContextForRequest(
    uint32_t requestId)
{
    auto ctxIdOpt = requestToContextId.get(requestId);
    if (!ctxIdOpt.has_value()) {
        return nullptr;
    }

    auto ctxOpt = contexts.get(ctxIdOpt.value());
    return ctxOpt.value_or(nullptr);
}

std::optional<int32_t> RpcContextRegistry::getContextIdForRequest(
    uint32_t requestId)
{
    return requestToContextId.get(requestId);
}

void RpcContextRegistry::clearRequest(uint32_t requestId)
{
    requestToContextId.erase(requestId);
}

void RpcContextRegistry::removeContext(int32_t contextId) {
    contexts.erase(contextId);
    SPDLOG_TRACE("RPC - Removed Context ID {}", contextId);
}

void RpcContextRegistry::clearAllRequestsForContext(int32_t targetContextId)
{
    requestToContextId.inspectAll(
          [this, targetContextId](const uint32_t& reqId, const uint32_t& ctxId) {
        if (ctxId == targetContextId) {
            this->requestToContextId.erase(reqId);
        }
    });
}

void RpcContextRegistry::setForwardingAddress(int32_t contextId,
                                              std::string newHost) 
{
    forwardingTable.insertOrAssign(contextId, std::move(newHost));
    contexts.erase(contextId); 
}

std::optional<std::string> RpcContextRegistry::getForwardingAddress(
    int32_t contextId)
{
    return forwardingTable.get(contextId);
}

} // namespace faabric::rpc