#include <faabric/rpc/RpcContextRegistry.h>
#include <faabric/util/logging.h>

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
          [this, targetContextId](const uint32_t& reqId, const int32_t& ctxId) {
        if (ctxId == targetContextId) {
            this->requestToContextId.erase(reqId);
        }
    });
}

} // namespace faabric::rpc