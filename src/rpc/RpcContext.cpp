#include <faabric/rpc/RpcContext.h>

#include <faabric/executor/ExecutorContext.h>
#include <faabric/planner/PlannerClient.h>
#include <faabric/rpc/rpc.h>
#include <faabric/rpc/RpcContextRegistry.h>
#include <faabric/rpc/RpcTracker.h>
#include <faabric/rpc/RpcTransportClient.h>
#include <faabric/transport/common.h>
#include <faabric/util/testing.h>
#include <faabric/util/logging.h>
#include <faabric/util/locks.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string_view>
#include <optional>
#include <thread>

namespace faabric::rpc {

static constexpr int32_t kNoCachedRespStatus = -1;
static constexpr std::string_view kFaabricScheme = "faabric://";

// -----------------------------------
// helpers
// -----------------------------------

static bool isFaabricUri(const std::string& uri)
{
    return uri.rfind(kFaabricScheme, 0) == 0;
}

ChannelInfo RpcContext::parseChannelInfo(const std::string& targetUri)
{
    if (!isFaabricUri(targetUri)) {
        throw std::runtime_error(
          fmt::format("External RPC URIs not implemented: {}", targetUri));
    }

    const std::string serviceName = targetUri.substr(kFaabricScheme.size());
    if (serviceName.empty()) {
        throw std::runtime_error("Empty service name in faabric URI");
    }

    auto endpoint = resolver->resolve(serviceName);
    if (!endpoint.has_value()) {
        throw std::runtime_error(
          fmt::format("Service '{}' not found", serviceName));
    }

    return ChannelInfo{
        .targetUri = targetUri,
        .isFaabric = true,
        .port = RPC_ASYNC_PORT,
        .targetHost = endpoint->host(),
        .targetAppId = endpoint->appid(),
        .targetMessageId = endpoint->messageid(),
    };
}

// -----------------------------------
// rpc context state
// -----------------------------------

RpcContext::RpcContext(int32_t ownerAppIdIn, int32_t ownerMsgIdIn)
  : RpcContext(
      ownerAppIdIn,
      ownerMsgIdIn,
      std::make_shared<PlannerRpcServiceResolver>())
{
    // NOTE: nextRequestId is 32-bit so we only permit 65535 requests
    // concurrently. 
    uint32_t ownerHash = static_cast<uint32_t>(
      std::hash<int64_t>{}((static_cast<int64_t>(ownerAppIdIn) << 32) |
                            static_cast<int64_t>(ownerMsgIdIn)));
    uint32_t seed = (ownerHash & 0xFFFFu) << 16;
    nextRequestId.store(seed | 1u, std::memory_order_relaxed);
}

RpcContext::RpcContext(int32_t ownerAppIdIn,
                       int32_t ownerMsgIdIn,
                       std::shared_ptr<RpcServiceResolver> resolverIn)
  : resolver(std::move(resolverIn))
  , ownerAppId(ownerAppIdIn)
  , ownerMsgId(ownerMsgIdIn)
{
    if (!resolver) {
        throw std::runtime_error("RpcContext requires non-null service resolver");
    }
}

// Lock should be held.
std::shared_ptr<RpcTransportClient> RpcContext::getOrCreateTransportLocked(
  const ChannelInfo& info)
{
    const std::string key =
      info.targetHost + ":" + std::to_string(info.port);

    auto it = targetToTransport.find(key);
    if (it != targetToTransport.end()) return it->second;

    auto transport = std::make_shared<RpcTransportClient>(
        info.targetHost, RPC_ASYNC_PORT, info.port, 5000);
    targetToTransport.emplace(key, transport);
    return transport;
}

int32_t RpcContext::createChannel(const std::string& targetUri)
{
    ChannelInfo info = parseChannelInfo(targetUri);

    const int32_t id = nextChannelId.fetch_add(1, std::memory_order_relaxed);

    {
        faabric::util::ScopedLock lock(mx);
        channels.emplace(id, std::move(info));
    }

    SPDLOG_DEBUG("RPC - Made channel {}", id);
    return id;
}

ChannelInfo RpcContext::getChannel(int32_t channelId)
{
    faabric::util::ScopedLock lock(mx);

    auto it = channels.find(channelId);
    if (it != channels.end()) {
        return it->second;
    }

    SPDLOG_ERROR("RPC - Wasm guest requested unknown channel ID {}", channelId);
    throw std::runtime_error("Unknown RPC channel ID requested by Wasm guest");
}

void RpcContext::closeChannel(int32_t channelId)
{
    faabric::util::ScopedLock lock(mx);
    channels.erase(channelId);
    SPDLOG_TRACE("RPC - Closed channel ID {}", channelId);
}

void RpcContext::clearLocal()
{
    faabric::util::ScopedLock lock(mx);
    channels.clear();
    requestToChannel.clear();
    targetToTransport.clear();
    ops.clear();
    nextChannelId.store(1, std::memory_order_relaxed);
}

// -----------------------------------
// serialisation + deserialisation
// -----------------------------------

faabric::RpcMigrationState RpcContext::serializeMigrationState() const
{
    faabric::RpcMigrationState migrationCtx;

    faabric::util::ScopedLock lock(mx);

    for (const auto& [channelId, info] : channels) {
        auto* channelState = migrationCtx.add_channels();
        channelState->set_channelid(channelId);
        channelState->set_targeturi(info.targetUri);
    }

    const auto now = std::chrono::steady_clock::now();

    for (const auto& [reqId, channelId] : requestToChannel) {
        auto* pendingReq = migrationCtx.add_pendingrequests();
        pendingReq->set_requestid(reqId);
        pendingReq->set_channelid(channelId);
        pendingReq->set_cachedstatuscode(kNoCachedRespStatus);

        auto opIt = ops.find(reqId);
        if (opIt == ops.end()) {
            pendingReq->set_timeoutremaining(-1);
            continue;
        }

        const auto& op = opIt->second;

        if (op.ready) {
            pendingReq->set_cachedresponse(op.response.payload());
            pendingReq->set_cachedstatuscode(op.response.statuscode());
        }

        if (op.deadline.has_value()) {
            auto remaining =
              std::chrono::duration_cast<std::chrono::milliseconds>(
                op.deadline.value() - now)
                .count();

            pendingReq->set_timeoutremaining(std::max<int64_t>(1, remaining));
        } else {
            pendingReq->set_timeoutremaining(-1);
        }
    }

    migrationCtx.set_originhost(faabric::util::getSystemConfig().endpointHost);

    return migrationCtx;
}

void RpcContext::deserializeMigrationState(
  const faabric::RpcMigrationState& migrationCtx)
{
    clearLocal();

    {
        faabric::util::ScopedLock lock(mx);

        int32_t highestChannelId = 0;

        for (const auto& channelState : migrationCtx.channels()) {
            ChannelInfo info = parseChannelInfo(channelState.targeturi());

            const int32_t channelId = channelState.channelid();
            channels.emplace(channelId, std::move(info));

            highestChannelId = std::max(highestChannelId, channelId);
        }

        nextChannelId.store(highestChannelId + 1, std::memory_order_relaxed);

        SPDLOG_DEBUG("RPC - Deserialising {} pending requests",
                    migrationCtx.pendingrequests_size());

        for (const auto& pendingReq : migrationCtx.pendingrequests()) {
            const uint32_t requestId = pendingReq.requestid();
            const int32_t channelId = pendingReq.channelid();

            auto chIt = channels.find(channelId);
            if (chIt == channels.end()) {
                throw std::runtime_error(
                fmt::format("RPC migration state references unknown channel {}",
                            channelId));
            }

            requestToChannel.emplace(requestId, channelId);
            getOrCreateTransportLocked(chIt->second);

            RpcOp op;
            if (pendingReq.cachedstatuscode() != kNoCachedRespStatus) {
                op.ready = true;
                op.response.set_requestid(requestId);
                op.response.set_statuscode(pendingReq.cachedstatuscode());
                op.response.set_payload(pendingReq.cachedresponse());
            }
            if (pendingReq.timeoutremaining() >= 0) {
                op.deadline = 
                  std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(pendingReq.timeoutremaining());
            }
            ops.emplace(requestId, std::move(op));
        }
    }

    auto& reg = getRpcContextRegistry();

    for (const auto& pendingReq : migrationCtx.pendingrequests()) {
        reg.registerInFlightRequest(
          pendingReq.requestid(), ownerAppId, ownerMsgId);
    }

    reg.registerContext(ownerAppId, ownerMsgId, shared_from_this());

    for (const auto& pendingReq : migrationCtx.pendingrequests()) {
        if (pendingReq.cachedstatuscode() != kNoCachedRespStatus) {
            // already restored as ready
            continue;
        }

        faabric::RpcFetchRequest fetch;
        fetch.set_requestid(pendingReq.requestid());
        fetch.set_replyhost(faabric::util::getSystemConfig().endpointHost);
        fetch.set_replyport(RPC_ASYNC_PORT);

        try {
            RpcTransportClient client(
                migrationCtx.originhost(),
                RPC_ASYNC_PORT,
                RPC_SYNC_PORT,
                5000);
            client.asyncSendFetch(fetch);
        } catch (const std::exception& e) {
            SPDLOG_ERROR("RPC - Failed to send FETCH for requestId={}: {}",
                        pendingReq.requestid(), e.what());
        }
    }
}

// -----------------------------------
// rpc unary request
// -----------------------------------

uint32_t RpcContext::startUnary(int32_t channelId,
                                const std::string& method,
                                const uint8_t* reqBuffer,
                                int32_t reqLength,
                                int32_t timeoutMs)
{
    std::shared_ptr<RpcTransportClient> transport;
    uint32_t requestId = 0;
    int32_t targetAppId = 0;
    int32_t targetMessageId = 0;
    std::string targetHost;
    std::string targetUri;

    {
        faabric::util::ScopedLock lock(mx);

        auto chIt = channels.find(channelId);
        if (chIt == channels.end()) {
            SPDLOG_ERROR("RPC - Wasm guest requested unknown channel ID {}",
                         channelId);
            throw std::runtime_error(
              "Unknown RPC channel ID requested by Wasm guest");
        }

        const ChannelInfo& info = chIt->second;
        if (!info.isFaabric) {
            throw std::runtime_error("External RPC is not implemented");
        }

        transport = getOrCreateTransportLocked(info);
        targetAppId = info.targetAppId;
        targetMessageId = info.targetMessageId;
        targetHost = info.targetHost;
        targetUri = info.targetUri;

        requestId = nextRequestId.fetch_add(1, std::memory_order_relaxed);

        RpcOp op;
        if (timeoutMs >= 0) {
            op.deadline =
              std::chrono::steady_clock::now() +
              std::chrono::milliseconds(timeoutMs);
        }

        ops.emplace(requestId, std::move(op));
        requestToChannel.emplace(requestId, channelId);
    }

    getRpcContextRegistry().registerInFlightRequest(
      requestId, ownerAppId, ownerMsgId);

    const auto& cfg = faabric::util::getSystemConfig();

    auto buildReq = [&](int32_t appId, int32_t msgId) {
        faabric::RpcRequest req;
        req.set_requestid(requestId);
        req.set_method(method);
        req.set_payload(reqBuffer, reqLength);
        req.set_targetappid(appId);
        req.set_targetmessageid(msgId);
        req.set_replyhost(cfg.endpointHost);
        req.set_replyport(RPC_ASYNC_PORT);
        return req;
    };

    // First attempt against the channel's cached resolution.
    try {
        transport->asyncSendRequest(requestId, buildReq(targetAppId,
                                                        targetMessageId));
        getRpcTracker().recordDependency(ownerAppId, ownerMsgId,
                                         targetAppId, targetMessageId,
                                         cfg.endpointHost, targetHost);
        return requestId;
    } catch (const std::exception& e) {
        SPDLOG_WARN("RPC - send for req={} to {} failed ({}); re-resolving {}",
                    requestId, targetHost, e.what(), targetUri);
    }

    // Send failed: the cached endpoint is likely stale (callee migrated and
    // the origin tombstone has expired). Re-resolve through the planner,
    // which always holds the current location, retarget the channel, and
    // retry once against the fresh endpoint.
    try {
        auto endpoint = reresolveChannel(channelId, targetUri);
        if (!endpoint.has_value()) {
            throw std::runtime_error(
              fmt::format("Re-resolution found no endpoint for {}", targetUri));
        }

        std::shared_ptr<RpcTransportClient> freshTransport;
        int32_t newAppId = endpoint->appid();
        int32_t newMsgId = endpoint->messageid();
        std::string newHost = endpoint->host();
        {
            faabric::util::ScopedLock lock(mx);
            auto chIt = channels.find(channelId);
            if (chIt == channels.end()) {
                throw std::runtime_error("Channel disappeared during re-resolve");
            }
            freshTransport = getOrCreateTransportLocked(chIt->second);
        }

        freshTransport->asyncSendRequest(requestId, buildReq(newAppId, newMsgId));
        getRpcTracker().recordDependency(ownerAppId, ownerMsgId,
                                         newAppId, newMsgId,
                                         cfg.endpointHost, newHost);
        SPDLOG_DEBUG("RPC - req={} re-resolved {} to app={} msg={} host={}",
                    requestId, targetUri, newAppId, newMsgId, newHost);
        return requestId;
    } catch (const std::exception& e) {
        SPDLOG_ERROR("RPC - req={} failed after re-resolve: {}",
                     requestId, e.what());
        faabric::util::ScopedLock lock(mx);
        auto it = ops.find(requestId);
        if (it != ops.end()) {
            it->second.ready = true;
            it->second.response.set_requestid(requestId);
            it->second.response.set_statuscode(Rpc_StatusCode::UNAVAILABLE);
            it->second.response.set_errormessage(
              "failed to send request after re-resolve");
        }
        return requestId;
    }
}

bool RpcContext::hasPendingRequest(uint32_t requestId)
{
    faabric::util::ScopedLock lock(mx);

    auto it = ops.find(requestId);
    if (it == ops.end()) {
        return false;
    }

    return !it->second.ready;
}

void RpcContext::eraseRequest(uint32_t requestId)
{
    {
        faabric::util::ScopedLock lock(mx);
        ops.erase(requestId);
        requestToChannel.erase(requestId);
    }

    getRpcContextRegistry().clearRequest(requestId);
}

// -----------------------------------
// rpc response
// -----------------------------------

static void synthesizeDeadlineExceededIfNeeded(uint32_t requestId, RpcOp& op)
{
    if (op.ready) return;
    if (!op.deadline.has_value()) return;
    if (std::chrono::steady_clock::now() < op.deadline.value()) return;

    op.ready = true;
    op.response.set_requestid(requestId);
    op.response.set_statuscode(Rpc_StatusCode::DEADLINE_EXCEEDED);
    op.response.set_errormessage("deadline exceeded");
}

bool RpcContext::testResponse(uint32_t requestId)
{
    faabric::util::ScopedLock lock(mx);

    auto it = ops.find(requestId);
    if (it == ops.end()) {
        return false;
    }

    synthesizeDeadlineExceededIfNeeded(requestId, it->second);

    return it->second.ready;
}

bool RpcContext::getResponse(uint32_t requestId, faabric::RpcResponse& out)
{
    {
        faabric::util::ScopedLock lock(mx);

        auto it = ops.find(requestId);
        if (it == ops.end()) {
            out.Clear();
            return false;
        }

        synthesizeDeadlineExceededIfNeeded(requestId, it->second);

        if (!it->second.ready) {
            SPDLOG_WARN("RPC - response for {} not ready", requestId);
            return false;
        }

        out = std::move(it->second.response);

        ops.erase(it);
        requestToChannel.erase(requestId);
    }

    getRpcContextRegistry().clearRequest(requestId);
    return true;
}

void RpcContext::onResponseReceived(const faabric::RpcResponse& resp)
{
    faabric::util::ScopedLock lock(mx);

    auto it = ops.find(resp.requestid());
    if (it == ops.end()) {
        SPDLOG_WARN("RPC - Response for unknown request {}", resp.requestid());
        return;
    }

    it->second.response = resp;
    it->second.ready = true;

    SPDLOG_DEBUG("RPC - Received response for {}", resp.requestid());
}

// -----------------------------------
// forwarding
// -----------------------------------

void RpcContext::setupForwarding(const std::string& newHost,
                                 std::chrono::milliseconds defaultTtl)
{
    namespace chrono = std::chrono;
    std::unordered_set<uint32_t> pendingIds;
    chrono::milliseconds maxRemaining{ 0 };
    bool anyUnbounded = false;

    {
        faabric::util::ScopedLock lock(mx);

        const auto now = chrono::steady_clock::now();

        for (const auto& [reqId, op] : ops) {
            if (op.ready) continue;

            pendingIds.insert(reqId);
            if (!op.deadline.has_value()) {
                anyUnbounded = true;
                continue;
            }

            auto remaining = chrono::duration_cast<chrono::milliseconds>(
              op.deadline.value() - now);

            maxRemaining = std::max(maxRemaining, remaining);
        }
    }

    auto ttl = maxRemaining * kTimeoutTtlMultiplier;
    if (anyUnbounded) {
        ttl = std::max(ttl, defaultTtl);
    }

    auto& reg = getRpcContextRegistry();

    for (uint32_t reqId : pendingIds) {
        reg.refreshRequestTtl(reqId, ttl);
    }

    reg.removeContext(ownerAppId, ownerMsgId);
}

std::optional<faabric::planner::ServiceEndpoint>
RpcContext::reresolveChannel(int32_t channelId, const std::string& targetUri)
{
    const std::string serviceName = targetUri.substr(kFaabricScheme.size());
    auto endpoint = resolver->resolve(serviceName);
    if (!endpoint.has_value()) {
        return std::nullopt;
    }

    faabric::util::ScopedLock lock(mx);
    auto chIt = channels.find(channelId);
    if (chIt != channels.end()) {
        ChannelInfo& info = chIt->second;
        info.targetAppId = endpoint->appid();
        info.targetMessageId = endpoint->messageid();
        info.targetHost = endpoint->host();
    }
    return endpoint;
}

} // namespace faabric::rpc