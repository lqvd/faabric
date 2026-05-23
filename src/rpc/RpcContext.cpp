#include <faabric/rpc/RpcContext.h>

#include <faabric/executor/ExecutorContext.h>
#include <faabric/rpc/rpc.h>
#include <faabric/rpc/RpcContextRegistry.h>
#include <faabric/rpc/RpcTransportClient.h>
#include <faabric/transport/common.h>
#include <faabric/util/logging.h>
#include <faabric/util/locks.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <stdexcept>
#include <string_view>
#include <optional>
#include <thread>

namespace faabric::rpc {

static constexpr int32_t kNoCachedRespStatus = -1;
static constexpr std::string_view kFaabricScheme = "faabric://";

std::atomic<uint32_t> RpcContext::nextRequestId{1};

// -----------------------------------
// helpers
// -----------------------------------

static bool isFaabricUri(const std::string& uri)
{
    return uri.rfind(kFaabricScheme, 0) == 0;
}

static int parseRpcPort(const std::string& uri)
{
    std::string rest = uri.substr(kFaabricScheme.size());
    auto colon = rest.rfind(':');
    return colon == std::string::npos
             ? RPC_ASYNC_PORT
             : std::stoi(rest.substr(colon + 1));
}

ChannelInfo parseChannelInfo(const std::string& targetUri)
{
    if (!isFaabricUri(targetUri)) {
        throw std::runtime_error(
            fmt::format("External RPC URIs are not implemented: {}", targetUri));
    }

    return ChannelInfo{
        .targetUri = targetUri,
        .isFaabric = true,
        .port = parseRpcPort(targetUri),
    };
}

// -----------------------------------
// rpc context state
// -----------------------------------

RpcContext::RpcContext(int32_t ownerMsgIdIn)
  : ownerMsgId(ownerMsgIdIn) {}

// Lock should be held.
std::shared_ptr<RpcTransportClient> RpcContext::getOrCreateTransportLocked(
  const ChannelInfo& info)
{
    // All faabric RPCs go through the local RpcServer... host is always local.
    // Port comes from the URI, identifies which RpcServer port to target.
    const std::string host = faabric::util::getSystemConfig().endpointHost;
    const std::string key = host + ":" + std::to_string(info.port);

    auto it = targetToTransport.find(key);
    if (it != targetToTransport.end()) {
        return it->second;
    }

    auto transport = std::make_shared<RpcTransportClient>(
        host, RPC_ASYNC_PORT, info.port, 5000);

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

            pendingReq->set_timeoutremaining(std::max<int64_t>(0, remaining));
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

        SPDLOG_INFO("RPC - Deserialising {} pending requests",
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
        reg.registerInFlightRequest(pendingReq.requestid(), ownerMsgId);
    }

    reg.registerContext(ownerMsgId, shared_from_this());

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

// Start an unary request and register into context.
uint32_t RpcContext::startUnary(int32_t channelId,
                                const std::string& method,
                                const uint8_t* reqBuffer,
                                int32_t reqLength,
                                int32_t timeoutMs)
{
    std::shared_ptr<RpcTransportClient> transport;
    uint32_t requestId = 0;

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

    getRpcContextRegistry().registerInFlightRequest(requestId, ownerMsgId);

    faabric::RpcRequest req;
    req.set_method(method);
    req.set_payload(reqBuffer, reqLength);
    req.set_requestid(requestId);

    try {
        transport->asyncSendRequest(requestId, req);
    } catch (...) {
        faabric::util::ScopedLock lock(mx);

        auto it = ops.find(requestId);
        if (it != ops.end()) {
            it->second.ready = true;
            it->second.response.set_requestid(requestId);
            it->second.response.set_statuscode(Rpc_StatusCode::UNAVAILABLE);
            it->second.response.set_errormessage("failed to send request");
        }
    }

    return requestId;
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

    SPDLOG_INFO("RPC - Received response for {}", resp.requestid());
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

    if (!pendingIds.empty()) {
        SPDLOG_INFO("PENDING ID SET!");
        reg.setForwardingAddress(
          ownerMsgId,
          newHost,
          std::move(pendingIds),
          ttl);
    }

    reg.removeContext(ownerMsgId);
}

} // namespace faabric::rpc