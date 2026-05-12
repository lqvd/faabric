#include <faabric/rpc/RpcContext.h>

#include <faabric/executor/ExecutorContext.h>
#include <faabric/rpc/rpc.h>
#include <faabric/rpc/RpcContextRegistry.h>
#include <faabric/transport/common.h>
#include <faabric/util/logging.h>

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

static bool isFaabricUri(const std::string& uri)
{
    return uri.rfind(kFaabricScheme, 0) == 0;
}

static std::pair<std::string, int> parseFaabricUri(const std::string& uri)
{
    std::string rest = uri.substr(kFaabricScheme.size());
    auto colon = rest.rfind(':');
    if (colon == std::string::npos) {
        return { rest, RPC_ASYNC_PORT };
    }

    std::string host = rest.substr(0, colon);
    int port = std::stoi(rest.substr(colon + 1));
    return { host, port };
}

RpcContext::RpcContext(int32_t ownerMsgIdIn)
  : ownerMsgId(ownerMsgIdIn) {}

RpcContext::~RpcContext()
{
    getRpcContextRegistry().clearAllRequestsForContext(ownerMsgId);
}

ChannelInfo RpcContext::parseChannelInfo(const std::string& targetUri)
{
    if (!isFaabricUri(targetUri)) {
        throw std::runtime_error(
            fmt::format("External RPC URIs are not implemented: {}", targetUri));
    }

    auto [host, port] = parseFaabricUri(targetUri);
    return ChannelInfo{
        .targetUri = targetUri,
        .isFaabric = true,
        .host = host,
        .port = port,
    };
}

std::string RpcContext::makeTargetKey(const ChannelInfo& info)
{
    return info.host + ":" + std::to_string(info.port);
}

std::shared_ptr<RpcClientTransport> RpcContext::getOrCreateTransport(
  const ChannelInfo& info)
{
    const std::string key = makeTargetKey(info);

    if (auto existing = targetToTransport.get(key)) {
        return existing.value();
    }

    // Keep whichever instance wins the race.
    auto [inserted, stored] =
        targetToTransport.tryEmplaceShared(key, info.host, RPC_ASYNC_PORT,
                                           info.port, 5000);

    if (inserted) {
        return stored;
    }

    return stored;
}

int32_t RpcContext::createChannel(const std::string& targetUri)
{
    ChannelInfo info = parseChannelInfo(targetUri);

    int32_t id = nextChannelId.fetch_add(1, std::memory_order_relaxed);
    channels.insertOrAssign(id, std::move(info));

    SPDLOG_DEBUG("RPC - Made channel {}", id);
    return id;
}

ChannelInfo RpcContext::getChannel(int32_t channelId)
{
    if (auto opt = channels.get(channelId)) {
        return opt.value();
    }

    SPDLOG_ERROR("RPC - Wasm guest requested unknown channel ID {}", channelId);
    throw std::runtime_error("Unknown RPC channel ID requested by Wasm guest");
}

void RpcContext::closeChannel(int32_t channelId)
{
    channels.erase(channelId);
    SPDLOG_TRACE("RPC - Closed channel ID {}", channelId);
}

void RpcContext::clear()
{
    SPDLOG_TRACE("RPC - Resetting RpcContext");
    channels.clear();
    targetToTransport.clear();
    requestToTransport.clear();
    nextChannelId.store(1, std::memory_order_relaxed);
}

faabric::RpcMigrationState RpcContext::serializeMigrationState() const
{
    faabric::RpcMigrationState migrationCtx;

    // Serialize Channels
    channels.inspectAll([&migrationCtx](const int32_t& id,
                                        const ChannelInfo& info) {
        auto* channelState = migrationCtx.add_channels();
        channelState->set_channelid(id);
        channelState->set_targeturi(info.targetUri);
    });

    // Serialize In-Flight Requests
    std::lock_guard<std::mutex> lock(opsMx);
    requestToChannel.inspectAll([&](const uint32_t& reqId, const int32_t& chId) {
        auto* pendingReq = migrationCtx.add_pendingrequests();
        pendingReq->set_requestid(reqId);
        pendingReq->set_channelid(chId);
        pendingReq->set_cachedstatuscode(kNoCachedRespStatus);

        auto it = ops.find(reqId);
        if (it != ops.end()) {
            if (it->second.ready) {
                pendingReq->set_cachedresponse(it->second.response.payload());
                pendingReq->set_cachedstatuscode(it->second.response.statuscode());
            }

            // If there is time remaining, set to time less migration time
            if (it->second.deadline.has_value()) {
                auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                    it->second.deadline.value() - std::chrono::steady_clock::now()).count();
                pendingReq->set_timeoutremaining(std::max(0L, remaining));
            } else {
                pendingReq->set_timeoutremaining(-1);
            }
        }
    });

    return migrationCtx;
}

void RpcContext::deserializeMigrationState(
    const faabric::RpcMigrationState& migrationCtx)
{
    clear();

    int32_t highestChannelId = 0;
    for (const auto& channelState : migrationCtx.channels()) {
        ChannelInfo info = parseChannelInfo(channelState.targeturi());
        channels.insertOrAssign(channelState.channelid(), std::move(info));
        highestChannelId = std::max(highestChannelId, channelState.channelid());
    }
    nextChannelId.store(highestChannelId + 1, std::memory_order_relaxed);

    SPDLOG_INFO("RPC - Deserialising {} pending requests",
                migrationCtx.pendingrequests_size());

    for (const auto& pendingReq : migrationCtx.pendingrequests()) {
        uint32_t requestId = pendingReq.requestid();
        int32_t channelId = pendingReq.channelid();

        requestToChannel.insertOrAssign(requestId, std::move(channelId));

        ChannelInfo info = getChannel(channelId);
        auto transport = getOrCreateTransport(info);
        requestToTransport.insertOrAssign(requestId, std::move(transport));

        // Restore the op slot... coroutine will find it ready when it resumes
        {
            std::lock_guard<std::mutex> lock(opsMx);
            RpcOp op;
            if (pendingReq.cachedstatuscode() != kNoCachedRespStatus) {
                op.ready = true;
                op.response.set_requestid(requestId);
                op.response.set_statuscode(pendingReq.cachedstatuscode());
                op.response.set_payload(pendingReq.cachedresponse());
            }
            if (pendingReq.timeoutremaining() >= 0) {
                op.deadline = std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(pendingReq.timeoutremaining());
            }
            // If no cached response, op.ready = false and coroutine will block
            // normally and onResponseReceived will wake it when it arrives
            ops.emplace(requestId, std::move(op));
        }

        getRpcContextRegistry().registerInFlightRequest(requestId, ownerMsgId);
    }

    getRpcContextRegistry().registerContext(ownerMsgId, shared_from_this());
}

uint32_t RpcContext::startUnary(int32_t channelId,
                                const std::string& method,
                                const uint8_t* reqBuffer,
                                int32_t reqLength,
                                int32_t timeoutMs)
{
    ChannelInfo info = getChannel(channelId);
    if (!info.isFaabric) {
        throw std::runtime_error("External RPC is not implemented");
    }

    auto transport = getOrCreateTransport(info);

    const uint32_t requestId =
        nextRequestId.fetch_add(1, std::memory_order_relaxed);

    // Register the op in RpcContext before sending, so onResponseReceived
    // can never arrive before the slot exists
    {
        std::lock_guard<std::mutex> lock(opsMx);
        RpcOp op;
        if (timeoutMs >= 0) {
            op.deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(timeoutMs);
        }
        ops.emplace(requestId, std::move(op));
    }

    getRpcContextRegistry().registerInFlightRequest(requestId, ownerMsgId);
    requestToChannel.insertOrAssign(requestId, std::move(channelId));

    faabric::RpcRequest req;
    req.set_method(method);
    req.set_payload(reqBuffer, reqLength);
    req.set_requestid(requestId);

    try {
        transport->sendRequestAsync(requestId, req);
        requestToTransport.insertOrAssign(requestId, std::move(transport));
    } catch (...) {
        std::lock_guard<std::mutex> lock(opsMx);
        auto it = ops.find(requestId);
        // synthesize a response
        if (it != ops.end()) {
            it->second.ready = true;
            it->second.response.set_requestid(requestId);
            it->second.response.set_statuscode(Rpc_StatusCode::UNAVAILABLE);
            it->second.response.set_errormessage("failed to send request");
        }
    }

    return requestId;
}

bool RpcContext::testResponse(uint32_t requestId)
{
    std::lock_guard<std::mutex> lock(opsMx);
    auto it = ops.find(requestId);
    if (it == ops.end()) {
        return false;
    }
    if (!it->second.ready && it->second.deadline.has_value() &&
        std::chrono::steady_clock::now() >= it->second.deadline.value()) {
        // Synthesize a DEADLINE_EXCEEDED response.
        it->second.ready = true;
        it->second.response.set_requestid(requestId);
        it->second.response.set_statuscode(Rpc_StatusCode::DEADLINE_EXCEEDED);
        it->second.response.set_errormessage("deadline exceeded");
    }
    return it->second.ready;
}

bool RpcContext::getResponse(uint32_t requestId, faabric::RpcResponse& out)
{
    std::unique_lock<std::mutex> lock(opsMx);
    auto it = ops.find(requestId);
    if (it == ops.end()) {
        out.set_statuscode(Rpc_StatusCode::INTERNAL);
        out.set_errormessage("getResponse called with unknown requestId");
        return true;
    }

    if (!it->second.ready && it->second.deadline.has_value() &&
        std::chrono::steady_clock::now() >= it->second.deadline.value()) {
        it->second.ready = true;
        it->second.response.set_requestid(requestId);
        it->second.response.set_statuscode(Rpc_StatusCode::DEADLINE_EXCEEDED);
        it->second.response.set_errormessage("deadline exceeded");
    }

    if (!it->second.ready) {
        SPDLOG_WARN("RPC - response for {} not ready", requestId);
        return false;
    }

    out = std::move(it->second.response);
    ops.erase(it);
    lock.unlock();

    requestToTransport.erase(requestId);
    requestToChannel.erase(requestId);
    getRpcContextRegistry().clearRequest(requestId);
    return true;
}

bool RpcContext::hasPendingRequest(uint32_t requestId)
{
    std::lock_guard<std::mutex> lock(opsMx);
    auto it = ops.find(requestId);
    if (it == ops.end()) {
        return false;
    }
    return !it->second.ready;
}

void RpcContext::eraseRequest(uint32_t requestId)
{
    {
        std::lock_guard<std::mutex> lock(opsMx);
        ops.erase(requestId);
    }
    requestToTransport.erase(requestId);
    requestToChannel.erase(requestId);
    getRpcContextRegistry().clearRequest(requestId);
}

void RpcContext::onResponseReceived(const faabric::RpcResponse& resp)
{
    {
        std::lock_guard<std::mutex> lock(opsMx);
        auto it = ops.find(resp.requestid());
        if (it == ops.end()) {
            SPDLOG_WARN("RPC - Response for unknown request {}", resp.requestid());
            return;
        }
        it->second.response = resp;
        it->second.ready = true;
    }
    SPDLOG_INFO("RPC - Received response for {}", resp.requestid());
}

void RpcContext::setupForwarding(const std::string& newHost,
                                 std::chrono::milliseconds defaultTtl)
{
    std::unordered_set<uint32_t> pendingIds;
    std::chrono::milliseconds maxRemaining{0};
    bool anyUnbounded = false;
    auto now = std::chrono::steady_clock::now();

    {
        std::lock_guard<std::mutex> lock(opsMx);
        for (const auto& [reqId, op] : ops) {
            if (op.ready) continue;
            pendingIds.insert(reqId);
            if (!op.deadline.has_value()) {
                anyUnbounded = true;
                continue;
            }
            auto remaining = 
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    op.deadline.value() - now);
            maxRemaining = std::max(maxRemaining, remaining);
        }
    }

    auto ttl = maxRemaining * kTimeoutTtlMultiplier;
    if (anyUnbounded) {
        ttl = std::max(ttl, defaultTtl);
    }

    getRpcContextRegistry().setForwardingAddress(
        ownerMsgId, newHost, std::move(pendingIds), ttl);
}

} // namespace faabric::rpc