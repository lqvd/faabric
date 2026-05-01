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
#include <thread>

namespace faabric::rpc {

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
        return { rest, RPC_SYNC_PORT };
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

    auto transport = std::make_shared<RpcClientTransport>(
      info.host,
      RPC_ASYNC_PORT,
      info.port,
      5000);

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
    requestToChannel.inspectAll([&](const uint32_t& reqId, const int32_t& chId) {
        auto* pendingReq = migrationCtx.add_pendingrequests();
        pendingReq->set_requestid(reqId);
        pendingReq->set_channelid(chId);
    });

    return migrationCtx;
}

void RpcContext::deserializeMigrationState(const faabric::RpcMigrationState& migrationCtx)
{
    clear();

    int32_t highestChannelId = 0;
    for (const auto& channelState : migrationCtx.channels()) {
        ChannelInfo info = parseChannelInfo(channelState.targeturi());
        channels.insertOrAssign(channelState.channelid(), std::move(info));
        highestChannelId = std::max(highestChannelId, channelState.channelid());
    }
    nextChannelId.store(highestChannelId + 1, std::memory_order_relaxed);

    for (const auto& pendingReq : migrationCtx.pendingrequests()) {
        uint32_t reqId = pendingReq.requestid();
        int32_t chId = pendingReq.channelid();

        requestToChannel.insertOrAssign(reqId, std::move(chId));

        ChannelInfo info = getChannel(chId);
        auto transport = getOrCreateTransport(info);
        requestToTransport.insertOrAssign(reqId, std::move(transport));

        // If a response was cached during migration, inject it into the
        // new transport so testResponse/getResponse work immediately
        if (!pendingReq.cachedresponse().empty()) {
            faabric::RpcResponse resp;
            resp.set_requestid(reqId);
            resp.set_statuscode(pendingReq.cachedstatuscode());
            resp.set_payload(pendingReq.cachedresponse());
            transport->onResponseReceived(resp);
            SPDLOG_INFO("RPC - Injected cached response for req {} on restore",
                        reqId);
        }

        getRpcContextRegistry().registerInFlightRequest(reqId, ownerMsgId);
    }

    // Register the context itself
    getRpcContextRegistry().registerContext(ownerMsgId, shared_from_this());
}

uint32_t RpcContext::startUnary(int32_t channelId,
                                const std::string& method,
                                const uint8_t* reqBuffer,
                                int32_t reqLength)
{
    ChannelInfo info = getChannel(channelId);

    if (!info.isFaabric) {
        throw std::runtime_error("External RPC is not implemented");
    }

    auto transport = getOrCreateTransport(info);

    const uint32_t requestId =
      nextRequestId.fetch_add(1, std::memory_order_relaxed);

    faabric::RpcRequest req;
    req.set_method(method);
    req.set_payload(reqBuffer, reqLength);
    req.set_requestid(requestId);

    getRpcContextRegistry().registerInFlightRequest(requestId, ownerMsgId);

    // Remember the channel so we can rebuild on migration.
    requestToChannel.insertOrAssign(requestId, std::move(channelId));

    try {
        transport->sendRequestAsync(requestId, req);
    } catch (...) {
        getRpcContextRegistry().clearRequest(requestId);
        requestToChannel.erase(requestId);
        throw;
    }

    requestToTransport.insertOrAssign(requestId, std::move(transport));
    return requestId;
}

bool RpcContext::testResponse(uint32_t requestId)
{
    auto transport = requestToTransport.get(requestId);
    if (!transport.has_value()) {
        return false;
    }

    return transport.value()->testResponse(requestId);
}

bool RpcContext::getResponse(uint32_t requestId, faabric::RpcResponse& out)
{
    auto transport = requestToTransport.get(requestId);
    if (!transport.has_value()) {
        return false;
    }

    if (transport.value()->getResponse(requestId, out)) {
        requestToTransport.erase(requestId);
        requestToChannel.erase(requestId);

        getRpcContextRegistry().clearRequest(requestId);
        return true;
    }

    return false;
}

bool RpcContext::hasPendingRequest(uint32_t requestId)
{
    auto transport = requestToTransport.get(requestId);
    if (!transport.has_value()) {
        return false;
    }

    return transport.value()->hasPendingRequest(requestId);
}

void RpcContext::eraseRequest(uint32_t requestId)
{
    auto transport = requestToTransport.get(requestId);
    if (transport.has_value()) {
        transport.value()->eraseRequest(requestId);
        requestToTransport.erase(requestId);
    }

    requestToChannel.erase(requestId);
    getRpcContextRegistry().clearRequest(requestId);
}

void RpcContext::beginQuiesce()
{
    context.store(QUIESCE, std::memory_order_release);
}

void RpcContext::endQuiesce()
{
    context.store(RUNNING, std::memory_order_release);
}

bool RpcContext::tryEnterCall()
{
    if (context.load(std::memory_order_acquire) == QUIESCE) {
        return false;
    }

    inFlightCalls.fetch_add(1, std::memory_order_acq_rel);

    if (context.load(std::memory_order_acquire) == QUIESCE) {
#ifndef NDEBUG
        const auto old = inFlightCalls.fetch_sub(1, std::memory_order_acq_rel);
        assert(old > 0);
#else
        inFlightCalls.fetch_sub(1, std::memory_order_acq_rel);
#endif
        return false;
    }

    return true;
}

void RpcContext::exitCall()
{
    if (inFlightCalls.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        if (context.load(std::memory_order_acquire) == QUIESCE) {
            quiesceCv.notify_all();
        }
    }
}

void RpcContext::onResponseReceived(const faabric::RpcResponse& resp)
{
    auto transport = requestToTransport.get(resp.requestid());
    if (!transport.has_value()) {
        SPDLOG_WARN("RPC - Response for unknown request {}", resp.requestid());
        return;
    }

    transport.value()->onResponseReceived(resp);
}

} // namespace faabric::rpc