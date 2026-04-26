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

std::atomic<int32_t> RpcContext::nextContextId{1};

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


RpcContext::RpcContext() 
  : contextId(nextContextId.fetch_add(1, std::memory_order_relaxed)) {}

RpcContext::~RpcContext()
{
    getRpcContextRegistry().removeContext(contextId);
    getRpcContextRegistry().clearAllRequestsForContext(contextId);
}

int32_t RpcContext::getContextId() const
{
    return contextId;
}

ChannelInfo RpcContext::parseChannelInfo(const std::string& targetUri)
{
    if (isFaabricUri(targetUri)) {
        auto [host, port] = parseFaabricUri(targetUri);
        return ChannelInfo{
            .targetUri = targetUri,
            .isFaabric = true,
            .host = host,
            .port = port,
        };
    }

    throw std::runtime_error(
      "External RPC URIs are not implemented in this prototype");
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
      targetToTransport.tryEmplaceShared(key, info.host, RPC_ASYNC_PORT, info.port, 5000);

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

std::vector<std::pair<int32_t, std::string>> RpcContext::serializeChannels() const
{
    std::vector<std::pair<int32_t, std::string>> result;

    channels.inspectAll([&result](const int32_t& id, const ChannelInfo& info) {
        result.emplace_back(id, info.targetUri);
    });

    return result;
}

void RpcContext::deserializeChannels(
  const std::vector<std::pair<int32_t, std::string>>& data)
{
    clear();

    for (const auto& [id, uri] : data) {
        ChannelInfo info = parseChannelInfo(uri);
        channels.insertOrAssign(id, std::move(info));
        nextChannelId.store(
          std::max(nextChannelId.load(std::memory_order_relaxed), id + 1),
          std::memory_order_relaxed);
    }
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

    getRpcContextRegistry().registerInFlightRequest(requestId, this->contextId);

    transport->sendRequestAsync(requestId, req);
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
}

void RpcContext::beginQuiesce()
{
    context.store(QUIESCE, std::memory_order_release);
}

void RpcContext::awaitQuiesced(uint32_t timeoutMs)
{
#ifndef NDEBUG
    assert(context.load(std::memory_order_acquire) == QUIESCE);
#endif

    const auto start = std::chrono::steady_clock::now();

    while (inFlightCalls.load(std::memory_order_acquire) != 0) {
        const auto now = std::chrono::steady_clock::now();
        const auto elapsed =
          std::chrono::duration_cast<std::chrono::milliseconds>(now - start)
            .count();

        if (elapsed >= timeoutMs) {
            SPDLOG_ERROR("RPC quiesce timed out after {}ms with {} in-flight calls",
                         timeoutMs,
                         inFlightCalls.load(std::memory_order_acquire));
            throw std::runtime_error("RPC quiesce timed out");
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
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