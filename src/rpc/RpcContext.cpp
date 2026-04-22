#include <faabric/rpc/RpcContext.h>

#include <faabric/executor/ExecutorContext.h>
#include <faabric/rpc/FaabricChannel.h>
#include <faabric/rpc/ExternalChannel.h>
#include <faabric/transport/common.h>
#include <faabric/util/logging.h>
#include <faabric/util/network.h>

#include <stdexcept>

namespace faabric::rpc {

// ---------------------------------------------------------------------------
// URI parsing helpers
// ---------------------------------------------------------------------------

static constexpr std::string_view kFaabricScheme = "faabric://";

static bool isFaabricUri(const std::string& uri)
{
    return uri.rfind(kFaabricScheme, 0) == 0;
}

/// Parse "faabric://host" or "faabric://host:port" into (host, port).
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

// ---------------------------------------------------------------------------
// RpcContext
// ---------------------------------------------------------------------------

int32_t RpcContext::createChannel(const std::string& targetUri)
{
    std::shared_ptr<IChannel> ch;

    if (isFaabricUri(targetUri)) {
        auto [host, port] = parseFaabricUri(targetUri);
        
        SPDLOG_TRACE("RPC - Creating FaabricChannel to {}:{}", host, port);
        ch = std::make_shared<FaabricChannel>(host, port);
    } else {
        SPDLOG_WARN("RPC - Creating ExternalChannel for URI {}", targetUri);
        ch = std::make_shared<ExternalChannel>(targetUri);
    }

    int32_t id = nextId++;
    channels.insertOrAssign(id, std::move(ch));
    return id;
}

std::shared_ptr<IChannel> RpcContext::getChannel(int32_t channelId)
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
    SPDLOG_TRACE("RPC - Closed and destroyed channel ID {}", channelId);
}

void RpcContext::clear()
{
    SPDLOG_TRACE("RPC - Resetting RpcContext. "
                 "Dropping all channels for live migration.");
    channels.clear();
    nextId.store(1, std::memory_order_relaxed);
}

std::vector<std::pair<int32_t, std::string>> RpcContext::serializeChannels() const
{
    std::vector<std::pair<int32_t, std::string>> result;
    
    // Extract just the channel IDs and target URIs
    channels.inspectAll([&result](const int32_t& id,
                                  const std::shared_ptr<IChannel>& channel) {
        result.emplace_back(id, channel->getTargetUri());
    });
    
    return result;
}

void RpcContext::deserializeChannels(
    const std::vector<std::pair<int32_t, std::string>>& data)
{
    clear();
    
    for (const auto& [id, uri] : data) {
        // Re-create channels with original IDs
        std::shared_ptr<IChannel> ch;
        if (isFaabricUri(uri)) {
            auto [host, port] = parseFaabricUri(uri);
            ch = std::make_shared<FaabricChannel>(host, port);
        } else {
            ch = std::make_shared<ExternalChannel>(uri);
        }
        channels.insertOrAssign(id, std::move(ch));
        nextId.store(std::max(nextId.load(std::memory_order_relaxed), id + 1),
                     std::memory_order_relaxed);
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
        // We were the last call; notify waiter if quiescing
        if (context.load(std::memory_order_acquire) == QUIESCE) {
            quiesceCv.notify_all();
        }
    }
}

RpcContext& getExecutingRpcContext()
{
    return faabric::executor::ExecutorContext::get()->getRpcContext();
}

} // namespace faabric::rpc