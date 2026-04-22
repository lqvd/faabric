#pragma once

#include <faabric/rpc/IChannel.h>
#include <faabric/util/concurrent_map.h>

#include <memory>
#include <string>

namespace faabric::rpc {

enum RpcContextMode
{
    RUNNING = 0,
    QUIESCE = 1,
};

/**
 * RpcContext — per-Wasm-execution handle table mapping int32 channel IDs to
 * IChannel implementations.
 *
 * URI routing in createChannel():
 *   faabric://host      -> FaabricChannel on port RPC_SYNC_PORT (nng)
 *   faabric://host:port -> FaabricChannel on the given port (nng)
 *   <anything else>     -> ExternalChannel (throws for now)
 */
class RpcContext
{
  public:
    RpcContext() = default;

    // Factory: parse the URI scheme and create the right IChannel.
    int32_t createChannel(const std::string& targetUri);

    std::shared_ptr<IChannel> getChannel(int32_t channelId);

    void closeChannel(int32_t channelId);

    // Drop all channels.
    void clear();

    std::vector<std::pair<int32_t, std::string>> serializeChannels() const;
    void deserializeChannels(const std::vector<std::pair<int32_t, std::string>>& data);

    // Enter quiescent mode and set context mode to QUIESCE.
    void beginQuiesce();
    void awaitQuiesced(uint32_t timeoutMs);
    void endQuiesce();

    // Call before starting an RPC. If context mode is RUNNING, register that
    // an in-flight call is taking place and return true. If context mode is
    // QUIESCE, return false.
    bool tryEnterCall();
    // Decrement in-flight call count.
    void exitCall();

  private:
    std::atomic<int32_t> nextId{1};
    faabric::util::ConcurrentMap<int32_t, std::shared_ptr<IChannel>> channels;

    std::atomic<RpcContextMode> context{RUNNING};
    std::atomic<uint32_t> inFlightCalls{0};

    std::condition_variable quiesceCv;
};

RpcContext& getExecutingRpcContext();

} // namespace faabric::rpc