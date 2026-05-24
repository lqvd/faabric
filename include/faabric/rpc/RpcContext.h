#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/rpc/RpcTransportClient.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace faabric::rpc {

using namespace std::chrono_literals;
static constexpr std::chrono::milliseconds kDefaultForwardingTtl = 30s;

struct ChannelInfo {
    std::string targetUri;
    bool isFaabric;
    int port;
    std::string targetHost;
    int32_t targetAppId = 0;
    int32_t targetMessageId = 0;
};

struct RpcOp
{
    bool ready = false;
    faabric::RpcResponse response;
    std::optional<std::chrono::steady_clock::time_point> deadline;
};

class RpcContext : public std::enable_shared_from_this<RpcContext>
{
  public:
    RpcContext(int32_t ownerMsgIdIn);

    ~RpcContext() = default;

    // ------
    // Channel handling
    // ------

    int32_t createChannel(const std::string& targetUri);

    ChannelInfo getChannel(int32_t channelId);

    void closeChannel(int32_t channelId);

    // ------
    // Migration serialisation and deserialisation
    // ------

    faabric::RpcMigrationState serializeMigrationState() const;

    void deserializeMigrationState(
      const faabric::RpcMigrationState& migrationCtx);

    // ------
    // Communication
    // ------

    uint32_t startUnary(int32_t channelId,
                        const std::string& method,
                        const uint8_t* reqBuffer,
                        int32_t reqLength,
                        int32_t timeoutMs = -1);

    bool testResponse(uint32_t requestId);

    bool getResponse(uint32_t requestId, faabric::RpcResponse& out);

    bool hasPendingRequest(uint32_t requestId);

    void eraseRequest(uint32_t requestId);

    // Called by the RPC server thread when a response arrives over the network
    void onResponseReceived(const faabric::RpcResponse& resp);

    // ------
    // Forwarding
    // ------
    void setupForwarding(const std::string& newHost,
                         std::chrono::milliseconds defaultTtl
                            = kDefaultForwardingTtl);

  private:
    static std::atomic<uint32_t> nextRequestId;
    std::atomic<int32_t> nextChannelId{ 1 };

    const int32_t ownerMsgId;

    mutable std::mutex mx;

    std::unordered_map<int32_t, ChannelInfo> channels;
    std::unordered_map<uint32_t, int32_t> requestToChannel;
    std::unordered_map<uint32_t, RpcOp> ops;
    std::unordered_map<std::string, std::shared_ptr<RpcTransportClient>>
      targetToTransport;

    void clearLocal();

    std::shared_ptr<RpcTransportClient> getOrCreateTransportLocked(
      const ChannelInfo& info);
};

} // namespace faabric::rpc