#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/rpc/RpcClientTransport.h>
#include <faabric/util/concurrent_map.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace faabric::rpc {

struct ChannelInfo
{
    std::string targetUri;
    bool isFaabric;
    std::string host;
    int port;
};

enum RpcContextMode
{
    RUNNING = 0,
    QUIESCE = 1,
};

class RpcContext : public std::enable_shared_from_this<RpcContext>
{
  public:
    RpcContext(int32_t ownerMsgIdIn);
    ~RpcContext();

    // ------
    // Channel handling
    // ------

    int32_t createChannel(const std::string& targetUri);

    ChannelInfo getChannel(int32_t channelId);

    void closeChannel(int32_t channelId);

    void clear();

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
                        int32_t reqLength);

    bool testResponse(uint32_t requestId);

    bool getResponse(uint32_t requestId, faabric::RpcResponse& out);

    bool hasPendingRequest(uint32_t requestId);

    void eraseRequest(uint32_t requestId);

    void onResponseReceived(const faabric::RpcResponse& resp);

    // ------
    // Quiescense
    // ------

    void beginQuiesce();

    void endQuiesce();

    bool tryEnterCall();

    void exitCall();

  private:
    const int32_t ownerMsgId;

    static std::atomic<uint32_t> nextRequestId;

    std::atomic<int32_t> nextChannelId{ 1 };

    faabric::util::ConcurrentMap<int32_t, ChannelInfo> channels;
    faabric::util::ConcurrentMap<uint32_t, int32_t> requestToChannel;

    std::atomic<RpcContextMode> context{ RUNNING };
    std::atomic<uint32_t> inFlightCalls{ 0 };

    mutable std::mutex quiesceMx;
    std::condition_variable quiesceCv;

    template <typename Key>
    using ConcurrentMapToTransport =
        faabric::util::ConcurrentMap<Key, std::shared_ptr<RpcClientTransport>>;
    ConcurrentMapToTransport<std::string> targetToTransport;
    ConcurrentMapToTransport<uint32_t> requestToTransport;

    int32_t getOwnerMsgId() const { return ownerMsgId; }

    static ChannelInfo parseChannelInfo(const std::string& targetUri);
    static std::string makeTargetKey(const ChannelInfo& info);

    std::shared_ptr<RpcClientTransport> getOrCreateTransport(
      const ChannelInfo& info);
};

} // namespace faabric::rpc