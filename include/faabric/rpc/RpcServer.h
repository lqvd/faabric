#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/MessageEndpointServer.h>
#include <faabric/util/queue.h>

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#ifndef NDEBUG
namespace tests {
class RpcServerFixture;
class RpcServerDeliveryFixture;
}
#endif

namespace faabric::rpc {

static constexpr int kRpcTimeoutMs = 5000;

struct PendingInvocation
{
    uint32_t requestId = 0;
    std::string method;
    std::string payload;
    std::string replyHost;
    int32_t replyPort = 0;

    int32_t targetAppId = 0;
    int32_t targetMessageId = 0;
    int32_t targetGroupId = 0;
    int32_t targetGroupIdx = 0;
};

struct ServiceInstanceKey
{
    int32_t appId = 0;
    int32_t messageId = 0;

    bool operator==(const ServiceInstanceKey& other) const
    {
        return appId == other.appId && messageId == other.messageId;
    }
};

struct ServiceInstanceKeyHash
{
    std::size_t operator()(const ServiceInstanceKey& key) const
    {
        const auto h1 = std::hash<int32_t>{}(key.appId);
        const auto h2 = std::hash<int32_t>{}(key.messageId);

        // Fine for this key. Avoids needing boost/hash_combine.
        return h1 ^ (h2 << 1);
    }
};

struct ServiceForwardingEntry
{
    std::string host;
    std::chrono::steady_clock::time_point expiry;
};

struct RpcFunctionTarget
{
    std::string user;
    std::string function;
};

class RpcServer final : public faabric::transport::MessageEndpointServer
{
  public:
    RpcServer();
    ~RpcServer();

    void start(int timeoutMs = DEFAULT_SOCKET_TIMEOUT_MS) override;
    void stop() override;

    // Deliver an RpcResponse. Performs forwarding if host has migrated.
    void deliverResponse(const faabric::RpcResponse& response);

    // Register a service on this host.
    void registerServiceInstance(int32_t appId, int32_t messageId);

    // Unregister a service on this host.
    void unregisterServiceInstance(int32_t appId, int32_t messageId);

    // Get a pending Rpc invocation request. Either returns a pending invocation
    // or `std::nullopt` if none exists.
    std::optional<PendingInvocation> tryDequeueInvocation(int32_t appId,
                                                          int32_t messageId);

    // Called during migration. Set up forwardin, remove local delivery,
    // drains queued invocations, and forwards them.
    void migrateServiceQueue(int32_t appId,
                             int32_t messageId,
                             const std::string& dstHost,
                             std::chrono::milliseconds ttl);

  protected:
    void doAsyncRecv(transport::Message& message) override;

    std::unique_ptr<google::protobuf::Message> doSyncRecv(
      transport::Message& message) override;

  private:
#ifndef NDEBUG
    friend class tests::RpcServerFixture;
    friend class tests::RpcServerDeliveryFixture;
#endif

    void recvInvoke(std::span<const uint8_t> buffer);
    void recvResponse(std::span<const uint8_t> buffer);
    void recvFetch(std::span<const uint8_t> buffer);

    std::optional<RpcFunctionTarget> resolveMethod(
      const std::string& method) const;

    void sendErrorResponseToReplyHost(const faabric::RpcRequest& req,
                                      int32_t statusCode,
                                      const std::string& errorMessage);

    // -----------------------------------
    // service invocation and forwarding private helpers
    // -----------------------------------

    std::mutex servicesMx;

    std::unordered_map<ServiceInstanceKey,
                       std::shared_ptr<faabric::util::Queue<PendingInvocation>>,
                       ServiceInstanceKeyHash> serviceQueues;

    std::unordered_map<ServiceInstanceKey,
                       ServiceForwardingEntry,
                       ServiceInstanceKeyHash> serviceForwarding;

    void dispatchRpcToLocalService(const faabric::RpcRequest& req);

    void enqueueInvocation(int32_t appId,
                           int32_t messageId,
                           PendingInvocation invocation);

    void setServiceForwardingAddress(int32_t appId,
                                     int32_t messageId,
                                     const std::string& host,
                                     std::chrono::milliseconds ttl);

    std::optional<std::string> getServiceForwardingAddress(int32_t appId,
                                                           int32_t messageId);

    void forwardInvokeToHost(const faabric::RpcRequest& req,
                             const std::string& host);
};

RpcServer& getRpcServer();

} // namespace faabric::rpc