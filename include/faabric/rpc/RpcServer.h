#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/MessageEndpointServer.h>
#include <faabric/util/queue.h>

#include <atomic>
#include <optional>
#include <future>
#include <span>
#include <string>
#include <thread>

namespace faabric::rpc {

struct PendingResponse {
    uint32_t requestId;
    int appId;
    int msgId;
    std::future<std::shared_ptr<faabric::Message>> fut;
};

struct RpcFunctionTarget
{
    std::string user;
    std::string function;
};

// RpcServer uses MessageEndpointServer for its ingress transport layer
// (NNG socket management, worker thread pool, start/stop lifecycle).
// Unlike other MessageEndpointServer subclasses, request processing is
// fully asynchronous — doAsyncRecv only dispatches; completion happens
// via the reactor and sender threads. RpcServer is architecturally a
// gateway between the RPC wire protocol and the faabric executor system.
class RpcServer final : public faabric::transport::MessageEndpointServer
{
  public:
    RpcServer();
    ~RpcServer();

    void start(int timeoutMs = DEFAULT_SOCKET_TIMEOUT_MS) override;
    void stop() override;

    void deliverResponse(const faabric::RpcResponse& response);

  protected:
    void doAsyncRecv(transport::Message& message) override;

    std::unique_ptr<google::protobuf::Message> doSyncRecv(
      transport::Message& message) override;

  private:
    int rpcTimeoutMs = 5000;

    faabric::util::Queue<faabric::RpcResponse> outboundQueue;
    std::thread senderThread;
    std::atomic<bool> senderRunning{ false };

    std::thread reactorThread;
    std::atomic<bool> reactorRunning{ false };
    std::mutex pendingMx;
    std::condition_variable pendingCv;
    std::vector<PendingResponse> pending;

    void startSender();
    void stopSender();
    void senderLoop();

    void reactorLoop();
    void startReactor();
    void stopReactor();
    void registerPending(uint32_t requestId, int appId, int msgId,
                        std::future<std::shared_ptr<faabric::Message>> fut);

    void recvInvoke(std::span<const uint8_t> buffer);
    void recvResponse(std::span<const uint8_t> buffer);

    void dispatchRpcToFaasm(const faabric::RpcRequest& req,
                            const RpcFunctionTarget& target);

    std::optional<RpcFunctionTarget> resolveMethod(
      const std::string& method) const;

    void sendErrorResponseToReplyHost(const faabric::RpcRequest& req,
                                      int32_t statusCode,
                                      const std::string& errorMessage);
};

} // namespace faabric::rpc