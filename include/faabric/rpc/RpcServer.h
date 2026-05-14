#pragma once

#include <faabric/rpc/rpc.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/MessageEndpointServer.h>
#include <faabric/util/queue.h>

#include <atomic>
#include <thread>
#include <string>
#include <unordered_map>

namespace faabric::rpc {

using RpcHandler = std::function<Rpc_Status(
    const uint8_t* reqData, 
    size_t reqLen, 
    std::vector<uint8_t>& respData
)>;

class RpcServer : public faabric::transport::MessageEndpointServer
{
  public:
    RpcServer();
    ~RpcServer();

    void registerHandler(const std::string& method, RpcHandler handler);
    void RegisterService(std::shared_ptr<Service> service);

  protected:
    void doAsyncRecv(transport::Message& message) override;
    std::unique_ptr<google::protobuf::Message> doSyncRecv(
        transport::Message& message) override;

  private:
    std::unordered_map<std::string, RpcHandler> routingTable;

    faabric::util::Queue<faabric::RpcResponse> outboundQueue;
    std::thread senderThread;
    std::atomic<bool> senderRunning{ false };

    void senderLoop();
    void startSender();
    void stopSender();
};

} // namespace faabric::rpc