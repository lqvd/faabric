#pragma once

#include <faabric/transport/MessageEndpointServer.h>
#include <faabric/rpc/rpc.h>

#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

namespace faabric::rpc {

using RpcHandler = std::function<Rpc_Status(
    const uint8_t* reqData, 
    size_t reqLen, 
    std::vector<uint8_t>& respData
)>;

class RpcServer final : public faabric::transport::MessageEndpointServer
{
  public:
    RpcServer();

    void registerHandler(const std::string& method, RpcHandler handler);

  protected:
    std::unique_ptr<google::protobuf::Message> doSyncRecv(
        transport::Message& message) = 0;

    void doAsyncRecv(transport::Message& message) override;

  private:
    void sendResponse(const transport::Message& requestMsg,
                      const faabric::RpcResponse& resp);

    std::unordered_map<std::string, RpcHandler> routingTable;
};

} // namespace faabric::rpc