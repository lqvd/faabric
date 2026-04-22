#pragma once

#include <faabric/transport/MessageEndpointServer.h>
#include <faabric/rpc/rpc.h>

#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

namespace faabric::rpc {

// The signature for a subsystem handler. 
// Takes raw protobuf bytes in, returns status and populates raw protobuf bytes
// out.
using RpcHandler = std::function<Rpc_Status(
    const uint8_t* reqData, 
    size_t reqLen, 
    std::vector<uint8_t>& respData
)>;

class RpcServer final : public faabric::transport::MessageEndpointServer
{
  public:
    RpcServer();

    // Subsystems (like SnapshotServer or FunctionCallServer) will call this
    // on startup to register their methods.
    // e.g., registerHandler("/faabric.snapshot.SnapshotService/PushSnapshot",
    //                       pushSnapshotCb);
    void registerHandler(const std::string& method, RpcHandler handler);

    void Wait() {
        this->start();
    }

  protected:
    std::unique_ptr<google::protobuf::Message> doSyncRecv(
        transport::Message& message) override;

    void doAsyncRecv(transport::Message& message) override;

  private:
    std::unordered_map<std::string, RpcHandler> routingTable;
};

} // namespace faabric::rpc