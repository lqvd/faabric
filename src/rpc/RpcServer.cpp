#include <faabric/rpc/RpcServer.h>

#include <faabric/rpc/ExternalChannel.h>
#include <faabric/rpc/FaabricChannel.h>
#include <faabric/rpc/rpc.h>
#include <faabric/transport/common.h>
#include <faabric/util/logging.h>
#include <faabric/util/network.h>

#include <stdexcept>

namespace faabric::rpc {

RpcServer::RpcServer()
  : faabric::transport::MessageEndpointServer(
      RPC_ASYNC_PORT,
      RPC_SYNC_PORT,       
      "rpc",
      4 // arbitrary for now
    ) {}

void RpcServer::registerHandler(const std::string& method,
                                RpcHandler handler) {
  routingTable[method] = handler;
}

void RpcServer::doAsyncRecv(transport::Message& message) {
    // TODO
}

std::unique_ptr<google::protobuf::Message> RpcServer::doSyncRecv(
  transport::Message& message)
{
    // Parse the RpcRequest payload.
    faabric::RpcRequest req;
    if (!req.ParseFromArray(message.udata().data(), message.udata().size())) {
        SPDLOG_ERROR("RPC Server - Failed to parse RpcRequest");
        
        auto resp = std::make_unique<faabric::RpcResponse>();
        resp->set_statuscode(Rpc_StatusCode::INTERNAL);
        return resp;
    }

    // We use the string "method" (e.g. "/faabric.snapshot.SnapshotService/PushSnapshot")
    // to find the matching subsystem handler in our map.
    auto it = routingTable.find(req.method());
    if (it == routingTable.end()) {
        SPDLOG_ERROR("RPC Server - Unimplemented method {}", req.method());

        auto resp = std::make_unique<faabric::RpcResponse>();
        resp->set_statuscode(Rpc_StatusCode::UNIMPLEMENTED);
        return resp;
    }

    std::vector<uint8_t> handlerResponseBytes;

    auto status = it->second(
        reinterpret_cast<const uint8_t*>(req.payload().data()),
        req.payload().size(),
        handlerResponseBytes
    );

    // 5. Wrap whatever the subsystem generated back into the RpcResponse envelope
    auto resp = std::make_unique<faabric::RpcResponse>();
    resp->set_statuscode(status.code);
    resp->set_payload(handlerResponseBytes.data(), handlerResponseBytes.size());

    // MessageEndpointServer takes this return value, serializes it, 
    // and sends it back over NNG back to the caller.
    return resp;
}

} // namespace faabric::rpc