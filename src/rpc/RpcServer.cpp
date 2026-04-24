#include <faabric/rpc/RpcServer.h>

#include <faabric/rpc/rpc.h>
#include <faabric/rpc/RpcContext.h>
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

void RpcServer::doAsyncRecv(transport::Message& message)
{
    uint8_t header = message.getMessageCode();
    int sequenceNum = message.getSequenceNum();
    switch (header) {
        case faabric::rpc::INVOKE: {
            faabric::RpcRequest req;

            if (!req.ParseFromArray(message.data().data(),
                                    message.data().size())) {
                SPDLOG_ERROR("RPC - Failed to parse RpcRequest");
                return;
            }

            auto it = routingTable.find(req.method());
            if (it == routingTable.end()) {
                SPDLOG_ERROR("RPC - Unknown method {}", req.method());

                faabric::RpcResponse resp;
                resp.set_requestid(req.requestid());
                resp.set_statuscode(Rpc_StatusCode::UNIMPLEMENTED);

                sendResponse(message, resp);
                return;
            }

            std::vector<uint8_t> output;

            Rpc_Status status = it->second(
                reinterpret_cast<const uint8_t*>(req.payload().data()),
                req.payload().size(),
                output
            );

            faabric::RpcResponse resp;
            resp.set_requestid(req.requestid());
            resp.set_statuscode(status.code);

            if (!output.empty()) {
                resp.set_payload(output.data(), output.size());
            }

            sendResponse(message, resp);
            break;
        }
        case faabric::rpc::RESPONSE: {
            faabric::RpcResponse resp;
            if (!resp.ParseFromArray(message.data().data(),
                                     message.data().size())) {
                SPDLOG_ERROR("Failed to parse RpcResponse");
                return;
            }

            auto& rpcContext = getExecutingRpcContext();
            rpcContext.onResponseReceived(resp);
            break;
        }
        default: {
            SPDLOG_ERROR("Invalid RPC header: {}", header);
            throw std::runtime_error("Invalid RPC message");
        }
    }
}

void RpcServer::sendResponse(const faabric::RpcRequest& req,
                             const faabric::RpcResponse& resp)
{
    std::string buffer;
    if (!resp.SerializeToString(&buffer)) {
        throw std::runtime_error("Failed to serialise RpcResponse");
    }

    faabric::transport::MessageEndpointClient client(
        req.replyhost(),
        req.replyport(),
        RPC_SYNC_PORT,
        5000
    );

    client.asyncSend(
        faabric::rpc::RESPONSE,
        reinterpret_cast<const uint8_t*>(buffer.data()),
        buffer.size(),
        req.getSequenceNum() // reuse or ignore
    );
}

} // namespace faabric::rpc