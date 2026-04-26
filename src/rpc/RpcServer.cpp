#include <faabric/rpc/RpcServer.h>

#include <faabric/rpc/rpc.h>
#include <faabric/rpc/RpcContext.h>
#include <faabric/rpc/RpcContextRegistry.h>
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
                                RpcHandler handler)
{
  routingTable[method] = handler;
}

std::unique_ptr<google::protobuf::Message> RpcServer::doSyncRecv(
    transport::Message& message)
{
    throw std::runtime_error("Rpc server does not support sync recv");
}

void RpcServer::doAsyncRecv(transport::Message& message)
{
    uint8_t header = message.getMessageCode();
    switch (header) {
        case faabric::rpc::RpcMessageType::INVOKE: {
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

                sendResponse(req, resp);
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

            sendResponse(req, resp);
            break;
        }
        case faabric::rpc::RpcMessageType::RESPONSE: {
            faabric::RpcResponse resp;
            if (!resp.ParseFromArray(message.data().data(),
                                     message.data().size())) {
                SPDLOG_ERROR("Failed to parse RpcResponse");
                return;
            }

            auto& registry = getRpcContextRegistry();
            auto ctx = registry.getContextForRequest(resp.requestid());

            if (ctx) {
                SPDLOG_TRACE("RPC - Routing response {} to context {}", 
                             resp.requestid(), ctx->getContextId());

                ctx->onResponseReceived(resp);
                registry.clearRequest(resp.requestid());
            } else {
                SPDLOG_WARN("RPC - Orphaned response received for request ID {}", 
                            resp.requestid());
            }
            break;
        }
        default: {
            throw std::runtime_error(
              fmt::format("Unrecognized async call header: {}", header));
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

    // TODO: What if the reply host migrates before this?
    faabric::transport::MessageEndpointClient client(
        req.replyhost(),
        req.replyport(),
        RPC_SYNC_PORT,
        5000
    );

    client.asyncSend(
        faabric::rpc::RpcMessageType::RESPONSE,
        reinterpret_cast<const uint8_t*>(buffer.data()),
        buffer.size()
    );
}

} // namespace faabric::rpc