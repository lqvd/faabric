#include <faabric/rpc/RpcServer.h>

#include <faabric/rpc/rpc.h>
#include <faabric/rpc/RpcContext.h>
#include <faabric/rpc/RpcContextRegistry.h>
#include <faabric/rpc/RpcMessageType.h>
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
                SPDLOG_ERROR("RPC - Failed to parse RpcResponse");
                return;
            }

            auto& registry = getRpcContextRegistry();
            const uint32_t requestId = resp.requestid();

            auto msgIdxOpt = registry.getMsgIdxForRequest(requestId);
            if (!msgIdxOpt.has_value()) {
                SPDLOG_WARN("RPC - Orphaned response for unknown request {}",
                            requestId);
                return;
            }
            const int32_t msgIdx = msgIdxOpt.value();

            // Local context still here, deliver directly.
            if (auto ctx = registry.getContextForRequest(requestId)) {
                SPDLOG_TRACE("RPC - Routing response {} to msg {}",
                             requestId, msgIdx);
                ctx->onResponseReceived(resp);
                return;
            }

            // Context has migrated away, proxy to the new host if known.
            auto forwardHost = registry.getForwardingAddress(msgIdx);
            if (!forwardHost.has_value()) {
                SPDLOG_WARN("RPC - Response {} for msg {} has no local context "
                            "and no forwarding address; dropping",
                            requestId, msgIdx);
                registry.clearRequest(requestId);
                return;
            }

            SPDLOG_TRACE("RPC - Proxying response {} for msg {} to migrated host {}",
                        requestId, msgIdx, forwardHost.value());

            // TODO: maybe pool MessageEndpointClient instances per host instead of
            // constructing one per forwarded response.
            std::string buffer;
            if (!resp.SerializeToString(&buffer)) {
                SPDLOG_ERROR("RPC - Failed to re-serialise response {} for proxy",
                             requestId);
                return;
            }

            faabric::transport::MessageEndpointClient client(
                forwardHost.value(), RPC_ASYNC_PORT, RPC_SYNC_PORT, 5000);

            client.asyncSend(faabric::rpc::RpcMessageType::RESPONSE,
                             reinterpret_cast<const uint8_t*>(buffer.data()),
                             buffer.size());

            // Hand-off complete — the migrated host owns this request now.
            registry.clearRequest(requestId);
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