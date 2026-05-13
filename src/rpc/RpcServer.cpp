#include <faabric/rpc/RpcServer.h>

#include <faabric/rpc/rpc.h>
#include <faabric/rpc/RpcContext.h>
#include <faabric/rpc/RpcContextRegistry.h>
#include <faabric/rpc/RpcTransportClient.h>
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
      RPC_INPROC_LABEL,
      4 // arbitrary for now
    ) {}

void RpcServer::registerHandler(const std::string& method,
                                RpcHandler handler)
{
  routingTable[method] = handler;
}

void RpcServer::RegisterService(std::shared_ptr<Service> service) {
    for (const auto& method : service->Methods()) {
        registerHandler(method,
            [service, method](const uint8_t* req, size_t len,
                              std::vector<uint8_t>& resp) {
                return service->HandleCall(method, req, len, resp);
            });
    }
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

            SPDLOG_INFO("RPC - Request arrived for requestId={}", req.requestid());

            faabric::rpc::RpcTransportClient client(req.replyhost(),
                                                    req.replyport(),
                                                    RPC_SYNC_PORT,
                                                    5000);
            faabric::RpcResponse resp;
            resp.set_requestid(req.requestid());

            auto it = routingTable.find(req.method());
            if (it == routingTable.end()) {
                SPDLOG_ERROR("RPC - Unknown method {}", req.method());
                resp.set_statuscode(Rpc_StatusCode::UNIMPLEMENTED);
                client.asyncSendResponse(resp);
                return;
            }

            Rpc_Status status;
            std::vector<uint8_t> output;
            try {
                status = it->second(
                    BYTES_CONST(req.payload().data()),
                    req.payload().size(),
                    output
                );
            } catch (const std::exception& e) {
                resp.set_statuscode(Rpc_StatusCode::INTERNAL);
                resp.set_errormessage(e.what());
                client.asyncSendResponse(resp);
                return;
            } catch (...) {
                resp.set_statuscode(Rpc_StatusCode::INTERNAL);
                resp.set_errormessage("unknown exception in handler");
                client.asyncSendResponse(resp);
                return;
            }

            resp.set_statuscode(status.code);

            if (!output.empty()) {
                resp.set_payload(output.data(), output.size());
            }

            client.asyncSendResponse(resp);
            break;
        }
        case faabric::rpc::RpcMessageType::RESPONSE: {
            faabric::RpcResponse resp;
            if (!resp.ParseFromArray(message.data().data(),
                                     message.data().size())) {
                SPDLOG_ERROR("RPC - Failed to parse RpcResponse");
                return;
            }

            SPDLOG_INFO("RPC - Response arrived for requestId={}",
                        resp.requestid());

            auto& registry = getRpcContextRegistry();
            const uint32_t requestId = resp.requestid();

            auto msgIdxOpt = registry.getMsgIdxForRequest(requestId);
            if (!msgIdxOpt.has_value()) {
                SPDLOG_WARN("RPC - Orphaned response for unknown request {}",
                            requestId);
                return;
            }
            const int32_t msgIdx = msgIdxOpt.value();

            // Has it migrated?
            auto forwardHost = registry.getForwardingAddress(msgIdx);
            if (forwardHost.has_value()) {
                SPDLOG_INFO("RPC - Proxying response {} for msg {} to migrated "
                            "host {}", requestId, msgIdx, forwardHost.value());

                faabric::rpc::RpcTransportClient client(forwardHost.value(),
                                                        RPC_ASYNC_PORT,
                                                        RPC_SYNC_PORT,
                                                        5000);
                client.asyncSendResponse(resp);

                // Hand-off complete — the migrated host owns this request now.
                registry.markForwarded(msgIdx, requestId);
                registry.clearRequest(requestId);
                return;
            }

            // Local context still here, deliver directly.
            if (auto ctx = registry.getContextForRequest(requestId)) {
                SPDLOG_INFO("RPC - Routing response {} to msg {}",
                             requestId, msgIdx);
                ctx->onResponseReceived(resp);
                return;
            }

            SPDLOG_WARN("RPC - Response {} for msg {} has no local context "
                        "and no forwarding address; dropping",
                        requestId, msgIdx);
            break;
        }
        default: {
            throw std::runtime_error(
              fmt::format("Unrecognized async call header: {}", header));
        }
    }
}

} // namespace faabric::rpc