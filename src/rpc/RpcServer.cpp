#include <faabric/rpc/RpcServer.h>

#include <faabric/rpc/rpc.h>
#include <faabric/rpc/RpcContext.h>
#include <faabric/rpc/RpcContextRegistry.h>
#include <faabric/rpc/RpcTransportClient.h>
#include <faabric/rpc/RpcMessageType.h>
#include <faabric/transport/common.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>
#include <faabric/util/queue.h>

#include <stdexcept>

namespace faabric::rpc {

RpcServer::RpcServer()
  : faabric::transport::MessageEndpointServer(
      RPC_ASYNC_PORT,
      RPC_SYNC_PORT,
      RPC_INPROC_LABEL,
      faabric::util::getSystemConfig().rpcServerThreads,
    )
{
    startSender();
}

RpcServer::~RpcServer()
{
    stopSender();
}

void RpcServer::startSender()
{
    senderRunning.store(true);
    senderThread = std::thread([this] { senderLoop(); });
}

void RpcServer::stopSender()
{
    senderRunning.store(false);
    if (senderThread.joinable()) {
        senderThread.join();
    }
}

void RpcServer::registerHandler(const std::string& method, RpcHandler handler)
{
    routingTable[method] = handler;
}

void RpcServer::RegisterService(std::shared_ptr<Service> service)
{
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

// The only thread that performs delivery. Recv workers never touch a socket
// or a context — they only ever enqueue. Routing is resolved here, at the
// last possible moment, so a request that migrates while sitting in the
// queue is handled with no special-casing.
void RpcServer::senderLoop()
{
    while (true) {
        faabric::RpcResponse msg;
        try {
            msg = outboundQueue.dequeue(200);
        } catch (const faabric::util::QueueTimeoutException&) {
            if (!senderRunning.load()) {
                break;
            }
            continue;
        }

        const uint32_t requestId = msg.requestid();
        Destination dest =
            getRpcContextRegistry().resolveDestination(requestId);

        switch (dest.kind) {
            case Destination::LOCAL: {
                // Re-check: resolveDestination snapshotted under lock, but
                // the context could migrate between resolve and here. If it
                // has, drop — a context never comes back, so the response
                // will have been (or will be) forwarded via the registry's
                // forwarding address on a later path.
                if (auto ctx =
                      getRpcContextRegistry().getContextForRequest(requestId)) {
                    SPDLOG_INFO("RPC - Delivering response {} to local context",
                                requestId);
                    ctx->onResponseReceived(msg);
                } else {
                    SPDLOG_WARN("RPC - Context for {} vanished before local "
                                "delivery; dropping", requestId);
                }
                break;
            }
            case Destination::REMOTE: {
                SPDLOG_INFO("RPC - Forwarding response {} to {}",
                            requestId, dest.host);
                try {
                    auto client = RpcTransportClient(
                        dest.host, dest.port, RPC_SYNC_PORT, 5000);
                    client.asyncSendResponse(msg);

                    auto& registry = getRpcContextRegistry();
                    auto msgIdxOpt = registry.getMsgIdxForRequest(requestId);
                    if (msgIdxOpt.has_value()) {
                        registry.markForwarded(msgIdxOpt.value(), requestId);
                    }
                    registry.clearRequest(requestId);
                } catch (const std::exception& e) {
                    SPDLOG_ERROR("RPC - Failed to forward response {} to {}: {}",
                                 requestId, dest.host, e.what());
                }
                break;
            }

            case Destination::UNDELIVERABLE: {
                SPDLOG_WARN("RPC - Response {} undeliverable (no context, no "
                            "forwarding address); dropping", requestId);
                break;
            }
        }
    }

    // Drain anything left so in-flight responses get a final delivery attempt.
    faabric::RpcResponse leftover;
    while (true) {
        try {
            leftover = outboundQueue.dequeue(1);
        } catch (const faabric::util::QueueTimeoutException&) {
            break;
        }
        Destination dest = getRpcContextRegistry()
                             .resolveDestination(leftover.requestid());
        if (dest.kind == Destination::REMOTE) {
            try {
                auto client = RpcTransportClient(
                    dest.host, dest.port, RPC_SYNC_PORT, 5000);
                client.asyncSendResponse(leftover);
            } catch (const std::exception& e) {
                SPDLOG_ERROR("RPC - Drain: failed to forward {}: {}",
                             leftover.requestid(), e.what());
            }
        } else if (dest.kind == Destination::LOCAL) {
            if (auto ctx = getRpcContextRegistry().getContextForRequest(
                  leftover.requestid())) {
                ctx->onResponseReceived(leftover);
            }
        }
    }
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

            SPDLOG_INFO("RPC - Request arrived for requestId={}",
                        req.requestid());

            faabric::RpcResponse resp;
            resp.set_requestid(req.requestid());

            auto it = routingTable.find(req.method());
            if (it == routingTable.end()) {
                SPDLOG_ERROR("RPC - Unknown method {}", req.method());
                resp.set_statuscode(Rpc_StatusCode::UNIMPLEMENTED);
            } else {
                Rpc_Status status;
                std::vector<uint8_t> output;
                try {
                    status = it->second(
                        BYTES_CONST(req.payload().data()),
                        req.payload().size(),
                        output);
                } catch (const std::exception& e) {
                    resp.set_statuscode(Rpc_StatusCode::INTERNAL);
                    resp.set_errormessage(e.what());
                } catch (...) {
                    resp.set_statuscode(Rpc_StatusCode::INTERNAL);
                    resp.set_errormessage("unknown exception in handler");
                }

                if (resp.statuscode() == Rpc_StatusCode::OK) {
                    resp.set_statuscode(status.code);
                    if (!output.empty()) {
                        resp.set_payload(output.data(), output.size());
                    }
                }
            }

            // Recv workers do CPU work only — never a socket, never a
            // context. Hand the response to the sender thread, which
            // resolves routing and delivers.
            outboundQueue.enqueue(resp);
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

            // Identical handling to a freshly-produced response: resolve
            // and deliver. Proxying a migrated response and replying to a
            // local request are the same operation.
            outboundQueue.enqueue(resp);
            break;
        }

        default: {
            throw std::runtime_error(
              fmt::format("Unrecognized async call header: {}", header));
        }
    }
}

} // namespace faabric::rpc