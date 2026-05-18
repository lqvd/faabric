#include <faabric/rpc/RpcServer.h>

#include <faabric/planner/PlannerClient.h>
#include <faabric/rpc/rpc.h>
#include <faabric/rpc/RpcContext.h>
#include <faabric/rpc/RpcContextRegistry.h>
#include <faabric/rpc/RpcTransportClient.h>
#include <faabric/rpc/RpcMessageType.h>
#include <faabric/transport/common.h>
#include <faabric/util/batch.h>
#include <faabric/util/config.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/queue.h>

#include <future>
#include <stdexcept>

namespace faabric::rpc {

RpcServer::RpcServer()
  : faabric::transport::MessageEndpointServer(
      RPC_ASYNC_PORT,
      RPC_SYNC_PORT,
      RPC_INPROC_LABEL,
      faabric::util::getSystemConfig().rpcServerThreads)
{}

RpcServer::~RpcServer()
{
    stopSender();
}

void RpcServer::start(int timeoutMs)
{
    startSender();
    startReactor();
    faabric::transport::MessageEndpointServer::start();
}

void RpcServer::stop()
{
    faabric::transport::MessageEndpointServer::stop();
    stopReactor();
    stopSender();
}

// -----------------------------------
// sender
// -----------------------------------

void RpcServer::startSender()
{
    bool expected = false;
    if (!senderRunning.compare_exchange_strong(expected, true)) {
        return;
    }

    senderThread = std::thread([this] { senderLoop(); });
}

void RpcServer::stopSender()
{
    senderRunning.store(false);

    if (senderThread.joinable()) {
        senderThread.join();
    }
}

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
        deliverResponse(msg);
    }

    // Drain anything left so in-flight responses get a final delivery attempt.
    while (true) {
        faabric::RpcResponse leftover;

        try {
            leftover = outboundQueue.dequeue(1);
        } catch (const faabric::util::QueueTimeoutException&) {
            break;
        }

        deliverResponse(leftover);
    }
}

// -----------------------------------
// reactor
// -----------------------------------

void RpcServer::startReactor()
{
    bool expected = false;
    if (!reactorRunning.compare_exchange_strong(expected, true)) {
        return;
    }
    reactorThread = std::thread([this] { reactorLoop(); });
}

void RpcServer::stopReactor()
{
    reactorRunning.store(false);
    {
        faabric::util::UniqueLock lock(pendingMx);
        pendingCv.notify_all();
    }
    if (reactorThread.joinable()) {
        reactorThread.join();
    }
}

void RpcServer::reactorLoop()
{
    using namespace std::chrono_literals;

    while (reactorRunning.load()) {
        std::vector<PendingResponse> local;

        {
            faabric::util::UniqueLock lock(pendingMx);
            pendingCv.wait_for(lock, 5ms, [this] {
                return !pending.empty() || !reactorRunning.load();
            });
            local.swap(pending);
        }

        if (local.empty()) {
            continue;
        }

        // Check each future once. Ready ones get translated and enqueued;
        // not-ready ones go back into pending for the next pass.
        std::vector<PendingResponse> stillPending;
        stillPending.reserve(local.size());

        for (auto& p : local) {
            auto status = p.fut.wait_for(0ms);
            if (status != std::future_status::ready) {
                stillPending.push_back(std::move(p));
                continue;
            }

            try {
                auto resultMsg = p.fut.get();
                faabric::RpcResponse resp;
                resp.set_requestid(p.requestId);
                resp.set_statuscode(resultMsg->returnvalue() == 0
                                     ? Rpc_StatusCode::OK
                                     : Rpc_StatusCode::INTERNAL);
                if (!resultMsg->outputdata().empty()) {
                    resp.set_payload(resultMsg->outputdata());
                }
                outboundQueue.enqueue(std::move(resp));
            } catch (const std::exception& e) {
                SPDLOG_ERROR("RPC - Failed to translate result for req {}: {}",
                             p.requestId, e.what());
                faabric::RpcResponse errResp;
                errResp.set_requestid(p.requestId);
                errResp.set_statuscode(Rpc_StatusCode::INTERNAL);
                errResp.set_errormessage(e.what());
                outboundQueue.enqueue(std::move(errResp));
            }

            faabric::planner::getPlannerClient().clearMessageResultPromise(
              p.msgId);
        }

        if (!stillPending.empty()) {
            faabric::util::UniqueLock lock(pendingMx);
            for (auto& p : stillPending) {
                pending.push_back(std::move(p));
            }
        }
    }
}

// -----------------------------------
// message endpoint server
// -----------------------------------

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
            recvInvoke(message.udata());
            break;
        }

        case faabric::rpc::RpcMessageType::RESPONSE: {
            recvResponse(message.udata());
            break;
        }

        default: {
            throw std::runtime_error(
              fmt::format("Unrecognized async RPC header: {}", header));
        }
    }
}

void RpcServer::recvInvoke(std::span<const uint8_t> buffer)
{
    faabric::RpcRequest req;

    if (!req.ParseFromArray(buffer.data(), buffer.size())) {
        SPDLOG_ERROR("RPC - Failed to parse RpcRequest");
        return;
    }

    SPDLOG_INFO("RPC - Request arrived for requestId={} method={}",
                req.requestid(),
                req.method());

    auto target = resolveMethod(req.method());
    if (!target.has_value()) {
        SPDLOG_ERROR("RPC - Unknown method {}", req.method());

        sendErrorResponseToReplyHost(req, Rpc_StatusCode::UNIMPLEMENTED,
                                     "Unknown RPC method: " + req.method());
        return;
    }

    dispatchRpcToFaasm(req, target.value());
}

void RpcServer::recvResponse(std::span<const uint8_t> buffer)
{
    faabric::RpcResponse resp;

    if (!resp.ParseFromArray(buffer.data(), buffer.size())) {
        SPDLOG_ERROR("RPC - Failed to parse RpcResponse");
        return;
    }

    SPDLOG_INFO("RPC - Response arrived for requestId={}",
                resp.requestid());

    outboundQueue.enqueue(std::move(resp));
}

// -----------------------------------
// rpc dispatch
// -----------------------------------

void RpcServer::dispatchRpcToFaasm(const faabric::RpcRequest& req,
                                   const RpcFunctionTarget& target)
{
    auto batch =
      faabric::util::batchExecFactory(target.user, target.function, 1);

    batch->set_type(faabric::BatchExecuteRequest::RPC);

    auto& msg = batch->mutable_messages()->at(0);
    msg.set_type(faabric::Message::CALL);
    msg.set_user(target.user);
    msg.set_function(target.function);

    msg.set_isrpc(true);
    msg.set_rpcservice(req.method());

    msg.set_rpcrequestid(req.requestid());
    msg.set_rpcreplyhost(req.replyhost());
    msg.set_rpcreplyport(req.replyport());

    msg.set_inputdata(req.payload());

    const int32_t msgId = msg.id();
    const int32_t appId = msg.appid();

    // Register request to msg mapping
    auto& registry = getRpcContextRegistry();
    registry.registerInFlightRequest(req.requestid(), msgId);

    try {
        // Register interest for the result BEFORE callFunctions so we don't
        // miss the planner's notification on fast completions.
        auto fut = faabric::planner::getPlannerClient()
                       .getMessageResultAsync(appId, msgId);
        faabric::planner::getPlannerClient().callFunctions(batch);
        registerPending(req.requestid(), appId, msgId, std::move(fut));
    } catch (const std::exception& e) {
        SPDLOG_ERROR("RPC - Failed to dispatch requestId={}: {}",
                     req.requestid(), e.what());
        faabric::planner::getPlannerClient().clearMessageResultPromise(msgId);
        registry.clearRequest(req.requestid());
        sendErrorResponseToReplyHost(req, Rpc_StatusCode::INTERNAL, e.what());
    }
}

void RpcServer::registerPending(
    uint32_t requestId, int appId, int msgId,
    std::future<std::shared_ptr<faabric::Message>> fut)
{
    {
        faabric::util::UniqueLock lock(pendingMx);
        pending.push_back({requestId, appId, msgId, std::move(fut)});
    }
    pendingCv.notify_one();
}

// -----------------------------------
// response (from invoke) handling
// -----------------------------------

void RpcServer::deliverResponse(const faabric::RpcResponse& msg)
{
    const uint32_t requestId = msg.requestid();
    auto& registry = getRpcContextRegistry();

    ResponseTarget dest = registry.getResponseTarget(requestId);

    if (dest.kind == ResponseTarget::LOCAL) {
        if (auto ctx = registry.getContextForRequest(requestId)) {
            SPDLOG_INFO("RPC - Delivering response {} to local context",
                        requestId);
            ctx->onResponseReceived(msg);
            return;
        }

        // context can migrate after first getResponseTarget()
        dest = registry.getResponseTarget(requestId);
    }

    if (dest.kind == ResponseTarget::REMOTE) {
        SPDLOG_INFO("RPC - Forwarding response {} to {}:{}",
                    requestId,
                    dest.host,
                    dest.port);

        try {
            RpcTransportClient client(dest.host,
                                      dest.port,
                                      RPC_SYNC_PORT,
                                      rpcTimeoutMs);
            client.asyncSendResponse(msg);

            auto msgIdxOpt = registry.getMsgIdxForRequest(requestId);
            if (msgIdxOpt.has_value()) {
                registry.markForwarded(msgIdxOpt.value(), requestId);
            }

            registry.clearRequest(requestId);
        } catch (const std::exception& e) {
            SPDLOG_ERROR("RPC - Failed to forward response {} to {}:{}: {}",
                         requestId,
                         dest.host,
                         dest.port,
                         e.what());
        }
        return;
    }

    SPDLOG_WARN("RPC - Response {} undeliverable "
                "(no context, no forwarding address); dropping",
                requestId);
}

// -----------------------------------
// helpers
// -----------------------------------

void RpcServer::sendErrorResponseToReplyHost(const faabric::RpcRequest& req,
                                             int32_t statusCode,
                                             const std::string& errorMessage)
{
    faabric::RpcResponse resp;
    resp.set_requestid(req.requestid());
    resp.set_statuscode(statusCode);
    resp.set_errormessage(errorMessage);

    try {
        RpcTransportClient client(req.replyhost(),
                                  req.replyport(),
                                  RPC_SYNC_PORT,
                                  rpcTimeoutMs);
        client.asyncSendResponse(resp);
    } catch (const std::exception& e) {
        SPDLOG_ERROR("RPC - Failed to send error response {} to {}:{}: {}",
                     req.requestid(),
                     req.replyhost(),
                     req.replyport(),
                     e.what());
    }
}


std::optional<RpcFunctionTarget> RpcServer::resolveMethod(
  const std::string& method) const
{
    // Expected form: /<package>.<service>/<method>
    // Example: /faabric.rpc.PingSvc/Ping

    if (method.empty() || method[0] != '/') {
        SPDLOG_ERROR("RPC - Invalid method name '{}': missing leading '/'",
                     method);
        return std::nullopt;
    }

    const auto slashPos = method.find('/', 1);
    if (slashPos == std::string::npos) {
        SPDLOG_ERROR("RPC - Invalid method name '{}': missing method slash",
                     method);
        return std::nullopt;
    }

    const std::string fullService = method.substr(1, slashPos - 1);
    if (fullService.empty()) {
        SPDLOG_ERROR("RPC - Invalid method name '{}': empty service", method);
        return std::nullopt;
    }

    const auto dotPos = fullService.rfind('.');
    if (dotPos == std::string::npos) {
        SPDLOG_ERROR("RPC - Invalid service name '{}': missing package",
                     fullService);
        return std::nullopt;
    }

    RpcFunctionTarget target;
    target.user = fullService.substr(0, dotPos);
    target.function = fullService.substr(dotPos + 1);

    if (target.user.empty() || target.function.empty()) {
        SPDLOG_ERROR("RPC - Invalid target for method '{}': user='{}' function='{}'",
                     method,
                     target.user,
                     target.function);
        return std::nullopt;
    }

    return target;
}

} // namespace faabric::rpc