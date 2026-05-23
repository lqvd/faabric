#include <faabric/rpc/RpcServer.h>

#include <faabric/planner/planner.pb.h>
#include <faabric/planner/PlannerClient.h>
#include <faabric/rpc/rpc.h>
#include <faabric/rpc/RpcContext.h>
#include <faabric/rpc/RpcContextRegistry.h>
#include <faabric/rpc/RpcTransportClient.h>
#include <faabric/rpc/RpcMessageType.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/common.h>
#include <faabric/util/batch.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>

#include <stdexcept>

namespace faabric::rpc {

RpcServer::RpcServer()
  : faabric::transport::MessageEndpointServer(
      RPC_ASYNC_PORT,
      RPC_SYNC_PORT,
      RPC_INPROC_LABEL,
      faabric::util::getSystemConfig().rpcServerThreads)
{}

RpcServer::~RpcServer() { }

void RpcServer::start(int timeoutMs)
{
    faabric::transport::MessageEndpointServer::start();
}

void RpcServer::stop()
{
    faabric::transport::MessageEndpointServer::stop();
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
        case faabric::rpc::RpcMessageType::FETCH: {
            recvFetch(message.udata());
            break;
        }
        default: {
            throw std::runtime_error(
              fmt::format("Unrecognized async RPC header: {}", header));
        }
    }
}

// -----------------------------------
// inbound INVOKE
// -----------------------------------

void RpcServer::recvInvoke(std::span<const uint8_t> buffer)
{
    faabric::RpcRequest req;
    if (!req.ParseFromArray(buffer.data(), buffer.size())) {
        SPDLOG_ERROR("RPC - Failed to parse RpcRequest");
        return;
    }

    SPDLOG_INFO("RPC - Targeted INVOKE arrived requestId={} method={} app={} msg={}",
                req.requestid(),
                req.method(),
                req.targetappid(),
                req.targetmessageid());

    if (req.targetappid() == 0 || req.targetmessageid() == 0) {
        sendErrorResponseToReplyHost(
          req,
          Rpc_StatusCode::INVALID_ARGUMENT,
          "RPC invoke missing target service instance");
        return;
    }

    try {
        dispatchRpcToLocalService(req);
        return;
    } catch (const std::exception& e) {
        auto forwardingHost = getServiceForwardingAddress(
            req.targetappid(),
            req.targetmessageid());

        if (forwardingHost.has_value()) {
            SPDLOG_INFO("RPC - Forwarding late INVOKE {} for app={} msg={} to {}",
                        req.requestid(),
                        req.targetappid(),
                        req.targetmessageid(),
                        forwardingHost.value());

            forwardInvokeToHost(req, forwardingHost.value());
            return;
        }

        SPDLOG_WARN("RPC - No local service instance or forwarding for app={}"
                    "msg={}: {}",
                    req.targetappid(),
                    req.targetmessageid(),
                    e.what());

        sendErrorResponseToReplyHost(
          req,
          Rpc_StatusCode::UNAVAILABLE,
          "Target service instance not available");
    }
}

// -----------------------------------
// inbound RESPONSE
// -----------------------------------

void RpcServer::recvResponse(std::span<const uint8_t> buffer)
{
    faabric::RpcResponse resp;
    if (!resp.ParseFromArray(buffer.data(), buffer.size())) {
        SPDLOG_ERROR("RPC - Failed to parse RpcResponse");
        return;
    }
    deliverResponse(resp);
}

// -----------------------------------
// inbound FETCH
// -----------------------------------

void RpcServer::recvFetch(std::span<const uint8_t> buffer)
{
    faabric::RpcFetchRequest fetch;
    if (!fetch.ParseFromArray(buffer.data(), buffer.size())) {
        SPDLOG_ERROR("RPC - Failed to parse RpcFetchRequest");
        return;
    }

    const uint32_t requestId = fetch.requestid();
    auto& registry = getRpcContextRegistry();

    auto cached = registry.consumeForwardedResponse(requestId);
    if (cached.has_value()) {
        SPDLOG_INFO("RPC - FETCH for {} — response ready, sending to {}:{}",
                    requestId, fetch.replyhost(), fetch.replyport());
        RpcTransportClient client(fetch.replyhost(), fetch.replyport(),
                                  RPC_SYNC_PORT, kRpcTimeoutMs);
        client.asyncSendResponse(cached.value());
    } else {
        SPDLOG_INFO("RPC - FETCH for {} — not ready, registering",
                    requestId);
        registry.registerPendingFetch(requestId,
                                      fetch.replyhost(),
                                      fetch.replyport());
    }
}

// -----------------------------------
// response delivery
// -----------------------------------

void RpcServer::deliverResponse(const faabric::RpcResponse& msg)
{
    const uint32_t requestId = msg.requestid();
    auto& registry = getRpcContextRegistry();

    // Local delivery, route directly to context
    if (auto ctx = registry.getContextForRequest(requestId)) {
        SPDLOG_INFO("RPC - Delivering response {} to local context", requestId);
        ctx->onResponseReceived(msg);
        return;
    }

    // Remote, lookup forwarding address
    auto msgIdx = registry.getMsgIdxForRequest(requestId);
    if (!msgIdx.has_value()) {
        SPDLOG_WARN("RPC - Response {} undeliverable; dropping", requestId);
        return;
    }

    auto destHost = registry.getForwardingAddress(msgIdx.value());
    if (!destHost.has_value()) {
        SPDLOG_WARN("RPC - No forwarding address for response {}; dropping", requestId);
        return;
    }

    // Check if target already sent a FETCH
    auto pendingFetch = registry.consumePendingFetch(requestId);
    const std::string& host = pendingFetch.has_value() ? pendingFetch->host : destHost.value();
    const int port = pendingFetch.has_value() ? pendingFetch->port : RPC_ASYNC_PORT;

    if (!pendingFetch.has_value()) {
        registry.cacheForwardedResponse(requestId, msg);
    }

    SPDLOG_INFO("RPC - Forwarding response {} to {}:{}", requestId, host, port);
    try {
        RpcTransportClient client(host, port, RPC_SYNC_PORT, kRpcTimeoutMs);
        client.asyncSendResponse(msg);
        registry.markForwarded(msgIdx.value(), requestId);
        registry.clearRequest(requestId);
    } catch (const std::exception& e) {
        SPDLOG_ERROR("RPC - Failed to forward response {} to {}:{}: {}",
                     requestId, host, port, e.what());
    }
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
                                  kRpcTimeoutMs);
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
        SPDLOG_ERROR("RPC - Invalid method name '{}': missing leading '/'", method);
        return std::nullopt;
    }

    const auto slashPos = method.find('/', 1);
    if (slashPos == std::string::npos) {
        SPDLOG_ERROR("RPC - Invalid method name '{}': missing method slash", method);
        return std::nullopt;
    }

    const std::string fullService = method.substr(1, slashPos - 1);
    if (fullService.empty()) {
        SPDLOG_ERROR("RPC - Invalid method name '{}': empty service", method);
        return std::nullopt;
    }

    const auto dotPos = fullService.rfind('.');
    if (dotPos == std::string::npos) {
        SPDLOG_ERROR("RPC - Invalid service name '{}': missing package", fullService);
        return std::nullopt;
    }

    RpcFunctionTarget target;
    target.user = fullService.substr(0, dotPos);
    target.function = fullService.substr(dotPos + 1);

    if (target.user.empty() || target.function.empty()) {
        SPDLOG_ERROR("RPC - Invalid target for method '{}': user='{}' function='{}'",
                     method, target.user, target.function);
        return std::nullopt;
    }

    return target;
}

// -----------------------------------
// service instances
// -----------------------------------

void RpcServer::registerServiceInstance(int32_t appId, int32_t messageId)
{
    if (appId == 0 || messageId == 0) {
        throw std::runtime_error(
          "Cannot register service instance with zero app/message id");
    }

    const ServiceInstanceKey key{ appId, messageId };

    {
        std::scoped_lock lock(servicesMx);

        serviceQueues.try_emplace(
          key,
          std::make_shared<faabric::util::Queue<PendingInvocation>>());

        // If this host now owns the instance, local stale forwarding is wrong.
        serviceForwarding.erase(key);
    }

    SPDLOG_INFO("RPC - Registered service instance app={} msg={}",
                appId,
                messageId);
}

void RpcServer::unregisterServiceInstance(int32_t appId, int32_t messageId)
{
    const ServiceInstanceKey key{ appId, messageId };

    {
        std::scoped_lock lock(servicesMx);
        serviceQueues.erase(key);
    }

    SPDLOG_INFO("RPC - Unregistered service instance app={} msg={}",
                appId,
                messageId);
}

void RpcServer::enqueueInvocation(int32_t appId,
                                  int32_t messageId,
                                  PendingInvocation invocation)
{
    std::shared_ptr<faabric::util::Queue<PendingInvocation>> queue;

    {
        std::scoped_lock lock(servicesMx);

        const ServiceInstanceKey key{ appId, messageId };

        auto fwdIt = serviceForwarding.find(key);
        if (fwdIt != serviceForwarding.end()) {
            const auto now = std::chrono::steady_clock::now();

            if (now < fwdIt->second.expiry) {
                throw std::runtime_error(
                  fmt::format("Service instance app={} msg={} forwarded to {}",
                              appId,
                              messageId,
                              fwdIt->second.host));
            }

            serviceForwarding.erase(fwdIt);
        }

        auto qIt = serviceQueues.find(key);
        if (qIt == serviceQueues.end()) {
            throw std::runtime_error(
              fmt::format("No local service instance for app={} msg={}",
                          appId,
                          messageId));
        }

        queue = qIt->second;
    }

    queue->enqueue(std::move(invocation));
}

std::optional<PendingInvocation> RpcServer::tryDequeueInvocation(
  int32_t appId,
  int32_t messageId)
{
    std::shared_ptr<faabric::util::Queue<PendingInvocation>> queue;

    {
        std::scoped_lock lock(servicesMx);

        const ServiceInstanceKey key{ appId, messageId };
        auto it = serviceQueues.find(key);
        if (it == serviceQueues.end()) {
            return std::nullopt;
        }

        queue = it->second;
    }

    try {
        return queue->dequeue(0);
    } catch (const faabric::util::QueueTimeoutException&) {
        return std::nullopt;
    }
}

void RpcServer::dispatchRpcToLocalService(const faabric::RpcRequest& req)
{
    PendingInvocation inv{
        .requestId = req.requestid(),
        .method = req.method(),
        .payload = req.payload(),
        .replyHost = req.replyhost(),
        .replyPort = req.replyport(),
        .targetAppId = req.targetappid(),
        .targetMessageId = req.targetmessageid(),
        .targetGroupId = req.targetgroupid(),
        .targetGroupIdx = req.targetgroupidx(),
    };

    enqueueInvocation(req.targetappid(), req.targetmessageid(), std::move(inv));
}

// -----------------------------------
// service forwarding
// -----------------------------------

void RpcServer::setServiceForwardingAddress(int32_t appId,
                                            int32_t messageId,
                                            const std::string& host,
                                            std::chrono::milliseconds ttl)
{
    if (appId == 0 || messageId == 0) {
        throw std::runtime_error(
          "Cannot set forwarding for zero app/message id");
    }

    if (host.empty()) {
        throw std::runtime_error("Cannot set empty service forwarding host");
    }

    if (ttl.count() <= 0) {
        throw std::runtime_error(
          "Cannot set non-positive service forwarding TTL");
    }

    const ServiceInstanceKey key{ appId, messageId };

    {
        std::scoped_lock lock(servicesMx);
        serviceForwarding[key] = ServiceForwardingEntry{
            .host = host,
            .expiry = std::chrono::steady_clock::now() + ttl,
        };
    }

    SPDLOG_INFO("RPC - Set service forwarding for app={} msg={} to {} for {}ms",
                appId,
                messageId,
                host,
                ttl.count());
}

std::optional<std::string> RpcServer::getServiceForwardingAddress(
  int32_t appId,
  int32_t messageId)
{
    const ServiceInstanceKey key{ appId, messageId };

    std::scoped_lock lock(servicesMx);

    auto it = serviceForwarding.find(key);
    if (it == serviceForwarding.end()) {
        return std::nullopt;
    }

    const auto now = std::chrono::steady_clock::now();
    if (now >= it->second.expiry) {
        SPDLOG_DEBUG("RPC - Expired service forwarding for app={} msg={}",
                     appId,
                     messageId);
        serviceForwarding.erase(it);
        return std::nullopt;
    }

    return it->second.host;
}

void RpcServer::forwardInvokeToHost(const faabric::RpcRequest& req,
                                    const std::string& host)
{
    SPDLOG_INFO("RPC - Forwarding INVOKE {} for app={} msg={} to {}",
                req.requestid(),
                req.targetappid(),
                req.targetmessageid(),
                host);

    try {
        RpcTransportClient client(host,
                                  RPC_ASYNC_PORT,
                                  RPC_SYNC_PORT,
                                  kRpcTimeoutMs);
        client.asyncSendRequest(req.requestid(), req);
    } catch (const std::exception& e) {
        SPDLOG_ERROR("RPC - Failed to forward INVOKE {} to {}: {}",
                     req.requestid(),
                     host,
                     e.what());
        sendErrorResponseToReplyHost(req,
                                     Rpc_StatusCode::INTERNAL,
                                     "Failed to forward to service host");
    }
}

void RpcServer::migrateServiceQueue(int32_t appId,
                                    int32_t messageId,
                                    const std::string& dstHost,
                                    std::chrono::milliseconds ttl)
{
    if (appId == 0 || messageId == 0) {
        throw std::runtime_error(
          "Cannot migrate service queue for zero app/message id");
    }

    if (dstHost.empty()) {
        throw std::runtime_error("Cannot migrate service queue to empty host");
    }

    if (ttl.count() <= 0) {
        throw std::runtime_error(
          "Cannot migrate service queue with non-positive TTL");
    }

    std::shared_ptr<faabric::util::Queue<PendingInvocation>> queue;

    {
        std::scoped_lock lock(servicesMx);

        const ServiceInstanceKey key{ appId, messageId };

        // First switch this service instance into forwarding mode. Any later
        // enqueue attempt will fail and recvInvoke will forward instead.
        serviceForwarding[key] = ServiceForwardingEntry{
            .host = dstHost,
            .expiry = std::chrono::steady_clock::now() + ttl,
        };

        // Remove local delivery and keep the queue pointer so we can drain it
        // outside the mutex.
        auto qIt = serviceQueues.find(key);
        if (qIt != serviceQueues.end()) {
            queue = qIt->second;
            serviceQueues.erase(qIt);
        }
    }

    std::vector<PendingInvocation> drained;

    if (queue != nullptr) {
        while (true) {
            try {
                drained.emplace_back(queue->dequeue(0));
            } catch (const faabric::util::QueueTimeoutException&) {
                break;
            }
        }
    }

    for (const auto& inv : drained) {
        faabric::RpcRequest req;
        req.set_requestid(inv.requestId);
        req.set_method(inv.method);
        req.set_payload(inv.payload);
        req.set_replyhost(inv.replyHost);
        req.set_replyport(inv.replyPort);

        req.set_targetappid(inv.targetAppId);
        req.set_targetmessageid(inv.targetMessageId);
        req.set_targetgroupid(inv.targetGroupId);
        req.set_targetgroupidx(inv.targetGroupIdx);

        forwardInvokeToHost(req, dstHost);
    }

    SPDLOG_INFO("RPC - Migrated service queue app={} msg={} to {} "
                "with {} drained invocations and forwarding TTL {}ms",
                appId,
                messageId,
                dstHost,
                drained.size(),
                ttl.count());
}

// -----------------------------------
// static getter
// -----------------------------------

RpcServer& getRpcServer()
{
    static RpcServer server;
    return server;
}

} // namespace faabric::rpc