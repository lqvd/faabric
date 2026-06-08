#include <faabric/rpc/RpcServer.h>

#include <faabric/planner/planner.pb.h>
#include <faabric/planner/PlannerClient.h>
#include <faabric/rpc/rpc.h>
#include <faabric/rpc/RpcContext.h>
#include <faabric/rpc/RpcContextRegistry.h>
#include <faabric/rpc/RpcTransportClient.h>
#include <faabric/rpc/RpcTracker.h>
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
        case faabric::rpc::RpcMessageType::INVOKE_FETCH: {
            recvInvocationFetch(message.udata());
            break;
        }
        case faabric::rpc::RpcMessageType::SHUTDOWN_SERVICE: {
            recvShutdown(message.udata());
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
        SPDLOG_WARN("RPC - Could not dispatch INVOKE {} for app={} msg={}: {}",
                    req.requestid(),
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

    if (!registry.hasRequest(requestId)) {
        SPDLOG_WARN("RPC - FETCH for unknown/expired request {}; ignoring",
                    requestId);
        return;
    }

    auto cached = registry.consumeCachedResponse(requestId);
    if (!cached.has_value()) {
        SPDLOG_INFO("RPC - FETCH for {} arrived before response; registering {}:{}",
                    requestId, fetch.replyhost(), fetch.replyport());

        registry.registerPendingFetch(
          requestId, fetch.replyhost(), fetch.replyport());
        return;
    }

    SPDLOG_INFO("RPC - FETCH for {} matched cached response; sending to {}:{}",
                requestId,
                fetch.replyhost(),
                fetch.replyport());

    try {
        RpcTransportClient client(fetch.replyhost(), fetch.replyport(),
                                  RPC_SYNC_PORT, kRpcTimeoutMs);

        client.asyncSendResponse(cached.value());
        registry.clearRequest(requestId);
    } catch (const std::exception& e) {
        SPDLOG_ERROR("RPC - Failed to send cached response {} to {}:{}: {}",
                     requestId, fetch.replyhost(), fetch.replyport(), e.what());
        registry.cacheResponse(requestId, cached.value());
    }
}

// -----------------------------------
// inbound SHUTDOWN
// -----------------------------------

void RpcServer::recvShutdown(std::span<const uint8_t> buffer)
{
    faabric::RpcShutdownRequest req;
    if (!req.ParseFromArray(buffer.data(), buffer.size())) {
        SPDLOG_ERROR("RPC - Failed to parse RpcShutdownRequest");
        return;
    }
    requestShutdown(req.targetappid(), req.targetmessageid());
}

// -----------------------------------
// response delivery
// -----------------------------------

void RpcServer::deliverResponse(const faabric::RpcResponse& resp)
{
    const uint32_t requestId = resp.requestid();
    auto& registry = getRpcContextRegistry();

    if (auto ctx = registry.getContextForRequest(requestId)) {
        SPDLOG_INFO("RPC - Delivering response {} to local context", requestId);
        ctx->onResponseReceived(resp);
        return;
    }

    if (!registry.hasRequest(requestId)) {
        SPDLOG_WARN("RPC - Response {} undeliverable; unknown/expired request",
                    requestId);
        return;
    }

    auto pendingFetch = registry.consumePendingFetch(requestId);
    if (!pendingFetch.has_value()) {
        SPDLOG_INFO("RPC - Response {} arrived before FETCH; caching", requestId);
        registry.cacheResponse(requestId, resp);
        return;
    }

    SPDLOG_INFO("RPC - Response {} matched FETCH; sending to {}:{}",
                requestId,
                pendingFetch->host,
                pendingFetch->port);

    try {
        RpcTransportClient client(
          pendingFetch->host, pendingFetch->port, RPC_SYNC_PORT, kRpcTimeoutMs);
        client.asyncSendResponse(resp);
        registry.clearRequest(requestId);
    } catch (const std::exception& e) {
        SPDLOG_ERROR("RPC - Failed to send response {} to {}:{}: {}",
                     requestId,
                     pendingFetch->host,
                     pendingFetch->port,
                     e.what());

        registry.cacheResponse(requestId, resp);
    }
}

// -----------------------------------
// invocation delivery
// -----------------------------------

void RpcServer::recvInvocationFetch(std::span<const uint8_t> buffer)
{
    faabric::RpcInvocationFetchRequest fetch;
    if (!fetch.ParseFromArray(buffer.data(), buffer.size())) {
        SPDLOG_ERROR("RPC - Failed to parse RpcInvocationFetchRequest");
        return;
    }

    const ServiceInstanceKey key{
        fetch.targetappid(),
        fetch.targetmessageid(),
    };

    std::deque<PendingInvocation> pending;

    {
        std::scoped_lock lock(servicesMx);

        auto it = serviceMigrations.find(key);
        if (it == serviceMigrations.end()) {
            // Either the service was never migrated from here, or the
            // migration entry already expired and was erased. If it expired
            // with a non-empty backlog, those invocations are already lost —
            // so we must NOT let the entry be erased on the expiry path while
            // a backlog remains (see change 2). Here we can only report.
            SPDLOG_WARN("RPC - INVOKE_FETCH for unknown migrated service "
                        "app={} msg={} (no migration entry)",
                        fetch.targetappid(),
                        fetch.targetmessageid());
            return;
        }

        auto& migration = it->second;

        // Record destination first so any invocation that races in after we
        // drop the lock is forwarded rather than dropped.
        migration.destination = ServiceForwardTarget{
            .host = fetch.replyhost(),
            .port = fetch.replyport(),
        };

        // Swap out the backlog to forward outside the lock.
        pending.swap(migration.pending);

        // Refresh the tombstone window relative to "now" so a slow fetch does
        // not leave a gap for stale clients between drain and re-discovery.
        migration.expiry =
        std::chrono::steady_clock::now() +
        std::chrono::milliseconds(kRpcTimeoutMs);
    }

    SPDLOG_INFO("RPC - INVOKE_FETCH for app={} msg={} draining {} "
                "invocations to {}:{}",
                fetch.targetappid(),
                fetch.targetmessageid(),
                pending.size(),
                fetch.replyhost(),
                fetch.replyport());

    for (const auto& inv : pending) {
        auto req = pendingInvocationToRpcRequest(inv);
        forwardInvokeToHost(req, fetch.replyhost(), fetch.replyport());
    }
}

void RpcServer::fetchMigratedServiceQueue(const std::string& originHost,
                                          int32_t appId,
                                          int32_t messageId)
{
    if (originHost.empty()) {
        throw std::runtime_error(
          "Cannot fetch migrated service queue from empty origin host");
    }

    if (appId == 0 || messageId == 0) {
        throw std::runtime_error(
          "Cannot fetch migrated service queue for zero app/message id");
    }

    const auto& conf = faabric::util::getSystemConfig();

    faabric::RpcInvocationFetchRequest fetch;
    fetch.set_targetappid(appId);
    fetch.set_targetmessageid(messageId);
    fetch.set_replyhost(conf.endpointHost);
    fetch.set_replyport(RPC_ASYNC_PORT);

    SPDLOG_INFO("RPC - Fetching migrated service queue app={} msg={} "
                "from {} to {}:{}",
                appId,
                messageId,
                originHost,
                fetch.replyhost(),
                fetch.replyport());

    RpcTransportClient client(originHost,
                              RPC_ASYNC_PORT,
                              RPC_SYNC_PORT,
                              kRpcTimeoutMs);

    client.asyncSendInvocationFetch(fetch);
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

        serviceQueues.try_emplace(key);

        // If this host now owns the instance, stale migration state is wrong.
        serviceMigrations.erase(key);
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
        shutdownRequested.erase(key);
    }

    SPDLOG_INFO("RPC - Unregistered service instance app={} msg={}",
                appId,
                messageId);
}

faabric::RpcRequest RpcServer::pendingInvocationToRpcRequest(
  const PendingInvocation& invocation) const
{
    faabric::RpcRequest req;
    req.set_requestid(invocation.requestId);
    req.set_method(invocation.method);
    req.set_payload(invocation.payload);
    req.set_replyhost(invocation.replyHost);
    req.set_replyport(invocation.replyPort);

    req.set_targetappid(invocation.targetAppId);
    req.set_targetmessageid(invocation.targetMessageId);
    req.set_targetgroupid(invocation.targetGroupId);
    req.set_targetgroupidx(invocation.targetGroupIdx);

    return req;
}

void RpcServer::enqueueInvocation(int32_t appId,
                                  int32_t messageId,
                                  PendingInvocation invocation)
{
    std::optional<ServiceForwardTarget> forwardTarget;
    std::optional<faabric::RpcRequest> forwardReq;

    {
        std::scoped_lock lock(servicesMx);

        const ServiceInstanceKey key{ appId, messageId };

        if (shutdownRequested.contains(key)) {
            throw std::runtime_error(
              fmt::format("Service instance app={} msg={} is shutting down",
                          appId,
                          messageId));
        }

        auto qIt = serviceQueues.find(key);
        if (qIt != serviceQueues.end()) {
            qIt->second.emplace_back(std::move(invocation));
            return;
        }

        auto migIt = serviceMigrations.find(key);
        if (migIt != serviceMigrations.end()) {
            auto& migration = migIt->second;

            const auto now = std::chrono::steady_clock::now();
            if (now >= migration.expiry) {
                // Only reclaim if the migration actually completed: backlog drained
                // and a destination recorded. Otherwise the fetch never arrived and
                // the backlog must survive for a late INVOKE_FETCH.
                if (migration.pending.empty()
                      && migration.destination.has_value()) {
                    serviceMigrations.erase(migIt);
                    throw std::runtime_error(
                    fmt::format("Expired migration state for app={} msg={}",
                                appId, messageId));
                }

                // Extend window.
                migration.expiry = now + std::chrono::milliseconds(kRpcTimeoutMs);
            }

            if (!migration.destination.has_value()) {
                migration.pending.emplace_back(std::move(invocation));
                return;
            }

            forwardTarget = migration.destination.value();
            forwardReq = pendingInvocationToRpcRequest(invocation);
        }

        if (!forwardTarget.has_value()) {
            throw std::runtime_error(
              fmt::format("No local service instance for app={} msg={}",
                          appId,
                          messageId));
        }
    }

    // Network I/O outside servicesMx.
    forwardInvokeToHost(forwardReq.value(),
                        forwardTarget->host,
                        forwardTarget->port);
}

std::optional<PendingInvocation> RpcServer::tryDequeueInvocation(
  int32_t appId,
  int32_t messageId)
{
    std::scoped_lock lock(servicesMx);

    const ServiceInstanceKey key{ appId, messageId };

    auto it = serviceQueues.find(key);
    if (it == serviceQueues.end()) {
        return std::nullopt;
    }

    auto& queue = it->second;
    if (queue.empty()) {
        return std::nullopt;
    }

    PendingInvocation invocation = std::move(queue.front());
    queue.pop_front();

    return invocation;
}

void RpcServer::dispatchRpcToLocalService(const faabric::RpcRequest& req)
{
    PendingInvocation inv{
        .requestId = req.requestid(),
        .method = req.method(),
        .payload = req.payload(),
        .replyHost = req.replyhost(),
        .replyPort = req.replyport() > 0 ? req.replyport() : RPC_ASYNC_PORT,
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

void RpcServer::forwardInvokeToHost(const faabric::RpcRequest& req,
                                    const std::string& host,
                                    int32_t port)
{
    SPDLOG_INFO("RPC - Forwarding INVOKE {} for app={} msg={} to {}:{}",
                req.requestid(),
                req.targetappid(),
                req.targetmessageid(),
                host,
                port);

    try {
        RpcTransportClient client(host, port, RPC_SYNC_PORT, kRpcTimeoutMs);
        client.asyncSendRequest(req.requestid(), req);
    } catch (const std::exception& e) {
        SPDLOG_ERROR("RPC - Failed to forward INVOKE {} to {}:{}: {}",
                     req.requestid(),
                     host,
                     port,
                     e.what());

        sendErrorResponseToReplyHost(req,
                                     Rpc_StatusCode::INTERNAL,
                                     "Failed to forward to service host");
    }
}

void RpcServer::beginServiceQueueMigration(int32_t appId,
                                           int32_t messageId,
                                           std::chrono::milliseconds ttl)
{
    if (appId == 0 || messageId == 0) {
        throw std::runtime_error(
          "Cannot migrate service queue for zero app/message id");
    }

    if (ttl.count() <= 0) {
        throw std::runtime_error(
          "Cannot migrate service queue with non-positive TTL");
    }

    const ServiceInstanceKey key{ appId, messageId };
    std::size_t pendingSize = 0;

    {
        std::scoped_lock lock(servicesMx);

        auto& migration = serviceMigrations[key];
        migration.destination.reset();
        migration.expiry = std::chrono::steady_clock::now() + ttl;

        auto qIt = serviceQueues.find(key);
        if (qIt != serviceQueues.end()) {
            auto& serviceQueue = qIt->second;

            while (!serviceQueue.empty()) {
                migration.pending.emplace_back(std::move(serviceQueue.front()));
                serviceQueue.pop_front();
            }

            serviceQueues.erase(qIt);
        }

        pendingSize = migration.pending.size();
    }

    SPDLOG_INFO("RPC - Service app={} msg={} entered pending pull with {} "
                "pending invocations",
                appId,
                messageId,
                pendingSize);
}


// -----------------------------------
// shutdown
// -----------------------------------

void RpcServer::requestShutdown(int32_t appId, int32_t messageId)
{
    if (appId == 0 || messageId == 0) {
        throw std::runtime_error(
          "Cannot request shutdown for zero app/message id");
    }

    const ServiceInstanceKey key{ appId, messageId };

    {
        std::scoped_lock lock(servicesMx);
        shutdownRequested.insert(key);
    }

    SPDLOG_INFO("RPC - Shutdown requested for service instance app={} msg={}",
                appId, messageId);
}

bool RpcServer::isShutdownRequested(int32_t appId, int32_t messageId)
{
    const ServiceInstanceKey key{ appId, messageId };
    std::scoped_lock lock(servicesMx);
    return shutdownRequested.contains(key);
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