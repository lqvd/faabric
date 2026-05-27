#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/rpc/RpcContext.h>
#include <faabric/rpc/RpcContextRegistry.h>
#include <faabric/rpc/RpcServer.h>
#include <faabric/rpc/rpc.h>
#include <faabric/transport/common.h>

#include <algorithm>
#include <chrono>
#include <span>
#include <string>
#include <thread>

using namespace faabric::rpc;

namespace tests {

class RpcServerDeliveryFixture : public RpcContextBaseFixture
{
  protected:
    faabric::rpc::RpcServer server;

    int32_t localChannel()
    {
        return ctx->createChannel(makeFaabricUri());
    }

    uint32_t startPendingUnary(const std::string& method = "/demo.Echo/Ping",
                               const std::string& payload = "request",
                               int32_t timeoutMs = -1)
    {
        int32_t ch = localChannel();

        return ctx->startUnary(
          ch,
          method,
          reinterpret_cast<const uint8_t*>(payload.data()),
          static_cast<int32_t>(payload.size()),
          timeoutMs);
    }

    void simulateMigrationAway(uint32_t requestId)
    {
        auto& reg = getRpcContextRegistry();

        REQUIRE(reg.getAppMsgIdForRequest(requestId).has_value());

        // The request remains known to this host, but the context object is no
        // longer local. This is the fetch-first migrated state.
        reg.removeContext(appId, msgId);

        REQUIRE_FALSE(reg.getContextForRequest(requestId));
        REQUIRE(reg.getAppMsgIdForRequest(requestId).has_value());
    }

    faabric::RpcResponse makeRpcResponse(uint32_t requestId,
                                         const std::string& payload = {},
                                         int32_t status = Rpc_StatusCode::OK)
    {
        faabric::RpcResponse resp;
        resp.set_requestid(requestId);
        resp.set_statuscode(status);

        if (!payload.empty()) {
            resp.set_payload(payload);
        }

        return resp;
    }

    faabric::RpcFetchRequest makeFetch(uint32_t requestId,
                                       const std::string& host = LOCALHOST,
                                       int port = RPC_ASYNC_PORT)
    {
        faabric::RpcFetchRequest fetch;
        fetch.set_requestid(requestId);
        fetch.set_replyhost(host);
        fetch.set_replyport(port);
        return fetch;
    }

    void injectResponse(uint32_t requestId,
                        const std::string& payload = {},
                        int32_t status = Rpc_StatusCode::OK)
    {
        server.deliverResponse(makeRpcResponse(requestId, payload, status));
    }

    void recvFetch(faabric::RpcFetchRequest& fetch)
    {
        std::string bytes;
        REQUIRE(fetch.SerializeToString(&bytes));

        server.recvFetch(std::span<const uint8_t>(
          reinterpret_cast<const uint8_t*>(bytes.data()),
          bytes.size()));
    }

    void recvResponse(faabric::RpcResponse& resp)
    {
        std::string bytes;
        REQUIRE(resp.SerializeToString(&bytes));

        server.recvResponse(std::span<const uint8_t>(
          reinterpret_cast<const uint8_t*>(bytes.data()),
          bytes.size()));
    }

    void recvInvoke(faabric::RpcRequest& req)
    {
        std::string bytes;
        REQUIRE(req.SerializeToString(&bytes));

        server.recvInvoke(std::span<const uint8_t>(
          reinterpret_cast<const uint8_t*>(bytes.data()),
          bytes.size()));
    }

    bool waitUntilReady(uint32_t requestId, int timeoutMs = 1000)
    {
        const auto deadline =
          std::chrono::steady_clock::now() +
          std::chrono::milliseconds(timeoutMs);

        while (!ctx->testResponse(requestId)) {
            if (std::chrono::steady_clock::now() >= deadline) {
                return false;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        return true;
    }

    static std::optional<faabric::RpcResponse> findSentResponse(
      uint32_t requestId)
    {
        auto sent = faabric::rpc::getMockRpcResponses();

        auto it = std::find_if(
          sent.begin(),
          sent.end(),
          [requestId](const faabric::RpcResponse& r) {
              return r.requestid() == requestId;
          });

        if (it == sent.end()) {
            return std::nullopt;
        }

        return *it;
    }

    static void requireNoSentResponse(uint32_t requestId)
    {
        REQUIRE_FALSE(findSentResponse(requestId).has_value());
    }

    static faabric::RpcResponse requireSentResponse(uint32_t requestId)
    {
        auto resp = findSentResponse(requestId);
        REQUIRE(resp.has_value());
        return resp.value();
    }
};

TEST_CASE_METHOD(RpcServerDeliveryFixture,
                 "Test RPC server delivers response to registered local context",
                 "[rpc]")
{
    uint32_t requestId = startPendingUnary();

    injectResponse(requestId, "response-payload", Rpc_StatusCode::OK);

    auto resp = pollResult(requestId);

    REQUIRE(resp.requestid() == requestId);
    REQUIRE(resp.statuscode() == Rpc_StatusCode::OK);
    REQUIRE(resp.payload() == "response-payload");
}

TEST_CASE_METHOD(RpcServerDeliveryFixture,
                 "Test RPC server delivers error response to registered local context",
                 "[rpc]")
{
    uint32_t requestId = startPendingUnary();

    injectResponse(requestId, "", Rpc_StatusCode::INTERNAL);

    auto resp = pollResult(requestId);

    REQUIRE(resp.requestid() == requestId);
    REQUIRE(resp.statuscode() == Rpc_StatusCode::INTERNAL);
    REQUIRE(resp.payload().empty());
}

TEST_CASE_METHOD(RpcServerDeliveryFixture,
                 "Test RPC server drops response with no registry entry",
                 "[rpc]")
{
    uint32_t requestId = startPendingUnary();

    getRpcContextRegistry().clearRequest(requestId);

    injectResponse(requestId, "should-not-arrive", Rpc_StatusCode::OK);

    REQUIRE_FALSE(waitUntilReady(requestId, 100));
    requireNoSentResponse(requestId);

    faabric::RpcResponse actual;
    REQUIRE_FALSE(ctx->getResponse(requestId, actual));

    ctx->eraseRequest(requestId);
}

TEST_CASE_METHOD(RpcServerDeliveryFixture,
                 "Test RPC server caches migrated response before FETCH",
                 "[rpc]")
{
    uint32_t requestId = startPendingUnary();

    auto& reg = getRpcContextRegistry();
    simulateMigrationAway(requestId);

    injectResponse(requestId, "cached-payload", Rpc_StatusCode::OK);

    auto cached = reg.consumeCachedResponse(requestId);
    REQUIRE(cached.has_value());
    REQUIRE(cached->requestid() == requestId);
    REQUIRE(cached->statuscode() == Rpc_StatusCode::OK);
    REQUIRE(cached->payload() == "cached-payload");

    requireNoSentResponse(requestId);

    ctx->eraseRequest(requestId);
}

TEST_CASE_METHOD(RpcServerDeliveryFixture,
                 "Test RPC server sends cached response when FETCH arrives after response",
                 "[rpc]")
{
    uint32_t requestId = startPendingUnary();

    auto& reg = getRpcContextRegistry();
    simulateMigrationAway(requestId);

    injectResponse(requestId, "fetch-after-response", Rpc_StatusCode::OK);

    REQUIRE(reg.consumeCachedResponse(requestId).has_value());

    // Put it back because the assertion above consumes it.
    reg.cacheResponse(
      requestId,
      makeRpcResponse(requestId, "fetch-after-response", Rpc_StatusCode::OK));

    requireNoSentResponse(requestId);

    auto fetch = makeFetch(requestId);
    recvFetch(fetch);

    auto sent = requireSentResponse(requestId);
    REQUIRE(sent.requestid() == requestId);
    REQUIRE(sent.statuscode() == Rpc_StatusCode::OK);
    REQUIRE(sent.payload() == "fetch-after-response");

    REQUIRE_FALSE(reg.consumeCachedResponse(requestId).has_value());
    REQUIRE_FALSE(reg.getAppMsgIdForRequest(requestId).has_value());
}

TEST_CASE_METHOD(RpcServerDeliveryFixture,
                 "Test RPC server registers FETCH when response has not arrived",
                 "[rpc]")
{
    uint32_t requestId = startPendingUnary();

    auto& reg = getRpcContextRegistry();
    simulateMigrationAway(requestId);

    auto fetch = makeFetch(requestId);
    recvFetch(fetch);

    requireNoSentResponse(requestId);
    REQUIRE_FALSE(reg.consumeCachedResponse(requestId).has_value());

    auto pendingFetch = reg.consumePendingFetch(requestId);
    REQUIRE(pendingFetch.has_value());
    REQUIRE(pendingFetch->host == LOCALHOST);
    REQUIRE(pendingFetch->port == RPC_ASYNC_PORT);

    ctx->eraseRequest(requestId);
}

TEST_CASE_METHOD(RpcServerDeliveryFixture,
                 "Test RPC server sends response to pending FETCH address",
                 "[rpc]")
{
    uint32_t requestId = startPendingUnary();

    auto& reg = getRpcContextRegistry();
    simulateMigrationAway(requestId);

    auto fetch = makeFetch(requestId);
    recvFetch(fetch);

    injectResponse(requestId, "fetch-before-response", Rpc_StatusCode::OK);

    auto sent = requireSentResponse(requestId);
    REQUIRE(sent.requestid() == requestId);
    REQUIRE(sent.statuscode() == Rpc_StatusCode::OK);
    REQUIRE(sent.payload() == "fetch-before-response");

    REQUIRE_FALSE(reg.consumePendingFetch(requestId).has_value());
    REQUIRE_FALSE(reg.getAppMsgIdForRequest(requestId).has_value());
}

TEST_CASE_METHOD(RpcServerDeliveryFixture,
                 "Test RPC server clears request after cached response is fetched",
                 "[rpc]")
{
    uint32_t requestId = startPendingUnary();

    auto& reg = getRpcContextRegistry();
    simulateMigrationAway(requestId);

    injectResponse(requestId, "clear-after-fetch", Rpc_StatusCode::OK);

    auto fetch = makeFetch(requestId);
    recvFetch(fetch);

    REQUIRE_FALSE(reg.getAppMsgIdForRequest(requestId).has_value());
    REQUIRE_FALSE(reg.consumeCachedResponse(requestId).has_value());
    REQUIRE_FALSE(reg.consumePendingFetch(requestId).has_value());
}

TEST_CASE_METHOD(RpcServerDeliveryFixture,
                 "Test RPC server clears request after response is sent to pending FETCH",
                 "[rpc]")
{
    uint32_t requestId = startPendingUnary();

    auto& reg = getRpcContextRegistry();
    simulateMigrationAway(requestId);

    auto fetch = makeFetch(requestId);
    recvFetch(fetch);

    injectResponse(requestId, "clear-after-response", Rpc_StatusCode::OK);

    REQUIRE_FALSE(reg.getAppMsgIdForRequest(requestId).has_value());
    REQUIRE_FALSE(reg.consumeCachedResponse(requestId).has_value());
    REQUIRE_FALSE(reg.consumePendingFetch(requestId).has_value());
}

TEST_CASE_METHOD(RpcServerDeliveryFixture,
                 "Test RPC server ignores FETCH for unknown request",
                 "[rpc]")
{
    uint32_t requestId = nextRequestId();

    auto fetch = makeFetch(requestId);
    recvFetch(fetch);

    requireNoSentResponse(requestId);

    auto& reg = getRpcContextRegistry();
    REQUIRE_FALSE(reg.consumeCachedResponse(requestId).has_value());
    REQUIRE_FALSE(reg.consumePendingFetch(requestId).has_value());
}

TEST_CASE_METHOD(RpcServerDeliveryFixture,
                 "Test RPC server drops response for expired migrated request",
                 "[rpc]")
{
    uint32_t requestId = startPendingUnary();

    auto& reg = getRpcContextRegistry();
    simulateMigrationAway(requestId);

    reg.refreshRequestTtl(requestId, std::chrono::milliseconds(1));
    std::this_thread::sleep_for(std::chrono::milliseconds(5));

    injectResponse(requestId, "expired-response", Rpc_StatusCode::OK);

    requireNoSentResponse(requestId);
    REQUIRE_FALSE(reg.consumeCachedResponse(requestId).has_value());
    REQUIRE_FALSE(reg.consumePendingFetch(requestId).has_value());
    REQUIRE_FALSE(reg.getAppMsgIdForRequest(requestId).has_value());

    ctx->eraseRequest(requestId);
}

TEST_CASE_METHOD(RpcServerDeliveryFixture,
                 "Test RPC server ignores FETCH for expired migrated request",
                 "[rpc]")
{
    uint32_t requestId = startPendingUnary();

    auto& reg = getRpcContextRegistry();
    simulateMigrationAway(requestId);

    reg.refreshRequestTtl(requestId, std::chrono::milliseconds(1));
    std::this_thread::sleep_for(std::chrono::milliseconds(5));

    auto fetch = makeFetch(requestId);
    recvFetch(fetch);

    requireNoSentResponse(requestId);
    REQUIRE_FALSE(reg.consumeCachedResponse(requestId).has_value());
    REQUIRE_FALSE(reg.consumePendingFetch(requestId).has_value());
    REQUIRE_FALSE(reg.getAppMsgIdForRequest(requestId).has_value());

    ctx->eraseRequest(requestId);
}

} // namespace tests