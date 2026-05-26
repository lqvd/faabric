#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/rpc/RpcContext.h>
#include <faabric/rpc/RpcContextRegistry.h>
#include <faabric/rpc/RpcServer.h>
#include <faabric/rpc/rpc.h>
#include <faabric/transport/common.h>

#include <algorithm>
#include <chrono>
#include <thread>

using namespace faabric::rpc;

namespace tests {

class RpcServerDeliveryFixture : public RpcServerFixture
{
  public:
    RpcServerDeliveryFixture()
    {
        faabric::util::setMockMode(true);
    }

    ~RpcServerDeliveryFixture()
    {
        faabric::rpc::clearMockRpcMessages();
        faabric::util::setMockMode(false);
    }

  protected:
    faabric::rpc::RpcServer server;

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

    // Leave the local RpcContext op alive, but remove the global routing entry.
    // If deliverResponse bypasses the registry, this test will fail.
    getRpcContextRegistry().clearRequest(requestId);

    injectResponse(requestId, "should-not-arrive", Rpc_StatusCode::OK);

    REQUIRE_FALSE(waitUntilReady(requestId, 100));

    // The op should still exist locally, but it should not be ready.
    faabric::RpcResponse actual;
    REQUIRE_FALSE(ctx->getResponse(requestId, actual));

    ctx->eraseRequest(requestId);
}

TEST_CASE_METHOD(RpcServerDeliveryFixture,
                 "Test RPC server forwards response for migrated context",
                 "[rpc]")
{
    uint32_t requestId = startPendingUnary();

    auto& reg = getRpcContextRegistry();

    // Simulate migration away from this host:
    // - request still maps to msgId
    // - local context has been removed
    // - forwarding address exists
    reg.removeContext(appId, msgId);
    reg.setForwardingAddress(
      appId,
      msgId,
      LOCALHOST,
      { requestId },
      std::chrono::milliseconds(30000));

    injectResponse(requestId, "forwarded-payload", Rpc_StatusCode::OK);

    auto sent = faabric::rpc::getMockRpcResponses();

    auto it = std::find_if(
      sent.begin(),
      sent.end(),
      [requestId](const faabric::RpcResponse& r) {
          return r.requestid() == requestId;
      });

    REQUIRE(it != sent.end());
    REQUIRE(it->requestid() == requestId);
    REQUIRE(it->statuscode() == Rpc_StatusCode::OK);
    REQUIRE(it->payload() == "forwarded-payload");
}

TEST_CASE_METHOD(RpcServerDeliveryFixture,
                 "Test RPC server clears request after forwarding response",
                 "[rpc]")
{
    uint32_t requestId = startPendingUnary();

    auto& reg = getRpcContextRegistry();

    reg.removeContext(appId, msgId);
    reg.setForwardingAddress(
      appId,
      msgId,
      LOCALHOST,
      { requestId },
      std::chrono::milliseconds(30000));

    injectResponse(requestId, "forwarded-payload", Rpc_StatusCode::OK);

    REQUIRE_FALSE(reg.getAppMsgIdForRequest(requestId).has_value());
}

TEST_CASE_METHOD(RpcServerDeliveryFixture,
                 "Test RPC server does not forward after forwarding TTL expires",
                 "[rpc]")
{
    uint32_t requestId = startPendingUnary();

    auto& reg = getRpcContextRegistry();

    reg.removeContext(appId, msgId);
    reg.setForwardingAddress(
      appId,
      msgId,
      LOCALHOST,
      { requestId },
      std::chrono::milliseconds(1));

    std::this_thread::sleep_for(std::chrono::milliseconds(5));

    injectResponse(requestId, "expired-forward", Rpc_StatusCode::OK);

    auto sent = faabric::rpc::getMockRpcResponses();

    auto it = std::find_if(
      sent.begin(),
      sent.end(),
      [requestId](const faabric::RpcResponse& r) {
          return r.requestid() == requestId;
      });

    REQUIRE(it == sent.end());

    // If delivery was undeliverable, the request should not have completed
    // locally either.
    REQUIRE_FALSE(waitUntilReady(requestId, 100));

    ctx->eraseRequest(requestId);
}

TEST_CASE_METHOD(RpcServerDeliveryFixture,
                 "Test RPC server caches response when FETCH has not yet arrived",
                 "[rpc]")
{
    uint32_t requestId = startPendingUnary();

    auto& reg = getRpcContextRegistry();

    // Simulate migration: no local context, forwarding address set
    reg.removeContext(appId, msgId);
    reg.setForwardingAddress(
      appId,
      msgId,
      LOCALHOST,
      { requestId },
      std::chrono::milliseconds(30000));

    // Inject response before any FETCH arrives
    injectResponse(requestId, "cached-payload", Rpc_StatusCode::OK);

    // Response should have been cached, not yet sent
    auto cached = reg.consumeForwardedResponse(requestId);
    REQUIRE(cached.has_value());
    REQUIRE(cached->requestid() == requestId);
    REQUIRE(cached->payload() == "cached-payload");

    // Nothing should have been sent over the wire yet
    auto sent = faabric::rpc::getMockRpcResponses();
    REQUIRE(sent.empty());

    ctx->eraseRequest(requestId);
}

TEST_CASE_METHOD(RpcServerDeliveryFixture,
                 "Test RPC server sends cached response when FETCH arrives after response",
                 "[rpc]")
{
    uint32_t requestId = startPendingUnary();

    auto& reg = getRpcContextRegistry();

    reg.removeContext(appId, msgId);
    reg.setForwardingAddress(
      appId,
      msgId,
      LOCALHOST,
      { requestId },
      std::chrono::milliseconds(30000));

    // Response arrives first — gets cached internally
    injectResponse(requestId, "fetch-after-response", Rpc_StatusCode::OK);

    // Nothing sent yet
    REQUIRE(faabric::rpc::getMockRpcResponses().empty());

    // FETCH arrives after — should consume the cache and send immediately
    faabric::RpcFetchRequest fetch;
    fetch.set_requestid(requestId);
    fetch.set_replyhost(LOCALHOST);
    fetch.set_replyport(RPC_ASYNC_PORT);
    recvFetch(fetch);

    auto sent = faabric::rpc::getMockRpcResponses();
    auto it = std::find_if(sent.begin(), sent.end(),
        [requestId](const faabric::RpcResponse& r) {
            return r.requestid() == requestId;
        });
    REQUIRE(it != sent.end());
    REQUIRE(it->payload() == "fetch-after-response");

    // Cache should be empty — consumed by recvFetch
    REQUIRE_FALSE(reg.consumeForwardedResponse(requestId).has_value());

    ctx->eraseRequest(requestId);
}

TEST_CASE_METHOD(RpcServerDeliveryFixture,
                 "Test RPC server uses pending FETCH host when response arrives after FETCH",
                 "[rpc]")
{
    uint32_t requestId = startPendingUnary();

    auto& reg = getRpcContextRegistry();

    reg.removeContext(appId, msgId);
    reg.setForwardingAddress(
      appId,
      msgId,
      "wrong-host",  // forwarding table has stale host
      { requestId },
      std::chrono::milliseconds(30000));

    // FETCH arrives first, registering the real reply address
    reg.registerPendingFetch(requestId, LOCALHOST, RPC_ASYNC_PORT);

    // Response arrives — should use the FETCH host, not the forwarding table
    injectResponse(requestId, "fetch-before-response", Rpc_StatusCode::OK);

    auto sent = faabric::rpc::getMockRpcResponses();
    auto it = std::find_if(sent.begin(), sent.end(),
        [requestId](const faabric::RpcResponse& r) {
            return r.requestid() == requestId;
        });
    REQUIRE(it != sent.end());
    REQUIRE(it->payload() == "fetch-before-response");

    // Pending fetch should have been consumed
    REQUIRE_FALSE(reg.consumePendingFetch(requestId).has_value());

    ctx->eraseRequest(requestId);
}

}