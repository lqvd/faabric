#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/rpc/rpc.h>
#include <faabric/rpc/RpcContextRegistry.h>
#include <faabric/transport/common.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

using namespace faabric::rpc;

namespace tests {

class RpcContextTestFixture : public RpcBaseTestFixture
{
  public:
    RpcContextTestFixture()
    {
        // Context tests should not require a real server/transport.
        faabric::util::setMockMode(true);
    }

    ~RpcContextTestFixture()
    {
        faabric::util::setMockMode(false);
        faabric::rpc::clearMockRpcMessages();
    }

  protected:
    static std::string makeFaabricUri(
      const std::string& host = faabric::util::getSystemConfig().endpointHost,
      int port = RPC_ASYNC_PORT)
    {
        return "faabric://" + host + ":" + std::to_string(port);
    }

    static uint32_t startUnaryWithPayload(
      std::shared_ptr<faabric::rpc::RpcContext> ctx,
      int32_t channelId,
      const std::string& method = "/demo.echo/Echo",
      const std::string& payload = "hello",
      int32_t timeoutMs = -1)
    {
        return ctx->startUnary(
          channelId,
          method,
          reinterpret_cast<const uint8_t*>(payload.data()),
          static_cast<int32_t>(payload.size()),
          timeoutMs);
    }

    static faabric::RpcResponse makeResponse(uint32_t requestId,
                                             int32_t statusCode =
                                             Rpc_StatusCode::OK,
                                             const std::string& payload = {})
    {
        faabric::RpcResponse resp;
        resp.set_requestid(requestId);
        resp.set_statuscode(statusCode);
        if (!payload.empty()) {
            resp.set_payload(payload);
        }
        return resp;
    }

    static void checkChannelInfo(const faabric::rpc::ChannelInfo& info,
                                 const std::string& expectedUri,
                                 const std::string& expectedHost,
                                 int expectedPort = RPC_ASYNC_PORT)
    {
        REQUIRE(info.targetUri == expectedUri);
        REQUIRE(info.isFaabric);
        REQUIRE(info.host == expectedHost);
        REQUIRE(info.port == expectedPort);
    }

    static faabric::RpcResponse getReadyResponse(
      std::shared_ptr<faabric::rpc::RpcContext> ctx,
      uint32_t requestId)
    {
        REQUIRE(ctx->testResponse(requestId));

        faabric::RpcResponse resp;
        REQUIRE(ctx->getResponse(requestId, resp));
        return resp;
    }
};

TEST_CASE_METHOD(RpcContextTestFixture,
                 "Test creating RPC channel with valid faabric URI",
                 "[rpc]")
{
    const std::string host = faabric::util::getSystemConfig().endpointHost;
    const std::string uri = makeFaabricUri(host);

    int32_t channelId = ctx->createChannel(uri);

    REQUIRE(channelId > 0);

    auto info = ctx->getChannel(channelId);
    checkChannelInfo(info, uri, host);
}

TEST_CASE_METHOD(RpcContextTestFixture,
                 "Test creating RPC channel with non-faabric URI throws",
                 "[rpc]")
{
    REQUIRE_THROWS(ctx->createChannel("http://example.com:1234"));
    REQUIRE_THROWS(ctx->createChannel("grpc://example.com:1234"));
    REQUIRE_THROWS(ctx->createChannel("example.com:1234"));
}

TEST_CASE_METHOD(RpcContextTestFixture,
                 "Test starting unary RPC registers pending request",
                 "[rpc]")
{
    auto& reg = faabric::rpc::getRpcContextRegistry();

    int32_t channelId = ctx->createChannel(makeFaabricUri());
    uint32_t requestId = startUnaryWithPayload(ctx, channelId);

    REQUIRE(requestId > 0);
    REQUIRE(ctx->hasPendingRequest(requestId));
    REQUIRE(!ctx->testResponse(requestId));

    auto actualMsgId = reg.getMsgIdxForRequest(requestId);
    REQUIRE(actualMsgId.has_value());
    REQUIRE(actualMsgId.value() == msgId);
}

TEST_CASE_METHOD(RpcContextTestFixture,
                 "Test response is not ready immediately after unary start",
                 "[rpc]")
{
    int32_t channelId = ctx->createChannel(makeFaabricUri());
    uint32_t requestId = startUnaryWithPayload(ctx, channelId);

    REQUIRE(!ctx->testResponse(requestId));

    faabric::RpcResponse out;
    REQUIRE(!ctx->getResponse(requestId, out));
}

TEST_CASE_METHOD(RpcContextTestFixture,
                 "Test matching RPC response becomes ready",
                 "[rpc]")
{
    int32_t channelId = ctx->createChannel(makeFaabricUri());
    uint32_t requestId = startUnaryWithPayload(ctx, channelId);

    auto resp = makeResponse(requestId, Rpc_StatusCode::OK, "response-data");
    ctx->onResponseReceived(resp);

    REQUIRE(ctx->testResponse(requestId));
}

TEST_CASE_METHOD(RpcContextTestFixture,
                 "Test getting RPC response clears pending operation",
                 "[rpc]")
{
    int32_t channelId = ctx->createChannel(makeFaabricUri());
    uint32_t requestId = startUnaryWithPayload(ctx, channelId);

    ctx->onResponseReceived(
      makeResponse(requestId, Rpc_StatusCode::OK, "response-data"));

    faabric::RpcResponse actual;
    REQUIRE(ctx->getResponse(requestId, actual));

    REQUIRE(actual.requestid() == requestId);
    REQUIRE(actual.statuscode() == Rpc_StatusCode::OK);
    REQUIRE(actual.payload() == "response-data");

    REQUIRE(!ctx->hasPendingRequest(requestId));
    REQUIRE(!ctx->testResponse(requestId));

    faabric::RpcResponse secondRead;
    REQUIRE(!ctx->getResponse(requestId, secondRead));
}

TEST_CASE_METHOD(RpcContextTestFixture,
                 "Test unrelated RPC response does not complete operation",
                 "[rpc]")
{
    int32_t channelId = ctx->createChannel(makeFaabricUri());
    uint32_t requestId = startUnaryWithPayload(ctx, channelId);

    uint32_t otherRequestId = requestId + 1000;
    ctx->onResponseReceived(
      makeResponse(otherRequestId, Rpc_StatusCode::OK, "wrong-response"));

    REQUIRE(!ctx->testResponse(requestId));

    faabric::RpcResponse actual;
    REQUIRE(!ctx->getResponse(requestId, actual));
}

TEST_CASE_METHOD(RpcContextTestFixture,
                 "Test RPC response timeout synthesizes deadline exceeded",
                 "[rpc]")
{
    int32_t channelId = ctx->createChannel(makeFaabricUri());

    uint32_t requestId = startUnaryWithPayload(
      ctx,
      channelId,
      "/demo.echo/Echo",
      "payload",
      1);

    REQUIRE(!ctx->testResponse(requestId));

    std::this_thread::sleep_for(std::chrono::milliseconds(5));

    REQUIRE(ctx->testResponse(requestId));

    faabric::RpcResponse actual;
    REQUIRE(ctx->getResponse(requestId, actual));

    REQUIRE(actual.requestid() == requestId);
    REQUIRE(actual.statuscode() == Rpc_StatusCode::DEADLINE_EXCEEDED);
}

TEST_CASE_METHOD(RpcContextTestFixture,
                 "Test closing RPC channel removes it",
                 "[rpc]")
{
    int32_t channelId = ctx->createChannel(makeFaabricUri());

    REQUIRE_NOTHROW(ctx->getChannel(channelId));

    ctx->closeChannel(channelId);

    REQUIRE_THROWS(ctx->getChannel(channelId));
}

TEST_CASE_METHOD(RpcContextTestFixture,
                 "Test serialising and deserialising RPC migration state",
                 "[rpc]")
{
    const std::string host = faabric::util::getSystemConfig().endpointHost;
    const std::string uri = makeFaabricUri(host);

    int32_t channelId = ctx->createChannel(uri);
    uint32_t requestId = startUnaryWithPayload(
      ctx,
      channelId,
      "/demo.echo/Echo",
      "migration-payload");

    auto migrationState = ctx->serializeMigrationState();

    int32_t newMsgId = faabric::util::generateGid();
    auto newCtx = makeContext(newMsgId);
    newCtx->deserializeMigrationState(migrationState);

    auto restoredChannel = newCtx->getChannel(channelId);
    checkChannelInfo(restoredChannel, uri, host);

    REQUIRE(newCtx->hasPendingRequest(requestId));
    REQUIRE(!newCtx->testResponse(requestId));
}

TEST_CASE_METHOD(RpcContextTestFixture,
                 "Test deserialising RPC migration state with cached response",
                 "[rpc]")
{
    int32_t channelId = ctx->createChannel(makeFaabricUri());
    uint32_t requestId = startUnaryWithPayload(
      ctx,
      channelId,
      "/demo.echo/Echo",
      "migration-payload");

    ctx->onResponseReceived(
      makeResponse(requestId, Rpc_StatusCode::OK, "cached-response"));

    auto migrationState = ctx->serializeMigrationState();

    int32_t newMsgId = faabric::util::generateGid();
    auto newCtx = makeContext(newMsgId);
    newCtx->deserializeMigrationState(migrationState);

    REQUIRE(newCtx->testResponse(requestId));

    faabric::RpcResponse actual;
    REQUIRE(newCtx->getResponse(requestId, actual));

    REQUIRE(actual.requestid() == requestId);
    REQUIRE(actual.statuscode() == Rpc_StatusCode::OK);
    REQUIRE(actual.payload() == "cached-response");
}

TEST_CASE_METHOD(RpcContextTestFixture,
                 "Test RPC setupForwarding registers forwarding address",
                 "[rpc]")
{
    auto& reg = faabric::rpc::getRpcContextRegistry();

    int32_t channelId = ctx->createChannel(makeFaabricUri());
    uint32_t requestA = startUnaryWithPayload(
      ctx,
      channelId,
      "/demo.echo/Echo",
      "payload-a");
    uint32_t requestB = startUnaryWithPayload(
      ctx,
      channelId,
      "/demo.echo/Echo",
      "payload-b");

    const std::string newHost = "new-rpc-host";

    ctx->setupForwarding(newHost, std::chrono::milliseconds(30000));

    auto targetA = reg.getResponseTarget(requestA);
    auto targetB = reg.getResponseTarget(requestB);

    checkRemote(targetA, newHost);
    checkRemote(targetB, newHost);

    REQUIRE(ctx->hasPendingRequest(requestA));
    REQUIRE(ctx->hasPendingRequest(requestB));
}

TEST_CASE_METHOD(RpcContextTestFixture,
                 "Test clearing RPC context removes channels and pending requests",
                 "[rpc]")
{
    auto& reg = faabric::rpc::getRpcContextRegistry();

    int32_t channelId = ctx->createChannel(makeFaabricUri());
    uint32_t requestId = startUnaryWithPayload(ctx, channelId);

    REQUIRE(ctx->hasPendingRequest(requestId));
    REQUIRE(reg.getMsgIdxForRequest(requestId).has_value());

    ctx->clear();

    REQUIRE(!ctx->hasPendingRequest(requestId));
    REQUIRE(!reg.getMsgIdxForRequest(requestId).has_value());
    REQUIRE_THROWS(ctx->getChannel(channelId));
}

}