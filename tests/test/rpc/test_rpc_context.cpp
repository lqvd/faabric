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

TEST_CASE_METHOD(RpcContextBaseFixture,
                 "Test creating RPC channel with valid faabric URI",
                 "[rpc]")
{
    const std::string uri = makeFaabricUri();

    int32_t channelId = ctx->createChannel(uri);

    REQUIRE(channelId > 0);

    auto info = ctx->getChannel(channelId);
    checkChannelInfo(info, uri);
}

TEST_CASE_METHOD(RpcContextBaseFixture,
                 "Test creating RPC channel with non-faabric URI throws",
                 "[rpc]")
{
    REQUIRE_THROWS(ctx->createChannel("http://example.com:1234"));
    REQUIRE_THROWS(ctx->createChannel("grpc://example.com:1234"));
    REQUIRE_THROWS(ctx->createChannel("example.com:1234"));
}

TEST_CASE_METHOD(RpcContextBaseFixture,
                 "Test starting unary RPC registers pending request",
                 "[rpc]")
{
    auto& reg = faabric::rpc::getRpcContextRegistry();

    int32_t channelId = ctx->createChannel(makeFaabricUri());
    uint32_t requestId = startUnaryWithPayload(ctx, channelId);

    REQUIRE(requestId > 0);
    REQUIRE(ctx->hasPendingRequest(requestId));
    REQUIRE(!ctx->testResponse(requestId));

    auto actualId = reg.getAppMsgIdForRequest(requestId);
    REQUIRE(actualId.has_value());
    REQUIRE(actualId.value().msgId == msgId);
    REQUIRE(actualId.value().appId == appId);
}

TEST_CASE_METHOD(RpcContextBaseFixture,
                 "Test response is not ready immediately after unary start",
                 "[rpc]")
{
    int32_t channelId = ctx->createChannel(makeFaabricUri());
    uint32_t requestId = startUnaryWithPayload(ctx, channelId);

    REQUIRE(!ctx->testResponse(requestId));

    faabric::RpcResponse out;
    REQUIRE(!ctx->getResponse(requestId, out));
}

TEST_CASE_METHOD(RpcContextBaseFixture,
                 "Test matching RPC response becomes ready",
                 "[rpc]")
{
    int32_t channelId = ctx->createChannel(makeFaabricUri());
    uint32_t requestId = startUnaryWithPayload(ctx, channelId);

    auto resp = makeResponse(requestId, Rpc_StatusCode::OK, "response-data");
    ctx->onResponseReceived(resp);

    REQUIRE(ctx->testResponse(requestId));
}

TEST_CASE_METHOD(RpcContextBaseFixture,
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

TEST_CASE_METHOD(RpcContextBaseFixture,
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

TEST_CASE_METHOD(RpcContextBaseFixture,
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

TEST_CASE_METHOD(RpcContextBaseFixture,
                 "Test closing RPC channel removes it",
                 "[rpc]")
{
    int32_t channelId = ctx->createChannel(makeFaabricUri());

    REQUIRE_NOTHROW(ctx->getChannel(channelId));

    ctx->closeChannel(channelId);

    REQUIRE_THROWS(ctx->getChannel(channelId));
}

TEST_CASE_METHOD(RpcContextBaseFixture,
                 "Test serialising and deserialising RPC migration state",
                 "[rpc]")
{
    const std::string uri = makeFaabricUri();

    int32_t channelId = ctx->createChannel(uri);
    uint32_t requestId = startUnaryWithPayload(
      ctx,
      channelId,
      "/demo.echo/Echo",
      "migration-payload");

    auto migrationState = ctx->serializeMigrationState();

    int32_t newMsgId = faabric::util::generateGid();
    int32_t newAppId = faabric::util::generateGid();
    auto newCtx = makeMockContext(newAppId, newMsgId);
    newCtx->deserializeMigrationState(migrationState);

    auto restoredChannel = newCtx->getChannel(channelId);
    checkChannelInfo(restoredChannel, uri);

    REQUIRE(newCtx->hasPendingRequest(requestId));
    REQUIRE(!newCtx->testResponse(requestId));
}

TEST_CASE_METHOD(RpcContextBaseFixture,
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
    int32_t newAppId = faabric::util::generateGid();
    auto newCtx = makeMockContext(newAppId, newMsgId);
    newCtx->deserializeMigrationState(migrationState);

    REQUIRE(newCtx->testResponse(requestId));

    faabric::RpcResponse actual;
    REQUIRE(newCtx->getResponse(requestId, actual));

    REQUIRE(actual.requestid() == requestId);
    REQUIRE(actual.statuscode() == Rpc_StatusCode::OK);
    REQUIRE(actual.payload() == "cached-response");
}

}