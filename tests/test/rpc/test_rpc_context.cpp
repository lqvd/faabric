#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/rpc/RpcContext.h>
#include <faabric/transport/MessageEndpoint.h>

using namespace faabric::rpc;
using namespace faabric::scheduler;

namespace tests {

TEST_CASE_METHOD(RpcTestFixture, "Test RPC context and loopback channel", "[rpc]")
{
    int port = nextPort();
    std::string targetUri = makeFaabricUri(port);

    faabric::transport::SyncRecvMessageEndpoint server(port);

    RpcCallResult result;
    auto clientThread = startClientCall(
      targetUri, "/pkg.TestSvc/TestMethod", "test payload", result);

    faabric::transport::Message reqMsg = server.recv();
    faabric::RpcRequest req;
    REQUIRE(req.ParseFromArray(reqMsg.data().data(), reqMsg.data().size()));
    REQUIRE(req.method() == "/pkg.TestSvc/TestMethod");
    REQUIRE(req.payload() == "test payload");

    sendProtoResponse(server, 0, "test response");

    if (clientThread.joinable()) {
        clientThread.join();
    }

    REQUIRE(result.error.empty());
    REQUIRE(result.status == 0);
    REQUIRE(result.response == "test response");
}

TEST_CASE_METHOD(RpcTestFixture, "Test RPC returns remote non-zero status", "[rpc]")
{
    int port = nextPort();
    std::string targetUri = makeFaabricUri(port);

    faabric::transport::SyncRecvMessageEndpoint server(port);

    RpcCallResult result;
    auto clientThread = startClientCall(
      targetUri, "/pkg.TestSvc/TestMethod", "test payload", result);

    faabric::transport::Message reqMsg = server.recv();
    faabric::RpcRequest req;
    REQUIRE(req.ParseFromArray(reqMsg.data().data(), reqMsg.data().size()));

    sendProtoResponse(server, 7, "ignored");

    if (clientThread.joinable()) {
        clientThread.join();
    }

    REQUIRE(result.error.empty());
    REQUIRE(result.status == 7);
    REQUIRE(result.response.empty());
}

TEST_CASE_METHOD(RpcTestFixture, "Test RPC malformed response returns internal", "[rpc]")
{
    int port = nextPort();
    std::string targetUri = makeFaabricUri(port);

    faabric::transport::SyncRecvMessageEndpoint server(port);

    RpcCallResult result;
    auto clientThread = startClientCall(
      targetUri, "/pkg.TestSvc/TestMethod", "test payload", result);

    faabric::transport::Message reqMsg = server.recv();
    faabric::RpcRequest req;
    REQUIRE(req.ParseFromArray(reqMsg.data().data(), reqMsg.data().size()));

    sendRawResponse(server, "not-a-protobuf-response");

    if (clientThread.joinable()) {
        clientThread.join();
    }

    REQUIRE(result.error.empty());
    REQUIRE(result.status == 13);
    REQUIRE(result.response.empty());
}

TEST_CASE_METHOD(RpcTestFixture, "Test RPC context serialize and deserialize", "[rpc]")
{
    int portA = nextPort();
    int portB = nextPort();

    std::string uriA = makeFaabricUri(portA);
    std::string uriB = makeFaabricUri(portB);

    int32_t idA = ctx.createChannel(uriA);
    int32_t idB = ctx.createChannel(uriB);

    auto serialized = ctx.serializeChannels();
    REQUIRE(serialized.size() == 2);

    ctx.clear();
    ctx.deserializeChannels(serialized);

    auto chA = ctx.getChannel(idA);
    auto chB = ctx.getChannel(idB);

    REQUIRE(chA->getTargetUri() == uriA);
    REQUIRE(chB->getTargetUri() == uriB);

    // Ensure next ID advanced after restore
    int32_t idC = ctx.createChannel(makeFaabricUri(nextPort()));
    REQUIRE(idC > std::max(idA, idB));
}

TEST_CASE_METHOD(RpcTestFixture, "Test RPC context getChannel throws for unknown id", "[rpc]")
{
    REQUIRE_THROWS_AS(ctx.getChannel(9999), std::runtime_error);
}

TEST_CASE_METHOD(RpcTestFixture, "Test RPC context closeChannel no-op for unknown id", "[rpc]")
{
    // Should not throw
    REQUIRE_NOTHROW(ctx.closeChannel(9999));

    // Context should still be usable after a no-op close
    int32_t id = ctx.createChannel(makeFaabricUri(nextPort()));
    REQUIRE(id == 1);
}

TEST_CASE_METHOD(RpcTestFixture, "Test RPC context clear resets channel ids", "[rpc]")
{
    int32_t idA = ctx.createChannel(makeFaabricUri(nextPort()));
    int32_t idB = ctx.createChannel(makeFaabricUri(nextPort()));

    REQUIRE(idA == 1);
    REQUIRE(idB == 2);

    ctx.clear();

    int32_t idC = ctx.createChannel(makeFaabricUri(nextPort()));
    REQUIRE(idC == 1);
}

TEST_CASE_METHOD(RpcTestFixture, "Test RPC context closes are reflected in serialize", "[rpc]")
{
    int32_t idA = ctx.createChannel(makeFaabricUri(nextPort()));
    int32_t idB = ctx.createChannel(makeFaabricUri(nextPort()));

    ctx.closeChannel(idA);

    auto serialized = ctx.serializeChannels();
    REQUIRE(serialized.size() == 1);
    REQUIRE(serialized.at(0).first == idB);
}

TEST_CASE_METHOD(RpcTestFixture, "Test RPC context deserialize mixed URI schemes", "[rpc]")
{
    int32_t idA = 42;
    int32_t idB = 84;

    std::string uriFaabric = makeFaabricUri(nextPort());
    std::string uriExternal = "https://example.org:443";

    std::vector<std::pair<int32_t, std::string>> data = {
        { idA, uriFaabric },
        { idB, uriExternal },
    };

    ctx.deserializeChannels(data);

    auto chA = ctx.getChannel(idA);
    auto chB = ctx.getChannel(idB);

    REQUIRE(chA->getTargetUri() == uriFaabric);
    REQUIRE(chB->getTargetUri() == uriExternal);

    // Next ID should advance past max restored ID
    int32_t idC = ctx.createChannel(makeFaabricUri(nextPort()));
    REQUIRE(idC > std::max(idA, idB));
}

TEST_CASE_METHOD(RpcTestFixture, "Test RPC context parse default faabric port URI", "[rpc]")
{
    // No explicit port in URI
    int32_t id = ctx.createChannel("faabric://127.0.0.1");
    auto ch = ctx.getChannel(id);

    // Canonical URI should include default port once channel is constructed
    REQUIRE(ch->getTargetUri() == "faabric://127.0.0.1:8015");
}

TEST_CASE_METHOD(RpcTestFixture, "Test RPC context rejects invalid faabric port", "[rpc]")
{
    REQUIRE_THROWS_AS(ctx.createChannel("faabric://127.0.0.1:notaport"),
                      std::invalid_argument);
}


TEST_CASE_METHOD(RpcTestFixture, "RPC quiesce blocks new calls and resumes after endQuiesce", "[rpc][migration]") {
    std::atomic<bool> waiterFinished = false;
        // Simulate one in-flight RPC
    REQUIRE(ctx.tryEnterCall());

    // Start quiescing; new admissions should fail
    ctx.beginQuiesce();
    REQUIRE_FALSE(ctx.tryEnterCall());

    std::jthread waiter([&] {
        ctx.awaitQuiesced(1000);
        waiterFinished.store(true);
    });

    // Waiter must still be blocked because one call is still in flight
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    REQUIRE_FALSE(waiterFinished.load());

    // Drain the in-flight call
    ctx.exitCall();

    if (waiter.joinable()) {
        waiter.join();
    }
    REQUIRE(waiterFinished.load());

    // Service resumes
    ctx.endQuiesce();
    REQUIRE(ctx.tryEnterCall());
    ctx.exitCall();
}



} // namespace tests