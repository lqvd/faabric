#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/rpc/RpcContext.h>
#include <faabric/transport/MessageEndpoint.h>

using namespace faabric::rpc;
using namespace faabric::scheduler;

namespace tests {

TEST_CASE_METHOD(RpcTestFixture,
                 "Test RPC context serialize and deserialize",
                 "[rpc]")
{
    int portA = nextPort();
    int portB = nextPort();

    std::string uriA = makeFaabricUri(portA);
    std::string uriB = makeFaabricUri(portB);

    int32_t idA = ctx->createChannel(uriA);
    int32_t idB = ctx->createChannel(uriB);

    faabric::RpcMigrationState migSt = ctx->serializeMigrationState();
    
    REQUIRE(migSt.channels_size() == 2);

    ctx->clear();

    ctx->deserializeMigrationState(migSt);

    auto chA = ctx->getChannel(idA);
    auto chB = ctx->getChannel(idB);

    REQUIRE(chA.targetUri == uriA);
    REQUIRE(chB.targetUri == uriB);

    // Ensure next ID advanced after restore
    int32_t idC = ctx->createChannel(makeFaabricUri(nextPort()));
    REQUIRE(idC > std::max(idA, idB));
}

TEST_CASE_METHOD(RpcTestFixture,
                 "Test RPC context getChannel throws for unknown id",
                 "[rpc]")
{
    REQUIRE_THROWS_AS(ctx->getChannel(9999), std::runtime_error);
}

TEST_CASE_METHOD(RpcTestFixture,
                 "Test RPC context closeChannel no-op for unknown id",
                 "[rpc]")
{
    // Should not throw
    REQUIRE_NOTHROW(ctx->closeChannel(9999));

    // Context should still be usable after a no-op close
    int32_t id = ctx->createChannel(makeFaabricUri(nextPort()));
    REQUIRE(id == 1);
}

TEST_CASE_METHOD(RpcTestFixture,
                 "Test RPC context clear resets channel ids",
                 "[rpc]")
{
    int32_t idA = ctx->createChannel(makeFaabricUri(nextPort()));
    int32_t idB = ctx->createChannel(makeFaabricUri(nextPort()));

    REQUIRE(idA == 1);
    REQUIRE(idB == 2);

    ctx->clear();

    int32_t idC = ctx->createChannel(makeFaabricUri(nextPort()));
    REQUIRE(idC == 1);
}

TEST_CASE_METHOD(RpcTestFixture,
                 "Test RPC context closes are reflected in serialize",
                 "[rpc]")
{
    int32_t idA = ctx->createChannel(makeFaabricUri(nextPort()));
    int32_t idB = ctx->createChannel(makeFaabricUri(nextPort()));

    ctx->closeChannel(idA);
    auto migSt = ctx->serializeMigrationState();
    
    REQUIRE(migSt.channels_size() == 1);
    REQUIRE(migSt.channels(0).channelid() == idB);
}

TEST_CASE_METHOD(RpcTestFixture,
                 "Test RPC context parse default faabric port URI",
                 "[rpc]")
{
    int32_t id = ctx->createChannel("faabric://127.0.0.1");
    auto ch = ctx->getChannel(id);

    REQUIRE(ch.targetUri == "faabric://127.0.0.1");
    REQUIRE(ch.port == 8013);
}

TEST_CASE_METHOD(RpcTestFixture,
                 "Test RPC context rejects invalid faabric port",
                 "[rpc]")
{
    REQUIRE_THROWS_AS(ctx->createChannel("faabric://127.0.0.1:notaport"),
                      std::invalid_argument);
}


TEST_CASE_METHOD(RpcTestFixture,
                 "RPC quiesce blocks new calls and resumes after endQuiesce",
                 "[rpc][migration]") {
    REQUIRE(ctx->tryEnterCall());
    ctx->exitCall();

    ctx->beginQuiesce();
    
    REQUIRE_FALSE(ctx->tryEnterCall());
    ctx->endQuiesce();

    REQUIRE(ctx->tryEnterCall());
    ctx->exitCall();
}

} // namespace tests