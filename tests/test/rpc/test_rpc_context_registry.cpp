#include <catch2/catch.hpp>

#include <fixtures.h>

#include <faabric/rpc/RpcContextRegistry.h>
#include <faabric/transport/common.h>
#include <faabric/util/logging.h>

#include <chrono>
#include <memory>
#include <thread>

using namespace faabric::rpc;

namespace tests {

TEST_CASE_METHOD(RpcContextRegistryFixture,
                 "Test resolving RPC destination to local context",
                 "[rpc]")
{
    auto& reg = getRpcContextRegistry();

    int32_t msgId = 123;
    uint32_t requestId = nextRequestId();

    auto ctx = makeContext(msgId);
    reg.registerContext(msgId, ctx);
    reg.registerInFlightRequest(requestId, msgId);

    auto dest = reg.getResponseTarget(requestId);
    checkLocal(reg, dest, requestId, msgId);
}

TEST_CASE_METHOD(RpcContextRegistryFixture,
                 "Test resolving RPC destination to remote forwarding address",
                 "[rpc]")
{
    auto& reg = getRpcContextRegistry();

    int32_t msgId = 123;
    uint32_t requestId = nextRequestId();
    std::string forwardHost = "other-host";

    reg.registerInFlightRequest(requestId, msgId);

    // Simulate migration: request still maps to msgId, but msgId no longer has
    // a local context. Instead, it has a forwarding address.
    reg.setForwardingAddress(
      msgId,
      forwardHost,
      { requestId },
      std::chrono::milliseconds(30000));

    auto dest = reg.getResponseTarget(requestId);
    checkRemote(dest, forwardHost);
}

TEST_CASE_METHOD(RpcContextRegistryFixture,
                 "Test resolving RPC destination with no context or forwarding entry",
                 "[rpc]")
{
    auto& reg = getRpcContextRegistry();

    uint32_t requestId = nextRequestId();

    auto dest = reg.getResponseTarget(requestId);
    checkUndeliverable(dest);
}

TEST_CASE_METHOD(RpcContextRegistryFixture,
                 "Test expired forwarding address is undeliverable",
                 "[rpc]")
{
    auto& reg = getRpcContextRegistry();

    int32_t msgId = 123;
    uint32_t requestId = nextRequestId();
    std::string forwardHost = "other-host";

    reg.registerInFlightRequest(requestId, msgId);
    reg.setForwardingAddress(
      msgId,
      forwardHost,
      { requestId },
      std::chrono::milliseconds(1));

    std::this_thread::sleep_for(std::chrono::milliseconds(5));

    auto dest = reg.getResponseTarget(requestId);
    checkUndeliverable(dest);
}

TEST_CASE_METHOD(RpcContextRegistryFixture,
                 "Test clearing in-flight RPC request",
                 "[rpc]")
{
    auto& reg = getRpcContextRegistry();

    int32_t msgId = 123;
    uint32_t requestId = nextRequestId();

    auto ctx = makeContext(msgId);
    reg.registerContext(msgId, ctx);
    reg.registerInFlightRequest(requestId, msgId);

    auto before = reg.getResponseTarget(requestId);
    checkLocal(reg, before, requestId, msgId);

    reg.clearRequest(requestId);

    auto after = reg.getResponseTarget(requestId);
    checkUndeliverable(after);
}

TEST_CASE_METHOD(RpcContextRegistryFixture,
                 "Test resetting RPC context registry",
                 "[rpc]")
{
    auto& reg = getRpcContextRegistry();

    int32_t localMsgId = 123;
    int32_t remoteMsgId = 456;

    uint32_t localRequestId = nextRequestId();
    uint32_t remoteRequestId = nextRequestId();

    auto ctx = makeContext(localMsgId);
    reg.registerContext(localMsgId, ctx);
    reg.registerInFlightRequest(localRequestId, localMsgId);

    reg.registerInFlightRequest(remoteRequestId, remoteMsgId);
    reg.setForwardingAddress(
      remoteMsgId,
      "other-host",
      { remoteRequestId },
      std::chrono::milliseconds(30000));

    reg.reset();

    checkUndeliverable(reg.getResponseTarget(localRequestId));
    checkUndeliverable(reg.getResponseTarget(remoteRequestId));
}

}