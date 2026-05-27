#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/rpc/RpcTracker.h>

using namespace faabric::rpc;

namespace tests {

class RpcTrackerFixture
{
  public:
    RpcTrackerFixture()
    {
        getRpcTracker().clear();
    }

    ~RpcTrackerFixture()
    {
        getRpcTracker().clear();
    }
};

TEST_CASE_METHOD(RpcTrackerFixture,
                 "RPC tracker records dependency deltas",
                 "[rpc][tracker]")
{
    auto& tracker = getRpcTracker();

    tracker.recordDependency(1, 10, 2, 20, "host-a", "host-b");
    tracker.recordDependency(1, 10, 2, 20, "host-a", "host-b");

    auto deltas = tracker.snapshotAndResetDeltas();

    REQUIRE(deltas.size() == 1);
    REQUIRE(deltas.at(0).caller.appId == 1);
    REQUIRE(deltas.at(0).caller.msgId == 10);
    REQUIRE(deltas.at(0).callee.appId == 2);
    REQUIRE(deltas.at(0).callee.msgId == 20);
    REQUIRE(deltas.at(0).callerHost == "host-a");
    REQUIRE(deltas.at(0).calleeHost == "host-b");
    REQUIRE(deltas.at(0).observations == 2);

    REQUIRE(tracker.snapshotAndResetDeltas().empty());
}

TEST_CASE_METHOD(RpcTrackerFixture,
                 "RPC tracker separates distinct dependency edges",
                 "[rpc][tracker]")
{
    auto& tracker = getRpcTracker();

    tracker.recordDependency(1, 10, 2, 20, "host-a", "host-b");
    tracker.recordDependency(1, 10, 3, 30, "host-a", "host-c");
    tracker.recordDependency(4, 40, 2, 20, "host-d", "host-b");

    auto deltas = tracker.snapshotAndResetDeltas();

    REQUIRE(deltas.size() == 3);
}

TEST_CASE_METHOD(RpcTrackerFixture,
                 "RPC tracker ignores invalid dependency records",
                 "[rpc][tracker]")
{
    auto& tracker = getRpcTracker();

    tracker.recordDependency(0, 10, 2, 20, "host-a", "host-b");
    tracker.recordDependency(1, 0, 2, 20, "host-a", "host-b");
    tracker.recordDependency(1, 10, 0, 20, "host-a", "host-b");
    tracker.recordDependency(1, 10, 2, 0, "host-a", "host-b");
    tracker.recordDependency(1, 10, 2, 20, "", "host-b");
    tracker.recordDependency(1, 10, 2, 20, "host-a", "");

    REQUIRE(tracker.snapshotAndResetDeltas().empty());
}

TEST_CASE_METHOD(RpcTrackerFixture,
                 "RPC telemetry snapshot converts deltas to protobuf",
                 "[rpc][tracker]")
{
    auto& tracker = getRpcTracker();

    tracker.recordDependency(1, 10, 2, 20, "host-a", "host-b");

    auto batch = snapshotRpcDependencyTelemetry();

    REQUIRE(batch.edges_size() == 1);

    const auto& e = batch.edges(0);
    REQUIRE(e.callerappid() == 1);
    REQUIRE(e.callermsgid() == 10);
    REQUIRE(e.calleeappid() == 2);
    REQUIRE(e.calleemsgid() == 20);
    REQUIRE(e.callerhost() == "host-a");
    REQUIRE(e.calleehost() == "host-b");
    REQUIRE(e.observations() == 1);

    REQUIRE(snapshotRpcDependencyTelemetry().edges_size() == 0);
}

} // namespace tests