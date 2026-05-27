#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/rpc/RpcDependencyGraph.h>

using namespace faabric::rpc;

namespace tests {

class RpcDependencyGraphFixture : public BatchSchedulerFixture
{
  public:
    RpcDependencyGraphFixture()
    {
        graph.clear();
    }

    ~RpcDependencyGraphFixture()
    {
        graph.clear();
    }

  protected:
    RpcDependencyGraph graph;

    using Node = RpcDependencyGraph::ServiceNode;

    static Node node(int32_t appId, int32_t msgId)
    {
        return Node{ .appId = appId, .msgId = msgId };
    }

    static void addEdge(faabric::RpcDependencyBatch& batch,
                        Node caller,
                        Node callee,
                        const std::string& callerHost,
                        const std::string& calleeHost,
                        uint64_t observations = 1)
    {
        auto* edge = batch.add_edges();

        edge->set_callerappid(caller.appId);
        edge->set_callermsgid(caller.msgId);
        edge->set_calleeappid(callee.appId);
        edge->set_calleemsgid(callee.msgId);

        edge->set_callerhost(callerHost);
        edge->set_calleehost(calleeHost);
        edge->set_observations(observations);
    }

    static faabric::RpcDependencyBatch makeBatch(Node caller,
                                                 Node callee,
                                                 const std::string& callerHost,
                                                 const std::string& calleeHost,
                                                 uint64_t observations = 1)
    {
        faabric::RpcDependencyBatch batch;
        addEdge(batch, caller, callee, callerHost, calleeHost, observations);
        return batch;
    }
};

TEST_CASE_METHOD(RpcDependencyGraphFixture,
                 "RPC dependency graph ignores edges below observation threshold",
                 "[rpc][dependency-graph]")
{
    auto caller = node(1, 10);
    auto callee = node(2, 20);

    auto batch = makeBatch(caller, callee, "host-a", "host-b", 1);
    graph.mergeTelemetry(batch);

    graph.setPlacement(caller, "host-a");
    graph.setPlacement(callee, "host-b");

    REQUIRE(graph.remoteIncidentEdgeCost(caller) == 0);
    REQUIRE(graph.remoteIncidentEdgeCost(callee) == 0);
}

TEST_CASE_METHOD(RpcDependencyGraphFixture,
                 "RPC dependency graph aggregates accepted dependency edges",
                 "[rpc][dependency-graph]")
{
    auto caller = node(1, 10);
    auto callee = node(2, 20);

    auto batch = makeBatch(caller, callee, "host-a", "host-b", 3);
    graph.mergeTelemetry(batch);

    graph.setPlacement(caller, "host-a");
    graph.setPlacement(callee, "host-b");

    REQUIRE(graph.remoteIncidentEdgeCost(caller) == 3);
    REQUIRE(graph.remoteIncidentEdgeCost(callee) == 3);
}

TEST_CASE_METHOD(RpcDependencyGraphFixture,
                 "RPC dependency graph aggregates repeated telemetry for same edge",
                 "[rpc][dependency-graph]")
{
    auto caller = node(1, 10);
    auto callee = node(2, 20);

    graph.mergeTelemetry(makeBatch(caller, callee, "host-a", "host-b", 2));
    graph.mergeTelemetry(makeBatch(caller, callee, "host-a", "host-b", 5));

    graph.setPlacement(caller, "host-a");
    graph.setPlacement(callee, "host-b");

    REQUIRE(graph.remoteIncidentEdgeCost(caller) == 7);
    REQUIRE(graph.remoteIncidentEdgeCost(callee) == 7);
}

TEST_CASE_METHOD(RpcDependencyGraphFixture,
                 "RPC dependency graph gives zero remote cost for colocated services",
                 "[rpc][dependency-graph]")
{
    auto caller = node(1, 10);
    auto callee = node(2, 20);

    graph.mergeTelemetry(makeBatch(caller, callee, "host-a", "host-b", 4));

    graph.setPlacement(caller, "host-a");
    graph.setPlacement(callee, "host-a");

    REQUIRE(graph.remoteIncidentEdgeCost(caller) == 0);
    REQUIRE(graph.remoteIncidentEdgeCost(callee) == 0);
}

TEST_CASE_METHOD(RpcDependencyGraphFixture,
                 "RPC dependency graph updates remote cost when placement changes",
                 "[rpc][dependency-graph]")
{
    auto caller = node(1, 10);
    auto callee = node(2, 20);

    graph.mergeTelemetry(makeBatch(caller, callee, "host-a", "host-b", 4));

    graph.setPlacement(caller, "host-a");
    graph.setPlacement(callee, "host-b");

    REQUIRE(graph.remoteIncidentEdgeCost(caller) == 4);

    graph.setPlacement(callee, "host-a");

    REQUIRE(graph.remoteIncidentEdgeCost(caller) == 0);
    REQUIRE(graph.remoteIncidentEdgeCost(callee) == 0);
}

TEST_CASE_METHOD(RpcDependencyGraphFixture,
                 "RPC dependency graph removes placement from cost calculation",
                 "[rpc][dependency-graph]")
{
    auto caller = node(1, 10);
    auto callee = node(2, 20);

    graph.mergeTelemetry(makeBatch(caller, callee, "host-a", "host-b", 4));

    graph.setPlacement(caller, "host-a");
    graph.setPlacement(callee, "host-b");

    REQUIRE(graph.remoteIncidentEdgeCost(caller) == 4);

    graph.removePlacement(callee);

    REQUIRE(graph.remoteIncidentEdgeCost(caller) == 0);
}

TEST_CASE_METHOD(RpcDependencyGraphFixture,
                 "RPC dependency graph recommends colocating with strongest dependency",
                 "[rpc][dependency-graph]")
{
    auto target = node(1, 10);
    auto weakNeighbour = node(2, 20);
    auto strongNeighbour = node(3, 30);

    graph.setPlacement(weakNeighbour, "host-b");
    graph.setPlacement(strongNeighbour, "host-c");

    faabric::RpcDependencyBatch batch;
    addEdge(batch, weakNeighbour, target, "host-b", "host-a", 2);
    addEdge(batch, strongNeighbour, target, "host-c", "host-a", 6);
    graph.mergeTelemetry(batch);

    auto hostMap =
      buildHostMap({ "host-a", "host-b", "host-c" },
                   { 4, 4, 4 },
                   { 0, 0, 0 });

    auto rec = graph.recommendHost(target, hostMap);

    REQUIRE(rec.has_value());
    REQUIRE(rec.value() == "host-c");
}

TEST_CASE_METHOD(RpcDependencyGraphFixture,
                 "RPC dependency graph does not recommend a full host",
                 "[rpc][dependency-graph]")
{
    auto target = node(1, 10);
    auto neighbour = node(2, 20);

    graph.setPlacement(neighbour, "host-b");

    graph.mergeTelemetry(makeBatch(neighbour, target, "host-b", "host-a", 6));

    auto hostMap =
      buildHostMap({ "host-a", "host-b" },
                   { 4, 1 },
                   { 0, 1 });

    auto rec = graph.recommendHost(target, hostMap);

    REQUIRE_FALSE(rec.has_value());
}

TEST_CASE_METHOD(RpcDependencyGraphFixture,
                 "RPC dependency graph falls back to available dependent host",
                 "[rpc][dependency-graph]")
{
    auto target = node(1, 10);
    auto fullStrongNeighbour = node(2, 20);
    auto availableWeakNeighbour = node(3, 30);

    graph.setPlacement(fullStrongNeighbour, "host-b");
    graph.setPlacement(availableWeakNeighbour, "host-c");

    faabric::RpcDependencyBatch batch;
    addEdge(batch, fullStrongNeighbour, target, "host-b", "host-a", 8);
    addEdge(batch, availableWeakNeighbour, target, "host-c", "host-a", 4);
    graph.mergeTelemetry(batch);

    auto hostMap =
      buildHostMap({ "host-a", "host-b", "host-c" },
                   { 4, 1, 4 },
                   { 0, 1, 0 });

    auto rec = graph.recommendHost(target, hostMap);

    REQUIRE(rec.has_value());
    REQUIRE(rec.value() == "host-c");
}

TEST_CASE_METHOD(RpcDependencyGraphFixture,
                 "RPC dependency graph returns no recommendation without accepted edges",
                 "[rpc][dependency-graph]")
{
    auto target = node(1, 10);
    auto neighbour = node(2, 20);

    graph.setPlacement(neighbour, "host-b");

    graph.mergeTelemetry(makeBatch(neighbour, target, "host-b", "host-a", 1));

    auto hostMap =
      buildHostMap({ "host-a", "host-b" },
                   { 4, 4 },
                   { 0, 0 });

    auto rec = graph.recommendHost(target, hostMap);

    REQUIRE_FALSE(rec.has_value());
}

TEST_CASE_METHOD(RpcDependencyGraphFixture,
                 "RPC dependency graph clears edges and placements",
                 "[rpc][dependency-graph]")
{
    auto caller = node(1, 10);
    auto callee = node(2, 20);

    graph.mergeTelemetry(makeBatch(caller, callee, "host-a", "host-b", 4));

    graph.setPlacement(caller, "host-a");
    graph.setPlacement(callee, "host-b");

    REQUIRE(graph.remoteIncidentEdgeCost(caller) == 4);

    graph.clear();

    REQUIRE(graph.remoteIncidentEdgeCost(caller) == 0);

    auto hostMap =
      buildHostMap({ "host-a", "host-b" },
                   { 4, 4 },
                   { 0, 0 });

    REQUIRE_FALSE(graph.recommendHost(caller, hostMap).has_value());
}

}