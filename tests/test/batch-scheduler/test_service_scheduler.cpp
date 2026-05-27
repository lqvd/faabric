#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/ServiceLocalityScheduler.h>
#include <faabric/rpc/RpcDependencyGraph.h>
#include <faabric/rpc/RpcTracker.h>
#include <faabric/util/batch.h>

using namespace faabric::batch_scheduler;
using namespace faabric::rpc;

namespace tests {

class ServiceSchedulerTelemetryFixture : public BatchSchedulerFixture
{
  public:
    ServiceSchedulerTelemetryFixture()
    {
        conf.batchSchedulerMode = "service";
        batchScheduler = getBatchScheduler();
        getRpcTracker().clear();
        getRpcDependencyGraph().clear();
    }

    ~ServiceSchedulerTelemetryFixture()
    {
        getRpcTracker().clear();
        getRpcDependencyGraph().clear();
    }

  protected:
    using Node = RpcDependencyGraph::ServiceNode;

    static Node node(int32_t appId, int32_t msgId)
    {
        return Node{ .appId = appId, .msgId = msgId };
    }

    static void recordDependency(uint32_t observations,
                                 Node caller,
                                 Node callee,
                                 const std::string& callerHost,
                                 const std::string& calleeHost)
    {
        auto& tracker = getRpcTracker();

        for (uint32_t i = 0; i < observations; i++) {
            tracker.recordDependency(caller.appId,
                                     caller.msgId,
                                     callee.appId,
                                     callee.msgId,
                                     callerHost,
                                     calleeHost);
        }
    }

    static void mergeTrackerTelemetryIntoGraph()
    {
        auto telemetry = snapshotRpcDependencyTelemetry();

        REQUIRE(telemetry.edges_size() > 0);

        getRpcDependencyGraph().mergeTelemetry(telemetry);
    }

    static void markAsRpcLongRunning(faabric::BatchExecuteRequest& req)
    {
        REQUIRE(req.messages_size() == 1);

        auto& msg = req.mutable_messages()->at(0);
        msg.set_isrpc(true);
        msg.set_islongrunning(true);
    }
};

TEST_CASE_METHOD(ServiceSchedulerTelemetryFixture,
                 "Service scheduler uses RPC telemetry for service migration",
                 "[rpc][service-scheduler]")
{
    /*
     * Equal capacity. If the service scheduler falls back to bin-pack, it will
     * pick foo. If it uses the RPC dependency graph, it should pick bar.
     */
    auto hostMap = buildHostMap(
      { "foo", "bar" },
      { 4, 4 },
      { 0, 0 });

    /*
     * This is the migration request that the service scheduler is deciding.
     * It must look like a DIST_CHANGE request for a single long-running RPC
     * service message.
     */
    ber = faabric::util::batchExecFactory("svc", "callee", 1);
    ber->set_type(faabric::BatchExecuteRequest::MIGRATION);
    markAsRpcLongRunning(*ber);

    const auto& targetMsg = ber->messages(0);
    const int32_t appId = ber->appid();
    const int32_t targetMsgId = targetMsg.id();

    const Node target = node(appId, targetMsgId);

    /*
     * The old in-flight request must have the same app id and be a SERVICE
     * request, otherwise ServiceLocalityScheduler::isRpcServiceMigration()
     * returns false and the scheduler falls back to bin-pack.
     */
    auto oldReq = std::make_shared<faabric::BatchExecuteRequest>(*ber);
    oldReq->set_type(faabric::BatchExecuteRequest::SERVICE);
    markAsRpcLongRunning(*oldReq);

    auto inFlightReqs = buildInFlightReqs(
      oldReq,
      1,
      { "old-target-host" });

    /*
     * Existing services that the migrating target communicates with. The graph
     * placements are seeded directly because this test is about whether the
     * scheduler consumes telemetry-derived graph state.
     */
    const Node weakCaller = node(appId, targetMsgId + 100);
    const Node strongCaller = node(appId, targetMsgId + 200);

    auto& graph = getRpcDependencyGraph();

    graph.setPlacement(target, "old-target-host");
    graph.setPlacement(weakCaller, "foo");
    graph.setPlacement(strongCaller, "bar");

    recordDependency(
      2,
      weakCaller,
      target,
      "foo",
      "old-target-host");

    recordDependency(
      8,
      strongCaller,
      target,
      "bar",
      "old-target-host");

    mergeTrackerTelemetryIntoGraph();

    auto expectedDecision = buildExpectedDecision(ber, { "bar" });

    REQUIRE(ber->type() == BatchExecuteRequest_BatchExecuteType_MIGRATION);
    REQUIRE(inFlightReqs.contains(ber->appid()));
    REQUIRE(ber->messages_size() == 1);
    REQUIRE(ber->messages(0).isrpc());
    REQUIRE(ber->messages(0).islongrunning());

    actualDecision = *batchScheduler->makeSchedulingDecision(
      hostMap,
      inFlightReqs,
      ber);

    compareSchedulingDecisions(actualDecision, expectedDecision);
}

TEST_CASE_METHOD(ServiceSchedulerTelemetryFixture,
                 "Service scheduler does not migrate without RPC telemetry",
                 "[rpc][service-scheduler]")
{
    auto hostMap = buildHostMap(
      { "foo", "bar" },
      { 4, 4 },
      { 0, 0 });

    ber = faabric::util::batchExecFactory("svc", "callee", 1);
    ber->set_type(BatchExecuteRequest_BatchExecuteType_MIGRATION);
    markAsRpcLongRunning(*ber);

    auto oldReq = std::make_shared<faabric::BatchExecuteRequest>(*ber);
    oldReq->set_type(BatchExecuteRequest_BatchExecuteType_SERVICE);
    markAsRpcLongRunning(*oldReq);

    auto inFlightReqs = buildInFlightReqs(oldReq, 1, { "old-target-host" });

    auto& graph = getRpcDependencyGraph();
    graph.setPlacement(
      node(ber->appid(), ber->messages(0).id()),
      "old-target-host");

    actualDecision = *batchScheduler->makeSchedulingDecision(
      hostMap,
      inFlightReqs,
      ber);

    compareSchedulingDecisions(actualDecision, DO_NOT_MIGRATE_DECISION);
}

TEST_CASE_METHOD(ServiceSchedulerTelemetryFixture,
                 "Service scheduler ignores weak RPC telemetry",
                 "[rpc][service-scheduler]")
{
    auto hostMap = buildHostMap(
      { "foo", "bar" },
      { 4, 4 },
      { 0, 0 });

    ber = faabric::util::batchExecFactory("svc", "callee", 1);
    ber->set_type(BatchExecuteRequest_BatchExecuteType_MIGRATION);
    markAsRpcLongRunning(*ber);

    const int32_t appId = ber->appid();
    const int32_t targetMsgId = ber->messages(0).id();

    auto oldReq = std::make_shared<faabric::BatchExecuteRequest>(*ber);
    oldReq->set_type(BatchExecuteRequest_BatchExecuteType_SERVICE);
    markAsRpcLongRunning(*oldReq);

    auto inFlightReqs = buildInFlightReqs(oldReq, 1, { "old-target-host" });

    auto target = node(appId, targetMsgId);
    auto caller = node(appId, targetMsgId + 100);

    auto& graph = getRpcDependencyGraph();
    graph.setPlacement(target, "old-target-host");
    graph.setPlacement(caller, "bar");

    recordDependency(
      1,
      caller,
      target,
      "bar",
      "old-target-host");

    mergeTrackerTelemetryIntoGraph();

    actualDecision = *batchScheduler->makeSchedulingDecision(
      hostMap,
      inFlightReqs,
      ber);

    compareSchedulingDecisions(actualDecision, DO_NOT_MIGRATE_DECISION);
}

TEST_CASE_METHOD(ServiceSchedulerTelemetryFixture,
                 "Service scheduler does not migrate to full dependency host",
                 "[rpc][service-scheduler]")
{
    auto hostMap = buildHostMap(
      { "foo", "bar" },
      { 4, 1 },
      { 0, 1 });

    ber = faabric::util::batchExecFactory("svc", "callee", 1);
    ber->set_type(BatchExecuteRequest_BatchExecuteType_MIGRATION);
    markAsRpcLongRunning(*ber);

    const int32_t appId = ber->appid();
    const int32_t targetMsgId = ber->messages(0).id();

    auto oldReq = std::make_shared<faabric::BatchExecuteRequest>(*ber);
    oldReq->set_type(BatchExecuteRequest_BatchExecuteType_SERVICE);
    markAsRpcLongRunning(*oldReq);

    auto inFlightReqs = buildInFlightReqs(oldReq, 1, { "old-target-host" });

    auto target = node(appId, targetMsgId);
    auto caller = node(appId, targetMsgId + 100);

    auto& graph = getRpcDependencyGraph();
    graph.setPlacement(target, "old-target-host");
    graph.setPlacement(caller, "bar");

    recordDependency(
      8,
      caller,
      target,
      "bar",
      "old-target-host");

    mergeTrackerTelemetryIntoGraph();

    actualDecision = *batchScheduler->makeSchedulingDecision(
      hostMap,
      inFlightReqs,
      ber);

    compareSchedulingDecisions(actualDecision, DO_NOT_MIGRATE_DECISION);
}

TEST_CASE_METHOD(ServiceSchedulerTelemetryFixture,
                 "Service scheduler cooldown suppresses repeated recommendation",
                 "[rpc][service-scheduler]")
{
    auto hostMap = buildHostMap(
      { "foo", "bar" },
      { 4, 4 },
      { 0, 0 });

    ber = faabric::util::batchExecFactory("svc", "callee", 1);
    ber->set_type(BatchExecuteRequest_BatchExecuteType_MIGRATION);
    markAsRpcLongRunning(*ber);

    const int32_t appId = ber->appid();
    const int32_t targetMsgId = ber->messages(0).id();

    auto oldReq = std::make_shared<faabric::BatchExecuteRequest>(*ber);
    oldReq->set_type(BatchExecuteRequest_BatchExecuteType_SERVICE);
    markAsRpcLongRunning(*oldReq);

    auto inFlightReqs = buildInFlightReqs(oldReq, 1, { "old-target-host" });

    auto target = node(appId, targetMsgId);
    auto caller = node(appId, targetMsgId + 100);

    auto& graph = getRpcDependencyGraph();
    graph.setPlacement(target, "old-target-host");
    graph.setPlacement(caller, "bar");

    recordDependency(
      8,
      caller,
      target,
      "bar",
      "old-target-host");

    mergeTrackerTelemetryIntoGraph();

    auto expectedDecision = buildExpectedDecision(ber, { "bar" });

    actualDecision = *batchScheduler->makeSchedulingDecision(
      hostMap,
      inFlightReqs,
      ber);

    compareSchedulingDecisions(actualDecision, expectedDecision);

    auto secondDecision = *batchScheduler->makeSchedulingDecision(
      hostMap,
      inFlightReqs,
      ber);

    compareSchedulingDecisions(secondDecision, DO_NOT_MIGRATE_DECISION);
}

} // namespace tests