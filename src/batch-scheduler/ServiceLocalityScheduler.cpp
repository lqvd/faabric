#include <faabric/batch-scheduler/ServiceLocalityScheduler.h>

#include <faabric/rpc/RpcDependencyGraph.h>
#include <faabric/util/logging.h>

namespace faabric::batch_scheduler {

std::shared_ptr<SchedulingDecision>
ServiceLocalityScheduler::makeSchedulingDecision(
  HostMap& hostMap,
  const InFlightReqs& inFlightReqs,
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    DecisionType decisionType = getDecisionType(inFlightReqs, req);

    if (isRpcServiceMigration(inFlightReqs, req, decisionType)) {
        return makeRpcMigrationDecision(hostMap, inFlightReqs, req);
    }

    // Use Binpack for all other scheduling decisions...
    return fallback.makeSchedulingDecision(hostMap, inFlightReqs, req);
}

bool ServiceLocalityScheduler::isRpcServiceMigration(
  const InFlightReqs& inFlightReqs,
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  DecisionType decisionType) const
{
    if (decisionType != DecisionType::DIST_CHANGE) {
        return false;
    }

    if (!inFlightReqs.contains(req->appid())) {
        return false;
    }

    if (req->messages_size() != 1) {
        return false;
    }

    const auto& msg = req->messages(0);

    return msg.isrpc() && msg.islongrunning();
}

std::shared_ptr<SchedulingDecision>
ServiceLocalityScheduler::makeRpcMigrationDecision(
  HostMap& hostMap,
  const InFlightReqs& inFlightReqs,
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    if (req->messages_size() != 1) {
        return std::make_shared<SchedulingDecision>(DO_NOT_MIGRATE_DECISION);
    }

    const auto& msg = req->messages(0);

    const int32_t appId = msg.appid() == 0 ? req->appid() : msg.appid();

    faabric::rpc::RpcDependencyGraph::ServiceNode node{
        .appId = appId,
        .msgId = msg.id(),
    };

    auto targetHost =
      faabric::rpc::getRpcDependencyGraph().recommendHost(node, hostMap);

    if (!targetHost.has_value()) {
        SPDLOG_DEBUG(
          "Service locality scheduler: no migration target for app={} msg={}",
          appId,
          msg.id());

        return std::make_shared<SchedulingDecision>(DO_NOT_MIGRATE_DECISION);
    }

    SPDLOG_INFO("MAKING DECISION!");

    auto decision = std::make_shared<SchedulingDecision>(req->appid(), 0);

    // Single-message SERVICE BER, so this preserves the service identity.
    decision->addMessage(targetHost.value(), msg);

    SPDLOG_INFO(
      "Service locality scheduler: app={} msg={} scheduled for migration to {}",
      appId,
      msg.id(),
      targetHost.value());

    return decision;
}

bool ServiceLocalityScheduler::isFirstDecisionBetter(
  std::shared_ptr<SchedulingDecision> decisionA,
  std::shared_ptr<SchedulingDecision> decisionB)
{
    throw std::runtime_error(
      "ServiceLocalityScheduler::isFirstDecisionBetter should not be called");
}

std::vector<Host> ServiceLocalityScheduler::getSortedHosts(
  HostMap& hostMap,
  const InFlightReqs& inFlightReqs,
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  const DecisionType& decisionType)
{
    throw std::runtime_error(
      "ServiceLocalityScheduler::getSortedHosts should not be called");
}

}