#pragma once

#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/BinPackScheduler.h>
#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/util/batch.h>

namespace faabric::batch_scheduler {

class ServiceLocalityScheduler final : public BatchScheduler
{
  public:
    std::shared_ptr<SchedulingDecision> makeSchedulingDecision(
      HostMap& hostMap,
      const InFlightReqs& inFlightReqs,
      std::shared_ptr<faabric::BatchExecuteRequest> req) override;

  private:
    BinPackScheduler fallback;

    bool isRpcServiceMigration(
      const InFlightReqs& inFlightReqs,
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      DecisionType decisionType) const;

    std::shared_ptr<SchedulingDecision> makeRpcMigrationDecision(
      HostMap& hostMap,
      const InFlightReqs& inFlightReqs,
      std::shared_ptr<faabric::BatchExecuteRequest> req);

    bool isFirstDecisionBetter(
      std::shared_ptr<SchedulingDecision> decisionA,
      std::shared_ptr<SchedulingDecision> decisionB) override;

    std::vector<Host> getSortedHosts(
      HostMap& hostMap,
      const InFlightReqs& inFlightReqs,
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      const DecisionType& decisionType) override;
};

}