#include <faabric/batch-scheduler/ForcedMigrationScheduler.h>
#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/util/batch.h>
#include <faabric/util/logging.h>

#include <atomic>

namespace faabric::batch_scheduler {

static std::map<std::string, int> getHostFreqCount(
  std::shared_ptr<SchedulingDecision> decision)
{
    std::map<std::string, int> hostFreqCount;
    for (const auto& host : decision->hosts) {
        hostFreqCount[host] += 1;
    }
    return hostFreqCount;
}

// A global counter of DIST_CHANGE checks. Every `migrateEveryNChecks` we
// force a migration; otherwise we decline. This makes the policy periodic
// and deterministic, which is what a correctness experiment wants.
static std::atomic<int> distChangeCounter{ 0 };
static constexpr int migrateEveryNChecks = 1; // force on every check

// For the forced scheduler "better" is not meaningful: we always migrate
// when asked to. This method should not be relied upon.
bool ForcedMigrationScheduler::isFirstDecisionBetter(
  std::shared_ptr<SchedulingDecision> decisionA,
  std::shared_ptr<SchedulingDecision> decisionB)
{
    throw std::runtime_error(
      "isFirstDecisionBetter not supported for FORCED scheduler");
}

std::vector<Host> ForcedMigrationScheduler::getSortedHosts(
  HostMap& hostMap,
  const InFlightReqs& inFlightReqs,
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  const DecisionType& decisionType)
{
    // For NEW / SCALE_CHANGE we behave like a plain bin-packer: sort by
    // available capacity, largest first. We only diverge on DIST_CHANGE.
    std::vector<Host> sortedHosts;
    for (auto [ip, host] : hostMap) {
        sortedHosts.push_back(host);
    }

    auto isFirstHostLarger = [&](const Host& a, const Host& b) -> bool {
        int nAvailableA = numSlotsAvailable(a);
        int nAvailableB = numSlotsAvailable(b);
        if (nAvailableA != nAvailableB) {
            return nAvailableA > nAvailableB;
        }
        int nSlotsA = numSlots(a);
        int nSlotsB = numSlots(b);
        if (nSlotsA != nSlotsB) {
            return nSlotsA > nSlotsB;
        }
        return a->ip > b->ip;
    };

    // On DIST_CHANGE we free the app's own slots first (same as BinPack)
    // so the subsequent bin-pack has a fresh shot at re-placing it.
    if (decisionType == DecisionType::DIST_CHANGE) {
        auto oldDecision = inFlightReqs.at(req->appid()).second;
        auto hostFreqCount = getHostFreqCount(oldDecision);
        for (auto h : sortedHosts) {
          if (hostFreqCount.contains(h->ip)) {
            freeSlots(h, hostFreqCount.at(h->ip));
          }
        }
    }

    std::sort(sortedHosts.begin(), sortedHosts.end(), isFirstHostLarger);
    return sortedHosts;
}

// Produce a valid decision that differs from `oldDecision` by cyclically
// rotating each message onto the *next* host (in sorted order) that has a
// free slot. Capacity is respected because we decrement a working copy of
// the per-host availability as we go. If no rotation is possible (e.g. only
// one host has any slots) we fall back to the old placement, which the
// caller will detect as "no change" and decline.
static std::shared_ptr<SchedulingDecision> rotatePlacement(
  std::shared_ptr<SchedulingDecision> oldDecision,
  std::vector<Host>& sortedHosts)
{
    auto decision = std::make_shared<SchedulingDecision>(oldDecision->appId,
                                                         oldDecision->groupId);

    std::map<std::string, int> avail;
    std::vector<std::string> hostOrder;
    for (const auto& h : sortedHosts) {
        avail[h->ip] = std::max<int>(0, h->slots - h->usedSlots);
        hostOrder.push_back(h->ip);
    }

    auto nextHostAfter = [&](const std::string& current) -> std::string {
        // Find `current` in hostOrder, then scan forward (wrapping) for the
        // first *different* host that still has a free slot.
        int n = hostOrder.size();
        int startIdx = 0;
        for (int i = 0; i < n; i++) {
            if (hostOrder[i] == current) {
                startIdx = i;
                break;
            }
        }
        for (int off = 1; off <= n; off++) {
            const auto& cand = hostOrder[(startIdx + off) % n];
            if (cand != current && avail[cand] > 0) {
                return cand;
            }
        }
        // No alternative host: keep current if it still has room.
        if (avail[current] > 0) {
            return current;
        }
        throw std::runtime_error("No host with free slots during rotation");
    };

    for (int i = 0; i < oldDecision->hosts.size(); i++) {
        const auto& oldHost = oldDecision->hosts.at(i);
        std::string newHost = nextHostAfter(oldHost);
        avail.at(newHost) -= 1;

        decision->addMessageInPosition(i,
                                       newHost,
                                       oldDecision->messageIds.at(i),
                                       oldDecision->appIdxs.at(i),
                                       oldDecision->groupIdxs.at(i),
                                       -1);
    }

    return decision;
}

static bool decisionsDiffer(std::shared_ptr<SchedulingDecision> a,
                            std::shared_ptr<SchedulingDecision> b)
{
    if (a->hosts.size() != b->hosts.size()) {
        return true;
    }
    for (int i = 0; i < a->hosts.size(); i++) {
        if (a->hosts.at(i) != b->hosts.at(i)) {
            return true;
        }
    }
    return false;
}

std::shared_ptr<SchedulingDecision>
ForcedMigrationScheduler::makeSchedulingDecision(
  HostMap& hostMap,
  const InFlightReqs& inFlightReqs,
  std::shared_ptr<BatchExecuteRequest> req)
{
    auto decisionType = getDecisionType(inFlightReqs, req);
    auto sortedHosts = getSortedHosts(hostMap, inFlightReqs, req, decisionType);

    // DIST_CHANGE: the forced-migration path.
    if (decisionType == DecisionType::DIST_CHANGE) {
        int n = distChangeCounter.fetch_add(1) + 1;

        // Only force a migration every Nth check.
        if (n % migrateEveryNChecks != 0) {
            return std::make_shared<SchedulingDecision>(DO_NOT_MIGRATE_DECISION);
        }

        auto oldDecision = inFlightReqs.at(req->appid()).second;
        auto rotated = rotatePlacement(oldDecision, sortedHosts);

        // If we genuinely couldn't move anything, don't claim a migration.
        if (!decisionsDiffer(rotated, oldDecision)) {
            return std::make_shared<SchedulingDecision>(DO_NOT_MIGRATE_DECISION);
        }

        return rotated;
    }

    // NEW / SCALE_CHANGE: ordinary bin-pack.
    auto decision = std::make_shared<SchedulingDecision>(req->appid(), 0);
    auto it = sortedHosts.begin();
    int numLeftToSchedule = req->messages_size();
    int msgIdx = 0;
    while (it < sortedHosts.end()) {
        int numOnThisHost =
          std::min<int>(numLeftToSchedule, numSlotsAvailable(*it));
        for (int i = 0; i < numOnThisHost; i++) {
            decision->addMessage(getIp(*it), req->messages(msgIdx));
            msgIdx++;
        }
        numLeftToSchedule -= numOnThisHost;
        if (numLeftToSchedule == 0) {
            break;
        }
        it++;
    }

    if (numLeftToSchedule > 0) {
        return std::make_shared<SchedulingDecision>(NOT_ENOUGH_SLOTS_DECISION);
    }

    return decision;
}
}