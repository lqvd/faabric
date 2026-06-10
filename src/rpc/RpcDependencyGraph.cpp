#include <faabric/rpc/RpcDependencyGraph.h>

#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

#include <chrono>

namespace faabric::rpc {

RpcDependencyGraph& getRpcDependencyGraph()
{
    static RpcDependencyGraph graph;
    return graph;
}

bool RpcDependencyGraph::isAcceptedEdge(const EdgeState& state) const
{
    return state.observations >= MIN_OBSERVATIONS;
}

bool RpcDependencyGraph::hostHasCapacity(
  const faabric::batch_scheduler::HostMap& hostMap,
  const std::string& host)
{
    auto it = hostMap.find(host);
    if (it == hostMap.end()) {
        return false;
    }
    const auto& h = it->second;
    return std::max<int>(0, h->slots - h->usedSlots) > 0;
}

// -----------------------------------
// telemetry / placement
// -----------------------------------

void RpcDependencyGraph::mergeTelemetry(
  const faabric::RpcDependencyBatch& batch)
{
    const auto now = std::chrono::steady_clock::now();

    faabric::util::FullLock lock(mx);

    for (const auto& e : batch.edges()) {
        ServiceNode caller{ .appId = e.callerappid(),
                            .msgId = e.callermsgid() };
        ServiceNode callee{ .appId = e.calleeappid(),
                            .msgId = e.calleemsgid() };

        if (caller.appId == 0 || caller.msgId == 0 ||
            callee.appId == 0 || callee.msgId == 0) {
            continue;
        }

        if (!e.callerhost().empty()) {
            placement[caller] = e.callerhost();
        }
        if (!e.calleehost().empty()) {
            placement[callee] = e.calleehost();
        }

        EdgeKey key{ .caller = caller, .callee = callee };
        auto& state = edges[key];
        state.observations += e.observations();
        state.lastSeen = now;
    }
}

void RpcDependencyGraph::setPlacement(ServiceNode node,
                                      const std::string& host)
{
    if (node.appId == 0 || node.msgId == 0 || host.empty()) {
        return;
    }

    faabric::util::FullLock lock(mx);
    placement[node] = host;
}

void RpcDependencyGraph::removePlacement(ServiceNode node)
{
    faabric::util::FullLock lock(mx);
    placement.erase(node);
    lastRecommendedAt.erase(node);
}

void RpcDependencyGraph::clear()
{
    faabric::util::FullLock lock(mx);
    placement.clear();
    edges.clear();
    lastRecommendedAt.clear();
}

// -------------------------------------------------------------
// Graph queries
// -------------------------------------------------------------

void RpcDependencyGraph::pruneStaleEdgesUnlocked()
{
    const auto now = std::chrono::steady_clock::now();

    for (auto it = edges.begin(); it != edges.end();) {
        if (now - it->second.lastSeen > EDGE_TTL) {
            it = edges.erase(it);
        } else {
            ++it;
        }
    }
}

std::unordered_set<std::string>
RpcDependencyGraph::candidateHostsForUnlocked(ServiceNode node) const
{
    std::unordered_set<std::string> hosts;

    for (const auto& [edge, state] : edges) {
        if (!isAcceptedEdge(state)) {
            continue;
        }

        if (edge.caller == node) {
            auto it = placement.find(edge.callee);
            if (it != placement.end()) {
                hosts.insert(it->second);
            }
        }
        if (edge.callee == node) {
            auto it = placement.find(edge.caller);
            if (it != placement.end()) {
                hosts.insert(it->second);
            }
        }
    }

    return hosts;
}

// Weighted cost: each cross-host incident edge contributes its observation
// count, so high-traffic relationships dominate placement decisions.
uint64_t RpcDependencyGraph::incidentEdgeCostIfUnlocked(
  ServiceNode node,
  const std::string& candidateHost) const
{
    uint64_t cost = 0;

    for (const auto& [edge, state] : edges) {
        if (!isAcceptedEdge(state)) {
            continue;
        }

        const bool incident = edge.caller == node || edge.callee == node;
        if (!incident) {
            continue;
        }

        std::string callerHost;
        std::string calleeHost;

        if (edge.caller == node) {
            callerHost = candidateHost;
        } else {
            auto it = placement.find(edge.caller);
            if (it == placement.end()) {
                continue;
            }
            callerHost = it->second;
        }

        if (edge.callee == node) {
            calleeHost = candidateHost;
        } else {
            auto it = placement.find(edge.callee);
            if (it == placement.end()) {
                continue;
            }
            calleeHost = it->second;
        }

        if (callerHost != calleeHost) {
            cost += state.observations;
        }
    }

    return cost;
}

// -------------------------------------------------------------
// Public read API
// -------------------------------------------------------------

void RpcDependencyGraph::pruneStaleEdges()
{
    faabric::util::FullLock lock(mx);
    pruneStaleEdgesUnlocked();
}

uint64_t RpcDependencyGraph::remoteIncidentEdgeCost(ServiceNode node) const
{
    faabric::util::SharedLock lock(mx);

    auto it = placement.find(node);
    if (it == placement.end()) {
        return 0;
    }

    return incidentEdgeCostIfUnlocked(node, it->second);
}

// -------------------------------------------------------------
// recommendation
// -------------------------------------------------------------

std::optional<std::string> RpcDependencyGraph::recommendHost(
  ServiceNode node,
  const faabric::batch_scheduler::HostMap& hostMap)
{
    faabric::util::FullLock lock(mx);

    // Cooldown: refuse to suggest moving a node we just suggested moving.
    const auto now = std::chrono::steady_clock::now();
    auto cooldownIt = lastRecommendedAt.find(node);
    if (cooldownIt != lastRecommendedAt.end() &&
        now - cooldownIt->second < COOLDOWN) {
        return std::nullopt;
    }

    pruneStaleEdgesUnlocked();

    auto currentIt = placement.find(node);
    if (currentIt == placement.end()) {
        return std::nullopt;
    }

    const std::string currentHost = currentIt->second;
    const uint64_t currentCost =
      incidentEdgeCostIfUnlocked(node, currentHost);

    auto candidates = candidateHostsForUnlocked(node);
    if (candidates.empty()) {
        return std::nullopt;
    }

    std::optional<std::string> bestHost;
    uint64_t bestCost = currentCost;

    for (const auto& candidateHost : candidates) {
        if (candidateHost == currentHost) {
            continue;
        }
        if (!hostHasCapacity(hostMap, candidateHost)) {
            continue;
        }

        uint64_t candidateCost =
          incidentEdgeCostIfUnlocked(node, candidateHost);
        if (candidateCost < bestCost) {
            bestCost = candidateCost;
            bestHost = candidateHost;
        }
    }

    if (!bestHost.has_value()) {
        return std::nullopt;
    }

    const uint64_t benefit = currentCost - bestCost;
    if (benefit < MIN_BENEFIT) {
        return std::nullopt;
    }

    // Record the recommendation timestamp so we don't oscillate.
    lastRecommendedAt[node] = now;

    SPDLOG_DEBUG(
      "RPC dependency graph recommends app={} msg={} move {} -> {} "
      "(weighted cost {} -> {}, benefit {})",
      node.appId,
      node.msgId,
      currentHost,
      bestHost.value(),
      currentCost,
      bestCost,
      benefit);

    return bestHost;
}

} // namespace faabric::rpc