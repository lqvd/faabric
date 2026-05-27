#pragma once

#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/proto/faabric.pb.h>

#include <chrono>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace faabric::rpc {

class RpcDependencyGraph
{
  public:
    struct ServiceNode
    {
        int32_t appId = 0;
        int32_t msgId = 0;

        bool operator==(const ServiceNode& other) const
        {
            return appId == other.appId && msgId == other.msgId;
        }
    };

    // -----------------------------------
    // telemetry / placement
    // -----------------------------------

    void mergeTelemetry(const faabric::RpcDependencyBatch& batch);

    void setPlacement(ServiceNode node, const std::string& host);

    void removePlacement(ServiceNode node);

    void clear();

    // -----------------------------------
    // recommendation
    // -----------------------------------

    std::optional<std::string> recommendHost(
      ServiceNode node,
      const faabric::batch_scheduler::HostMap& hostMap);

    // -----------------------------------
    // public edges API
    // -----------------------------------

    uint64_t remoteIncidentEdgeCost(ServiceNode node) const;

    void pruneStaleEdges();

  private:
    mutable std::shared_mutex mx;

    static constexpr uint64_t MIN_OBSERVATIONS = 2;
    static constexpr uint64_t MIN_BENEFIT = 2;
    static constexpr auto EDGE_TTL = std::chrono::seconds(10);
    static constexpr auto COOLDOWN = std::chrono::seconds(5);

    // -----------------------------------
    // structures
    // -----------------------------------

    struct ServiceNodeHash
    {
        size_t operator()(const ServiceNode& n) const
        {
            size_t h1 = std::hash<int32_t>{}(n.appId);
            size_t h2 = std::hash<int32_t>{}(n.msgId);
            return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
        }
    };

    struct EdgeKey
    {
        ServiceNode caller;
        ServiceNode callee;

        bool operator==(const EdgeKey& other) const
        {
            return caller == other.caller && callee == other.callee;
        }
    };

    struct EdgeKeyHash
    {
        size_t operator()(const EdgeKey& e) const
        {
            ServiceNodeHash h;
            size_t h1 = h(e.caller);
            size_t h2 = h(e.callee);
            return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
        }
    };

    struct EdgeState
    {
        uint64_t observations = 0;
        std::chrono::steady_clock::time_point lastSeen =
          std::chrono::steady_clock::now();
    };

    // -----------------------------------
    // maps
    // -----------------------------------

    using PlacementMap =
      std::unordered_map<ServiceNode, std::string, ServiceNodeHash>;
    using EdgeMap = std::unordered_map<EdgeKey, EdgeState, EdgeKeyHash>;
    using CooldownMap = std::unordered_map<ServiceNode,
                                           std::chrono::steady_clock::time_point,
                                           ServiceNodeHash>;

    PlacementMap placement;
    EdgeMap edges;
    CooldownMap lastRecommendedAt;

    // -----------------------------------
    // private helpers
    // -----------------------------------

    // The helpers below assume the caller already holds the lock.

    bool isAcceptedEdge(const EdgeState& state) const;

    std::unordered_set<std::string> candidateHostsForUnlocked(
      ServiceNode node) const;

    uint64_t incidentEdgeCostIfUnlocked(
      ServiceNode node,
      const std::string& candidateHost) const;

    void pruneStaleEdgesUnlocked();

    static bool hostHasCapacity(
      const faabric::batch_scheduler::HostMap& hostMap,
      const std::string& host);
};

RpcDependencyGraph& getRpcDependencyGraph();

} // namespace faabric::rpc