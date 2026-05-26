#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace faabric::rpc {

class RpcTracker
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

    struct DependencyDelta
    {
        ServiceNode caller;
        ServiceNode callee;

        std::string callerHost;
        std::string calleeHost;

        uint64_t observations = 0;
    };

    void recordDependency(int32_t callerAppId,
                          int32_t callerMsgId,
                          int32_t calleeAppId,
                          int32_t calleeMsgId,
                          const std::string& callerHost,
                          const std::string& calleeHost);

    std::vector<DependencyDelta> snapshotAndResetDeltas();

    void clear();

  private:
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
        size_t operator()(const EdgeKey& k) const
        {
            size_t h1 = std::hash<int32_t>{}(k.caller.appId);
            size_t h2 = std::hash<int32_t>{}(k.caller.msgId);
            size_t h3 = std::hash<int32_t>{}(k.callee.appId);
            size_t h4 = std::hash<int32_t>{}(k.callee.msgId);

            // Basic hash combine
            size_t h = h1;
            h ^= h2 + 0x9e3779b9 + (h << 6) + (h >> 2);
            h ^= h3 + 0x9e3779b9 + (h << 6) + (h >> 2);
            h ^= h4 + 0x9e3779b9 + (h << 6) + (h >> 2);
            return h;
        }
    };

    struct EdgeState
    {
        std::string callerHost;
        std::string calleeHost;

        uint64_t observations = 0;

        std::chrono::steady_clock::time_point lastSeen =
          std::chrono::steady_clock::now();
    };

    mutable std::mutex mx_;
    std::unordered_map<EdgeKey, EdgeState, EdgeKeyHash> edges_;
};

RpcTracker& getRpcTracker();

} // namespace faabric::rpc