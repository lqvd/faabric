#include <faabric/rpc/RpcTracker.h>

namespace faabric::rpc {

void RpcTracker::recordDependency(int32_t callerAppId,
                                  int32_t callerMsgId,
                                  int32_t calleeAppId,
                                  int32_t calleeMsgId,
                                  const std::string& callerHost,
                                  const std::string& calleeHost)
{
    if (callerAppId == 0 || callerMsgId == 0 ||
        calleeAppId == 0 || calleeMsgId == 0) {
        return;
    }

    if (callerHost.empty() || calleeHost.empty()) {
        return;
    }

    std::scoped_lock lock(mx_);

    EdgeKey key{
        .caller = ServiceNode{ .appId = callerAppId, .msgId = callerMsgId },
        .callee = ServiceNode{ .appId = calleeAppId, .msgId = calleeMsgId },
    };

    auto& state = edges_[key];
    state.callerHost = callerHost;
    state.calleeHost = calleeHost;
    state.observations++;
    state.lastSeen = std::chrono::steady_clock::now();
}

std::vector<RpcTracker::DependencyDelta>
RpcTracker::snapshotAndResetDeltas()
{
    std::scoped_lock lock(mx_);

    std::vector<DependencyDelta> deltas;
    deltas.reserve(edges_.size());

    for (const auto& [edge, state] : edges_) {
        if (state.observations == 0) {
            continue;
        }

        deltas.push_back(DependencyDelta{
          .caller = edge.caller,
          .callee = edge.callee,
          .callerHost = state.callerHost,
          .calleeHost = state.calleeHost,
          .observations = state.observations,
        });
    }

    edges_.clear();

    return deltas;
}

void RpcTracker::clear()
{
    std::scoped_lock lock(mx_);
    edges_.clear();
}

RpcTracker& getRpcTracker()
{
    static RpcTracker t;
    return t;
}

} // namespace faabric::rpc