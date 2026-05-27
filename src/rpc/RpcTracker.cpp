#include <faabric/rpc/RpcTracker.h>

#include <faabric/proto/faabric.pb.h>

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

faabric::RpcDependencyBatch snapshotRpcDependencyTelemetry()
{
    auto deltas = getRpcTracker().snapshotAndResetDeltas();

    faabric::RpcDependencyBatch batch;

    for (const auto& d : deltas) {
        auto* edge = batch.add_edges();

        edge->set_callerappid(d.caller.appId);
        edge->set_callermsgid(d.caller.msgId);
        edge->set_calleeappid(d.callee.appId);
        edge->set_calleemsgid(d.callee.msgId);

        edge->set_callerhost(d.callerHost);
        edge->set_calleehost(d.calleeHost);

        edge->set_observations(d.observations);
    }

    return batch;
}

RpcTracker& getRpcTracker()
{
    static RpcTracker t;
    return t;
}

} // namespace faabric::rpc