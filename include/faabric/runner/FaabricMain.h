#pragma once

#include <faabric/executor/ExecutorFactory.h>
#include <faabric/scheduler/FunctionCallServer.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotServer.h>
#include <faabric/state/StateServer.h>
#include <faabric/rpc/RpcServer.h>
#include <faabric/transport/PointToPointServer.h>
#include <faabric/util/config.h>

namespace faabric::runner {
class FaabricMain
{
  public:
    FaabricMain(std::shared_ptr<faabric::executor::ExecutorFactory> fac);

    void startBackground();

    void startRunner();

    void startFunctionCallServer();

    void startStateServer();

    void startSnapshotServer();

    void startPointToPointServer();

    void startRpcServer();
    void registerRpcHandler(const std::string& name,
                            faabric::rpc::RpcHandler handler);

    void shutdown();

  private:
    faabric::state::StateServer stateServer;
    faabric::scheduler::FunctionCallServer functionServer;
    faabric::snapshot::SnapshotServer snapshotServer;
    faabric::transport::PointToPointServer pointToPointServer;
    faabric::rpc::RpcServer rpcServer;
};
}
