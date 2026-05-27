#include <faabric/rpc/RpcServiceResolver.h>

#include <faabric/planner/PlannerClient.h>
#include <faabric/rpc/RpcContextRegistry.h>

namespace faabric::rpc {

std::optional<faabric::planner::ServiceEndpoint>
PlannerRpcServiceResolver::resolve(const std::string& serviceName)
{
    return faabric::planner::getPlannerClient()
      .resolveServiceEndpoint(serviceName);
}

std::optional<faabric::planner::ServiceEndpoint>
MockRpcServiceResolver::resolve(const std::string& serviceName)
{
    if (!services.contains(serviceName)) {
        return std::nullopt;
    }

    auto appMsgIds = services[serviceName];

    faabric::planner::ServiceEndpoint endpoint;
    endpoint.set_servicename(serviceName);
    endpoint.set_host(faabric::util::getSystemConfig().endpointHost);
    endpoint.set_appid(appMsgIds.appId);
    endpoint.set_messageid(appMsgIds.msgId);
    return endpoint;
}

void MockRpcServiceResolver::addService(
  const std::string& serviceName, int32_t appId, int32_t msgId)
{
    services[serviceName] = { .appId = appId, .msgId = msgId };
}

} // namespace faabric::rpc