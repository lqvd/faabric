#pragma once

#include <faabric/planner/planner.pb.h>

#include <unordered_map>

namespace faabric::rpc {


/* Service resolution interface for RpcContext.
 * Mainly so we can mock things when testing.
 */
class RpcServiceResolver
{
  public:
    virtual ~RpcServiceResolver() = default;

    virtual std::optional<faabric::planner::ServiceEndpoint>
    resolve(const std::string& serviceName) = 0;
};

/* Service resolution through the planner.
 */
class PlannerRpcServiceResolver final : public RpcServiceResolver
{
  public:
    std::optional<faabric::planner::ServiceEndpoint> resolve(
      const std::string& serviceName) override;
};

/* Mock service resolution.
 */
class MockRpcServiceResolver final : public faabric::rpc::RpcServiceResolver
{
  public:
    std::optional<faabric::planner::ServiceEndpoint> resolve(
      const std::string& serviceName) override;

    void addService(
      const std::string& serviceName, int32_t appId, int32_t msgId);

  private:
    struct RpcServiceLocation
    {
        int32_t appId;
        int32_t msgId;
    };

    std::unordered_map<std::string, RpcServiceLocation> services;
};

} // namespace faabric::rpc