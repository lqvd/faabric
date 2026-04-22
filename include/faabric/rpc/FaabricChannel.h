#pragma once

#include <faabric/rpc/IChannel.h>
#include <faabric/transport/MessageEndpoint.h>

#include <string>
#include <vector>

namespace faabric::rpc {

class FaabricChannel final : public IChannel
{
  public:
    /**
     * @param host  IP address or hostname of the target Faasm worker.
     * @param port  Port on which the target's RPC server listens
     *              (default: RPC_SYNC_PORT).
     */
    FaabricChannel(const std::string& host, int port);
    FaabricChannel(const std::string& host);

    int syncCall(const std::string&    method,
                 const uint8_t*        reqBuffer,
                 int32_t               reqLength,
                 std::vector<uint8_t>& out) override;

    std::string getTargetUri() const override;

  private:
    std::unique_ptr<faabric::transport::SyncSendMessageEndpoint> endpoint;
    std::string targetHost;
    int targetPort;
};

} // namespace faabric::rpc
