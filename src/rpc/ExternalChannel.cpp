#include <faabric/rpc/ExternalChannel.h>
#include <faabric/util/logging.h>

#include <stdexcept>

namespace faabric::rpc {

ExternalChannel::ExternalChannel(const std::string& uriIn)
  : uri(uriIn)
{
    SPDLOG_WARN("RPC - ExternalChannel created for URI {} but external RPC "
                "transport is not yet implemented. Calls will throw.",
                uri);
}

int ExternalChannel::syncCall(const std::string& /*method*/,
                               const uint8_t*     /*reqBuffer*/,
                               int32_t            /*reqLength*/,
                               std::vector<uint8_t>& /*out*/)
{
    // TODO: choose a protocol for external (non-Faasm) RPC hosts.
    throw std::runtime_error(
        "ExternalChannel: RPC to non-Faasm host '" + uri +
        "' is not yet implemented. See ExternalChannel.cpp.");
}

std::string ExternalChannel::getTargetUri() const
{
    return uri;
}

} // namespace faabric::rpc
