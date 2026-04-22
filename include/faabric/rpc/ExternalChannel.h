#pragma once

#include <faabric/rpc/IChannel.h>

#include <string>
#include <vector>

namespace faabric::rpc {

class ExternalChannel final : public IChannel
{
  public:
    explicit ExternalChannel(const std::string& uri);

    int syncCall(const std::string&    method,
                 const uint8_t*        reqBuffer,
                 int32_t               reqLength,
                 std::vector<uint8_t>& out) override;

    std::string getTargetUri() const override;

  private:
    std::string uri;
};

} // namespace faabric::rpc
