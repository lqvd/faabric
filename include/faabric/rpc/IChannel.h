#pragma once
// This file keeps the name GrpcChannel.h for build-system compatibility.
// The class it declares is the transport-agnostic IChannel abstract interface.
// No Google gRPC types appear anywhere in this header or its includes.

#include <cstdint>
#include <string>
#include <vector>

namespace faabric::rpc {

/**
 * IChannel — one logical RPC connection to a remote host.
 *
 * Implementations:
 *   FaabricChannel  — nng SyncSendMessageEndpoint (faabric://host[:port])
 *   ExternalChannel — external / non-Faasm hosts (TODO: protocol TBD)
 */
class IChannel
{
  public:
    virtual ~IChannel() = default;

    /**
     * Synchronous unary call.
     *
     * @param method     Fully-qualified method, e.g. "/pkg.Svc/Method"
     * @param reqBuffer  Serialised protobuf request bytes
     * @param reqLength  Byte count of reqBuffer
     * @param out        Populated with serialised protobuf response bytes
     * @return           0 on success; gRPC-compatible status code on error
     */
    virtual int syncCall(const std::string&    method,
                         const uint8_t*        reqBuffer,
                         int32_t               reqLength,
                         std::vector<uint8_t>& out) = 0;

    virtual std::string getTargetUri() const = 0;
};

} // namespace faabric::rpc