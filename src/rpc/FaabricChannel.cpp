#include <faabric/rpc/FaabricChannel.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/rpc/rpc.h>
#include <faabric/transport/common.h>
#include <faabric/util/logging.h>
#include <faabric/util/timing.h>

#include <thread>
#include <chrono>

namespace faabric::rpc {

#define RPC_RETRY_TIMEOUT_MS 5000
#define MAX_RPC_RETRY_ATTEMPTS 12

#define MAKE_MSG_ENDPOINT(host, port, timeout)                     \
    std::make_unique<faabric::transport::SyncSendMessageEndpoint>( \
        host, port, RPC_RETRY_TIMEOUT_MS);                         \

FaabricChannel::FaabricChannel(const std::string& host, int port)
  : targetHost(host),
    targetPort(port)
{
    endpoint = MAKE_MSG_ENDPOINT(host, port, RPC_RETRY_TIMEOUT_MS);
    SPDLOG_TRACE("RPC - Created FaabricChannel to {}:{}", host, port);
}

FaabricChannel::FaabricChannel(const std::string& host)
  : targetHost(host),
    targetPort(RPC_SYNC_PORT)
{
    endpoint = MAKE_MSG_ENDPOINT(host, RPC_SYNC_PORT, RPC_RETRY_TIMEOUT_MS);
    SPDLOG_TRACE("RPC - Created FaabricChannel to {}:{}", host, RPC_SYNC_PORT);
}

int FaabricChannel::syncCall(const std::string&    method,
                             const uint8_t*        reqBuffer,
                             int32_t               reqLength,
                             std::vector<uint8_t>& out)
{
    faabric::RpcRequest req;
    req.set_method(method);
    req.set_payload(reqBuffer, reqLength);

    std::string serialised;
    if (!req.SerializeToString(&serialised)) {
        SPDLOG_ERROR("RPC - Failed to serialise RpcRequest for method {}",
                     method);
        return Rpc_StatusCode::INTERNAL;
    }

    for (int attempt = 0; attempt < MAX_RPC_RETRY_ATTEMPTS; attempt++) {
        try {
            faabric::transport::Message responseMsg = endpoint->sendAwaitResponse(
                0,
                reinterpret_cast<const uint8_t*>(serialised.data()),
                serialised.size());

            faabric::RpcResponse resp;
            if (!resp.ParseFromArray(responseMsg.data().data(),
                                     responseMsg.data().size())) {
                SPDLOG_ERROR("RPC - Failed to parse RpcResponse for method {}",
                             method);
                return Rpc_StatusCode::INTERNAL;
            }

            if (resp.statuscode() != 0) {
                SPDLOG_ERROR("RPC - Remote error {} for method {}",
                             resp.statuscode(), method);
                return resp.statuscode();
            }

            const std::string& payload = resp.payload();
            out.assign(payload.begin(), payload.end());
            return 0;
        } catch (faabric::transport::MessageTimeoutException& ex) {
            SPDLOG_WARN(
                "RPC - Timeout calling {} on {}. Attempt {}/{} (msg: {})", 
                method, targetHost, attempt + 1, MAX_RPC_RETRY_ATTEMPTS,
                ex.what());

            // Open a new endpoint and resolve host again.
            endpoint =
                MAKE_MSG_ENDPOINT(targetHost, targetPort, RPC_RETRY_TIMEOUT_MS);
            // Sleep briefly to let the network / migrating task settle
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        } catch (std::exception& ex) {
             SPDLOG_ERROR("RPC - Hard networking error: {}", ex.what());
             return Rpc_StatusCode::INTERNAL;
        }
    }

    SPDLOG_ERROR("RPC - Exhausted all retries calling {} on {}",
                 method, targetHost);
    return Rpc_StatusCode::INTERNAL;
}

#undef MAKE_MSG_ENDPOINT

std::string FaabricChannel::getTargetUri() const
{
    return "faabric://" + targetHost + ":" + std::to_string(targetPort);
}

} // namespace faabric::rpc
