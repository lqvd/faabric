#include <faabric/rpc/RpcClientTransport.h>

#include <faabric/rpc/rpc.h>
#include <faabric/rpc/RpcMessageType.h>
#include <faabric/transport/common.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>

#include <stdexcept>

namespace faabric::rpc {

RpcClientTransport::RpcClientTransport(const std::string& hostIn,
                                       int asyncPortIn,
                                       int syncPortIn,
                                       int timeoutMs)
  : client(hostIn, asyncPortIn, syncPortIn, timeoutMs)
{}

void RpcClientTransport::sendRequestAsync(uint32_t requestId,
                                          const faabric::RpcRequest& reqIn)
{
    faabric::RpcRequest req = reqIn;
    req.set_requestid(requestId);
    req.set_replyhost(faabric::util::getSystemConfig().endpointHost);
    req.set_replyport(RPC_ASYNC_PORT);

    {
        std::lock_guard<std::mutex> lock(mx);
        auto [it, inserted] = ops.emplace(requestId, RpcOp{});
        if (!inserted) {
            throw std::runtime_error("Duplicate RPC request ID");
        }
    }

    std::string buffer;
    if (!req.SerializeToString(&buffer)) {
        std::lock_guard<std::mutex> lock(mx);
        ops.erase(requestId);
        throw std::runtime_error("Failed to serialise RpcRequest");
    }

    try {
        client.asyncSend(faabric::rpc::RpcMessageType::INVOKE,
                         reinterpret_cast<const uint8_t*>(buffer.data()),
                         buffer.size(),
                         requestId);
    } catch (...) {
        std::lock_guard<std::mutex> lock(mx);
        ops.erase(requestId);
        throw;
    }

    SPDLOG_TRACE("RPC - Sent async request {}", requestId);
}

bool RpcClientTransport::testResponse(uint32_t requestId)
{
    std::lock_guard<std::mutex> lock(mx);

    auto it = ops.find(requestId);
    if (it == ops.end()) {
        return false;
    }

    return it->second.ready;
}

bool RpcClientTransport::peekResponse(uint32_t requestId,
                                       faabric::RpcResponse& out)
{
    std::lock_guard<std::mutex> lock(mx);

    auto it = ops.find(requestId);
    if (it == ops.end() || !it->second.ready) {
        return false;
    }

    if (it->second.failed) {
        throw std::runtime_error("RPC operation completed with failure");
    }

    out = it->second.response;
    return true;
}

bool RpcClientTransport::getResponse(uint32_t requestId,
                                     faabric::RpcResponse& out)
{
    std::lock_guard<std::mutex> lock(mx);

    auto it = ops.find(requestId);
    if (it == ops.end() || !it->second.ready) {
        return false;
    }

    if (it->second.failed) {
        ops.erase(it);
        throw std::runtime_error("RPC operation completed with failure");
    }

    out = it->second.response;
    ops.erase(it);
    return true;
}

faabric::RpcResponse RpcClientTransport::waitForResponse(uint32_t requestId)
{
    std::unique_lock<std::mutex> lock(mx);

    cv.wait(lock, [&] {
        auto it = ops.find(requestId);
        return it != ops.end() && it->second.ready;
    });

    auto it = ops.find(requestId);
    if (it == ops.end()) {
        throw std::runtime_error("Unknown RPC request ID");
    }

    if (it->second.failed) {
        ops.erase(it);
        throw std::runtime_error("RPC operation completed with failure");
    }

    faabric::RpcResponse resp = it->second.response;
    ops.erase(it);
    return resp;
}

void RpcClientTransport::onResponseReceived(const faabric::RpcResponse& resp)
{
    std::lock_guard<std::mutex> lock(mx);

    auto it = ops.find(resp.requestid());
    if (it == ops.end()) {
        SPDLOG_WARN("RPC - Received response for unknown request {}",
                    resp.requestid());
        return;
    }

    it->second.ready = true;
    it->second.failed = false;
    it->second.response = resp;

    cv.notify_all();
}

bool RpcClientTransport::hasPendingRequest(uint32_t requestId)
{
    std::lock_guard<std::mutex> lock(mx);

    auto it = ops.find(requestId);
    if (it == ops.end()) {
        return false;
    }

    return !it->second.ready;
}

void RpcClientTransport::eraseRequest(uint32_t requestId)
{
    std::lock_guard<std::mutex> lock(mx);
    ops.erase(requestId);
}

} // namespace faabric::rpc