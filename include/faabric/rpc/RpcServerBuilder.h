#pragma once

#include <faabric/rpc/RpcServer.h>
#include <memory>

namespace faabric::rpc {

class ServerBuilder {
public:
    ServerBuilder() : server_(std::make_unique<RpcServer>()) {}

    void AddListeningPort(const std::string& addr, void* creds = nullptr) {}

    template <typename T>
    void RegisterService(T* service) {
        service->RegisterWithFaabric(server_.get());
    }

    std::unique_ptr<RpcServer> BuildAndStart() {
        // Just yield the server. The user will call Wait() on it!
        return std::move(server_);
    }

private:
    std::unique_ptr<RpcServer> server_;
};

}