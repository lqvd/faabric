#include <catch2/catch.hpp>
#include <faabric/rpc/RpcServer.h>
#include <faabric/rpc/FaabricChannel.h>
#include <faabric/proto/faabric.pb.h>

using namespace faabric::rpc;

namespace tests {

TEST_CASE("Test RPC server routing and execution", "[rpc][server]")
{
    // 1. Spin up the server
    RpcServer server;
    
    // 2. Register a dummy handler for a specific method
    bool handlerCalled = false;
    server.registerHandler("/pkg.Svc/DummyMethod", 
        [&handlerCalled](const uint8_t* reqData, size_t reqLen, std::vector<uint8_t>& respData) {
            handlerCalled = true;
            
            // Dummy logic: just return an EmptyResponse
            faabric::EmptyResponse resp;
            auto size = resp.ByteSizeLong();
            respData.resize(size);
            resp.SerializeToArray(respData.data(), size);
            
            return Rpc_Status{ Rpc_StatusCode::OK, "" };
        });

    server.start(); // Starts the background listening threads

    // 3. Create a channel pointing to this host (the server we just started)
    FaabricChannel channel("localhost");

    // 4. Send a dummy payload
    std::string method = "/pkg.Svc/DummyMethod";
    std::string payload = "test data";
    std::vector<uint8_t> outBuffer;

    int status = channel.syncCall(
        method,
        reinterpret_cast<const uint8_t*>(payload.data()),
        payload.size(),
        outBuffer
    );

    // 5. Verify the RPC Server correctly routed the request and returned OK
    REQUIRE(status == Rpc_StatusCode::OK);
    REQUIRE(handlerCalled);

    server.stop();
}

TEST_CASE("Test RPC server handles missing methods correctly", "[rpc][server]")
{
    RpcServer server;
    server.start();

    FaabricChannel channel("localhost");
    std::vector<uint8_t> outBuffer;

    // Send a method that hasn't been registered
    int status = channel.syncCall("/missing/method", nullptr, 0, outBuffer);

    // The RpcServer should trap this and return UNIMPLEMENTED without crashing
    REQUIRE(status == Rpc_StatusCode::UNIMPLEMENTED);

    server.stop();
}

} // namespace tests