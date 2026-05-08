#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/rpc/RpcContext.h>
#include <faabric/rpc/RpcServer.h>

using namespace faabric::rpc;

namespace tests {

TEST_CASE_METHOD(RpcTestFixture,
                 "Channel reports target URI",
                 "[rpc]")
{
    int32_t id = ctx->createChannel("faabric://localhost");
    REQUIRE(ctx->getChannel(id).targetUri == "faabric://localhost");
}

TEST_CASE_METHOD(RpcTestFixture,
                 "createChannel rejects external URI",
                 "[rpc]")
{
    REQUIRE_THROWS_AS(ctx->createChannel("https://example.com:443"),
                      std::runtime_error);
}

TEST_CASE_METHOD(RpcTestFixture,
                 "createChannel error contains target URI",
                 "[rpc]")
{
    const std::string uri = "https://example.com:443";
    
    REQUIRE_THROWS_WITH(ctx->createChannel(uri),
                        Catch::Matchers::Contains(uri));
}

// TEST_CASE_METHOD(RpcTestFixture,
//                  "Channel call fails when no server is listening",
//                  "[rpc]")
// {
//     faabric::RpcResponse result;

//     std::jthread clientThread([&] {
//         try {
//             int32_t channelId = ctx->createChannel("faabric://localhost");
//             uint32_t requestId = ctx->startUnary(
//               channelId,
//               "/pkg.TestSvc/TestMethod",
//               reinterpret_cast<const uint8_t*>("test"),
//               4);

//             faabric::RpcResponse resp;
//             if (!pollResultTimed(requestId, resp)) {
//                 ctx->eraseRequest(requestId);
//                 resp.set_statuscode(Rpc_StatusCode::DEADLINE_EXCEEDED);
//             }
//             result = resp;
//             ctx->closeChannel(channelId);
//         } catch (const std::exception& ex) {
//             result.set_statuscode(Rpc_StatusCode::INTERNAL);
//             result.set_errormessage(ex.what());
//         }
//     });

//     if (clientThread.joinable())
//         clientThread.join();

//     REQUIRE(result.statuscode() != Rpc_StatusCode::OK);
//     REQUIRE(result.payload().empty());
// }

TEST_CASE_METHOD(RpcServerFixture,
                 "Channel unary call round-trip",
                 "[rpc]")
{
    registerHandler("/pkg.TestSvc/TestMethod",
      [](const uint8_t* data, size_t len, std::vector<uint8_t>& out) {
          out.assign(data, data + len);
          return Rpc_Status{ Rpc_StatusCode::OK, "" };
      });

    int32_t ch = localChannel();
    auto resp = doCall(ch, "/pkg.TestSvc/TestMethod", "test payload");

    REQUIRE(resp.errormessage().empty());
    REQUIRE(resp.statuscode() == Rpc_StatusCode::OK);
    REQUIRE(resp.payload() == "test payload");
    ctx->closeChannel(ch);
}

TEST_CASE_METHOD(RpcServerFixture,
                 "Channel preserves binary payload bytes",
                 "[rpc]")
{
    const std::string reqPayload("\x00\x01\x02\x7f\x80\xff", 6);
    registerHandler("/pkg.TestSvc/Binary",
      [](const uint8_t* data, size_t len, std::vector<uint8_t>& out) {
          out.assign(data, data + len);
          return Rpc_Status{ Rpc_StatusCode::OK, "" };
      });

    int32_t ch = localChannel();
    auto resp = doCall(ch, "/pkg.TestSvc/Binary", reqPayload);

    REQUIRE(resp.errormessage().empty());
    REQUIRE(resp.statuscode() == Rpc_StatusCode::OK);
    REQUIRE(resp.payload().size() == reqPayload.size());
    REQUIRE(resp.payload() == reqPayload);
    ctx->closeChannel(ch);
}

TEST_CASE_METHOD(RpcServerFixture,
                 "Channel maps non-zero remote status",
                 "[rpc]")
{
    registerHandler("/pkg.TestSvc/TestMethod",
      [](const uint8_t*, size_t, std::vector<uint8_t>&) {
          return Rpc_Status{ 7, "" };
      });

    int32_t ch = localChannel();
    auto resp = doCall(ch, "/pkg.TestSvc/TestMethod", "test payload");

    REQUIRE(resp.errormessage().empty());
    REQUIRE(resp.statuscode() == 7);
    REQUIRE(resp.payload().empty());
    ctx->closeChannel(ch);
}

TEST_CASE_METHOD(RpcServerFixture,
                 "Channel handles unregistered method", "[rpc]")
{
    int32_t ch = localChannel();
    auto resp = doCall(ch, "/missing/method");

    REQUIRE(resp.statuscode() == Rpc_StatusCode::UNIMPLEMENTED);
    REQUIRE(resp.payload().empty());
    ctx->closeChannel(ch);
}

TEST_CASE_METHOD(RpcServerFixture,
                "Channel handles empty payload and empty response",
                "[rpc]")
{
    registerHandler("/pkg.TestSvc/Empty",
      [](const uint8_t*, size_t, std::vector<uint8_t>&) {
          return Rpc_Status{ Rpc_StatusCode::OK, "" };
      });

    int32_t ch = localChannel();
    auto resp = doCall(ch, "/pkg.TestSvc/Empty");

    REQUIRE(resp.errormessage().empty());
    REQUIRE(resp.statuscode() == Rpc_StatusCode::OK);
    REQUIRE(resp.payload().empty());
    ctx->closeChannel(ch);
}

TEST_CASE_METHOD(RpcServerFixture,
                 "Channel supports large payload roundtrip",
                 "[rpc]")
{
    std::string reqPayload(1024 * 1024, '\0');
    for (size_t i = 0; i < reqPayload.size(); i++)
        reqPayload[i] = static_cast<char>(i % 251);

    registerHandler("/pkg.TestSvc/Large",
      [](const uint8_t* data, size_t len, std::vector<uint8_t>& out) {
          out.assign(data, data + len);
          return Rpc_Status{ Rpc_StatusCode::OK, "" };
      });

    int32_t ch = localChannel();
    auto resp = doCall(ch, "/pkg.TestSvc/Large", reqPayload);

    REQUIRE(resp.errormessage().empty());
    REQUIRE(resp.statuscode() == Rpc_StatusCode::OK);
    REQUIRE(resp.payload().size() == reqPayload.size());
    REQUIRE(resp.payload() == reqPayload);
    ctx->closeChannel(ch);
}

TEST_CASE_METHOD(RpcServerFixture,
                 "Channel supports repeated calls on one channel",
                 "[rpc]")
{
    registerHandler("/pkg.TestSvc/TestMethod",
      [](const uint8_t* data, size_t len, std::vector<uint8_t>& out) {
          out.assign(data, data + len);
          return Rpc_Status{ Rpc_StatusCode::OK, "" };
      });

    int32_t ch = localChannel();
    for (int i = 0; i < 3; i++) {
        const std::string payload = "payload-" + std::to_string(i);
        auto resp = doCall(ch, "/pkg.TestSvc/TestMethod", payload);

        REQUIRE(resp.statuscode() == Rpc_StatusCode::OK);
        REQUIRE(resp.payload() == payload);
    }
    ctx->closeChannel(ch);
}

} // namespace tests