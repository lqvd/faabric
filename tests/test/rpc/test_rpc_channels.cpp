#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/rpc/ExternalChannel.h>
#include <faabric/rpc/FaabricChannel.h>

namespace tests {

TEST_CASE_METHOD(RpcTestFixture, "FaabricChannel reports target URI", "[rpc][channels]")
{
    int port = nextPort();
    faabric::rpc::FaabricChannel ch("127.0.0.1", port);

    REQUIRE(ch.getTargetUri() == makeFaabricUri(port));
}

TEST_CASE_METHOD(RpcTestFixture, "FaabricChannel unary call round-trip", "[rpc][channels]")
{
    int port = nextPort();
    faabric::transport::SyncRecvMessageEndpoint server(port);

    RpcCallResult result;

    std::jthread clientThread([&] {
        try {
            faabric::rpc::FaabricChannel ch("127.0.0.1", port);

            std::vector<uint8_t> outBuffer;
            std::string payload = "test payload";

            result.status = 
                ch.syncCall("/pkg.TestSvc/TestMethod",
                            reinterpret_cast<const uint8_t*>(payload.data()),
                            payload.size(),
                            outBuffer);

            result.response.assign(outBuffer.begin(), outBuffer.end());
        } catch (const std::exception& ex) {
            result.error = ex.what();
        }
    });

    auto reqMsg = server.recv();
    faabric::RpcRequest req;
    REQUIRE(req.ParseFromArray(reqMsg.data().data(), reqMsg.data().size()));
    REQUIRE(req.method() == "/pkg.TestSvc/TestMethod");
    REQUIRE(req.payload() == "test payload");

    sendProtoResponse(server, 0, "test response");

    if (clientThread.joinable()) {
        clientThread.join();
    }

    REQUIRE(result.error.empty());
    REQUIRE(result.status == 0);
    REQUIRE(result.response == "test response");
}

TEST_CASE_METHOD(RpcTestFixture, "FaabricChannel preserves binary payload bytes", "[rpc][channels]")
{
    int port = nextPort();
    faabric::transport::SyncRecvMessageEndpoint server(port);

    RpcCallResult result;
    std::string reqPayload(std::string("\x00\x01\x02\x7f\x80\xff", 6));
    std::string respPayload(std::string("\xff\x10\x00\x20\x7e", 5));

    std::jthread clientThread([&] {
        try {
            faabric::rpc::FaabricChannel ch("127.0.0.1", port);

            std::vector<uint8_t> outBuffer;
            result.status = ch.syncCall("/pkg.TestSvc/Binary",
                                        reinterpret_cast<const uint8_t*>(reqPayload.data()),
                                        reqPayload.size(),
                                        outBuffer);

            result.response.assign(outBuffer.begin(), outBuffer.end());
        } catch (const std::exception& ex) {
            result.error = ex.what();
        }
    });

    auto reqMsg = server.recv();
    faabric::RpcRequest req;
    REQUIRE(req.ParseFromArray(reqMsg.data().data(), reqMsg.data().size()));
    REQUIRE(req.method() == "/pkg.TestSvc/Binary");
    REQUIRE(req.payload().size() == reqPayload.size());
    REQUIRE(req.payload() == reqPayload);

    sendProtoResponse(server, 0, respPayload);

    if (clientThread.joinable()) {
        clientThread.join();
    }

    REQUIRE(result.error.empty());
    REQUIRE(result.status == 0);
    REQUIRE(result.response.size() == respPayload.size());
    REQUIRE(result.response == respPayload);
}

TEST_CASE_METHOD(RpcTestFixture, "FaabricChannel maps non-zero remote status", "[rpc][channels]")
{
    int port = nextPort();
    faabric::transport::SyncRecvMessageEndpoint server(port);

    RpcCallResult result;

    std::jthread clientThread([&] {
        try {
            faabric::rpc::FaabricChannel ch("127.0.0.1", port);

            std::vector<uint8_t> outBuffer;
            std::string payload = "test payload";

            result.status = ch.syncCall("/pkg.TestSvc/TestMethod",
                                        reinterpret_cast<const uint8_t*>(payload.data()),
                                        payload.size(),
                                        outBuffer);

            result.response.assign(outBuffer.begin(), outBuffer.end());
        } catch (const std::exception& ex) {
            result.error = ex.what();
        }
    });

    auto reqMsg = server.recv();
    faabric::RpcRequest req;
    REQUIRE(req.ParseFromArray(reqMsg.data().data(), reqMsg.data().size()));

    sendProtoResponse(server, 7, "ignored");

    if (clientThread.joinable()) {
        clientThread.join();
    }

    REQUIRE(result.error.empty());
    REQUIRE(result.status == 7);
    REQUIRE(result.response.empty());
}

TEST_CASE_METHOD(RpcTestFixture, "FaabricChannel maps malformed response to internal", "[rpc][channels]")
{
    int port = nextPort();
    faabric::transport::SyncRecvMessageEndpoint server(port);

    RpcCallResult result;

    std::jthread clientThread([&] {
        try {
            faabric::rpc::FaabricChannel ch("127.0.0.1", port);

            std::vector<uint8_t> outBuffer;
            std::string payload = "test payload";

            result.status = ch.syncCall("/pkg.TestSvc/TestMethod",
                                        reinterpret_cast<const uint8_t*>(payload.data()),
                                        payload.size(),
                                        outBuffer);

            result.response.assign(outBuffer.begin(), outBuffer.end());
        } catch (const std::exception& ex) {
            result.error = ex.what();
        }
    });

    auto reqMsg = server.recv();
    faabric::RpcRequest req;
    REQUIRE(req.ParseFromArray(reqMsg.data().data(), reqMsg.data().size()));

    sendRawResponse(server, "not-a-protobuf-response");

    if (clientThread.joinable()) {
        clientThread.join();
    }

    REQUIRE(result.error.empty());
    REQUIRE(result.status == 13);
    REQUIRE(result.response.empty());
}

TEST_CASE_METHOD(RpcTestFixture, "FaabricChannel supports repeated calls on one channel", "[rpc][channels]")
{
    int port = nextPort();
    faabric::transport::SyncRecvMessageEndpoint server(port);

    std::string clientError;
    std::vector<int> statuses(3, -1);
    std::vector<std::string> responses(3);

    std::jthread clientThread([&] {
        try {
            faabric::rpc::FaabricChannel ch("127.0.0.1", port);

            for (int i = 0; i < 3; i++) {
                std::string payload = "payload-" + std::to_string(i);
                std::vector<uint8_t> outBuffer;

                statuses.at(i) = ch.syncCall("/pkg.TestSvc/TestMethod",
                                             reinterpret_cast<const uint8_t*>(payload.data()),
                                             payload.size(),
                                             outBuffer);

                responses.at(i).assign(outBuffer.begin(), outBuffer.end());
            }
        } catch (const std::exception& ex) {
            clientError = ex.what();
        }
    });

    for (int i = 0; i < 3; i++) {
        auto reqMsg = server.recv();
        faabric::RpcRequest req;
        REQUIRE(req.ParseFromArray(reqMsg.data().data(), reqMsg.data().size()));
        REQUIRE(req.method() == "/pkg.TestSvc/TestMethod");
        REQUIRE(req.payload() == ("payload-" + std::to_string(i)));

        sendProtoResponse(server, 0, "response-" + std::to_string(i));
    }

    if (clientThread.joinable()) {
        clientThread.join();
    }

    REQUIRE(clientError.empty());
    for (int i = 0; i < 3; i++) {
        REQUIRE(statuses.at(i) == 0);
        REQUIRE(responses.at(i) == ("response-" + std::to_string(i)));
    }
}

TEST_CASE_METHOD(RpcTestFixture, "ExternalChannel returns URI and throws on syncCall", "[rpc][channels]")
{
    faabric::rpc::ExternalChannel ch("https://example.com:443");

    REQUIRE(ch.getTargetUri() == "https://example.com:443");

    std::vector<uint8_t> out;
    const std::string payload = "x";

    REQUIRE_THROWS_AS(
      ch.syncCall("/pkg.TestSvc/TestMethod",
                  reinterpret_cast<const uint8_t*>(payload.data()),
                  payload.size(),
                  out),
      std::runtime_error);
}

TEST_CASE_METHOD(RpcTestFixture, "FaabricChannel handles empty payload and empty response", "[rpc][channels]")
{
    int port = nextPort();
    faabric::transport::SyncRecvMessageEndpoint server(port);

    RpcCallResult result;

    std::jthread clientThread([&] {
        try {
            faabric::rpc::FaabricChannel ch("127.0.0.1", port);
            std::vector<uint8_t> outBuffer;

            const std::string emptyPayload;
            result.status = ch.syncCall("/pkg.TestSvc/Empty",
                                        reinterpret_cast<const uint8_t*>(emptyPayload.data()),
                                        emptyPayload.size(),
                                        outBuffer);

            result.response.assign(outBuffer.begin(), outBuffer.end());
        } catch (const std::exception& ex) {
            result.error = ex.what();
        }
    });

    auto reqMsg = server.recv();
    faabric::RpcRequest req;
    REQUIRE(req.ParseFromArray(reqMsg.data().data(), reqMsg.data().size()));
    REQUIRE(req.method() == "/pkg.TestSvc/Empty");
    REQUIRE(req.payload().empty());

    sendProtoResponse(server, 0, "");

    if (clientThread.joinable()) {
        clientThread.join();
    }

    REQUIRE(result.error.empty());
    REQUIRE(result.status == 0);
    REQUIRE(result.response.empty());
}

TEST_CASE_METHOD(RpcTestFixture, "FaabricChannel supports large payload roundtrip", "[rpc][channels]")
{
    int port = nextPort();
    faabric::transport::SyncRecvMessageEndpoint server(port);

    RpcCallResult result;

    std::string reqPayload(1024 * 1024, '\0');
    for (size_t i = 0; i < reqPayload.size(); i++) {
        reqPayload[i] = static_cast<char>(i % 251);
    }

    std::string respPayload(1024 * 1024, '\0');
    for (size_t i = 0; i < respPayload.size(); i++) {
        respPayload[i] = static_cast<char>((i * 7) % 251);
    }

    std::jthread clientThread([&] {
        try {
            faabric::rpc::FaabricChannel ch("127.0.0.1", port);
            std::vector<uint8_t> outBuffer;

            result.status = ch.syncCall("/pkg.TestSvc/Large",
                                        reinterpret_cast<const uint8_t*>(reqPayload.data()),
                                        reqPayload.size(),
                                        outBuffer);

            result.response.assign(outBuffer.begin(), outBuffer.end());
        } catch (const std::exception& ex) {
            result.error = ex.what();
        }
    });

    auto reqMsg = server.recv();
    faabric::RpcRequest req;
    REQUIRE(req.ParseFromArray(reqMsg.data().data(), reqMsg.data().size()));
    REQUIRE(req.method() == "/pkg.TestSvc/Large");
    REQUIRE(req.payload().size() == reqPayload.size());
    REQUIRE(req.payload() == reqPayload);

    sendProtoResponse(server, 0, respPayload);

    if (clientThread.joinable()) {
        clientThread.join();
    }

    REQUIRE(result.error.empty());
    REQUIRE(result.status == 0);
    REQUIRE(result.response.size() == respPayload.size());
    REQUIRE(result.response == respPayload);
}

TEST_CASE_METHOD(RpcTestFixture, "FaabricChannel call fails when no server is listening", "[rpc][channels][slow]")
{
    int port = nextPort();

    RpcCallResult result;

    std::jthread clientThread([&] {
        try {
            faabric::rpc::FaabricChannel ch("127.0.0.1", port);
            std::vector<uint8_t> outBuffer;
            std::string payload = "test payload";

            result.status = ch.syncCall("/pkg.TestSvc/TestMethod",
                                        reinterpret_cast<const uint8_t*>(payload.data()),
                                        payload.size(),
                                        outBuffer);

            result.response.assign(outBuffer.begin(), outBuffer.end());
        } catch (const std::exception& ex) {
            result.error = ex.what();
        }
    });

    if (clientThread.joinable()) {
        clientThread.join();
    }

    REQUIRE(result.status == -1);
    REQUIRE_FALSE(result.error.empty());
    REQUIRE(result.response.empty());
}

TEST_CASE_METHOD(RpcTestFixture, "ExternalChannel error contains target URI", "[rpc][channels]")
{
    const std::string uri = "https://example.com:443";
    faabric::rpc::ExternalChannel ch(uri);

    std::vector<uint8_t> out;
    const std::string payload = "x";

    try {
        ch.syncCall("/pkg.TestSvc/TestMethod",
                    reinterpret_cast<const uint8_t*>(payload.data()),
                    payload.size(),
                    out);
        FAIL("Expected syncCall to throw");
    } catch (const std::runtime_error& ex) {
        std::string msg = ex.what();
        REQUIRE(msg.find(uri) != std::string::npos);
    }
}

} // namespace tests