// #include <catch2/catch.hpp>

// #include "fixtures.h"

// #include <faabric/rpc/rpc.h>
// #include <faabric/rpc/RpcContext.h>
// #include <faabric/rpc/RpcContextRegistry.h>
// #include <faabric/rpc/RpcServer.h>
// #include <faabric/proto/faabric.pb.h>

// #include <atomic>
// #include <chrono>
// #include <thread>
// #include <vector>

// using namespace faabric::rpc;

// namespace tests {

// TEST_CASE_METHOD(RpcServerFixture,
//                  "RPC server routes to registered handler",
//                  "[rpc]")
// {
//     bool handlerCalled = false;
//     registerHandler("/pkg.Svc/Method",
//       [&](const uint8_t*, size_t, std::vector<uint8_t>&) {
//           handlerCalled = true;
//           return Rpc_Status{ Rpc_StatusCode::OK, "" };
//       });

//     int32_t ch = localChannel();
//     auto resp = doCall(ch, "/pkg.Svc/Method");

//     REQUIRE(handlerCalled);
//     REQUIRE(resp.statuscode() == Rpc_StatusCode::OK);
//     ctx->closeChannel(ch);
// }

// TEST_CASE_METHOD(RpcServerFixture,
//                  "RPC server returns UNIMPLEMENTED for unknown method",
//                  "[rpc]")
// {
//     int32_t ch = localChannel();
//     auto resp = doCall(ch, "/missing/method");

//     REQUIRE(resp.statuscode() == Rpc_StatusCode::UNIMPLEMENTED);
//     REQUIRE(resp.payload().empty());
//     ctx->closeChannel(ch);
// }

// TEST_CASE_METHOD(RpcServerFixture,
//                  "RPC server routes multiple methods independently",
//                  "[rpc]")
// {
//     std::atomic<int> calledA{ 0 }, calledB{ 0 };
//     registerHandler("/pkg.Svc/MethodA",
//       [&](const uint8_t*, size_t, std::vector<uint8_t>&) {
//           calledA++;
//           return Rpc_Status{ Rpc_StatusCode::OK, "" };
//       });
//     registerHandler("/pkg.Svc/MethodB",
//       [&](const uint8_t*, size_t, std::vector<uint8_t>&) {
//           calledB++;
//           return Rpc_Status{ Rpc_StatusCode::OK, "" };
//       });

//     int32_t ch = localChannel();
//     auto respA = doCall(ch, "/pkg.Svc/MethodA");
//     auto respB = doCall(ch, "/pkg.Svc/MethodB");

//     REQUIRE(calledA == 1);
//     REQUIRE(calledB == 1);
//     REQUIRE(respA.statuscode() == Rpc_StatusCode::OK);
//     REQUIRE(respB.statuscode() == Rpc_StatusCode::OK);
//     ctx->closeChannel(ch);
// }

// TEST_CASE_METHOD(RpcServerFixture,
//                  "RPC server last registration wins for duplicate method",
//                  "[rpc]")
// {
//     registerHandler("/pkg.Svc/Method",
//       [](const uint8_t*, size_t, std::vector<uint8_t>& out) {
//           out = { 'A' };
//           return Rpc_Status{ Rpc_StatusCode::OK, "" };
//       });
//     registerHandler("/pkg.Svc/Method",
//       [](const uint8_t*, size_t, std::vector<uint8_t>& out) {
//           out = { 'B' };
//           return Rpc_Status{ Rpc_StatusCode::OK, "" };
//       });

//     int32_t ch = localChannel();
//     auto resp = doCall(ch, "/pkg.Svc/Method");

//     REQUIRE(resp.statuscode() == Rpc_StatusCode::OK);
//     REQUIRE(resp.payload() == "B");
//     ctx->closeChannel(ch);
// }

// TEST_CASE_METHOD(RpcServerFixture,
//                  "RPC server delivers request payload to handler",
//                  "[rpc]")
// {
//     std::string captured;
//     registerHandler("/pkg.Svc/Echo",
//       [&](const uint8_t* data, size_t len, std::vector<uint8_t>&) {
//           captured.assign(reinterpret_cast<const char*>(data), len);
//           return Rpc_Status{ Rpc_StatusCode::OK, "" };
//       });

//     int32_t ch = localChannel();
//     doCall(ch, "/pkg.Svc/Echo", "hello server");

//     REQUIRE(captured == "hello server");
//     ctx->closeChannel(ch);
// }

// TEST_CASE_METHOD(RpcServerFixture,
//                  "RPC server echoes response payload to caller",
//                  "[rpc]")
// {
//     registerHandler("/pkg.Svc/Echo",
//       [](const uint8_t* data, size_t len, std::vector<uint8_t>& out) {
//           out.assign(data, data + len);
//           return Rpc_Status{ Rpc_StatusCode::OK, "" };
//       });

//     int32_t ch = localChannel();
//     auto resp = doCall(ch, "/pkg.Svc/Echo", "echo me");

//     REQUIRE(resp.statuscode() == Rpc_StatusCode::OK);
//     REQUIRE(resp.payload() == "echo me");
//     ctx->closeChannel(ch);
// }

// TEST_CASE_METHOD(RpcServerFixture,
//                  "RPC server produces no payload when handler writes nothing",
//                  "[rpc]")
// {
//     registerHandler("/pkg.Svc/Noop",
//       [](const uint8_t*, size_t, std::vector<uint8_t>&) {
//           return Rpc_Status{ Rpc_StatusCode::OK, "" };
//       });

//     int32_t ch = localChannel();
//     auto resp = doCall(ch, "/pkg.Svc/Noop");

//     REQUIRE(resp.statuscode() == Rpc_StatusCode::OK);
//     REQUIRE(resp.payload().empty());
//     ctx->closeChannel(ch);
// }

// TEST_CASE_METHOD(RpcServerFixture,
//                  "RPC server receives zero length when payload is empty",
//                  "[rpc]")
// {
//     size_t receivedLen = 1;
//     registerHandler("/pkg.Svc/Noop",
//       [&](const uint8_t*, size_t len, std::vector<uint8_t>&) {
//           receivedLen = len;
//           return Rpc_Status{ Rpc_StatusCode::OK, "" };
//       });

//     int32_t ch = localChannel();
//     doCall(ch, "/pkg.Svc/Noop");

//     REQUIRE(receivedLen == 0);
//     ctx->closeChannel(ch);
// }

// TEST_CASE_METHOD(RpcServerFixture,
//                  "RPC server preserves binary payload bytes",
//                  "[rpc]")
// {
//     const std::string reqPayload("\x00\x01\x02\x7f\x80\xff", 6);
//     registerHandler("/pkg.Svc/Binary",
//       [](const uint8_t* data, size_t len, std::vector<uint8_t>& out) {
//           out.assign(data, data + len);
//           return Rpc_Status{ Rpc_StatusCode::OK, "" };
//       });

//     int32_t ch = localChannel();
//     auto resp = doCall(ch, "/pkg.Svc/Binary", reqPayload);

//     REQUIRE(resp.statuscode() == Rpc_StatusCode::OK);
//     REQUIRE(resp.payload() == reqPayload);
//     ctx->closeChannel(ch);
// }

// TEST_CASE_METHOD(RpcServerFixture,
//                  "RPC server handles large payload",
//                  "[rpc]")
// {
//     std::string reqPayload(1024 * 1024, '\0');
//     for (size_t i = 0; i < reqPayload.size(); i++)
//         reqPayload[i] = static_cast<char>(i % 251);

//     registerHandler("/pkg.Svc/Large",
//       [](const uint8_t* data, size_t len, std::vector<uint8_t>& out) {
//           out.assign(data, data + len);
//           return Rpc_Status{ Rpc_StatusCode::OK, "" };
//       });

//     int32_t ch = localChannel();
//     auto resp = doCall(ch, "/pkg.Svc/Large", reqPayload);

//     REQUIRE(resp.statuscode() == Rpc_StatusCode::OK);
//     REQUIRE(resp.payload().size() == reqPayload.size());
//     REQUIRE(resp.payload() == reqPayload);
//     ctx->closeChannel(ch);
// }

// TEST_CASE_METHOD(RpcServerFixture,
//                  "RPC server propagates non-zero status from handler",
//                  "[rpc]")
// {
//     registerHandler("/pkg.Svc/Fail",
//       [](const uint8_t*, size_t, std::vector<uint8_t>&) {
//           return Rpc_Status{ Rpc_StatusCode::NOT_FOUND, "" };
//       });

//     int32_t ch = localChannel();
//     auto resp = doCall(ch, "/pkg.Svc/Fail");

//     REQUIRE(resp.statuscode() == Rpc_StatusCode::NOT_FOUND);
//     REQUIRE(resp.payload().empty());
//     ctx->closeChannel(ch);
// }

// TEST_CASE_METHOD(RpcServerFixture,
//                  "RPC server response carries correct request ID",
//                  "[rpc]")
// {
//     registerHandler("/pkg.Svc/Noop",
//       [](const uint8_t*, size_t, std::vector<uint8_t>&) {
//           return Rpc_Status{ Rpc_StatusCode::OK, "" };
//       });

//     int32_t ch = localChannel();
//     uint32_t requestId = ctx->startUnary(ch, "/pkg.Svc/Noop", nullptr, 0);
//     auto resp = pollResult(requestId);

//     REQUIRE(resp.requestid() == requestId);
//     ctx->closeChannel(ch);
// }

// TEST_CASE_METHOD(RpcServerFixture,
//                  "RPC server UNIMPLEMENTED response carries correct request ID",
//                  "[rpc]")
// {
//     int32_t ch = localChannel();
//     uint32_t requestId = ctx->startUnary(ch, "/missing/method", nullptr, 0);
//     auto resp = pollResult(requestId);

//     REQUIRE(resp.statuscode() == Rpc_StatusCode::UNIMPLEMENTED);
//     REQUIRE(resp.requestid() == requestId);
//     ctx->closeChannel(ch);
// }

// TEST_CASE_METHOD(RpcServerFixture,
//                  "RPC server handles sequential requests on one channel",
//                  "[rpc]")
// {
//     std::atomic<int> callCount{ 0 };
//     registerHandler("/pkg.Svc/Count",
//       [&](const uint8_t*, size_t, std::vector<uint8_t>&) {
//           callCount++;
//           return Rpc_Status{ Rpc_StatusCode::OK, "" };
//       });

//     int32_t ch = localChannel();
//     for (int i = 0; i < 5; i++) {
//         auto resp = doCall(ch, "/pkg.Svc/Count");
//         REQUIRE(resp.statuscode() == Rpc_StatusCode::OK);
//     }

//     REQUIRE(callCount == 5);
//     ctx->closeChannel(ch);
// }

// TEST_CASE_METHOD(RpcServerFixture,
//                  "RPC server invokes handlers concurrently",
//                  "[rpc]")
// {
//     // Thread pool is hardcoded to 4 in RpcServer::RpcServer().
//     constexpr int N = 4;
//     std::atomic<int> entered{ 0 };

//     registerHandler("/pkg.Svc/Block",
//       [&](const uint8_t*, size_t, std::vector<uint8_t>&) {
//           entered.fetch_add(1, std::memory_order_acq_rel);
//           const auto deadline =
//             std::chrono::steady_clock::now() + std::chrono::seconds(5);
//           while (entered.load(std::memory_order_acquire) < N) {
//               if (std::chrono::steady_clock::now() >= deadline)
//                   FAIL("Timed out — server is likely serialising dispatch");
//               std::this_thread::sleep_for(std::chrono::milliseconds(1));
//           }
//           return Rpc_Status{ Rpc_StatusCode::OK, "" };
//       });

//     std::vector<std::jthread> threads;
//     std::vector<int> statuses(N, -1);

//     for (int i = 0; i < N; i++) {
//         threads.emplace_back([&, i] {
//             // Each thread gets its own context under a unique synthetic msgId.
//             // nextTestMsgId() comes from RpcTestFixture and is process-unique.
//             int32_t threadMsgId = nextTestMsgId();

//             auto localCtx = std::make_shared<RpcContext>(threadMsgId);
//             getRpcContextRegistry().registerContext(threadMsgId, localCtx);

//             int32_t  ch  = localCtx->createChannel("faabric://localhost");
//             uint32_t rid = localCtx->startUnary(ch, "/pkg.Svc/Block", nullptr, 0);

//             while (!localCtx->testResponse(rid))
//                 std::this_thread::sleep_for(std::chrono::milliseconds(1));

//             faabric::RpcResponse resp;
//             localCtx->getResponse(rid, resp);
//             statuses.at(i) = resp.statuscode();

//             localCtx->closeChannel(ch);
//             getRpcContextRegistry().removeContext(threadMsgId);
//         });
//     }

//     for (auto& t : threads)
//         if (t.joinable())
//             t.join();

//     REQUIRE(entered.load() == N);
//     for (int i = 0; i < N; i++)
//         REQUIRE(statuses.at(i) == Rpc_StatusCode::OK);
// }

// } // namespace tests