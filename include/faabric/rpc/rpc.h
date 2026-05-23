#ifndef FAABRIC_RPC_H
#define FAABRIC_RPC_H

#include <cstdint>
#include <string>
#include <vector>

#ifdef __cplusplus
extern "C"
{
#endif

enum Rpc_StatusCode : int
{
    OK                  = 0,
    CANCELLED           = 1,
    UNKNOWN             = 2,
    INVALID_ARGUMENT    = 3,
    DEADLINE_EXCEEDED   = 4,
    NOT_FOUND           = 5,
    ALREADY_EXISTS      = 6,
    PERMISSION_DENIED   = 7,
    RESOURCE_EXHAUSTED  = 8,
    FAILED_PRECONDITION = 9,
    ABORTED             = 10,
    OUT_OF_RANGE        = 11,
    UNIMPLEMENTED       = 12,
    INTERNAL            = 13,
    UNAVAILABLE         = 14,
    DATA_LOSS           = 15,
    UNAUTHENTICATED     = 16,
};

int Rpc_ChannelCreate(const char* targetUri, int32_t* outChannelId);

int Rpc_ChannelClose(int32_t channelId);

int32_t __faasm_rpc_unary_start(int32_t channelId, 
                                const char* method, 
                                const uint8_t* reqBuf, 
                                int32_t reqLen, 
                                int32_t* outRequestId,
                                int32_t timeoutMs);

int32_t __faasm_rpc_test_response(int32_t requestId);

void __faasm_rpc_wait_migratable(int32_t requestId,
                                 int32_t wasmResumeTarget,
                                 int32_t frameOffset);

int32_t __faasm_rpc_get_response(int32_t requestId,
                                 int32_t* outRespOffset, 
                                 int32_t* outRespLen);

int32_t __faasm_rpc_send_response(uint32_t requestId,
                                  const char* replyHost,
                                  int32_t replyPort,
                                  int32_t statusCode,
                                  const uint8_t* payload,
                                  int32_t payloadLen,
                                  const char* errorMessage,
                                  int32_t errorMessageLen);

int32_t __faasm_rpc_get_request(int32_t wasmResumeTarget,
                                int32_t frameOffset,
                                uint32_t* outRequestIdPtr,
                                int32_t* outMethodOffsetPtr,
                                int32_t* outMethodLenPtr,
                                int32_t* outPayloadOffsetPtr,
                                int32_t* outPayloadLenPtr,
                                int32_t* outReplyHostOffsetPtr,
                                int32_t* outReplyHostLenPtr,
                                int32_t* outReplyPortPtr);

} // namespace faabric::rpc

#endif