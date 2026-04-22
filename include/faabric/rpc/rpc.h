#ifndef FAABRIC_RPC_H
#define FAABRIC_RPC_H

#include <cstdint>
#include <string>

#ifdef __cplusplus
extern "C"
{
#endif

typedef uint32_t Rpc_ChannelId;

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

struct Rpc_Status {
    int code;
    std::string message;
    bool ok() const { return code == 0; }
};

int Rpc_ChannelCreate(const char* targetUri, Rpc_ChannelId* outChannelId);

int Rpc_ChannelClose(Rpc_ChannelId channelId);

int Rpc_UnaryCall(Rpc_ChannelId channelId,
                  const char* method,
                  const uint8_t* reqBuf,
                  int32_t reqLen,
                  uint8_t** outRespBuf,
                  int32_t* outRespLen);

#ifdef __cplusplus
}
#endif

#endif