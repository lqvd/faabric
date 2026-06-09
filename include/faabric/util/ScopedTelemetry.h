#include <faabric/planner/PlannerClient.h>

#include <chrono>
#include <cstdint>
#include <string>

namespace faabric::util {

static inline int64_t usSince(std::chrono::steady_clock::time_point t)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now() - t).count();
}

// Use to report timing of a scope.
class ScopedTelemetry
{
  public:
    ScopedTelemetry(int32_t appId, int32_t msgId, std::string label)
      : appId(appId)
      , msgId(msgId)
      , label(std::move(label))
      , start(std::chrono::steady_clock::now())
    {}

    ~ScopedTelemetry()
    {
        try {
            auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - start).count();
            faabric::planner::getPlannerClient().reportTelemetry(
              appId, msgId, label, us);
        } catch (...) { }
    }

  private:
    int32_t appId;
    int32_t msgId;
    std::string label;
    std::chrono::steady_clock::time_point start;
};

}