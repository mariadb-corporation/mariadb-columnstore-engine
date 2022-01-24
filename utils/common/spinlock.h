#pragma once

#include <atomic>

namespace utils
{
inline void getSpinlock(std::atomic<bool>& lock)
{
  bool _false = false;
  while (!lock.compare_exchange_weak(_false, true, std::memory_order_acquire))
    _false = false;
}

inline bool trySpinlock(std::atomic<bool>& lock)
{
  bool _false = false;
  bool ret = lock.compare_exchange_weak(_false, true, std::memory_order_acquire);
  return ret;
}

inline void releaseSpinlock(std::atomic<bool>& lock)
{
  lock.store(false, std::memory_order_release);
}

}  // namespace utils
