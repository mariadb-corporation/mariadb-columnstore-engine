#pragma once

#include <atomic>

// To be refactored. There are two issues with this implentation.
// 1. It can be replaced with a lock despite the memory_order_acquire/release mem order. Needs
// std::atomic_flag instead.
// 2. https://www.realworldtech.com/forum/?threadid=189711&curpostid=189723
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

// c++20 offers a combination of wait/notify methods but
// I prefer to use a simpler version of uspace spin lock.
class USpaceSpinLock
{
 public:
  USpaceSpinLock(std::atomic_flag& flag) : flag_(flag)
  {
    while (flag_.test_and_set(std::memory_order_acquire))
    {
      ;
    }
  };
  ~USpaceSpinLock()
  {
    release();
  };
  inline void release()
  {
    flag_.clear(std::memory_order_release);
  }

 private:
  std::atomic_flag& flag_;
};

}  // namespace utils
