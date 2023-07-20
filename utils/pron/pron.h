#pragma once

#include <stdexcept>
#include <string>
#include <unordered_map>

namespace utils
{

class Pron
{
 private:
  Pron() = default;

 public:
  using StringMap = std::unordered_map<std::string, std::string>;

  static Pron& instance()
  {
    static Pron pron;
    return pron;
  }
  const StringMap& pron()
  {
    return pron_;
  }

  void pron(std::string& pron);

 private:
  StringMap pron_;
};

}  // namespace utils