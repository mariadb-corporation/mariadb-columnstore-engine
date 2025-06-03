#pragma once

#include <algorithm>
#include <string>

namespace datatypes
{
inline bool ASCIIStringCaseInsensetiveEquals(const std::string& left, const std::string& right)
{
  auto asciiHigher = [](char c) { return (c >= 'a' && c <= 'z') ? c - 'a' + 'A' : c; };
  return left.size() == right.size() &&
         std::equal(left.begin(), left.end(), right.begin(),
                    [&asciiHigher](char l, char r) { return asciiHigher(l) == asciiHigher(r); });
}
}  // namespace datatypes