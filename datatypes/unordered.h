#pragma once

#if (_LIBCPP_VERSION)
#include <unordered_set>
#include <unordered_map>
namespace std
{
namespace tr1
{
using std::hash;
using std::swap;
using std::unordered_map;
using std::unordered_multimap;
using std::unordered_set;
}  // namespace tr1
}  // namespace std
#else
#include <tr1/unordered_set>
#include <tr1/unordered_map>
#endif