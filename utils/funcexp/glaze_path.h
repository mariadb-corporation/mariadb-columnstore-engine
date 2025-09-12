#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <cctype>
#include <glaze/glaze.hpp>

namespace funcexp {
namespace glaze_path {

enum class StepKind { Key, KeyWildcard, Index, IndexWildcard, RecursiveDescent };

struct Step {
  StepKind kind{StepKind::Key};
  std::string key; // for Key
  int index{0};    // for Index (may be negative)
};

// Parse a simplified MariaDB/MySQL-like JSON path supporting:
//  $ root (optional)
//  .key segments
//  .* wildcard for any single key
//  [n] array index (supports negative)
//  [*] array wildcard
//  ** recursive descent (match any number of levels)
inline bool parse(std::string_view p, std::vector<Step>& out) {
  out.clear();
  size_t i = 0;
  auto at_end = [&]() { return i >= p.size(); };
  if (!at_end() && p[i] == '$') ++i;
  while (!at_end()) {
    if (p[i] == '.') {
      // consume dot and check for recursive descent
      ++i;
      if (!at_end() && p[i] == '*') {
        ++i;
        out.push_back(Step{StepKind::KeyWildcard, std::string(), 0});
        continue;
      }
      if (!at_end() && p[i] == '.') {
        // treat ".." as recursive descent
        // consume all consecutive '.' to be robust
        while (!at_end() && p[i] == '.') ++i;
        out.push_back(Step{StepKind::RecursiveDescent, std::string(), 0});
        continue;
      }
      // dot key
      size_t start = i;
      while (!at_end() && p[i] != '.' && p[i] != '[') ++i;
      if (start == i) return false;
      out.push_back(Step{StepKind::Key, std::string(p.substr(start, i - start)), 0});
      continue;
    }
    if (p[i] == '[') {
      ++i;
      if (!at_end() && p[i] == '*') {
        ++i;
        if (at_end() || p[i] != ']') return false;
        ++i;
        out.push_back(Step{StepKind::IndexWildcard, std::string(), 0});
        continue;
      }
      // parse index possibly negative
      size_t start = i;
      if (!at_end() && p[i] == '-') ++i;
      size_t num_start = i;
      while (!at_end() && std::isdigit(static_cast<unsigned char>(p[i]))) ++i;
      if (num_start == i) return false;
      int idx = std::stoi(std::string(p.substr(start, i - start)));
      if (at_end() || p[i] != ']') return false;
      ++i;
      out.push_back(Step{StepKind::Index, std::string(), idx});
      continue;
    }
    // bare key at root without leading dot
    size_t start = i;
    while (!at_end() && p[i] != '.' && p[i] != '[') ++i;
    if (start == i) return false;
    out.push_back(Step{StepKind::Key, std::string(p.substr(start, i - start)), 0});
  }
  return true;
}

inline void collect_descendants(const glz::json_t& node, std::vector<const glz::json_t*>& out) {
  out.push_back(&node);
  if (node.is_object()) {
    for (const auto& [k, v] : node.get_object()) collect_descendants(v, out);
  } else if (node.is_array()) {
    for (const auto& v : node.get_array()) collect_descendants(v, out);
  }
}

inline void match_impl(const glz::json_t& node, const std::vector<Step>& steps, size_t pos,
                       std::vector<const glz::json_t*>& out) {
  if (pos >= steps.size()) {
    out.push_back(&node);
    return;
  }
  const Step& st = steps[pos];
  switch (st.kind) {
    case StepKind::Key:
      if (node.is_object()) {
        const auto& obj = node.get_object();
        auto it = obj.find(st.key);
        if (it != obj.end()) match_impl(it->second, steps, pos + 1, out);
      }
      break;
    case StepKind::KeyWildcard:
      if (node.is_object()) {
        const auto& obj = node.get_object();
        for (const auto& [k, v] : obj) match_impl(v, steps, pos + 1, out);
      }
      break;
    case StepKind::Index:
      if (node.is_array()) {
        const auto& arr = node.get_array();
        int idx = st.index;
        if (idx < 0) idx = static_cast<int>(arr.size()) + idx;
        if (idx >= 0 && static_cast<size_t>(idx) < arr.size())
          match_impl(arr[static_cast<size_t>(idx)], steps, pos + 1, out);
      }
      break;
    case StepKind::IndexWildcard:
      if (node.is_array()) {
        const auto& arr = node.get_array();
        for (const auto& v : arr) match_impl(v, steps, pos + 1, out);
      }
      break;
    case StepKind::RecursiveDescent: {
      // Try matching at current node first
      match_impl(node, steps, pos + 1, out);
      // Then try all descendants
      if (node.is_object()) {
        for (const auto& [k, v] : node.get_object()) match_impl(v, steps, pos, out);
      } else if (node.is_array()) {
        for (const auto& v : node.get_array()) match_impl(v, steps, pos, out);
      }
      break;
    }
  }
}

// Find all matches and append to out as pointers to nodes within root
inline bool find_matches(const glz::json_t& root, std::string_view path,
                         std::vector<const glz::json_t*>& out) {
  std::vector<Step> steps;
  if (!parse(path, steps)) return false;
  match_impl(root, steps, 0, out);
  return true;
}

inline void match_impl_mut(glz::json_t& node, const std::vector<Step>& steps, size_t pos,
                           std::vector<glz::json_t*>& out) {
  if (pos >= steps.size()) { out.push_back(&node); return; }
  const Step& st = steps[pos];
  switch (st.kind) {
    case StepKind::Key:
      if (node.is_object()) {
        auto& obj = node.get_object();
        auto it = obj.find(st.key);
        if (it != obj.end()) match_impl_mut(it->second, steps, pos + 1, out);
      }
      break;
    case StepKind::KeyWildcard:
      if (node.is_object()) {
        auto& obj = node.get_object();
        for (auto& [k, v] : obj) match_impl_mut(v, steps, pos + 1, out);
      }
      break;
    case StepKind::Index:
      if (node.is_array()) {
        auto& arr = node.get_array();
        int idx = st.index;
        if (idx < 0) idx = static_cast<int>(arr.size()) + idx;
        if (idx >= 0 && static_cast<size_t>(idx) < arr.size())
          match_impl_mut(arr[static_cast<size_t>(idx)], steps, pos + 1, out);
      }
      break;
    case StepKind::IndexWildcard:
      if (node.is_array()) {
        auto& arr = node.get_array();
        for (auto& v : arr) match_impl_mut(v, steps, pos + 1, out);
      }
      break;
    case StepKind::RecursiveDescent:
      // Try here
      match_impl_mut(node, steps, pos + 1, out);
      if (node.is_object()) {
        for (auto& [k, v] : node.get_object()) match_impl_mut(v, steps, pos, out);
      } else if (node.is_array()) {
        for (auto& v : node.get_array()) match_impl_mut(v, steps, pos, out);
      }
      break;
  }
}

inline bool find_matches_mutable(glz::json_t& root, std::string_view path,
                                 std::vector<glz::json_t*>& out) {
  std::vector<Step> steps;
  if (!parse(path, steps)) return false;
  match_impl_mut(root, steps, 0, out);
  return true;
}

inline void find_matches_mutable_steps(glz::json_t& root, const std::vector<Step>& steps,
                                       std::vector<glz::json_t*>& out) {
  match_impl_mut(root, steps, 0, out);
}

} // namespace glaze_path
} // namespace funcexp
