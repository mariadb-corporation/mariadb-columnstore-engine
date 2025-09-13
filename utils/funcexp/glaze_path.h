#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <cctype>
#include <glaze/glaze.hpp>

namespace funcexp {
namespace glaze_path {

enum class StepKind { Key, KeyWildcard, Index, IndexWildcard, IndexRange, RecursiveDescent };

struct Step {
  StepKind kind{StepKind::Key};
  std::string key; // for Key
  // For Index
  int index{0};    // direct index (may be negative)
  bool from_end{false}; // when true, index represents offset from end: last-index
  // For IndexRange
  int start_index{0}; bool start_from_end{false};
  int end_index{0};   bool end_from_end{false};
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
      // capture until ']'
      size_t content_start = i;
      while (!at_end() && p[i] != ']') ++i;
      if (at_end()) return false;
      std::string content = std::string(p.substr(content_start, i - content_start));
      ++i; // consume ']'
      // trim spaces
      auto trim = [](std::string& s){
        size_t a = 0; while (a < s.size() && std::isspace(static_cast<unsigned char>(s[a]))) ++a;
        size_t b = s.size(); while (b > a && std::isspace(static_cast<unsigned char>(s[b-1]))) --b;
        s = s.substr(a, b - a);
      };
      trim(content);
      if (content == "*") {
        Step st; st.kind = StepKind::IndexWildcard;
        out.push_back(st);
        continue;
      }
      // helper to parse single index token: number | last | last-N
      auto parse_index_token = [&](const std::string& tok, int& idx, bool& from_end)->bool{
        std::string t = tok; trim(t);
        if (t.rfind("last", 0) == 0) {
          from_end = true;
          idx = 0;
          if (t.size() > 4) {
            if (t[4] != '-' || t.size() == 5) return false;
            std::string off = t.substr(5);
            if (off.empty()) return false;
            for (char c : off) if (!std::isdigit(static_cast<unsigned char>(c))) return false;
            idx = std::stoi(off);
          }
          return true;
        }
        // numeric index (may start with '-')
        if (t.empty()) return false;
        size_t k = 0; if (t[0] == '-') ++k; if (k >= t.size()) return false;
        for (; k < t.size(); ++k) if (!std::isdigit(static_cast<unsigned char>(t[k]))) return false;
        from_end = false; idx = std::stoi(t);
        return true;
      };
      // check for range "a to b"
      auto pos_to = content.find("to");
      if (pos_to != std::string::npos) {
        std::string left = content.substr(0, pos_to);
        std::string right = content.substr(pos_to + 2);
        trim(left); trim(right);
        Step st; st.kind = StepKind::IndexRange;
        if (!parse_index_token(left, st.start_index, st.start_from_end)) return false;
        if (!parse_index_token(right, st.end_index, st.end_from_end)) return false;
        out.push_back(st);
        continue;
      }
      // single index (supports last / last-N)
      Step st; st.kind = StepKind::Index;
      if (!parse_index_token(content, st.index, st.from_end)) return false;
      out.push_back(st);
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
        // Resolve from_end (last - offset)
        if (st.from_end) idx = static_cast<int>(arr.size()) - 1 - idx;
        // Resolve negative index as size + idx
        if (idx < 0) idx = static_cast<int>(arr.size()) + idx;
        if (idx >= 0 && static_cast<size_t>(idx) < arr.size())
          match_impl(arr[static_cast<size_t>(idx)], steps, pos + 1, out);
      }
      break;
    case StepKind::IndexRange:
      if (node.is_array()) {
        const auto& arr = node.get_array();
        auto sz = static_cast<int>(arr.size());
        int s = st.start_index;
        int e = st.end_index;
        if (st.start_from_end) s = sz - 1 - s; // last - offset
        if (st.end_from_end)   e = sz - 1 - e;
        if (s < 0) s = sz + s;
        if (e < 0) e = sz + e;
        // clamp
        if (s < 0) {
          s = 0;
        }
        if (e < 0) {
          e = 0;
        }
        if (s >= sz) {
          s = sz - 1;
        }
        if (e >= sz) {
          e = sz - 1;
        }
        if (s <= e) {
          for (int i = s; i <= e; ++i) match_impl(arr[static_cast<size_t>(i)], steps, pos + 1, out);
        }
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
        if (st.from_end) idx = static_cast<int>(arr.size()) - 1 - idx;
        if (idx < 0) idx = static_cast<int>(arr.size()) + idx;
        if (idx >= 0 && static_cast<size_t>(idx) < arr.size())
          match_impl_mut(arr[static_cast<size_t>(idx)], steps, pos + 1, out);
      }
      break;
    case StepKind::IndexRange:
      if (node.is_array()) {
        auto& arr = node.get_array();
        auto sz = static_cast<int>(arr.size());
        int s = st.start_index;
        int e = st.end_index;
        if (st.start_from_end) s = sz - 1 - s;
        if (st.end_from_end)   e = sz - 1 - e;
        if (s < 0) s = sz + s;
        if (e < 0) e = sz + e;
        if (s < 0) {
          s = 0;
        }
        if (e < 0) {
          e = 0;
        }
        if (s >= sz) {
          s = sz - 1;
        }
        if (e >= sz) {
          e = sz - 1;
        }
        if (s <= e) {
          for (int i = s; i <= e; ++i) match_impl_mut(arr[static_cast<size_t>(i)], steps, pos + 1, out);
        }
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
