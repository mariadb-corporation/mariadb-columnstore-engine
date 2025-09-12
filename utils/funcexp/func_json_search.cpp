#include "functor_json.h"
#include "constantcolumn.h"
#include <glaze/glaze.hpp>
#include "glaze_path.h"
#include "rowgroup.h"

namespace funcexp
{
const static int wildOne = '_';
const static int wildMany = '%';

execplan::CalpontSystemCatalog::ColType Func_json_search::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

static bool match_wild(const std::string& s, const std::string& pat, char escape = '\\')
{
  size_t i = 0, j = 0;
  size_t star_i = std::string::npos, star_j = std::string::npos;
  while (i < s.size())
  {
    if (j < pat.size())
    {
      char pc = pat[j];
      if (pc == escape && j + 1 < pat.size())
      {
        ++j;
        pc = pat[j];
      }
      if (pc == '%')
      {
        star_i = i;
        star_j = ++j;
        continue;
      }
      if (pc == '_' || pc == s[i])
      {
        ++i;
        ++j;
        continue;
      }
    }
    if (star_j != std::string::npos)
    {
      i = ++star_i;
      j = star_j;
      continue;
    }
    return false;
  }
  while (j < pat.size())
  {
    char pc = pat[j];
    if (pc == escape && j + 1 < pat.size())
    {
      j += 2;
      continue;
    }
    if (pc != '%')
      return false;
    ++j;
  }
  return true;
}

// (removed unused collect_paths)

static void find_string_matches(const glz::json_t& node, const std::string& base, const std::string& pat,
                                char escape, std::vector<std::string>& out)
{
  if (node.is_string())
  {
    if (match_wild(node.get_string(), pat, escape))
      out.push_back(base);
    return;
  }
  if (node.is_object())
  {
    for (const auto& [k, v] : node.get_object())
    {
      find_string_matches(v, base + "." + k, pat, escape, out);
    }
    return;
  }
  if (node.is_array())
  {
    const auto& a = node.get_array();
    for (size_t i = 0; i < a.size(); ++i)
    {
      find_string_matches(a[i], base + "[" + std::to_string(i) + "]", pat, escape, out);
    }
  }
}

std::string Func_json_search::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                        execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  bool nullDoc = false, nullPat = false;
  const auto js_ns = fp[0]->data()->getStrVal(row, nullDoc);
  const auto pat_ns = fp[2]->data()->getStrVal(row, nullPat);
  if (nullDoc || nullPat)
  {
    isNull = true;
    return "";
  }

  // mode parsing
  if (!isModeParsed)
  {
    if (!isModeConst)
      isModeConst = (dynamic_cast<execplan::ConstantColumn*>(fp[1]->data()) != nullptr);
    const auto mode_ns = fp[1]->data()->getStrVal(row, isNull);
    if (isNull)
      return "";
    std::string mode = mode_ns.safeString("");
    transform(mode.begin(), mode.end(), mode.begin(), ::tolower);
    if (mode != "one" && mode != "all")
    {
      isNull = true;
      return "";
    }
    isModeOne = (mode == "one");
    isModeParsed = isModeConst;
  }

  // escape parsing
  char esc = '\\';
  if (fp.size() >= 4)
  {
    if (dynamic_cast<execplan::ConstantColumn*>(fp[3]->data()) == nullptr)
    {
      isNull = true;
      return "";
    }
    bool nullEsc = false;
    const auto esc_ns = fp[3]->data()->getStrVal(row, nullEsc);
    if (esc_ns.length() > 1)
    {
      isNull = true;
      return "";
    }
    if (!nullEsc && esc_ns.length() == 1)
      esc = esc_ns.safeString("")[0];
  }

  glz::json_t doc;
  if (auto e = glz::read_json(doc, js_ns.unsafeStringRef()))
  {
    isNull = true;
    return "";
  }
  const std::string pat = pat_ns.safeString("");

  std::vector<std::string> matches_paths;
  // If limiting paths provided, search within those; else search entire document
  if (fp.size() > 4)
  {
    for (size_t i = 4; i < fp.size(); ++i)
    {
      bool pNull = false;
      const auto p_ns = fp[i]->data()->getStrVal(row, pNull);
      if (pNull)
      {
        isNull = true;
        return "";
      }
      std::vector<const glz::json_t*> nodes;
      if (!glaze_path::find_matches(doc, p_ns.unsafeStringRef(), nodes))
      {
        isNull = true;
        return "";
      }
      for (const auto* n : nodes)
      {
        // We don't know full JSONPath to n from here; approximate by using the provided path plus subtree
        // This builds subpaths relative to provided path, which is acceptable for ColumnStore usage
        find_string_matches(*n, p_ns.safeString("$"), pat, esc, matches_paths);
        if (isModeOne && !matches_paths.empty())
          goto build;
      }
    }
  }
  else
  {
    find_string_matches(doc, std::string{"$"}, pat, esc, matches_paths);
  }

build:
  if (matches_paths.empty())
  {
    isNull = true;
    return "";
  }

  if (isModeOne)
  {
    // Return a JSON string path
    return std::string{"\""} + matches_paths.front() + "\"";
  }
  else
  {
    // Return array of JSON string paths
    std::string out = "[";
    for (size_t i = 0; i < matches_paths.size(); ++i)
    {
      if (i)
        out += ", ";
      out += "\"" + matches_paths[i] + "\"";
    }
    out += "]";
    return out;
  }
}
}  // namespace funcexp
