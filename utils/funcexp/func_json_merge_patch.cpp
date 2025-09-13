#include <glaze/glaze.hpp>

#include "functor_json.h"
#include "rowgroup.h"

namespace
{
// RFC 7396 JSON Merge Patch
static void merge_patch_in_place(glz::json_t& target, const glz::json_t& patch)
{
  if (!patch.is_object())
  {
    target = patch;  // Entire document replaced
    return;
  }

  if (!target.is_object())
  {
    target = glz::json_t::object_t{};  // make it an object
  }

  auto& to = target.get_object();
  for (const auto& [k, pv] : patch.get_object())
  {
    if (pv.is_null())
    {
      to.erase(k);
    }
    else
    {
      auto it = to.find(k);
      if (it == to.end())
      {
        // When the patch member is an object and the key does not exist in target,
        // create a new object and merge recursively to honor deletion of null-leaf
        // members per RFC 7396. For non-object, assign directly.
        if (pv.is_object())
        {
          glz::json_t child = glz::json_t::object_t{};
          merge_patch_in_place(child, pv);
          to.emplace(k, std::move(child));
        }
        else
        {
          to.emplace(k, pv);
        }
      }
      else
      {
        merge_patch_in_place(it->second, pv);
      }
    }
  }
}
}  // namespace

namespace funcexp
{
execplan::CalpontSystemCatalog::ColType Func_json_merge_patch::operationType(
    FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& /*resultType*/)
{
  return fp[0]->data()->resultType();
}

std::string Func_json_merge_patch::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                             execplan::CalpontSystemCatalog::ColType& /*type*/)
{
  // JSON_MERGE_PATCH returns NULL if any argument is NULL
  bool hasNullArg = false;
  const auto base_ns = fp[0]->data()->getStrVal(row, hasNullArg);
  if (hasNullArg)
  {
    isNull = true;
    return "";
  }

  glz::json_t target;
  if (auto e = glz::read_json(target, base_ns.unsafeStringRef()))
  {
    isNull = true;
    return "";
  }

  // Preserve original target keys order for final serialization (map iteration order is sorted).
  std::vector<std::string> original_keys;
  if (target.is_object())
  {
    for (const auto& [k, v] : target.get_object()) original_keys.push_back(k);
  }

  for (size_t i = 1; i < fp.size(); ++i)
  {
    const auto patch_ns = fp[i]->data()->getStrVal(row, isNull);
    if (isNull)
    {
      // Per semantics: if any arg NULL -> NULL
      return "";
    }
    glz::json_t patch;
    if (auto e2 = glz::read_json(patch, patch_ns.unsafeStringRef()))
    {
      isNull = true;
      return "";
    }
    merge_patch_in_place(target, patch);
  }

  // Custom serialize objects to preserve: original keys first, then new keys
  if (target.is_object())
  {
    const auto& obj = target.get_object();
    std::string out;
    out.reserve(128);
    out.push_back('{');
    bool first = true;

    auto write_kv = [&](const std::string& key){
      auto it = obj.find(key);
      if (it == obj.end()) return; // key removed
      if (!first) out += ", ";
      first = false;
      out.push_back('"'); out += key; out.push_back('"'); out += ": ";
      std::string valbuf;
      if (auto w = writeJson(it->second, valbuf)) { /* on error, fall back to NULL */ valbuf = "null"; }
      out += valbuf;
    };

    // Original keys first
    for (const auto& k : original_keys) write_kv(k);
    // Then any new keys not in original
    for (const auto& [k, v] : obj)
    {
      if (std::find(original_keys.begin(), original_keys.end(), k) == original_keys.end())
        write_kv(k);
    }

    out.push_back('}');
    isNull = false;
    return out;
  }

  // Non-object: regular writer
  std::string out;
  if (auto w = writeJson(target, out))
  {
    isNull = true;
    return "";
  }
  isNull = false;
  return out;
}
}  // namespace funcexp
