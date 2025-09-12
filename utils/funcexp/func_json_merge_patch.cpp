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
    target = glz::json_t{};  // make it an object
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
        to.emplace(k, pv);
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

  std::string out;
  if (auto w = glz::write_json(target, out))
  {
    isNull = true;
    return "";
  }
  isNull = false;
  return out;
}
}  // namespace funcexp
