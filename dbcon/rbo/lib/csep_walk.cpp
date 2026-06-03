/* Copyright (C) 2026 MariaDB Corporation

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

#include "csep_walk.h"

#include "execplan/outerjoinonfilter.h"

namespace optimizer
{
namespace lib
{

void collectLeavesInOuterJoinOn(execplan::ParseTree* root,
                                std::vector<execplan::ParseTree*>& out,
                                std::function<bool(execplan::TreeNode*)> predicate)
{
  if (!root)
    return;

  if (auto* ojf = dynamic_cast<execplan::OuterJoinOnFilter*>(root->data()))
  {
    // Descend into the ON-clause parse tree and collect predicate-matching
    // leaves.
    std::vector<execplan::ParseTree*> stack{ojf->pt().get()};
    while (!stack.empty())
    {
      execplan::ParseTree* n = stack.back();
      stack.pop_back();
      if (!n)
        continue;
      if (predicate(n->data()))
      {
        out.push_back(n);
        continue;
      }
      stack.push_back(n->left());
      stack.push_back(n->right());
    }
    return;
  }

  collectLeavesInOuterJoinOn(root->left(), out, predicate);
  collectLeavesInOuterJoinOn(root->right(), out, predicate);
}

}  // namespace lib
}  // namespace optimizer
