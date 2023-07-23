/* Copyright (C) 2023 MariaDB Corporation.

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

#include "brmtypes.h"
#include "vss.h"
#include "sharedbrmiface.h"

namespace BRM
{
// int SlaveDBRMNode::loadState(string filename) throw()
// {
//   string emFilename = filename + "_em";
//   string vssFilename = filename + "_vss";
//   string vbbmFilename = filename + "_vbbm";
//   bool locked[2] = {false, false};
//   std::vector<bool> vssIsLocked(MasterSegmentTable::VssShmemTypes.size(), false);
//   assert(vssIsLocked.size() == vss_.size());

//   try
//   {
//     vbbm.lock(VBBM::WRITE);
//     locked[0] = true;
//     vss.lock(VSS::WRITE);
//     locked[1] = true;

//     loadExtentMap(emFilename);
//     vbbm.load(vbbmFilename);
//     vss.load(vssFilename);

//     for (size_t i = 0; auto& v : vss_)
//     {
//       assert(i < MasterSegmentTable::VssShmemTypes.size());
//       v->lock_(VSS::WRITE);
//       vssIsLocked[i] = true;
//       // The vss image filename numeric suffix begins with 1.
//       v->load(vssFilename + std::to_string(i + 1));
//       v->release(VSS::WRITE);
//       vssIsLocked[i] = false;
//       ++i;
//     }

//     vss.release(VSS::WRITE);
//     locked[1] = false;
//     vbbm.release(VBBM::WRITE);
//     locked[0] = false;
//   }
//   catch (exception& e)
//   {
//     assert(vssIsLocked.size() == vss_.size());
//     for (auto vl = begin(vssIsLocked); auto& v : vss_)
//     {
//       if (*vl)
//       {
//         v->release(VSS::WRITE);
//       }
//       ++vl;
//     }
//     if (locked[1])
//       vss.release(VSS::WRITE);

//     if (locked[0])
//       vbbm.release(VBBM::WRITE);

//     return -1;
//   }

//   return 0;
// }

SharedBRMIface::SharedBRMIface()
{
}

SharedBRMIface::~SharedBRMIface()
{
}

int SharedBRMIface::getUncommittedLBIDs_(const VER_t transID, vector<LBID_t>& lbidList,
                                         VssPtrVector& vss_) const
{
  return 0;
}

}  // namespace BRM
