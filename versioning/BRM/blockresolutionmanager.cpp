/* Copyright (C) 2014 InfiniDB, Inc.

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

/*****************************************************************************
 * $Id: blockresolutionmanager.cpp 1910 2013-06-18 15:19:15Z rdempsey $
 *
 ****************************************************************************/

#include <iostream>
#include <sys/types.h>
#include <memory>
#include <vector>
#include <values.h>
#include <sys/stat.h>

#include "brmtypes.h"
// #include "mastersegmenttable.h"
#include "extentmap.h"
#include "copylocks.h"
#include "vss.h"
#include "vbbm.h"
#include "slavecomm.h"
#include "blockresolutionmanager.h"
#include "IDBDataFile.h"
#include "IDBPolicy.h"

using namespace idbdatafile;
using namespace logging;
using namespace std;

namespace BRM
{
BlockResolutionManager::BlockResolutionManager(bool ronly) throw()
{
  // for (auto s : MasterSegmentTable::VssShmemTypes)
  // {
  //   vss_.emplace_back(std::unique_ptr<VSS>(new VSS(s)));
  // }

  vss_ = make_unique<VSSCluster>();

  if (ronly)
  {
    em.setReadOnly();
    vbbm.setReadOnly();
    copylocks.setReadOnly();
    // VSS by default is set to read-only.
    // vss_->setReadOnly();
  }
}

BlockResolutionManager::BlockResolutionManager(const BlockResolutionManager& brm)
{
  throw logic_error("BRM: Don't use the copy constructor.");
}

BlockResolutionManager::~BlockResolutionManager() throw()
{
}

BlockResolutionManager& BlockResolutionManager::operator=(const BlockResolutionManager& brm)
{
  throw logic_error("BRM: Don't use the = operator.");
}

int BlockResolutionManager::loadExtentMap(const string& filename, bool fixFL)
{
  em.load(filename, fixFL);
  return 0;
}

int BlockResolutionManager::saveExtentMap(const string& filename)
{
  em.save(filename);
  return 0;
}

int BlockResolutionManager::saveState(string filename) throw()
{
  string emFilename = filename + "_em";
  // string vssFilename = filename + "_vss";
  string vbbmFilename = filename + "_vbbm";
  string journalFilename = filename + "_journal";

  bool locked[2] = {false, false};
  // std::vector<bool> vssIsLocked(MasterSegmentTable::VssShmemTypes.size(), false);
  // assert(vssIsLocked.size() == vss_.size());

  try
  {
    vbbm.lock(VBBM::READ);
    locked[0] = true;

    vss_->lock_(VSSCluster::READ);

    saveExtentMap(emFilename);

    // truncate the file if already exists since no truncate in HDFS.
    const char* filename_p = journalFilename.c_str();

    IDBDataFile* journal =
        IDBDataFile::open(IDBPolicy::getType(filename_p, IDBPolicy::WRITEENG), filename_p, "wb", 0);
    delete journal;

    vbbm.save(vbbmFilename);

    vss_->save(filename);
    vss_->release(VSSCluster::READ);

    vbbm.release(VBBM::READ);
    locked[0] = false;
  }
  catch (exception& e)
  {
    // WIP
    vss_->releaseIfNeeded(VSSCluster::READ);
    // assert(vssIsLocked.size() == vss_.size());
    // for (size_t i = 0; auto& v : vss_)
    // {
    //   if (vssIsLocked[i++])
    //   {
    //     v->release(VSS::READ);
    //   }
    // }

    if (locked[0])
      vbbm.release(VBBM::READ);

    cout << e.what() << endl;
    return -1;
  }

  return 0;
}

int BlockResolutionManager::loadState(string filename, bool fixFL) throw()
{
  string emFilename = filename + "_em";
  // string vssFilename = filename + "_vss";
  string vbbmFilename = filename + "_vbbm";
  bool locked[2] = {false, false};
  // std::vector<bool> vssIsLocked(VssFactor, false);
  // assert(vssIsLocked.size() == vss_.size());

  try
  {
    vbbm.lock(VBBM::WRITE);
    locked[0] = true;

    vss_->lock_(VSSCluster::WRITE);

    // for (size_t i = 0; auto& v : vss_)
    // {
    //   assert(i < MasterSegmentTable::VssShmemTypes.size());
    //   v->lock_(VSS::WRITE);
    //   vssIsLocked[i++] = true;
    // }

    loadExtentMap(emFilename, fixFL);
    vbbm.load(vbbmFilename);

    // for (size_t i = 0; auto& v : vss_)
    // {
    //   assert(i < MasterSegmentTable::VssShmemTypes.size());
    //   // The vss image filename numeric suffix begins with 1.
    //   v->load(vssFilename + std::to_string(i + 1));
    //   v->release(VSS::WRITE);
    //   vssIsLocked[i++] = false;
    // }

    vss_->load(filename);

    vss_->release(VSSCluster::WRITE);

    vbbm.release(VBBM::WRITE);
    locked[0] = false;
  }
  catch (exception& e)
  {
    // assert(vssIsLocked.size() == vss_.size());
    // for (size_t i = 0; auto& v : vss_)
    // {
    //   if (vssIsLocked[i++])
    //   {
    //     v->release(VSS::WRITE);
    //   }
    // }

    vss_->releaseIfNeeded(VSSCluster::WRITE);

    if (locked[0])
      vbbm.release(VBBM::WRITE);

    cout << e.what() << endl;
    return -1;
  }

  return 0;
}

int BlockResolutionManager::replayJournal(string prefix) throw()
{
  SlaveComm sc;
  int err = -1;

  try
  {
    err = sc.replayJournal(prefix);
  }
  catch (exception& e)
  {
    cout << e.what();
  }

  return err;
}

}  // namespace BRM
