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
#include <vector>
#ifdef __linux__
#include <values.h>
#endif
#include <limits>
#include <sys/stat.h>

#include "brmtypes.h"
#include "rwlock.h"
#include "extentmap.h"
#include "copylocks.h"
#include "vss.h"
#include "vbbm.h"
#include "exceptclasses.h"
#include "slavecomm.h"
#define BLOCKRESOLUTIONMANAGER_DLLEXPORT
#include "blockresolutionmanager.h"
#undef BLOCKRESOLUTIONMANAGER_DLLEXPORT
#include "scopeexit.h"
#include "IDBDataFile.h"
#include "IDBPolicy.h"

using namespace idbdatafile;
using namespace logging;
using namespace std;

namespace BRM
{
BlockResolutionManager::BlockResolutionManager(bool ronly) throw()
{
  if (ronly)
  {
    em.setReadOnly();
    vss.setReadOnly();
    vbbm.setReadOnly();
    copylocks.setReadOnly();
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
  string vssFilename = filename + "_vss";
  string vbbmFilename = filename + "_vbbm";
  string journalFilename = filename + "_journal";

  /* The three files have to agree with each other, and lock(READ) no longer
     makes that happen - it is a pin, and a pin says nothing about which
     published version it caught. The read locks writers do exclude are what
     orders them, and they are needed only while the pins are being taken: a
     pinned area cannot change, so once everything is pinned the snapshot is
     fixed and the locks go back before any file is written. See
     DBRM::saveState(), which does the same and carries the ordering argument
     in full. */
  try
  {
    vbbm.ensureDataArea();
    vss.ensureDataArea();
    copylocks.ensureDataArea();

    vbbm.lockForSave();
    ScopeExit unlockVBBM([this] { vbbm.unlockForSave(); });
    vss.lockForSave();
    ScopeExit unlockVSS([this] { vss.unlockForSave(); });
    copylocks.lockForSave();
    ScopeExit unlockCL([this] { copylocks.unlockForSave(); });

    vbbm.lock(VBBM::READ);
    ScopeExit releaseVBBM([this] { vbbm.release(VBBM::READ); });
    vss.lock(VSS::READ);
    ScopeExit releaseVSS([this] { vss.release(VSS::READ); });
    em.pinForSave();
    ScopeExit unpinEM([this] { em.unpinForSave(); });

    // Everything is pinned; the writers can have the locks back now.
    unlockCL.releaseNow();
    unlockVSS.releaseNow();
    unlockVBBM.releaseNow();

    saveExtentMap(emFilename);

    // truncate teh file if already exists since no truncate in HDFS.
    const char* filename_p = journalFilename.c_str();

    IDBDataFile* journal =
        IDBDataFile::open(IDBPolicy::getType(filename_p, IDBPolicy::WRITEENG), filename_p, "wb", 0);
    delete journal;

    vbbm.save(vbbmFilename);
    vss.save(vssFilename);
  }
  catch (exception& e)
  {
    cout << e.what() << endl;
    return -1;
  }
  catch (...)
  {
    cout << "BlockResolutionManager::saveState(): caught an exception" << endl;
    return -1;
  }

  return 0;
}

int BlockResolutionManager::loadState(string filename, bool fixFL) throw()
{
  string emFilename = filename + "_em";
  string vssFilename = filename + "_vss";
  string vbbmFilename = filename + "_vbbm";
  bool locked[2] = {false, false};

  try
  {
    vbbm.lock(VBBM::WRITE);
    locked[0] = true;
    vss.lock(VSS::WRITE);
    locked[1] = true;

    loadExtentMap(emFilename, fixFL);
    vbbm.load(vbbmFilename);
    vss.load(vssFilename);

    vss.release(VSS::WRITE);
    locked[1] = false;
    vbbm.release(VBBM::WRITE);
    locked[0] = false;
  }
  catch (exception& e)
  {
    if (locked[1])
      vss.release(VSS::WRITE);

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
