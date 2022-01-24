/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016 MariaDB Corporation

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

/*
 * $Id: we_redistributecontrolthread.cpp 4450 2013-01-21 14:13:24Z rdempsey $
 */

#include <iostream>
#include <set>
#include <vector>
#include <cassert>
#include <stdexcept>
#include <sstream>
#include <string>
#include <unistd.h>
using namespace std;

#include "boost/scoped_ptr.hpp"
#include "boost/scoped_array.hpp"
#include "boost/thread/mutex.hpp"
#include "boost/filesystem/path.hpp"
#include "boost/filesystem/operations.hpp"
using namespace boost;

#include "mcsconfig.h"
#include "installdir.h"

#include "configcpp.h"
using namespace config;

#include "liboamcpp.h"
#include "oamcache.h"
using namespace oam;

#include "dbrm.h"
using namespace BRM;

#include "messagequeue.h"
#include "bytestream.h"
using namespace messageqcpp;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "we_messages.h"
#include "we_redistributedef.h"
#include "we_redistributecontrol.h"
#include "we_redistributecontrolthread.h"

namespace redistribute
{
// static variables
boost::mutex RedistributeControlThread::fActionMutex;
volatile bool RedistributeControlThread::fStopAction = false;
string RedistributeControlThread::fWesInUse;

void RedistributeControlThread::setStopAction(bool s)
{
  boost::mutex::scoped_lock lock(fActionMutex);
  fStopAction = s;
}

RedistributeControlThread::RedistributeControlThread(uint32_t act)
 : fAction(act), fMaxDbroot(0), fEntryCount(0), fErrorCode(RED_EC_OK)
{
}

RedistributeControlThread::~RedistributeControlThread()
{
  //	fWEClient->removeQueue(uniqueId);
}

void RedistributeControlThread::operator()()
{
  if (fAction == RED_CNTL_START)
    doRedistribute();
  else if (fAction == RED_CNTL_STOP)
    doStopAction();
}

void RedistributeControlThread::doRedistribute()
{
  if (setup() != 0)
    fErrorCode = RED_EC_CNTL_SETUP_FAIL;
  else if (makeRedistributePlan() != 0)
    fErrorCode = RED_EC_MAKEPLAN_FAIL;

  try
  {
    if (fErrorCode == RED_EC_OK && !fStopAction && fEntryCount > 0)
      executeRedistributePlan();
  }
  catch (const std::exception& ex)
  {
    fErrorMsg += ex.what();
    fErrorCode = RED_EC_EXECUTE_FAIL;
  }
  catch (...)
  {
    fErrorMsg += "Error when executing the plan.";
    fErrorCode = RED_EC_EXECUTE_FAIL;
  }

  uint32_t state = RED_STATE_FINISH;

  if (fErrorCode != RED_EC_OK)
    state = RED_STATE_FAILED;

  try
  {
    if (!fStopAction)
      fControl->updateState(state);
  }
  catch (const std::exception& ex)
  {
    fErrorMsg += ex.what();

    if (fErrorCode == RED_EC_OK)
      fErrorCode = RED_EC_UPDATE_STATE;
  }
  catch (...)
  {
    fErrorMsg += "Error when updating state.";

    if (fErrorCode == RED_EC_OK)
      fErrorCode = RED_EC_UPDATE_STATE;
  }

  if (fErrorMsg.empty())
    fControl->logMessage("finished @controlThread::doRedistribute");
  else
    fControl->logMessage(fErrorMsg + " @controlThread::doRedistribute");

  {
    boost::mutex::scoped_lock lock(fActionMutex);
    fWesInUse.clear();
  }
}

int RedistributeControlThread::setup()
{
  int ret = 0;

  try
  {
    //		fUniqueId = fDbrm.getUnique64();
    //		fWEClient = WriteEngine::WEClients::instance(WriteEngine::WEClients::REDISTRIBUTE);
    //		fWEClient->addQueue(uniqueId);
    fConfig = Config::makeConfig();
    fOamCache = oam::OamCache::makeOamCache();
    fControl = RedistributeControl::instance();
    //		fOam.reset(new oam::Oam);
    //		fDbrm.reset(new BRM::DBRM);

    vector<int>::iterator i = fControl->fSourceList.begin();

    for (; i != fControl->fSourceList.end(); i++)
    {
      fSourceSet.insert(*i);
      fDbrootSet.insert(*i);

      if (*i > fMaxDbroot)
        fMaxDbroot = *i;
    }

    vector<int>::iterator j = fControl->fDestinationList.begin();

    for (; j != fControl->fDestinationList.end(); j++)
    {
      fTargetSet.insert(*j);

      if (fDbrootSet.find(*j) == fDbrootSet.end())
      {
        fDbrootSet.insert(*j);
      }

      //			if (*j > fMaxDbroot)
      //				fMaxDbroot = *j;
    }
  }
  catch (const std::exception& ex)
  {
    fErrorMsg += ex.what();
    ret = 1;
  }
  catch (...)
  {
    ret = 1;
  }

  return ret;
}

int RedistributeControlThread::makeRedistributePlan()
{
  int ret = 0;

  try
  {
    if (fControl->fPlanFilePtr != NULL)
    {
      // should not happen, just in case.
      fclose(fControl->fPlanFilePtr);
      fControl->fPlanFilePtr = NULL;
    }

    // get all user table oids
    boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(0);
    vector<pair<CalpontSystemCatalog::OID, CalpontSystemCatalog::TableName> > tables = csc->getTables();
    vector<pair<CalpontSystemCatalog::OID, CalpontSystemCatalog::TableName> >::iterator i;

    for (i = tables.begin(); i != tables.end(); i++)
    {
      // in case, action is cancelled.
      if (fStopAction)
        break;

      // column oids
      CalpontSystemCatalog::RIDList cols = csc->columnRIDs(i->second, true);
      typedef std::map<PartitionInfo, RedistributeExtentEntry> PartitionExtentMap;
      PartitionExtentMap partitionMap;
      vector<EMEntry> entries;

      // sample the first column
      int rc = fControl->fDbrm->getExtents(cols[0].objnum, entries, false, false, true);

      if (rc != 0 || entries.size() == 0)
      {
        ostringstream oss;
        oss << "Error in DBRM getExtents; oid:" << cols[0].objnum << "; returnCode: " << rc;
        throw runtime_error(oss.str());
      }

      for (vector<EMEntry>::iterator j = entries.begin(); j != entries.end(); j++)
      {
        RedistributeExtentEntry redEntry;
        redEntry.oid = cols[0].objnum;
        redEntry.dbroot = j->dbRoot;
        redEntry.partition = j->partitionNum;
        redEntry.segment = j->segmentNum;
        redEntry.lbid = j->range.start;
        redEntry.range = j->range.size * 1024;

        PartitionInfo partInfo(j->dbRoot, j->partitionNum);
        partitionMap.insert(make_pair(partInfo, redEntry));
      }

      // sort partitions by dbroot
      vector<vector<int> > dbPartVec(fMaxDbroot + 1);
      uint64_t totalPartitionCount = 0;
      int maxPartitionId = 0;

      for (PartitionExtentMap::iterator j = partitionMap.begin(); j != partitionMap.end(); j++)
      {
        int dbroot = j->first.dbroot;

        if (fSourceSet.find(dbroot) != fSourceSet.end())
        {
          // only dbroot in source list needs attention
          dbPartVec[dbroot].push_back(j->first.partition);

          if (j->first.partition > maxPartitionId)
            maxPartitionId = j->first.partition;

          totalPartitionCount++;
        }
      }

      // sort the partitions
      for (vector<vector<int> >::iterator k = dbPartVec.begin(); k != dbPartVec.end(); k++)
        sort(k->begin(), k->end());

      // divide the dbroots into the source and target sets
      uint64_t average = totalPartitionCount / fTargetSet.size();
      // Remainder is the number of partitions that must be spread across some
      // of the dbroots such that no dbroot has more than average+1 partitions.
      uint64_t remainder = totalPartitionCount % fTargetSet.size();
      set<int> sourceDbroots;
      set<int> targetDbroots;
      //			list<pair<size_t, int> > targetList;  // to be ordered by partition size
      int64_t extra = remainder;

      for (set<int>::iterator j = fDbrootSet.begin(); j != fDbrootSet.end(); ++j)
      {
        if (fTargetSet.find(*j) == fTargetSet.end())
        {
          // Not a target (removed on command line). Always a source.
          sourceDbroots.insert(*j);
          continue;
        }

        // If a dbroot has exactly average+1 partitions and there's extras to be had,
        // then it is neither a source nor a target.
        if ((dbPartVec[*j].size() == average + 1) && extra)
        {
          --extra;
          continue;
        }

        if (dbPartVec[*j].size() > average)
        {
          // Sources are those dbroots with more than average partitions
          sourceDbroots.insert(*j);
        }
        else
        {
          // Targets are those with room ( <= average )
          targetDbroots.insert(*j);
        }
      }

      // At this point, there are two concepts of target. (1)Those in fTargetSet, which is the
      // set of dbroots the user wants partitions on and (2) those in targetDbroots, a subset of
      // fTargetSet, which is those that actually have room, based on average, for more data.

      // After redistribution, partition count for each dbroot is average or average+1.
      // When remainder > 0,  some targets will have (average+1) partitions.

      // loop through target dbroots and find partitions from sources to move to each.
      set<int>::iterator sourceDbroot = sourceDbroots.begin();
      int sourceCnt = sourceDbroots.size();

      for (set<int>::iterator targetDbroot = targetDbroots.begin(); targetDbroot != targetDbroots.end();
           ++targetDbroot)
      {
        // check if this target will have average + 1 partitions.
        uint64_t e = 0;

        if (extra-- > 0)
          e = 1;

        // A set of the partitions already on the target. We try not to move the same partition here.
        set<int> targetParts(dbPartVec[*targetDbroot].begin(), dbPartVec[*targetDbroot].end());

        if (targetParts.size() >= (average + e))
          continue;  // Don't move any partitions to this target

        // partitions to be moved to this target
        vector<PartitionInfo> planVec;

        // looking for source partitions start from partition 0
        bool done = false;  // if target got enough partitions, set to true.
        int loop = 0;  // avoid infinite loop. maxPartitionId is the last partition of one of the dbroots.
                       // It's a place to stop if all else fails.

        while (!done && loop <= maxPartitionId)
        {
          for (int p = loop++; p <= maxPartitionId && !done; ++p)
          {
            bool found = false;

            if (targetParts.find(p) ==
                targetParts.end())  // True if the partition is not on the target already
            {
              // try to find partition p in one of the source dbroots
              for (int x = 0; x < sourceCnt && !found; ++x)
              {
                vector<int>& sourceParts = dbPartVec[*sourceDbroot];
                // This partition needs to move if:
                // 1) source still has more than average partitions, or
                // 2) source is not a listed target (we want to empty source).
                bool bNotTarget =
                    fTargetSet.find(*sourceDbroot) == fTargetSet.end();  // true if source not in target list

                if (sourceParts.size() >= average || bNotTarget)
                {
                  vector<int>::iterator y = find(sourceParts.begin(), sourceParts.end(), p);

                  if ((y != sourceParts.end()) && (sourceParts.size() > targetParts.size() || bNotTarget))
                  {
                    targetParts.insert(p);
                    planVec.push_back(PartitionInfo(*sourceDbroot, p));
                    found = true;

                    // update the source
                    sourceParts.erase(y);
                  }
                }

                if (++sourceDbroot == sourceDbroots.end())
                  sourceDbroot = sourceDbroots.begin();
              }  // for source

              if (targetParts.size() == (average + e))
                done = true;
            }  // !find p
          }    // for p
        }      // while loop

        // dump the plan for the target to file
        dumpPlanToFile(i->first, planVec, *targetDbroot);
      }  // for target

      // It's possible that a source that is "removed" on the command line is not empty.
      // This can happen if a partition exists on all dbroots.

      // WCOL-786: use nextDbroot to start the loop looking for a suitible target
      // where we left off with the previous partition. This gives each target
      // an opportunity to get some of the data. In the case of dbroot removal,
      // there is often the same number of partitions on each remaining dbroot.
      // This logic tends to roundrobin which dbroot gets the next batch.
      set<int>::iterator nextDbroot = targetDbroots.begin();
      set<int>::iterator targetDbroot;
      int targetCnt = (int)targetDbroots.size();

      // Loop through the sources, looking for dbroots that are not targets that also still contain partitions
      for (set<int>::iterator sourceDbroot = sourceDbroots.begin(); sourceDbroot != sourceDbroots.end();
           ++sourceDbroot)
      {
        // Is this source in target list? If so, do nothing.
        if (fTargetSet.find(*sourceDbroot) != fTargetSet.end())
          continue;

        vector<int>& sourceParts = dbPartVec[*sourceDbroot];  // Partitions still on source

        // We can't erase from a vector we're iterating, so we need a kludge:
        for (int p = 0; p <= maxPartitionId; ++p)
        {
          vector<int>::iterator sourcePart = find(sourceParts.begin(), sourceParts.end(), p);

          if (sourcePart == sourceParts.end())
          {
            continue;
          }

          // Look through targets to see which can accept this partition. Find the one with the least
          // number of partitions. Someday we want to put with the dbroot having the fewest segments of
          // the partition.
          uint64_t partCount = std::numeric_limits<uint64_t>::max();
          int tdbroot = 0;
          targetDbroot = nextDbroot;

          // MCOL-786. Start at targetDbroot and loop around back to the same spot.
          for (int tbd = 0; tbd < targetCnt; ++tbd)
          {
            if (targetDbroot == targetDbroots.end())
            {
              targetDbroot = targetDbroots.begin();
            }

            if (dbPartVec[*targetDbroot].size() < partCount)
            {
              tdbroot = *targetDbroot;
              partCount = dbPartVec[*targetDbroot].size();
              nextDbroot = targetDbroot;
              ++nextDbroot;
            }

            ++targetDbroot;
          }

          if (tdbroot == 0)
          {
            continue;
          }

          set<int> targetParts(dbPartVec[tdbroot].begin(), dbPartVec[tdbroot].end());
          vector<PartitionInfo> planVec;  // partitions to be moved to this target
          targetParts.insert(p);
          planVec.push_back(PartitionInfo(*sourceDbroot, p));
          sourceParts.erase(sourcePart);
          dumpPlanToFile(i->first, planVec, tdbroot);
        }  // for sourceParts
      }    // for source
    }      // for tables
  }
  catch (const std::exception& ex)
  {
    fErrorMsg += ex.what();
    ret = 2;
  }
  catch (...)
  {
    ret = 2;
  }

  displayPlan();
  return ret;
}

void RedistributeControlThread::dumpPlanToFile(uint64_t oid, vector<PartitionInfo>& vec, int target)
{
  // open the plan file, if not already opened, to write.
  if (fControl->fPlanFilePtr == NULL)
  {
    errno = 0;
    fControl->fPlanFilePtr = fopen(fControl->fPlanFilePath.c_str(), "w+");

    if (fControl->fPlanFilePtr == NULL)
    {
      int e = errno;
      ostringstream oss;
      oss << "Failed to open redistribute.plan: " << strerror(e) << " (" << e << ")";
      throw runtime_error(oss.str());
    }
  }

  size_t entryNum = vec.size();
  scoped_array<RedistributePlanEntry> entries(new RedistributePlanEntry[entryNum]);

  for (uint64_t i = 0; i < entryNum; ++i)
  {
    entries[i].table = oid;
    entries[i].source = vec[i].dbroot;
    entries[i].partition = vec[i].partition;
    entries[i].destination = target;
    entries[i].status = RED_TRANS_READY;
  }

  errno = 0;
  size_t n = fwrite(entries.get(), sizeof(RedistributePlanEntry), entryNum, fControl->fPlanFilePtr);

  if (n != entryNum)  // need retry
  {
    int e = errno;
    ostringstream oss;
    oss << "Failed to write into redistribute.plan: " << strerror(e) << " (" << e << ")";
    throw runtime_error(oss.str());
  }

  fEntryCount += entryNum;
}

void RedistributeControlThread::displayPlan()
{
  // start from the first entry
  try
  {
    if (!fControl->fPlanFilePtr)
    {
      ostringstream oss;
      oss << "No data is schefuled to be moved" << endl;
      fControl->logMessage(oss.str());
      return;
    }

    rewind(fControl->fPlanFilePtr);

    ByteStream bs;
    uint32_t entryId = 0;
    long entrySize = sizeof(RedistributePlanEntry);
    fControl->logMessage(string("Redistribution Plan:"));

    while (entryId++ < fEntryCount)
    {
      RedistributePlanEntry entry;
      errno = 0;
      size_t n = fread(&entry, entrySize, 1, fControl->fPlanFilePtr);

      if (n != 1)
      {
        int e = errno;
        ostringstream oss;
        oss << "Failed to read from redistribute.plan: " << strerror(e) << " (" << e << ")";
        throw runtime_error(oss.str());
      }

      // Print this plan entry
      ostringstream oss;
      oss << "table oid " << entry.table << " partition " << entry.partition << " moves from dbroot "
          << entry.source << " to " << entry.destination << endl;
      fControl->logMessage(oss.str());
    }
  }
  catch (std::exception& e)
  {
    ostringstream oss;
    oss << "exception during display of plan: " << e.what() << endl;
    fControl->logMessage(oss.str());
  }
  catch (...)
  {
    ostringstream oss;
    oss << "exception during display of plan" << endl;
    fControl->logMessage(oss.str());
  }
}

int RedistributeControlThread::executeRedistributePlan()
{
  // update the info with total partitions to move
  fControl->setEntryCount(fEntryCount);

  // start from the first entry
  rewind(fControl->fPlanFilePtr);

  ByteStream bs;
  uint32_t entryId = 0;
  long entrySize = sizeof(RedistributePlanEntry);

  while (entryId++ < fEntryCount)
  {
    try
    {
      // skip system status check in case no OAM
      /*
                  if (getenv("SKIP_OAM_INIT") == NULL)
                  {
                      // make sure system is in active state
                      bool isActive = false;

                      while (!isActive)
                      {
                          bool noExcept = true;
                          SystemStatus systemstatus;

                          try
                          {
                              fControl->fOam->getSystemStatus(systemstatus);
                          }
                          catch (const std::exception& ex)
                          {
                              fErrorMsg += ex.what();
                              noExcept = false;
                          }
                          catch (...)
                          {
                              noExcept = false;
                          }

                          if (noExcept && ((isActive = (systemstatus.SystemOpState == oam::ACTIVE)) == false))
                              sleep(1);;
                      }
                  }
      */

      if (fStopAction)
        return RED_EC_USER_STOP;

      RedistributePlanEntry entry;
      errno = 0;
      size_t n = fread(&entry, entrySize, 1, fControl->fPlanFilePtr);

      if (n != 1)
      {
        int e = errno;
        ostringstream oss;
        oss << "Failed to read from redistribute.plan: " << strerror(e) << " (" << e << ")";
        throw runtime_error(oss.str());
      }

      if (entry.status != (int)RED_TRANS_READY)
        continue;

      // send the job to source dbroot
      size_t headerSize = sizeof(RedistributeMsgHeader);
      size_t entrySize = sizeof(RedistributePlanEntry);
      RedistributeMsgHeader header(entry.destination, entry.source, entryId, RED_ACTN_REQUEST);

      if (connectToWes(header.source) == 0)
      {
        bs.restart();
        entry.starttime = time(NULL);
        bs << (ByteStream::byte)WriteEngine::WE_SVR_REDISTRIBUTE;
        bs.append((const ByteStream::byte*)&header, headerSize);
        bs.append((const ByteStream::byte*)&entry, entrySize);
        fMsgQueueClient->write(bs);

        SBS sbs = fMsgQueueClient->read();
        entry.status = RED_TRANS_FAILED;

        if (sbs->length() == 0)
        {
          ostringstream oss;
          oss << "Zero byte read, Network error.  entryID=" << entryId;
          fErrorMsg = oss.str();
        }
        else if (sbs->length() < (headerSize + entrySize + 1))
        {
          ostringstream oss;
          oss << "Short message, length=" << sbs->length() << ". entryID=" << entryId;
          fErrorMsg = oss.str();
        }
        else
        {
          ByteStream::byte wesMsgId;
          *sbs >> wesMsgId;
          // Need check header info
          // const RedistributeMsgHeader* h = (const RedistributeMsgHeader*) sbs->buf();
          sbs->advance(headerSize);
          const RedistributePlanEntry* e = (const RedistributePlanEntry*)sbs->buf();
          sbs->advance(entrySize);
          entry.status = e->status;
          entry.endtime = time(NULL);
          //					if (entry.status == (int32_t) RED_TRANS_FAILED)
          //						*sbs >> fErrorMsg;
        }

        // done with this connection, may consider to reuse.
        fMsgQueueClient.reset();
      }
      else
      {
        entry.status = RED_TRANS_FAILED;
        ostringstream oss;
        oss << "Connect to PM failed."
            << ". entryID=" << entryId;
        fErrorMsg += oss.str();
      }

      if (!fErrorMsg.empty())
        throw runtime_error(fErrorMsg);

      errno = 0;
      int rc = fseek(fControl->fPlanFilePtr, -((long)entrySize), SEEK_CUR);

      if (rc != 0)
      {
        int e = errno;
        ostringstream oss;
        oss << "fseek is failed: " << strerror(e) << " (" << e << "); entry id=" << entryId;
        throw runtime_error(oss.str());
      }

      errno = 0;
      n = fwrite(&entry, entrySize, 1, fControl->fPlanFilePtr);

      if (n != 1)  // need retry
      {
        int e = errno;
        ostringstream oss;
        oss << "Failed to update redistribute.plan: " << strerror(e) << " (" << e
            << "); entry id=" << entryId;
        throw runtime_error(oss.str());
      }

      fflush(fControl->fPlanFilePtr);

      fControl->updateProgressInfo(entry.status, entry.endtime);
    }
    catch (const std::exception& ex)
    {
      fControl->logMessage(string("got exception when executing plan:") + ex.what());
    }
    catch (...)
    {
      fControl->logMessage("got unknown exception when executing plan.");
    }
  }

  return 0;
}

int RedistributeControlThread::connectToWes(int dbroot)
{
  int ret = 0;
  OamCache::dbRootPMMap_t dbrootToPM = fOamCache->getDBRootToPMMap();
  int pmId = (*dbrootToPM)[dbroot];
  ostringstream oss;
  oss << "pm" << pmId << "_WriteEngineServer";

  try
  {
    boost::mutex::scoped_lock lock(fActionMutex);
    fWesInUse = oss.str();
    fMsgQueueClient.reset(new MessageQueueClient(fWesInUse, fConfig));
  }
  catch (const std::exception& ex)
  {
    fErrorMsg = "Caught exception when connecting to " + oss.str() + " -- " + ex.what();
    ret = 1;
  }
  catch (...)
  {
    fErrorMsg = "Caught exception when connecting to " + oss.str() + " -- unknown";
    ret = 2;
  }

  if (ret != 0)
  {
    boost::mutex::scoped_lock lock(fActionMutex);
    fWesInUse.clear();

    fMsgQueueClient.reset();
  }

  return ret;
}

void RedistributeControlThread::doStopAction()
{
  fConfig = Config::makeConfig();
  fControl = RedistributeControl::instance();

  boost::mutex::scoped_lock lock(fActionMutex);

  if (!fWesInUse.empty())
  {
    // send the stop message to dbroots
    size_t headerSize = sizeof(RedistributeMsgHeader);
    RedistributeMsgHeader header(-1, -1, -1, RED_ACTN_STOP);

    try
    {
      fMsgQueueClient.reset(new MessageQueueClient(fWesInUse, fConfig));
      ByteStream bs;
      bs << (ByteStream::byte)WriteEngine::WE_SVR_REDISTRIBUTE;
      bs.append((const ByteStream::byte*)&header, headerSize);
      fMsgQueueClient->write(bs);

      SBS sbs;
      sbs = fMsgQueueClient->read();
      // no retry yet.
    }
    catch (const std::exception& ex)
    {
      fErrorMsg = "Caught exception when connecting to " + fWesInUse + " -- " + ex.what();
    }
    catch (...)
    {
      fErrorMsg = "Caught exception when connecting to " + fWesInUse + " -- unknown";
    }
  }

  if (!fErrorMsg.empty())
    fControl->logMessage(fErrorMsg + " @controlThread::doStop");
  else
    fControl->logMessage("User stop @controlThread::doStop");

  fWesInUse.clear();
  fMsgQueueClient.reset();
}

}  // namespace redistribute

// vim:ts=4 sw=4:
