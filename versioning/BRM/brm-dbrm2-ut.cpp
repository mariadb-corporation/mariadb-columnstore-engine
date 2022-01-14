/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016-2021 MariaDB Corporation

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
 * $Id: tdriver-dbrm2.cpp 1823 2013-01-21 14:13:09Z rdempsey $
 *
 ****************************************************************************/

#include <iostream>
#include <vector>
#include <pthread.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <stdexcept>
#include <pthread.h>
#include <sys/time.h>
#include <values.h>

#include <cppunit/extensions/HelperMacros.h>

#include "brm.h"
#include "extentmap.h"
#include "IDBPolicy.h"

//#define BRM_VERBOSE 1

using namespace BRM;
using namespace std;

int threadStop;
int oid = 1;
uint64_t opCount = 0;
LBID_t lbidCounter = 0;
VER_t nextTxnID = 1;
u_int64_t vbOffset = 0;
pthread_mutex_t pthreadMutex;
const std::vector<uint32_t> colWidthsAvailable = {1, 2, 4, 8, 16};
const DBRootT dbroot = 1;
const uint32_t KibiBlocks = 1024; 

struct Range
{
    LBID_t start, end, nextBlock;
    VER_t txnID;
    Range()
    {
        start = end = nextBlock = 0;
        txnID = 0;
    }
};

struct EMEntries
{
    LBID_t LBIDstart;
    uint32_t size;
    OID_t OID;
    uint32_t FBO;
    uint32_t HWM;
    uint32_t secondHWM;
    int32_t txnID;
    DBRootT dbroot;
    PartitionNumberT partNum;
    SegmentT segNum;
    struct EMEntries* next;
    EMEntries() : HWM(0), secondHWM(0), txnID(0), next(nullptr)
    { }
    EMEntries(const uint32_t aSize, const OID_t aOid, const uint32_t aFbo,
        const LBID_t aLBIDStart, EMEntries* aNext)
            : LBIDstart(aLBIDStart), size(aSize), OID(aOid), FBO(aFbo), HWM(0),
              secondHWM(0), txnID(0), next(aNext)
    { }
    EMEntries(const uint32_t aSize, const OID_t aOid, const uint32_t aFbo,
        const LBID_t aLBIDStart, EMEntries* aNext, const DBRootT aDbroot,
        const PartitionNumberT aPartNum, const SegmentT aSegNum)
            : LBIDstart(aLBIDStart), size(aSize), OID(aOid), FBO(aFbo), HWM(0),
              secondHWM(0), txnID(0), dbroot(aDbroot), partNum(aPartNum),
              segNum(aSegNum), next(aNext)
   { }
};
/*
static void* BRMRunner_2(void* arg)
{

    vector<Range> copyList, copiedList, committedList;
    vector<Range>::iterator rit;
    vector<LBID_t> writtenList;
    vector<LBID_t>::iterator lit;

    pthread_mutex_t listMutex;
    int op;
    uint32_t randstate;
    DBRM* brm;
    struct timeval tv;
    VER_t txnID;

    pthread_mutex_init(&listMutex, NULL);
    gettimeofday(&tv, NULL);
    randstate = static_cast<uint32_t>(tv.tv_usec);
    brm = new DBRM();

    while (!threadStop)
    {
        op = rand_r(&randstate) % 9;

        switch (op)
        {
            case 0:   // beginVBCopy
            {
                int blockCount, size, err;
                Range newEntry;
                VBRange_v vbRanges;
                VBRange_v::iterator vit;
                LBIDRange_v ranges;
                LBIDRange range;

                size = rand_r(&randstate) % 10000;

                pthread_mutex_lock(&pthreadMutex);
                newEntry.start = lbidCounter;
                lbidCounter += size;
                txnID = nextTxnID++;
                pthread_mutex_unlock(&pthreadMutex);

                newEntry.nextBlock = newEntry.start;
                newEntry.end = newEntry.start + size;
                range.start = newEntry.start;
                range.size = size;

                err = brm->beginVBCopy(txnID, dbroot, ranges, vbRanges);
                CPPUNIT_ASSERT(err == 0);

                for (blockCount = 0, vit = vbRanges.begin(); vit != vbRanges.end(); vit++)
                    blockCount += (*vit).size;

                CPPUNIT_ASSERT(blockCount == size);

                pthread_mutex_lock(&listMutex);
                copyList.push_back(newEntry);
                pthread_mutex_unlock(&listMutex);

                err = brm->beginVBCopy(txnID, dbroot, ranges, vbRanges);
                CPPUNIT_ASSERT(err == -1);
                break;
            }

            case 1:		// writeVBEntry
            {
                int randIndex;
                Range* entry;

                pthread_mutex_lock(&listMutex);

                if (copyList.size() == 0)
                    break;

                randIndex = rand_r(&randstate) % copyList.size();
                entry = &(copyList[randIndex]);
                entry->nextBlock++;
                txnID = entry->txnID;
                break;
            }

            default:
                cerr << "not finished yet" << endl;
        }
    }

    return NULL;
}
*/

/*
static void* BRMRunner_1(void* arg)
{

    // keep track of LBID ranges allocated here and
    // randomly allocate, lookup, delete, get/set HWM, and
    // destroy the EM object.

#ifdef BRM_VERBOSE
    int threadNum = reinterpret_cast<int>(arg);
#endif
    int op, listSize = 0, i;
    uint32_t randstate;
    struct EMEntries* head = NULL, *tmp;
    struct timeval tv;
    DBRM* brm;
    ExtentMap em;
    vector<LBID_t> lbids;
    LBID_t lbid;
    uint32_t colWidth;
    PartitionNumberT partNum;
    SegmentT segmentNum;
    execplan::CalpontSystemCatalog::ColDataType colDataType;

#ifdef BRM_VERBOSE
    cerr << "thread number " << threadNum << " started." << endl;
#endif

    gettimeofday(&tv, NULL);
    randstate = static_cast<uint32_t>(tv.tv_usec);
    brm = new DBRM();
    

    while (!threadStop)
    {
        auto randNumber = rand_r(&randstate);
        op = randNumber % 10;
        colWidth = colWidthsAvailable[randNumber % colWidthsAvailable.size()];
        partNum = randNumber % std::numeric_limits<PartitionNumberT>::max();
        segmentNum = randNumber % std::numeric_limits<SegmentT>::max();
        colDataType = (execplan::CalpontSystemCatalog::ColDataType) (randNumber % (int)execplan::CalpontSystemCatalog::ColDataType::TIMESTAMP);
#ifdef BRM_VERBOSE
        cerr << "next op is " << op << endl;
#endif

        switch (op)
        {
            case 0:   //allocate space for a new file
            {
                struct EMEntries* newEm;
                size_t size = rand_r(&randstate) % 102399 + 1;
                int entries, OID, allocdSize, err;
                uint32_t startBlockOffset;


                pthread_mutex_lock(&pthreadMutex);
                OID = oid++;
                opCount++;
                pthread_mutex_unlock(&pthreadMutex);
                lbids.clear();
                for (size_t i = 0; i < size; ++i)
                {
                    err = brm->createColumnExtent_DBroot(OID, colWidth, dbroot,
                        partNum, segmentNum, colDataType, lbid, allocdSize, startBlockOffset);
                    CPPUNIT_ASSERT(err == 0);
                    lbids.push_back(lbid);
                }

                entries = size / brm->getExtentSize();

                if ((size % brm->getExtentSize()) != 0)
                    entries++;

                if ((uint32_t)entries != lbids.size())
                    cerr << "entries = " << entries << " lbids.size = " << lbids.size() << endl;

                CPPUNIT_ASSERT((uint32_t)entries == lbids.size());

                for (i = 0 ; i < entries; i++)
                {

                    newEm = new EMEntries(brm->getExtentSize(), OID, brm->getExtentSize(), lbids[i],
                        head, dbroot, partNum, segmentNum);
                    head = newEm;
                    listSize++;
                }

#ifdef BRM_VERBOSE
                cerr << "created new space for OID " << newEm->OID << endl;
#endif
                em.checkConsistency();
                break;
            }

            case 1:		//allocate space for an existing file
            {
                if (listSize == 0)
                    break;

                struct EMEntries* newEm, *tmp;
                int size = rand_r(&randstate) % 102399 + 1;
                int fileRand = rand_r(&randstate) % listSize;
                int i, lastExtent, blockEnd, oid;
                int tmpHWM, entries, allocdSize, err;
                uint32_t startBlockOffset;
                vector<LBID_t> lbids;
                LBID_t lbid;

                for (i = 0, tmp = head; i < fileRand; i++)
                    tmp = tmp->next;

                oid = tmp->OID;

                for (lastExtent = 0, tmp = head; tmp != NULL; tmp = tmp->next)
                {
                    if (tmp->OID != oid)
                        continue;

                    tmpHWM = tmp->HWM;
                    blockEnd = tmp->FBO + tmp->size;

                    if (lastExtent < blockEnd)
                        lastExtent = blockEnd;
                }

                err = brm->createColumnExtentExactFile(oid, colWidth, dbroot,
                    partNum, segmentNum, colDataType, lbid, allocdSize, startBlockOffset);
                pthread_mutex_lock(&pthreadMutex);
                opCount++;
                pthread_mutex_unlock(&pthreadMutex);
                CPPUNIT_ASSERT(err == 0);

                entries = size / brm->getExtentSize();

                if ((size % brm->getExtentSize()) != 0)
                    entries++;

                CPPUNIT_ASSERT((uint32_t)entries == lbids.size());

                for (i = 0; i < entries; i++)
                {

                    newEm = new EMEntries((i != entries) ? brm->getExtentSize() : size % brm->getExtentSize(),
                       oid, lastExtent + (i * brm->getExtentSize()),
                       lbids[i], head, dbroot, partNum, segmentNum);
                    newEm->HWM = tmpHWM;
                    head = newEm;
                    listSize++;
                }

#ifdef BRM_VERBOSE
                cerr << "created another extent for OID " << newEm->OID << endl;
#endif
                em.checkConsistency();
                break;
            }

            case 2:  			//delete an OID
            {
                if (listSize == 0)
                    break;

                struct EMEntries* tmp, *prev;
                int fileRand = rand_r(&randstate) % listSize;
                int i, oid, err;

                for (i = 0, tmp = head; i < fileRand; i++)
                    tmp = tmp->next;

                oid = tmp->OID;

                err = brm->deleteOID(oid);
                pthread_mutex_lock(&pthreadMutex);
                opCount++;
                pthread_mutex_unlock(&pthreadMutex);
                CPPUNIT_ASSERT(err == 0);

                for (tmp = head; tmp != NULL;)
                {
                    if (tmp->OID == oid)
                    {
                        if (tmp == head)
                        {
                            head = head->next;
                            delete tmp;
                            tmp = head;
                        }
                        else
                        {
                            prev->next = tmp->next;
                            delete tmp;
                            tmp = prev->next;
                        }

                        listSize--;
                    }
                    else
                    {
                        prev = tmp;
                        tmp = tmp->next;
                    }
                }

#ifdef BRM_VERBOSE
                cerr << "deleted OID " << oid << endl;
#endif
                em.checkConsistency();
                break;
            }

            case 3:   //lookup by LBID
            {
                if (listSize == 0)
                    break;

                int entryRand = rand_r(&randstate) % listSize;
                int i, err, offset, oid;
                struct EMEntries* tmp;
                LBID_t target;
                uint32_t fbo;
                DBRootT localDbroot;
                PartitionNumberT localPartNum;
                SegmentT localSegmentNum;

                for (i = 0, tmp = head; i < entryRand; i++)
                    tmp = tmp->next;

                offset = rand_r(&randstate) % tmp->size;

                target = tmp->LBIDstart + offset;
                err = brm->lookupLocal(target, 0, false, oid, localDbroot, localPartNum,
                    localSegmentNum, fbo);
                pthread_mutex_lock(&pthreadMutex);
                opCount++;
                pthread_mutex_unlock(&pthreadMutex);
#ifdef BRM_VERBOSE
                cerr << "looked up LBID " << target << " got oid " << oid << " fbo " << fbo << endl;
                cerr << "   oid should be " << tmp->OID << " fbo should be " << offset + tmp->FBO << endl;
#endif
                CPPUNIT_ASSERT(err == 0);
                CPPUNIT_ASSERT(oid == tmp->OID);
                CPPUNIT_ASSERT(fbo == offset + tmp->FBO);
                em.checkConsistency();
                break;
            }

            case 4:   //lookup by OID, FBO
            {
                if (listSize == 0)
                    break;

                int entryRand = rand_r(&randstate) % listSize;
                int i, oid, err, offset;
                struct EMEntries* tmp;
                LBID_t lbid;

                for (i = 0, tmp = head; i < entryRand; i++)
                    tmp = tmp->next;

                offset = rand_r(&randstate) % tmp->size;
                oid = tmp->OID;

                err = brm->lookupLocal(oid, partNum, segmentNum, offset + tmp->FBO, lbid);

                pthread_mutex_lock(&pthreadMutex);
                opCount++;
                pthread_mutex_unlock(&pthreadMutex);
#ifdef BRM_VERBOSE
                cerr << "looked up OID " << oid << " fbo " << offset + tmp->FBO <<
                     " got lbid " << lbid << endl;
                cerr << "  lbid should be " << tmp->LBIDstart + offset << endl;
#endif
                CPPUNIT_ASSERT(err == 0);
                CPPUNIT_ASSERT(lbid == static_cast<LBID_t>(tmp->LBIDstart + offset));
                em.checkConsistency();
                break;
            }

            case 5:		//getHWM
            {
                if (listSize == 0)
                    break;

                int entryRand = rand_r(&randstate) % listSize;
                int i, err, status;
                struct EMEntries* tmp;
                uint32_t hwm;

                for (i = 0, tmp = head; i < entryRand; i++)
                    tmp = tmp->next;
                // WIP
                err = brm->getLocalHWM(tmp->OID, partNum, segmentNum, hwm, status);
                pthread_mutex_lock(&pthreadMutex);
                opCount++;
                pthread_mutex_unlock(&pthreadMutex);
                CPPUNIT_ASSERT(err == 0);
#ifdef BRM_VERBOSE
                cerr << "stored HWM for OID " << tmp->OID << " is " << tmp->HWM
                     << " BRM says it's " << hwm << endl;
#endif
                CPPUNIT_ASSERT(hwm == tmp->HWM);
                em.checkConsistency();
                break;
            }

            case 6: 		//setHWM
            {
                if (listSize == 0)
                    break;

                int entryRand = rand_r(&randstate) % listSize;
                int i, hwm, oid, err;
                struct EMEntries* tmp;

                for (i = 0, tmp = head; i < entryRand; i++)
                    tmp = tmp->next;

                oid = tmp->OID;
                hwm = rand_r(&randstate) % (tmp->FBO + brm->getExtentSize());
                err = brm->setLocalHWM(oid, tmp->partNum, tmp->segNum, hwm);
                pthread_mutex_lock(&pthreadMutex);
                opCount++;
                pthread_mutex_unlock(&pthreadMutex);
                CPPUNIT_ASSERT(err == 0);

                for (tmp = head; tmp != NULL; tmp = tmp->next)
                    if (tmp->OID == oid)
                        tmp->HWM = hwm;

#ifdef BRM_VERBOSE
                cerr << "setHWM of OID " << oid << " to " << hwm << endl;
#endif
                em.checkConsistency();
                break;
            }

            case 7:			// renew this EM object
            {
                delete brm;
                brm = new DBRM();
#ifdef BRM_VERBOSE
                cerr << "got a new BRM instance" << endl;
#endif
                em.checkConsistency();
                break;
            }
#if 0
            case 8:			//getBulkInsertVars
            {
                if (listSize == 0)
                    break;

                HWM_t hwm;
                VER_t txnID;
                int entryRand = rand_r(&randstate) % listSize;
                int i, err, offset;
                EMEntries* tmp;
                LBID_t lbid;

                for (i = 0, tmp = head; i < entryRand; i++)
                    tmp = tmp->next;

                offset = rand_r(&randstate) % tmp->size;
                lbid = tmp->LBIDstart + offset;
                err = brm->getBulkInsertVars(lbid, hwm, txnID);
                pthread_mutex_lock(&pthreadMutex);
                opCount++;
                pthread_mutex_unlock(&pthreadMutex);
                CPPUNIT_ASSERT(err == 0);
                CPPUNIT_ASSERT(hwm == tmp->secondHWM);
                CPPUNIT_ASSERT(txnID == tmp->txnID);
                break;
            }

            case 9:			//setBulkInsertVars
            {
                if (listSize == 0)
                    break;

                int entryRand = rand_r(&randstate) % listSize;
                int i, err, offset;
                EMEntries* tmp;

                for (i = 0, tmp = head; i < entryRand; i++)
                    tmp = tmp->next;

                offset = rand_r(&randstate) % tmp->size;
                tmp->secondHWM = rand_r(&randstate) % MAXINT;
                tmp->txnID = rand_r(&randstate) % MAXINT;
                err = brm->setBulkInsertVars(tmp->LBIDstart + offset,
                                             tmp->secondHWM, tmp->txnID);
                pthread_mutex_lock(&pthreadMutex);
                opCount++;
                pthread_mutex_unlock(&pthreadMutex);
                CPPUNIT_ASSERT(err == 0);
                break;
            }
#endif
            default:
                break;
        }
    }

    delete brm;

    while (head != NULL)
    {
        tmp = head->next;
        delete head;
        head = tmp;
    }

#ifdef BRM_VERBOSE
    cerr << "thread " << threadNum << " exiting" << endl;
#endif
    return NULL;
}
*/
DBRM brm_si;
/*
static void* BRMRunner_si(void* arg)
{

    // keep track of LBID ranges allocated here and
    // randomly allocate, lookup, delete, get/set HWM, and
    // destroy the EM object.

#ifdef BRM_VERBOSE
    int threadNum = reinterpret_cast<int>(arg);
#endif
    int op, listSize = 0, i;
    uint32_t randstate;
    struct EMEntries* head = NULL, *tmp;
    struct timeval tv;
    ExtentMap em;
    vector<LBID_t> lbids;

#ifdef BRM_VERBOSE
    cerr << "thread number " << threadNum << " started." << endl;
#endif

    gettimeofday(&tv, NULL);
    randstate = static_cast<uint32_t>(tv.tv_usec);

    while (!threadStop)
    {
        op = rand_r(&randstate) % 10;
#ifdef BRM_VERBOSE
        cerr << "next op is " << op << endl;
#endif

        switch (op)
        {
            case 0:   //allocate space for a new file
            {
                struct EMEntries* newEm;
                int size = rand_r(&randstate) % 102399 + 1;
                int entries, OID, allocdSize, err;

                pthread_mutex_lock(&pthreadMutex);
                OID = oid++;
                opCount++;
                pthread_mutex_unlock(&pthreadMutex);

                err = brm_si.createExtent(size, OID, lbids, allocdSize);
                CPPUNIT_ASSERT(err == 0);

                entries = size / brm_si.getExtentSize();

                if ((size % brm_si.getExtentSize()) != 0)
                    entries++;

                CPPUNIT_ASSERT((uint32_t)entries == lbids.size());

                for (i = 0 ; i < entries; i++)
                {

                    newEm = new EMEntries();
                    newEm->size = brm_si.getExtentSize();
                    newEm->OID = OID;
                    newEm->FBO = i * brm_si.getExtentSize();
                    newEm->LBIDstart = lbids[i];

                    newEm->next = head;
                    head = newEm;
                    listSize++;
                }

#ifdef BRM_VERBOSE
                cerr << "created new space for OID " << newEm->OID << endl;
#endif
                em.checkConsistency();
                break;
            }

            case 1:		//allocate space for an existing file
            {
                if (listSize == 0)
                    break;

                struct EMEntries* newEm, *tmp;
                int size = rand_r(&randstate) % 102399 + 1;
                int fileRand = rand_r(&randstate) % listSize;
                int i, lastExtent, blockEnd, oid;
                int tmpHWM, entries, allocdSize, err;
                vector<LBID_t> lbids;

                for (i = 0, tmp = head; i < fileRand; i++)
                    tmp = tmp->next;

                oid = tmp->OID;

                for (lastExtent = 0, tmp = head; tmp != NULL; tmp = tmp->next)
                {
                    if (tmp->OID != oid)
                        continue;

                    tmpHWM = tmp->HWM;
                    blockEnd = tmp->FBO + tmp->size;

                    if (lastExtent < blockEnd)
                        lastExtent = blockEnd;
                }

                err = brm_si.createExtent(size, oid, lbids, allocdSize);
                pthread_mutex_lock(&pthreadMutex);
                opCount++;
                pthread_mutex_unlock(&pthreadMutex);
                CPPUNIT_ASSERT(err == 0);

                entries = size / brm_si.getExtentSize();

                if ((size % brm_si.getExtentSize()) != 0)
                    entries++;

                CPPUNIT_ASSERT((uint32_t)entries == lbids.size());

                for (i = 0; i < entries; i++)
                {

                    newEm = new EMEntries();

                    if (i != entries)
                        newEm->size = brm_si.getExtentSize();
                    else
                        newEm->size = size % brm_si.getExtentSize();

                    newEm->OID = oid;
                    newEm->FBO = lastExtent + (i * brm_si.getExtentSize());
                    newEm->LBIDstart = lbids[i];
                    newEm->HWM = tmpHWM;

                    newEm->next = head;
                    head = newEm;
                    listSize++;
                }

#ifdef BRM_VERBOSE
                cerr << "created another extent for OID " << newEm->OID << endl;
#endif
                em.checkConsistency();
                break;
            }

            case 2:  			//delete an OID
            {
                if (listSize == 0)
                    break;

                struct EMEntries* tmp, *prev;
                int fileRand = rand_r(&randstate) % listSize;
                int i, oid, err;

                for (i = 0, tmp = head; i < fileRand; i++)
                    tmp = tmp->next;

                oid = tmp->OID;

                err = brm_si.deleteOID(oid);
                pthread_mutex_lock(&pthreadMutex);
                opCount++;
                pthread_mutex_unlock(&pthreadMutex);
                CPPUNIT_ASSERT(err == 0);

                for (tmp = head; tmp != NULL;)
                {
                    if (tmp->OID == oid)
                    {
                        if (tmp == head)
                        {
                            head = head->next;
                            delete tmp;
                            tmp = head;
                        }
                        else
                        {
                            prev->next = tmp->next;
                            delete tmp;
                            tmp = prev->next;
                        }

                        listSize--;
                    }
                    else
                    {
                        prev = tmp;
                        tmp = tmp->next;
                    }
                }

#ifdef BRM_VERBOSE
                cerr << "deleted OID " << oid << endl;
#endif
                em.checkConsistency();
                break;
            }

            case 3:   //lookup by LBID
            {
                if (listSize == 0)
                    break;

                int entryRand = rand_r(&randstate) % listSize;
                int i, err, offset, oid;
                struct EMEntries* tmp;
                LBID_t target;
                uint32_t fbo;

                for (i = 0, tmp = head; i < entryRand; i++)
                    tmp = tmp->next;

                offset = rand_r(&randstate) % tmp->size;

                target = tmp->LBIDstart + offset;
                err = brm_si.lookup(target, 0, false, oid, fbo);
                pthread_mutex_lock(&pthreadMutex);
                opCount++;
                pthread_mutex_unlock(&pthreadMutex);
#ifdef BRM_VERBOSE
                cerr << "looked up LBID " << target << " got oid " << oid << " fbo " << fbo << endl;
                cerr << "   oid should be " << tmp->OID << " fbo should be " << offset + tmp->FBO << endl;
#endif
                CPPUNIT_ASSERT(err == 0);
                CPPUNIT_ASSERT(oid == tmp->OID);
                CPPUNIT_ASSERT(fbo == offset + tmp->FBO);
                em.checkConsistency();
                break;
            }

            case 4:   //lookup by OID, FBO
            {
                if (listSize == 0)
                    break;

                int entryRand = rand_r(&randstate) % listSize;
                int i, oid, err, offset;
                struct EMEntries* tmp;
                LBID_t lbid;

                for (i = 0, tmp = head; i < entryRand; i++)
                    tmp = tmp->next;

                offset = rand_r(&randstate) % tmp->size;
                oid = tmp->OID;

                err = brm_si.lookup(oid, offset + tmp->FBO, lbid);
                pthread_mutex_lock(&pthreadMutex);
                opCount++;
                pthread_mutex_unlock(&pthreadMutex);
#ifdef BRM_VERBOSE
                cerr << "looked up OID " << oid << " fbo " << offset + tmp->FBO <<
                     " got lbid " << lbid << endl;
                cerr << "  lbid should be " << tmp->LBIDstart + offset << endl;
#endif
                CPPUNIT_ASSERT(err == 0);
                CPPUNIT_ASSERT(lbid == static_cast<LBID_t>(tmp->LBIDstart + offset));
                em.checkConsistency();
                break;
            }

            case 5:		//getHWM
            {
                if (listSize == 0)
                    break;

                int entryRand = rand_r(&randstate) % listSize;
                int i, err;
                struct EMEntries* tmp;
                uint32_t hwm;

                for (i = 0, tmp = head; i < entryRand; i++)
                    tmp = tmp->next;

                err = brm_si.getHWM(tmp->OID, hwm);
                pthread_mutex_lock(&pthreadMutex);
                opCount++;
                pthread_mutex_unlock(&pthreadMutex);
                CPPUNIT_ASSERT(err == 0);
#ifdef BRM_VERBOSE
                cerr << "stored HWM for OID " << tmp->OID << " is " << tmp->HWM
                     << " BRM says it's " << hwm << endl;
#endif
                CPPUNIT_ASSERT(hwm == tmp->HWM);
                em.checkConsistency();
                break;
            }

            case 6: 		//setHWM
            {
                if (listSize == 0)
                    break;

                int entryRand = rand_r(&randstate) % listSize;
                int i, hwm, oid, err;
                struct EMEntries* tmp;

                for (i = 0, tmp = head; i < entryRand; i++)
                    tmp = tmp->next;

                oid = tmp->OID;
                hwm = rand_r(&randstate) % (tmp->FBO + brm_si.getExtentSize());
                err = brm_si.setHWM(oid, hwm);
                pthread_mutex_lock(&pthreadMutex);
                opCount++;
                pthread_mutex_unlock(&pthreadMutex);
                CPPUNIT_ASSERT(err == 0);

                for (tmp = head; tmp != NULL; tmp = tmp->next)
                    if (tmp->OID == oid)
                        tmp->HWM = hwm;

#ifdef BRM_VERBOSE
                cerr << "setHWM of OID " << oid << " to " << hwm << endl;
#endif
                em.checkConsistency();
                break;
            }

            case 7:			//getBulkInsertVars
            {
                if (listSize == 0)
                    break;

                HWM_t hwm;
                VER_t txnID;
                int entryRand = rand_r(&randstate) % listSize;
                int i, err, offset;
                EMEntries* tmp;
                LBID_t lbid;

                for (i = 0, tmp = head; i < entryRand; i++)
                    tmp = tmp->next;

                offset = rand_r(&randstate) % tmp->size;
                lbid = tmp->LBIDstart + offset;
                err = brm_si.getBulkInsertVars(lbid, hwm, txnID);
                pthread_mutex_lock(&pthreadMutex);
                opCount++;
                pthread_mutex_unlock(&pthreadMutex);
                CPPUNIT_ASSERT(err == 0);
                CPPUNIT_ASSERT(hwm == tmp->secondHWM);
                CPPUNIT_ASSERT(txnID == tmp->txnID);
                break;
            }

            case 8:			//setBulkInsertVars
            {
                if (listSize == 0)
                    break;

                int entryRand = rand_r(&randstate) % listSize;
                int i, err, offset;
                EMEntries* tmp;

                for (i = 0, tmp = head; i < entryRand; i++)
                    tmp = tmp->next;

                offset = rand_r(&randstate) % tmp->size;
                tmp->secondHWM = rand_r(&randstate) % MAXINT;
                tmp->txnID = rand_r(&randstate) % MAXINT;
                err = brm_si.setBulkInsertVars(tmp->LBIDstart + offset,
                                               tmp->secondHWM, tmp->txnID);
                pthread_mutex_lock(&pthreadMutex);
                opCount++;
                pthread_mutex_unlock(&pthreadMutex);
                CPPUNIT_ASSERT(err == 0);
                break;
            }

            default:
                break;
        }
    }

    while (head != NULL)
    {
        tmp = head->next;
        delete head;
        head = tmp;
    }

#ifdef BRM_VERBOSE
    cerr << "thread " << threadNum << " exiting" << endl;
#endif
    return NULL;
}
*/

static void* EMRunner(void* arg)
{
    // keep track of LBID ranges allocated here and
    // randomly allocate, lookup, delete, get/set HWM, and
    // destroy the EM object.

#ifdef BRM_VERBOSE
    uint64_t threadNum = (uint64_t)arg;
#endif
    int op, listSize = 0;
    uint32_t randstate;
    struct EMEntries* head = NULL, *tmp;
    struct timeval tv;
    ExtentMap* em;
    LBID_t lbid;
    uint32_t colWidth;
    PartitionNumberT partNum;
    SegmentT segmentNum;
    execplan::CalpontSystemCatalog::ColDataType colDataType;

#ifdef BRM_VERBOSE
    cerr << "thread number " << threadNum << " started." << endl;
#endif

    gettimeofday(&tv, NULL);
    randstate = static_cast<uint32_t>(tv.tv_usec);
    //pthread_mutex_lock(&pthreadMutex);
    em = new ExtentMap();
    //pthread_mutex_unlock(&pthreadMutex);

    while (!threadStop)
    {
        auto randNumber = rand_r(&randstate);
        op = randNumber % 10;

        colWidth = colWidthsAvailable[randNumber % colWidthsAvailable.size()];
        partNum = randNumber % std::numeric_limits<PartitionNumberT>::max();
        segmentNum = randNumber % std::numeric_limits<SegmentT>::max();
        colDataType = (execplan::CalpontSystemCatalog::ColDataType) (randNumber % (int)execplan::CalpontSystemCatalog::ColDataType::TIMESTAMP);
#ifdef BRM_VERBOSE
        cerr << "next op is " << op << endl;
#endif

        switch (op)
        {
            case 0:   //allocate space for a new file
            {
                vector<struct EMEntry> emEntriesVec;
                struct EMEntries* newEm;
                size_t numberOfExtents = randNumber % 4 + 1;
                int OID;
                uint32_t startBlockOffset;

                pthread_mutex_lock(&pthreadMutex);
                OID = oid++;
                pthread_mutex_unlock(&pthreadMutex);

                em->getExtents(OID, emEntriesVec, false, false, true);
                size_t extentsNumberBefore = emEntriesVec.size();
                int allocdsize;
                for (size_t i = 0; i < numberOfExtents; ++i)
                {
                    em->createColumnExtent_DBroot(OID, colWidth, dbroot, colDataType,
                        partNum, segmentNum, lbid, allocdsize, startBlockOffset);
                    em->confirmChanges();

                    newEm = new EMEntries(allocdsize, OID, startBlockOffset, lbid,
                        head, dbroot, partNum, segmentNum);
                    head = newEm;
                    listSize++;
                }
                
                emEntriesVec.clear();
                em->getExtents(OID, emEntriesVec, false, false, true);
                size_t extentsNumberAfter = emEntriesVec.size();

                CPPUNIT_ASSERT(extentsNumberBefore + numberOfExtents == extentsNumberAfter);

#ifdef BRM_VERBOSE
                cerr << "created new space for OID " << newEm->OID << endl;
#endif
                //em->checkConsistency();
                break;
            }
/*
            case 1:		//allocate space for an existing file
            {
                if (listSize == 0)
                    break;

                struct EMEntries* newEm, *tmp;
                size_t size = rand_r(&randstate) % 10;
                int fileRand = rand_r(&randstate) % listSize;
                int i, lastExtent, blockEnd, oid;
                int tmpHWM, entries, allocdSize;
                uint32_t startBlockOffset;
                lbids.clear();

                for (i = 0, tmp = head; i < fileRand; i++)
                    tmp = tmp->next;

                oid = tmp->OID;

                for (lastExtent = 0, tmp = head; tmp != NULL; tmp = tmp->next)
                {
                    if (tmp->OID != oid)
                        continue;

                    tmpHWM = tmp->HWM;
                    blockEnd = tmp->FBO + tmp->size;

                    if (lastExtent < blockEnd)
                        lastExtent = blockEnd;
                }

                for (size_t i = 0; i < size; ++i)
                {
                    em->createColumnExtent_DBroot(oid, colWidth, dbroot, colDataType,
                        partNum, segmentNum, lbid, allocdSize, startBlockOffset);
                    em->confirmChanges();
                    lbids.push_back(lbid);
                }

                //em->createExtent(size, oid, lbids, allocdSize);
                //em->confirmChanges();

                entries = size / em->getExtentSize();

                if ((size % em->getExtentSize()) != 0)
                    entries++;

                CPPUNIT_ASSERT((uint32_t)entries == lbids.size());

                for (i = 0; i < entries; i++)
                {
                    newEm = new EMEntries((i != entries) ? em->getExtentSize() : size % em->getExtentSize(),
                       oid, lastExtent + (i * em->getExtentSize()),
                       lbids[i], head, dbroot, partNum, segmentNum);
                    newEm->HWM = tmpHWM;
                    head = newEm;
                    listSize++;
                }

#ifdef BRM_VERBOSE
                cerr << "created another extent for OID " << newEm->OID << endl;
#endif
                em->checkConsistency();
                break;
            }
*/

            case 2:  			//delete an OID
            {
                if (listSize == 0)
                    break;

                struct EMEntries* tmp, *prev;
                int fileRand = rand_r(&randstate) % listSize;
                int i, oid;

                for (i = 0, tmp = head; i < fileRand; i++)
                    tmp = tmp->next;

                oid = tmp->OID;

                em->deleteOID(oid);
                em->confirmChanges();

                vector<struct EMEntry> emEntriesVec;
                em->getExtents(oid, emEntriesVec, false, false, true);
                CPPUNIT_ASSERT(emEntriesVec.empty());

                for (tmp = head; tmp != NULL;)
                {
                    if (tmp->OID == oid)
                    {
                        if (tmp == head)
                        {
                            head = head->next;
                            delete tmp;
                            tmp = head;
                        }
                        else
                        {
                            prev->next = tmp->next;
                            delete tmp;
                            tmp = prev->next;
                        }

                        listSize--;
                    }
                    else
                    {
                        prev = tmp;
                        tmp = tmp->next;
                    }
                }

#ifdef BRM_VERBOSE
                cerr << "deleted OID " << oid << endl;
#endif
                //em->checkConsistency();
                break;
            }

            case 3:   //lookup by LBID
            {
                if (listSize == 0)
                    break;

                int entryRand = rand_r(&randstate) % listSize;
                int i, err, offset, oid;
                struct EMEntries* tmp;
                LBID_t target;
                uint32_t fbo;
                DBRootT localDbroot;
                PartitionNumberT localPartNum;
                SegmentT localSegmentNum;

                for (i = 0, tmp = head; i < entryRand; i++)
                    tmp = tmp->next;

                offset = rand_r(&randstate) % (tmp->size - 1);

                target = tmp->LBIDstart + offset;
                err = em->lookupLocal(target, oid, localDbroot, localPartNum, localSegmentNum, fbo);
#ifdef BRM_VERBOSE
                cerr << "looked up LBID " << target << " got oid " << oid << " fbo " << fbo << endl;
                cerr << "   oid should be " << tmp->OID << " fbo should be " << offset + tmp->FBO << endl;
                cerr << "op 3 fbo " << fbo << " offset + tmp->FBO " << offset + tmp->FBO << endl; 
#endif
                CPPUNIT_ASSERT(err == 0);
                CPPUNIT_ASSERT(oid == tmp->OID);
                CPPUNIT_ASSERT(fbo == offset + tmp->FBO);
                //em->checkConsistency();
                break;
            }

            case 4:   //lookup by OID, FBO
            {
                if (listSize == 0)
                    break;

                int entryRand = rand_r(&randstate) % listSize;
                int i, oid, err, offset;
                struct EMEntries* tmp;
                LBID_t lbid;

                for (i = 0, tmp = head; i < entryRand; i++)
                    tmp = tmp->next;

                offset = rand_r(&randstate) % (tmp->size - 1);
                oid = tmp->OID;

                err = em->lookupLocal(oid, tmp->partNum, tmp->segNum, offset + tmp->FBO, lbid);
#ifdef BRM_VERBOSE
                cerr << "looked up OID " << oid << " fbo " << offset + tmp->FBO <<
                     " got lbid " << lbid << endl;
                cerr << "  lbid should be " << tmp->LBIDstart + offset << endl;
#endif
                CPPUNIT_ASSERT(err == 0);
                CPPUNIT_ASSERT(lbid == tmp->LBIDstart + offset);
                //em->checkConsistency();
                break;
            }

            case 5:		//getHWM
            {
                if (listSize == 0)
                    break;

                int entryRand = rand_r(&randstate) % listSize;
                int i, status;
                struct EMEntries* tmp;
                uint32_t hwm;

                for (i = 0, tmp = head; i < entryRand; i++)
                    tmp = tmp->next;

                hwm = em->getLocalHWM(tmp->OID, tmp->partNum, tmp->segNum, status);
#ifdef BRM_VERBOSE
                cerr << "stored HWM for OID " << tmp->OID << " is " << tmp->HWM
                     << " BRM says it's " << hwm << endl;
#endif
                CPPUNIT_ASSERT(hwm == tmp->HWM);
                //em->checkConsistency();
                break;
            }

            case 6: 		//setHWM
            {
                if (listSize == 0)
                    break;

                int entryRand = rand_r(&randstate) % listSize;
                int i, hwm, oid;
                struct EMEntries* tmp;

                for (i = 0, tmp = head; i < entryRand; i++)
                    tmp = tmp->next;

                oid = tmp->OID;
                hwm = rand_r(&randstate) % (tmp->size - 1);
                bool firstNode = true;
                em->setLocalHWM(oid, tmp->partNum, tmp->segNum, hwm, firstNode);
                
                em->confirmChanges();

                tmp->HWM = hwm;

#ifdef BRM_VERBOSE
                cerr << "setHWM of OID " << oid << " to " << hwm << endl;
#endif
                //em->checkConsistency();
                break;
            }

/*
            case 7:			// renew this EM object
            {
                delete em;
                em = new ExtentMap();
#ifdef BRM_VERBOSE
                cerr << "got a new EM instance" << endl;
#endif
                em->checkConsistency();
                break;
            }
            case 8:			//getBulkInsertVars
            {
                if (listSize == 0)
                    break;

                HWM_t hwm;
                VER_t txnID;
                int entryRand = rand_r(&randstate) % listSize;
                int i, err, offset;
                EMEntries* tmp;
                LBID_t lbid;

                for (i = 0, tmp = head; i < entryRand; i++)
                    tmp = tmp->next;

                offset = rand_r(&randstate) % tmp->size;
                lbid = tmp->LBIDstart + offset;
                err = em->getBulkInsertVars(lbid, hwm, txnID);
                CPPUNIT_ASSERT(err == 0);
                CPPUNIT_ASSERT(hwm == tmp->secondHWM);
                CPPUNIT_ASSERT(txnID == tmp->txnID);
                break;
            }

            case 9:			//setBulkInsertVars
            {
                if (listSize == 0)
                    break;

                int entryRand = rand_r(&randstate) % listSize;
                int i, err, offset;
                EMEntries* tmp;

                for (i = 0, tmp = head; i < entryRand; i++)
                    tmp = tmp->next;

                offset = rand_r(&randstate) % tmp->size;
                tmp->secondHWM = rand_r(&randstate) % MAXINT;
                tmp->txnID = rand_r(&randstate) % MAXINT;
                err = em->setBulkInsertVars(tmp->LBIDstart + offset,
                                            tmp->secondHWM, tmp->txnID);
                em->confirmChanges();
                CPPUNIT_ASSERT(err == 0);
                break;
            }
*/
            default:
                break;
        }
    }

    delete em;

    while (head != NULL)
    {
        tmp = head->next;
        delete head;
        head = tmp;
    }

#ifdef BRM_VERBOSE
    cerr << "thread " << threadNum << " exiting" << endl;
#endif
    return NULL;
}

/*
ExtentMap em_si;
static void* EMRunner_si(void* arg)
{

    // keep track of LBID ranges allocated here and
    // randomly allocate, lookup, delete, get/set HWM, and
    // destroy the EM object.

#ifdef BRM_VERBOSE
    int threadNum = reinterpret_cast<int>(arg);
#endif
    int op, listSize = 0, i;
    uint32_t randstate;
    struct EMEntries* head = NULL, *tmp;
    struct timeval tv;
    vector<LBID_t> lbids;

#ifdef BRM_VERBOSE
    cerr << "thread number " << threadNum << " started." << endl;
#endif

    gettimeofday(&tv, NULL);
    randstate = static_cast<uint32_t>(tv.tv_usec);

    while (!threadStop)
    {
        op = rand_r(&randstate) % 9;
#ifdef BRM_VERBOSE
        cerr << "next op is " << op << endl;
#endif

        switch (op)
        {
            case 0:   //allocate space for a new file
            {
                struct EMEntries* newEm;
                int size = rand_r(&randstate) % 102399 + 1;
                int entries, OID, allocdSize;

                pthread_mutex_lock(&pthreadMutex);
                OID = oid++;
                pthread_mutex_unlock(&pthreadMutex);

                em_si.createExtent(size, OID, lbids, allocdSize);
                em_si.confirmChanges();

                entries = size / em_si.getExtentSize();

                if ((size % em_si.getExtentSize()) != 0)
                    entries++;

                CPPUNIT_ASSERT((uint32_t)entries == lbids.size());

                for (i = 0 ; i < entries; i++)
                {

                    newEm = new EMEntries();
                    newEm->size = em_si.getExtentSize();
                    newEm->OID = OID;
                    newEm->FBO = i * em_si.getExtentSize();
                    newEm->LBIDstart = lbids[i];

                    newEm->next = head;
                    head = newEm;
                    listSize++;
                }

#ifdef BRM_VERBOSE
                cerr << "created new space for OID " << newEm->OID << endl;
#endif
                em_si.checkConsistency();
                break;
            }

            case 1:		//allocate space for an existing file
            {
                if (listSize == 0)
                    break;

                struct EMEntries* newEm, *tmp;
                int size = rand_r(&randstate) % 102399 + 1;
                int fileRand = rand_r(&randstate) % listSize;
                int i, lastExtent, blockEnd, oid;
                int tmpHWM, entries, allocdSize;
                vector<LBID_t> lbids;

                for (i = 0, tmp = head; i < fileRand; i++)
                    tmp = tmp->next;

                oid = tmp->OID;

                for (lastExtent = 0, tmp = head; tmp != NULL; tmp = tmp->next)
                {
                    if (tmp->OID != oid)
                        continue;

                    tmpHWM = tmp->HWM;
                    blockEnd = tmp->FBO + tmp->size;

                    if (lastExtent < blockEnd)
                        lastExtent = blockEnd;
                }

                em_si.createExtent(size, oid, lbids, allocdSize);
                em_si.confirmChanges();

                entries = size / em_si.getExtentSize();

                if ((size % em_si.getExtentSize()) != 0)
                    entries++;

                CPPUNIT_ASSERT((uint32_t)entries == lbids.size());

                for (i = 0; i < entries; i++)
                {

                    newEm = new EMEntries();

                    if (i != entries)
                        newEm->size = em_si.getExtentSize();
                    else
                        newEm->size = size % em_si.getExtentSize();

                    newEm->OID = oid;
                    newEm->FBO = lastExtent + (i * em_si.getExtentSize());
                    newEm->LBIDstart = lbids[i];
                    newEm->HWM = tmpHWM;

                    newEm->next = head;
                    head = newEm;
                    listSize++;
                }

#ifdef BRM_VERBOSE
                cerr << "created another extent for OID " << newEm->OID << endl;
#endif
                em_si.checkConsistency();
                break;
            }

            case 2:  			//delete an OID
            {
                if (listSize == 0)
                    break;

                struct EMEntries* tmp, *prev;
                int fileRand = rand_r(&randstate) % listSize;
                int i, oid;

                for (i = 0, tmp = head; i < fileRand; i++)
                    tmp = tmp->next;

                oid = tmp->OID;

                em_si.deleteOID(oid);
                em_si.confirmChanges();

                for (tmp = head; tmp != NULL;)
                {
                    if (tmp->OID == oid)
                    {
                        if (tmp == head)
                        {
                            head = head->next;
                            delete tmp;
                            tmp = head;
                        }
                        else
                        {
                            prev->next = tmp->next;
                            delete tmp;
                            tmp = prev->next;
                        }

                        listSize--;
                    }
                    else
                    {
                        prev = tmp;
                        tmp = tmp->next;
                    }
                }

#ifdef BRM_VERBOSE
                cerr << "deleted OID " << oid << endl;
#endif
                em_si.checkConsistency();
                break;
            }

            case 3:   //lookup by LBID
            {
                if (listSize == 0)
                    break;

                int entryRand = rand_r(&randstate) % listSize;
                int i, err, offset, oid;
                struct EMEntries* tmp;
                LBID_t target;
                uint32_t fbo;

                for (i = 0, tmp = head; i < entryRand; i++)
                    tmp = tmp->next;

                offset = rand_r(&randstate) % tmp->size;

                target = tmp->LBIDstart + offset;
                err = em_si.lookup(target, oid, fbo);
#ifdef BRM_VERBOSE
                cerr << "looked up LBID " << target << " got oid " << oid << " fbo " << fbo << endl;
                cerr << "   oid should be " << tmp->OID << " fbo should be " << offset + tmp->FBO << endl;
#endif
                CPPUNIT_ASSERT(err == 0);
                CPPUNIT_ASSERT(oid == tmp->OID);
                CPPUNIT_ASSERT(fbo == offset + tmp->FBO);
                em_si.checkConsistency();
                break;
            }

            case 4:   //lookup by OID, FBO
            {
                if (listSize == 0)
                    break;

                int entryRand = rand_r(&randstate) % listSize;
                int i, oid, err, offset;
                struct EMEntries* tmp;
                LBID_t lbid;

                for (i = 0, tmp = head; i < entryRand; i++)
                    tmp = tmp->next;

                offset = rand_r(&randstate) % tmp->size;
                oid = tmp->OID;

                err = em_si.lookup(oid, offset + tmp->FBO, lbid);
#ifdef BRM_VERBOSE
                cerr << "looked up OID " << oid << " fbo " << offset + tmp->FBO <<
                     " got lbid " << lbid << endl;
                cerr << "  lbid should be " << tmp->LBIDstart + offset << endl;
#endif
                CPPUNIT_ASSERT(err == 0);
                CPPUNIT_ASSERT(lbid == tmp->LBIDstart + offset);
                em_si.checkConsistency();
                break;
            }

            case 5:		//getHWM
            {
                if (listSize == 0)
                    break;

                int entryRand = rand_r(&randstate) % listSize;
                int i;
                struct EMEntries* tmp;
                uint32_t hwm;

                for (i = 0, tmp = head; i < entryRand; i++)
                    tmp = tmp->next;

                hwm = em_si.getHWM(tmp->OID);
#ifdef BRM_VERBOSE
                cerr << "stored HWM for OID " << tmp->OID << " is " << tmp->HWM
                     << " BRM says it's " << hwm << endl;
#endif
                CPPUNIT_ASSERT(hwm == tmp->HWM);
                em_si.checkConsistency();
                break;
            }

            case 6: 		//setHWM
            {
                if (listSize == 0)
                    break;

                int entryRand = rand_r(&randstate) % listSize;
                int i, hwm, oid;
                struct EMEntries* tmp;

                for (i = 0, tmp = head; i < entryRand; i++)
                    tmp = tmp->next;

                oid = tmp->OID;
                hwm = rand_r(&randstate) % (tmp->FBO + em_si.getExtentSize());
                em_si.setHWM(oid, hwm);
                em_si.confirmChanges();

                for (tmp = head; tmp != NULL; tmp = tmp->next)
                    if (tmp->OID == oid)
                        tmp->HWM = hwm;

#ifdef BRM_VERBOSE
                cerr << "setHWM of OID " << oid << " to " << hwm << endl;
#endif
                em_si.checkConsistency();
                break;
            }

            case 7:			//getBulkInsertVars
            {
                if (listSize == 0)
                    break;

                HWM_t hwm;
                VER_t txnID;
                int entryRand = rand_r(&randstate) % listSize;
                int i, err, offset;
                EMEntries* tmp;
                LBID_t lbid;

                for (i = 0, tmp = head; i < entryRand; i++)
                    tmp = tmp->next;

                offset = rand_r(&randstate) % tmp->size;
                lbid = tmp->LBIDstart + offset;
                err = em_si.getBulkInsertVars(lbid, hwm, txnID);
                CPPUNIT_ASSERT(err == 0);
                CPPUNIT_ASSERT(hwm == tmp->secondHWM);
                CPPUNIT_ASSERT(txnID == tmp->txnID);
                break;
            }

            case 8:			//setBulkInsertVars
            {
                if (listSize == 0)
                    break;

                int entryRand = rand_r(&randstate) % listSize;
                int i, err, offset;
                EMEntries* tmp;

                for (i = 0, tmp = head; i < entryRand; i++)
                    tmp = tmp->next;

                offset = rand_r(&randstate) % tmp->size;
                tmp->secondHWM = rand_r(&randstate) % MAXINT;
                tmp->txnID = rand_r(&randstate) % MAXINT;
                err = em_si.setBulkInsertVars(tmp->LBIDstart + offset,
                                              tmp->secondHWM, tmp->txnID);
                em_si.confirmChanges();
                CPPUNIT_ASSERT(err == 0);
                break;
            }

            default:
                break;
        }
    }

    while (head != NULL)
    {
        tmp = head->next;
        delete head;
        head = tmp;
    }

#ifdef BRM_VERBOSE
    cerr << "thread " << threadNum << " exiting" << endl;
#endif
    return NULL;
}
*/


class LongBRMTests : public CppUnit::TestFixture
{

    CPPUNIT_TEST_SUITE(LongBRMTests);

  	CPPUNIT_TEST(longEMTest_1);
// 	CPPUNIT_TEST(longEMTest_2);
//    CPPUNIT_TEST(longBRMTest_1);
//    CPPUNIT_TEST(longBRMTest_2);

    CPPUNIT_TEST_SUITE_END();

private:
public:
    void longEMTest_1()
    {
        const int threadCount = 1;
        int i;
        pthread_t threads[threadCount];

        cerr << endl << "Multithreaded, multiple instance ExtentMap test.  "
             "This runs for 5 minutes." << endl;

        threadStop = 0;
        pthread_mutex_init(&pthreadMutex, nullptr);

        for (i = 0; i < threadCount; i++)
        {
            if (pthread_create(&threads[i], NULL, EMRunner,
                               reinterpret_cast<void*>(i + 1)) < 0)
                throw logic_error("Error creating threads for the ExtentMap test");

            usleep(1000);
        }

        sleep(3600);
        threadStop = 1;

        for (i = 0; i < threadCount; i++)
        {
            cerr << "Waiting for thread #" << i << endl;
            pthread_join(threads[i], nullptr);
        }
    }

/*
    void longEMTest_2()
    {
        const int threadCount = 10;
        int i;
        pthread_t threads[threadCount];

        cerr << endl << "Multithreaded, single instance ExtentMap test.  "
             "This runs for 5 minutes." << endl;

        threadStop = 0;
        pthread_mutex_init(&pthreadMutex, NULL);

        for (i = 0; i < threadCount; i++)
        {
            if (pthread_create(&threads[i], NULL, EMRunner_si,
                               reinterpret_cast<void*>(i + 1)) < 0)
                throw logic_error("Error creating threads for the ExtentMap test");

            usleep(1000);
        }

        sleep(60);
        threadStop = 1;

        for (i = 0; i < threadCount; i++)
        {
            cerr << "Waiting for thread #" << i << endl;
            pthread_join(threads[i], NULL);
        }
    }
    void longBRMTest_1()
    {
        const int threadCount = 10;
        int i;
        pthread_t threads[threadCount];

        cerr << endl << "Multithreaded, multiple instance DBRM test.  "
             "This runs for 5 minutes." << endl;

        threadStop = 0;
        pthread_mutex_init(&pthreadMutex, NULL);
        opCount = 0;

        for (i = 0; i < threadCount; i++)
        {
            if (pthread_create(&threads[i], NULL, BRMRunner_1,
                               reinterpret_cast<void*>(i + 1)) < 0)
                throw logic_error("Error creating threads for the DBRM test");

            usleep(1000);
        }

        sleep(300);
        threadStop = 1;

        for (i = 0; i < threadCount; i++)
        {
            cerr << "Waiting for thread #" << i << endl;
            pthread_join(threads[i], NULL);
        }

        cerr << "opCount = " << opCount << endl;
    }
    void longBRMTest_2()
    {
        const int threadCount = 10;
        int i;
        pthread_t threads[threadCount];

        cerr << endl << "Multithreaded, single instance DBRM test.  "
             "This runs for 5 minutes." << endl;

        threadStop = 0;
        pthread_mutex_init(&pthreadMutex, NULL);
        opCount = 0;

        for (i = 0; i < threadCount; i++)
        {
            if (pthread_create(&threads[i], NULL, BRMRunner_si,
                               reinterpret_cast<void*>(i + 1)) < 0)
                throw logic_error("Error creating threads for the DBRM test");

            usleep(1000);
        }

        sleep(300);
        threadStop = 1;

        for (i = 0; i < threadCount; i++)
        {
            cerr << "Waiting for thread #" << i << endl;
            pthread_join(threads[i], NULL);
        }

        cerr << "opCount = " << opCount << endl;
    }
*/

};
CPPUNIT_TEST_SUITE_REGISTRATION( LongBRMTests );

#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

int main( int argc, char** argv)
{
    CppUnit::TextUi::TestRunner runner;
    CppUnit::TestFactoryRegistry& registry = CppUnit::TestFactoryRegistry::getRegistry();
    runner.addTest( registry.makeTest() );
    idbdatafile::IDBPolicy::configIDBPolicy();
    bool wasSuccessful = runner.run( "", false );
    return (wasSuccessful ? 0 : 1);
}



