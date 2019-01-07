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

//  $Id: limitedorderby.cpp 9581 2013-05-31 13:46:14Z pleblanc $


#include <iostream>
//#define NDEBUG
#include <cassert>
#include <string>
using namespace std;

#include <boost/shared_array.hpp>
using namespace boost;

#include "errorids.h"
#include "exceptclasses.h"
using namespace logging;

#include "rowgroup.h"
using namespace rowgroup;

#include "jlf_common.h"
#include "limitedorderby.h"

using namespace ordering;

namespace joblist
{


// LimitedOrderBy class implementation
LimitedOrderBy::LimitedOrderBy() : fStart(0), fCount(-1)
{
    fRule.fIdbCompare = this;
}


LimitedOrderBy::~LimitedOrderBy()
{
}


void LimitedOrderBy::initialize(const RowGroup& rg, const JobInfo& jobInfo)
{
    fRm = jobInfo.rm;
    fSessionMemLimit = jobInfo.umMemLimit;
    fErrorCode = ERR_LIMIT_TOO_BIG;

    // locate column position in the rowgroup
    map<uint32_t, uint32_t> keyToIndexMap;

    for (uint64_t i = 0; i < rg.getKeys().size(); ++i)
    {
        if (keyToIndexMap.find(rg.getKeys()[i]) == keyToIndexMap.end())
            keyToIndexMap.insert(make_pair(rg.getKeys()[i], i));
    }

    vector<pair<uint32_t, bool> >::const_iterator i = jobInfo.orderByColVec.begin();

    for ( ; i != jobInfo.orderByColVec.end(); i++)
    {
        map<uint32_t, uint32_t>::iterator j = keyToIndexMap.find(i->first);
        idbassert(j != keyToIndexMap.end());

        // TODO Ordering direction in CSEP differs from
        // internal direction representation. This behavior should be fixed
        fOrderByCond.push_back(IdbSortSpec(j->second, i->second));
    }

    // limit row count info
    fStart = jobInfo.limitStart;
    fCount = jobInfo.limitCount;

//	fMemSize = (fStart + fCount) * rg.getRowSize();

    IdbOrderBy::initialize(rg);
}


uint64_t LimitedOrderBy::getKeyLength() const
{
    //return (fRow0.getSize() - 2);
    return fRow0.getColumnCount();
}


void LimitedOrderBy::processRow(const rowgroup::Row& row)
{
    // check if this is a distinct row
    if (fDistinct && fDistinctMap->find(row.getPointer()) != fDistinctMap->end())
        return;

    // @bug5312, limit count is 0, do nothing.
    if (fCount == 0)
        return;

    // if the row count is less than the limit
    if (fOrderByQueue.size() < fStart + fCount)
    {
        copyRow(row, &fRow0);
        OrderByRow newRow(fRow0, fRule);
        fOrderByQueue.push(newRow);

        // add to the distinct map
        if (fDistinct)
            fDistinctMap->insert(fRow0.getPointer());

        fRowGroup.incRowCount();
        fRow0.nextRow();

        if (fRowGroup.getRowCount() >= fRowsPerRG)
        {
            fDataQueue.push(fData);
            uint64_t newSize = fRowsPerRG * fRowGroup.getRowSize();
            fMemSize += newSize;

            if (!fRm->getMemory(newSize, fSessionMemLimit))
            {
                cerr << IDBErrorInfo::instance()->errorMsg(fErrorCode)
                     << " @" << __FILE__ << ":" << __LINE__;
                throw IDBExcept(fErrorCode);
            }

            fData.reinit(fRowGroup, fRowsPerRG);
            fRowGroup.setData(&fData);
            fRowGroup.resetRowGroup(0);
            fRowGroup.getRow(0, &fRow0);
        }
    }

    else if (fOrderByCond.size() > 0 && fRule.less(row.getPointer(), fOrderByQueue.top().fData))
    {
        OrderByRow swapRow = fOrderByQueue.top();
        row1.setData(swapRow.fData);

        copyRow(row, &row1);

        if (fDistinct)
        {
            fDistinctMap->erase(fOrderByQueue.top().fData);
            fDistinctMap->insert(row1.getPointer());
        }

        fOrderByQueue.pop();
        fOrderByQueue.push(swapRow);
    }
}

/*
 * The f() copies top element from an ordered queue into a row group. It 
 * does this backwards to syncronise sorting orientation with the server.
 * The top row from the queue goes last into the returned set.
 */ 
void LimitedOrderBy::finalize()
{
    queue<RGData> tempQueue;
    if (fRowGroup.getRowCount() > 0)
        fDataQueue.push(fData);

    if (fOrderByQueue.size() > 0)
    {
        uint64_t newSize = fRowsPerRG * fRowGroup.getRowSize();
        fMemSize += newSize;

        if (!fRm->getMemory(newSize, fSessionMemLimit))
        {
            cerr << IDBErrorInfo::instance()->errorMsg(fErrorCode)
                 << " @" << __FILE__ << ":" << __LINE__;
            throw IDBExcept(fErrorCode);
        }
        
        uint64_t offset = 0;
        uint64_t i = 0;
        list<RGData> tempRGDataList;
        
        // Skip first LIMIT rows in the the RowGroup
        if ( fCount <= fOrderByQueue.size() )
        {
            offset = fCount % fRowsPerRG;
            if(!offset && fCount > 0)
                offset = fRowsPerRG;
        }
        else
        {
            offset = fOrderByQueue.size() % fRowsPerRG;
            if(!offset && fOrderByQueue.size() > 0)
                offset = fRowsPerRG;
        }
        
        list<RGData>::iterator tempListIter = tempRGDataList.begin();
        
        i = 0;
        uint32_t rSize = fRow0.getSize();
        uint64_t preLastRowNumb = fRowsPerRG - 1;
        fData.reinit(fRowGroup, fRowsPerRG);
        fRowGroup.setData(&fData);
        fRowGroup.resetRowGroup(0);
        offset = offset != 0 ? offset - 1 : offset;
        fRowGroup.getRow(offset, &fRow0);
        
        while ((fOrderByQueue.size() > fStart) && (i++ < fCount))
        {
            const OrderByRow& topRow = fOrderByQueue.top();
            row1.setData(topRow.fData);
            copyRow(row1, &fRow0);
            //cerr << "LimitedOrderBy::finalize fRow0 " << fRow0.toString() << endl;
            fRowGroup.incRowCount();
            offset--;
            fRow0.prevRow(rSize);
            fOrderByQueue.pop();

            if(offset == (uint64_t)-1)
            {
                tempRGDataList.push_front(fData);
                fMemSize += newSize;

                if (!fRm->getMemory(newSize, fSessionMemLimit))
                {
                    cerr << IDBErrorInfo::instance()->errorMsg(fErrorCode)
                         << " @" << __FILE__ << ":" << __LINE__;
                    throw IDBExcept(fErrorCode);
                }
                
                fData.reinit(fRowGroup, fRowsPerRG);            
                fRowGroup.setData(&fData);
                fRowGroup.resetRowGroup(0); // ?
                fRowGroup.getRow(preLastRowNumb, &fRow0);
                offset = preLastRowNumb;
            }
        }
        // Push the last/only group into the queue.
        if (fRowGroup.getRowCount() > 0)
            tempRGDataList.push_front(fData);   
        
        for(tempListIter = tempRGDataList.begin(); tempListIter != tempRGDataList.end(); tempListIter++)        
            tempQueue.push(*tempListIter);
        
        fDataQueue = tempQueue;
    }
}


const string LimitedOrderBy::toString() const
{
    ostringstream oss;
    oss << "OrderBy   cols: ";
    vector<IdbSortSpec>::const_iterator i = fOrderByCond.begin();

    for (; i != fOrderByCond.end(); i++)
        oss << "(" << i->fIndex << ","
            << ((i->fAsc) ? "Asc" : "Desc") << ","
            << ((i->fNf) ? "null first" : "null last") << ") ";

    oss << " start-" << fStart << " count-" << fCount;

    if (fDistinct)
        oss << " distinct";

    oss << endl;

    return oss.str();
}


}
// vim:ts=4 sw=4:

