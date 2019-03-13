/*
   Copyright (c) 2017, MariaDB
   Copyright (C) 2014 InfiniDB, Inc.

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
   MA 02110-1301, USA.
*/

//#define NDEBUG
#include <cassert>
#include <sstream>
#include <iomanip>
using namespace std;

#include <boost/shared_ptr.hpp>
using namespace boost;

#include "loggingid.h"
#include "errorcodes.h"
#include "idberrorinfo.h"
using namespace logging;

#include "rowgroup.h"
using namespace rowgroup;

#include "idborderby.h"
using namespace ordering;

#include "windowfunctionstep.h"
using namespace joblist;

#include "windowfunctiontype.h"
#include "framebound.h"
#include "windowframe.h"
#include "windowfunction.h"


namespace windowfunction
{

WindowFunction::WindowFunction(boost::shared_ptr<WindowFunctionType>& f,
                               boost::shared_ptr<ordering::EqualCompData>& p,
                               boost::shared_ptr<OrderByData>& o,
                               boost::shared_ptr<WindowFrame>& w,
                               const RowGroup& g,
                               const Row& r) :
    fFunctionType(f), fPartitionBy(p), fOrderBy(o), fFrame(w), fRowGroup(g), fRow(r)
{
}


WindowFunction::~WindowFunction()
{
}


void WindowFunction::operator()()
{
    try
    {
        fRowData.reset(new vector<RowPosition>(fStep->getRowData()));

        if (fOrderBy->rule().fCompares.size() > 0)
            sort(fRowData->begin(), fRowData->size());

        // get partitions
        if (fPartitionBy.get() != NULL && !fStep->cancelled())
        {
            int64_t i = 0;
            int64_t j = 1;
            int64_t rowCnt = fRowData->size();

            for (j = 1; j < rowCnt; j++)
            {
                if ((*(fPartitionBy.get()))
                        (getPointer((*fRowData)[j - 1]), getPointer((*fRowData)[j])))
                    continue;

                fPartition.push_back(make_pair(i, j - 1));
                i = j;
            }

            fPartition.push_back(make_pair(i, j - 1));
        }
        else
        {
            fPartition.push_back(make_pair(0, fRowData->size()));
        }

        // compute partition by partition
        int64_t uft = fFrame->upper()->boundType();
        int64_t lft = fFrame->lower()->boundType();
        bool upperUbnd = (uft == WF__UNBOUNDED_PRECEDING || uft == WF__UNBOUNDED_FOLLOWING);
        bool lowerUbnd = (lft == WF__UNBOUNDED_PRECEDING || lft == WF__UNBOUNDED_FOLLOWING);
        bool upperCnrw = (uft == WF__CURRENT_ROW);
        bool lowerCnrw = (lft == WF__CURRENT_ROW);
        fFunctionType->setRowData(fRowData);
        fFunctionType->setRowMetaData(fRowGroup, fRow);
        fFrame->setRowData(fRowData);
        fFrame->setRowMetaData(fRowGroup, fRow);

        for (uint64_t k = 0; k < fPartition.size() && !fStep->cancelled(); k++)
        {
            fFunctionType->resetData();
            fFunctionType->partition(fPartition[k]);

            int64_t begin = fPartition[k].first;
            int64_t end   = fPartition[k].second;

            if (upperUbnd && lowerUbnd)
            {
                fFunctionType->operator()(begin, end, WF__BOUND_ALL);
            }
            else if (upperUbnd && lowerCnrw)
            {
                if (fFrame->unit() == WF__FRAME_ROWS)
                {
                    for (int64_t i = begin; i <= end && !fStep->cancelled(); i++)
                    {
                        fFunctionType->operator()(begin, i, i);
                    }
                }
                else
                {
                    for (int64_t i = begin; i <= end && !fStep->cancelled(); i++)
                    {
                        pair<int64_t, int64_t> w = fFrame->getWindow(begin, end, i);
                        int64_t j = i;

                        if (w.second > i)
                            j = w.second;

                        fFunctionType->operator()(begin, j, i);
                    }
                }
            }
            else if (upperCnrw && lowerUbnd)
            {
                if (fFrame->unit() == WF__FRAME_ROWS)
                {
                    for (int64_t i = end; i >= begin && !fStep->cancelled(); i--)
                    {
                        fFunctionType->operator()(i, end, i);
                    }
                }
                else
                {
                    for (int64_t i = end; i >= begin && !fStep->cancelled(); i--)
                    {
                        pair<int64_t, int64_t> w = fFrame->getWindow(begin, end, i);
                        int64_t j = i;

                        if (w.first < i)
                            j = w.first;

                        fFunctionType->operator()(j, end, i);
                    }
                }
            }
            else
            {
                pair<int64_t, int64_t> w;
                pair<int64_t, int64_t> prevFrame;
                int64_t b, e;
                bool firstTime = true;

                for (int64_t i = begin; i <= end && !fStep->cancelled(); i++)
                {
                    w = fFrame->getWindow(begin, end, i);
                    b = w.first;
                    e = w.second;

                    if (firstTime)
                    {
                        prevFrame = w;
                    }

                    // UDAnF functions may have a dropValue function implemented.
                    // If they do, we can optimize by calling dropValue() for those
                    // values leaving the window and nextValue for those entering, rather
                    // than a resetData() and then iterating over the entire window.
                    // Built-in functions may have this functionality added in the future.
                    // If b > e then the frame is entirely outside of the partition
                    // and there's no values to drop
                    if (!firstTime && (b <= e) && fFunctionType->dropValues(prevFrame.first, w.first))
                    {
                        // Adjust the beginning of the frame for nextValue
                        // to start where the previous frame left off.
                        b = prevFrame.second + 1;
                    }
                    else
                    {
                        // If dropValues failed or doesn't exist,
                        // calculate the entire frame.
                        fFunctionType->resetData();
                    }
                    fFunctionType->operator()(b, e, i); // UDAnF: Calls nextValue and evaluate
                    prevFrame = w;
                    firstTime = false;
                }
            }
        }
    }
    catch (IDBExcept& iex)
    {
        fStep->handleException(iex.what(), iex.errorCode());
    }
    catch (const std::exception& ex)
    {
        fStep->handleException(ex.what(), logging::ERR_EXECUTE_WINDOW_FUNCTION);
    }
    catch (...)
    {
        fStep->handleException("unknown exception", logging::ERR_EXECUTE_WINDOW_FUNCTION);
    }
}


void WindowFunction::setCallback(joblist::WindowFunctionStep* step, int id)
{
    fStep = step;
    fId = id;
    fFunctionType->setCallback(step);
    fFrame->setCallback(step);
}


const Row& WindowFunction::getRow() const
{
    return fRow;
}


void WindowFunction::sort(std::vector<RowPosition>::iterator v, uint64_t n)
{
    // recursive function termination condition.
    if (n < 2 || fStep->cancelled())
        return;

    RowPosition                   p = *(v + n / 2); // pivot value
    vector<RowPosition>::iterator l = v;            // low   address
    vector<RowPosition>::iterator h = v + (n - 1);  // high  address

    while (l <= h && !(fStep->cancelled()))
    {
        // Can use while here, but need check boundary and cancel status.
        if (fOrderBy->operator()(getPointer(*l), getPointer(p)))
        {
            l++;
        }
        else if (fOrderBy->operator()(getPointer(p), getPointer(*h)))
        {
            h--;
        }
        else
        {
            RowPosition t = *l;    // temp value for swap
            *l++ = *h;
            *h-- = t;
        }
    }

    sort(v, distance(v, h) + 1);
    sort(l, distance(l, v) + n);
}


}   //namespace
// vim:ts=4 sw=4:

