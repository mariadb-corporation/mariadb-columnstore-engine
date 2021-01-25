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

/****************************************************************************
* $Id: func_rand.cpp 3495 2013-01-21 14:09:51Z rdempsey $
*
*
****************************************************************************/

#include <my_global.h>
#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_export.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "rowgroup.h"
#include "constantcolumn.h"
using namespace execplan;

#include "dataconvert.h"


namespace funcexp
{
uint64_t maxValue = 0x3FFFFFFFL;

/**
 * Follow MySQL's rand() implementation in item_func.cpp to guarantee compatibility
 */
double Func_rand::getRand()
{
    uint64_t fSeed1_save = fSeed1;
    fSeed1 = (fSeed1 * 3 + fSeed2) % maxValue;

    // prevent the seed to repeat itself. e.g. seed1 = 1073741790; seed2 = 66;
    if (fSeed1_save == fSeed1)
        fSeed1 += 23;

    fSeed2 = (fSeed1 + fSeed2 + 33) % maxValue;
    if (fSeeds.size() > fSeedIndex) 
    {
        fSeeds[fSeedIndex] = std::make_pair(fSeed1, fSeed2);
    }
    else
    {
        fSeeds.push_back(std::make_pair(fSeed1, fSeed2));
    }

    return (((double) fSeed1) / (double)maxValue);
}

CalpontSystemCatalog::ColType Func_rand::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
    return resultType;
}


double Func_rand::getDoubleVal(rowgroup::Row& row,
                               FunctionParm& parm,
                               bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& op_ct)
{
    // NOTE: this function needs to use 32bit ints otherwise it will break for negative values
    uint32_t seedParm = 0;

    // Still on first row, multiple rands exist in statement. Need an additional seed.
    if(fFirstRow == row.getData())
    {
        fSeedSet = false;
        fSeedIndex += 1;
    }
    else if (fFirstRow == nullptr)
    {
        // Store first row
        fFirstRow = row.getData();
        fSeedIndex = 0;
    }
    else
    {
        // No longer on first row. Disable additional seeding
        if (!fMultipleSeedsSet)
        {
            fMultipleSeedsSet = true;
            fSeedIndex = 0;
        }
        else
        {
            fSeedIndex += 1;

            if (fSeedIndex == fSeeds.size())
            {
                fSeedIndex = 0;
            }
        }
    }

    // rand with parameter. if the parm is constanct, then a column is attached for fetching
    if (parm.size() == 1 || parm.size() == 2)
    {
        execplan::ConstantColumn* cc = dynamic_cast<execplan::ConstantColumn*>(parm[0].get()->data());

        if (!fSeedSet || !cc)
        {
            /* Copied from item_func.cpp */
            seedParm = parm[0]->data()->getIntVal(row, isNull);
            fSeed1 = (uint32_t)(seedParm * 0x10001L + 55555555L);
            fSeed2 = (uint32_t)(seedParm * 0x10000001L);
            fSeedSet = true;
            fSeeds.push_back(std::make_pair(fSeed1, fSeed2));
        }
        else
        {
            const std::pair<uint64_t, uint64_t> &seedPair = fSeeds[fSeedIndex];
            fSeed1 = seedPair.first;
            fSeed2 = seedPair.second;
        }
    }
    // rand without parameter. thd->rand are passed in. The 3rd is a simple column for fetching
    else
    {
        idbassert(parm.size() == 3);

        if (fSeed1 == 0)
        {
            fSeed1 = parm[0]->data()->getIntVal(row, isNull);
            fSeed2 = parm[1]->data()->getIntVal(row, isNull);
            fSeedSet = true;

            // Special case: statement such as select rand(), rand(1) so need to keep rand(1) seeded correctly
            fSeeds.push_back(std::make_pair(fSeed1, fSeed2));
        }
    }

    return getRand();
}


} // namespace funcexp
// vim:ts=4 sw=4:
