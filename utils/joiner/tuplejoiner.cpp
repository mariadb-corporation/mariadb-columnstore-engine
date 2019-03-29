/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2019 MariaDB Corporaton

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

#include "tuplejoiner.h"
#include <algorithm>
#include <vector>
#include <limits>
#ifdef _MSC_VER
#include <unordered_set>
#else
#include <tr1/unordered_set>
#endif
#include "hasher.h"
#include "lbidlist.h"

using namespace std;
using namespace rowgroup;
using namespace utils;
using namespace execplan;
using namespace joblist;

namespace joiner
{

TupleJoiner::TupleJoiner(
    const rowgroup::RowGroup& smallInput,
    const rowgroup::RowGroup& largeInput,
    uint32_t smallJoinColumn,
    uint32_t largeJoinColumn,
    JoinType jt) :
    smallRG(smallInput), largeRG(largeInput), joinAlg(INSERTING), joinType(jt),
    threadCount(1), typelessJoin(false), bSignedUnsignedJoin(false), uniqueLimit(100), finished(false)
{
    if (smallRG.getColTypes()[smallJoinColumn] == CalpontSystemCatalog::LONGDOUBLE)
    {
        STLPoolAllocator<pair<const long double, Row::Pointer> > alloc(64 * 1024 * 1024 + 1);
        _pool = alloc.getPoolAllocator();

        ld.reset(new ldhash_t(10, hasher(), ldhash_t::key_equal(), alloc));
    }
    else if (smallRG.usesStringTable())
    {
        STLPoolAllocator<pair<const int64_t, Row::Pointer> > alloc(64 * 1024 * 1024 + 1);
        _pool = alloc.getPoolAllocator();

        sth.reset(new sthash_t(10, hasher(), sthash_t::key_equal(), alloc));
    }
    else
    {
        STLPoolAllocator<pair<const int64_t, uint8_t*> > alloc(64 * 1024 * 1024 + 1);
        _pool = alloc.getPoolAllocator();

        h.reset(new hash_t(10, hasher(), hash_t::key_equal(), alloc));
    }

    smallRG.initRow(&smallNullRow);

    if (smallOuterJoin() || largeOuterJoin() || semiJoin() || antiJoin())
    {
        smallNullMemory = RGData(smallRG, 1);
        smallRG.setData(&smallNullMemory);
        smallRG.getRow(0, &smallNullRow);
        smallNullRow.initToNull();
    }

    smallKeyColumns.push_back(smallJoinColumn);
    largeKeyColumns.push_back(largeJoinColumn);
    discreteValues.reset(new bool[1]);
    cpValues.reset(new vector<int64_t>[1]);
    discreteValues[0] = false;

    if (smallRG.isUnsigned(smallKeyColumns[0]))
    {
        cpValues[0].push_back(numeric_limits<uint64_t>::max());
        cpValues[0].push_back(0);
    }
    else
    {
        cpValues[0].push_back(numeric_limits<int64_t>::max());
        cpValues[0].push_back(numeric_limits<int64_t>::min());
    }

    if (smallRG.isUnsigned(smallJoinColumn) != largeRG.isUnsigned(largeJoinColumn))
        bSignedUnsignedJoin = true;

    nullValueForJoinColumn = smallNullRow.getSignedNullValue(smallJoinColumn);
}

TupleJoiner::TupleJoiner(
    const rowgroup::RowGroup& smallInput,
    const rowgroup::RowGroup& largeInput,
    const vector<uint32_t>& smallJoinColumns,
    const vector<uint32_t>& largeJoinColumns,
    JoinType jt) :
    smallRG(smallInput), largeRG(largeInput), joinAlg(INSERTING),
    joinType(jt), threadCount(1), typelessJoin(true),
    smallKeyColumns(smallJoinColumns), largeKeyColumns(largeJoinColumns),
    bSignedUnsignedJoin(false), uniqueLimit(100), finished(false)
{
    STLPoolAllocator<pair<const TypelessData, Row::Pointer> > alloc(64 * 1024 * 1024 + 1);
    _pool = alloc.getPoolAllocator();

    ht.reset(new typelesshash_t(10, hasher(), typelesshash_t::key_equal(), alloc));
    smallRG.initRow(&smallNullRow);

    if (smallOuterJoin() || largeOuterJoin() || semiJoin() || antiJoin())
    {
        smallNullMemory = RGData(smallRG, 1);
        smallRG.setData(&smallNullMemory);
        smallRG.getRow(0, &smallNullRow);
        smallNullRow.initToNull();
    }

    for (uint32_t i = keyLength = 0; i < smallKeyColumns.size(); i++)
    {
        if (smallRG.getColTypes()[smallKeyColumns[i]] == CalpontSystemCatalog::CHAR ||
                smallRG.getColTypes()[smallKeyColumns[i]] == CalpontSystemCatalog::VARCHAR
                ||
                smallRG.getColTypes()[smallKeyColumns[i]] == CalpontSystemCatalog::TEXT)
        {
            keyLength += smallRG.getColumnWidth(smallKeyColumns[i]) + 1;  // +1 null char

            // MCOL-698: if we don't do this LONGTEXT allocates 32TB RAM
            if (keyLength > 65536)
                keyLength = 65536;
        }
        else if (smallRG.getColTypes()[smallKeyColumns[i]] == CalpontSystemCatalog::LONGDOUBLE)
        {
            keyLength += sizeof(long double);
        }
        else
        {
            keyLength += 8;
        }

        // Set bSignedUnsignedJoin if one or more join columns are signed to unsigned compares.
        if (smallRG.isUnsigned(smallKeyColumns[i]) != largeRG.isUnsigned(largeKeyColumns[i]))
        {
            bSignedUnsignedJoin = true;
        }
    }

    storedKeyAlloc = FixedAllocator(keyLength);

    discreteValues.reset(new bool[smallKeyColumns.size()]);
    cpValues.reset(new vector<int64_t>[smallKeyColumns.size()]);

    for (uint32_t i = 0; i < smallKeyColumns.size(); i++)
    {
        discreteValues[i] = false;
        if (isUnsigned(smallRG.getColTypes()[smallKeyColumns[i]]))
        {
            cpValues[i].push_back(static_cast<int64_t>(numeric_limits<uint64_t>::max()));
            cpValues[i].push_back(0);
        }
        else
        {
            cpValues[i].push_back(numeric_limits<int64_t>::max());
            cpValues[i].push_back(numeric_limits<int64_t>::min());
        }
    }
}

TupleJoiner::TupleJoiner() { }

TupleJoiner::TupleJoiner(const TupleJoiner& j)
{
    throw runtime_error("TupleJoiner(TupleJoiner) shouldn't be called.");
}

TupleJoiner& TupleJoiner::operator=(const TupleJoiner& j)
{
    throw runtime_error("TupleJoiner::operator=() shouldn't be called.");
    return *this;
}

TupleJoiner::~TupleJoiner()
{
    smallNullMemory = RGData();
}

bool TupleJoiner::operator<(const TupleJoiner& tj) const
{
    return size() < tj.size();
}

void TupleJoiner::insert(Row& r, bool zeroTheRid)
{
    /* when doing a disk-based join, only the first iteration on the large side
    will 'zeroTheRid'.  The successive iterations will need it unchanged. */
    if (zeroTheRid)
        r.zeroRid();

    updateCPData(r);

    if (joinAlg == UM)
    {
        if (typelessJoin)
        {
            TypelessData td = makeTypelessKey(r, smallKeyColumns, keyLength, &storedKeyAlloc,
                       largeRG, largeKeyColumns);
            if (td.len > 0)
            {
                ht->insert(pair<TypelessData, Row::Pointer>(td, r.getPointer()));
            }
        }
        else if (r.getColType(smallKeyColumns[0]) ==  execplan::CalpontSystemCatalog::LONGDOUBLE)
        {
            long double smallKey = r.getLongDoubleField(smallKeyColumns[0]);
            if (UNLIKELY(smallKey == joblist::LONGDOUBLENULL))
                ld->insert(pair<long double, Row::Pointer>(joblist::LONGDOUBLENULL, r.getPointer()));
            else
                ld->insert(pair<long double, Row::Pointer>(smallKey, r.getPointer()));
        }
        else if (!smallRG.usesStringTable())
        {
            int64_t smallKey;

            if (r.isUnsigned(smallKeyColumns[0]))
                smallKey = (int64_t)(r.getUintField(smallKeyColumns[0]));
            else
                smallKey = r.getIntField(smallKeyColumns[0]);

            if (UNLIKELY(smallKey == nullValueForJoinColumn))
                h->insert(pair<int64_t, uint8_t*>(getJoinNullValue(), r.getData()));
            else
                h->insert(pair<int64_t, uint8_t*>(smallKey, r.getData()));  // Normal path for integers
        }
        else
        {
            int64_t smallKey = r.getIntField(smallKeyColumns[0]);

            if (UNLIKELY(smallKey == nullValueForJoinColumn))
                sth->insert(pair<int64_t, Row::Pointer>(getJoinNullValue(), r.getPointer()));
            else
                sth->insert(pair<int64_t, Row::Pointer>(smallKey, r.getPointer()));
        }
    }
    else
    {
        rows.push_back(r.getPointer());
    }
}

void TupleJoiner::match(rowgroup::Row& largeSideRow, uint32_t largeRowIndex, uint32_t threadID,
                        vector<Row::Pointer>* matches)
{
    uint32_t i;
    bool isNull = hasNullJoinColumn(largeSideRow);

    matches->clear();

    if (inPM())
    {
        vector<uint32_t>& v = pmJoinResults[threadID][largeRowIndex];
        uint32_t size = v.size();

        for (i = 0; i < size; i++)
            if (v[i] < rows.size())
                matches->push_back(rows[v[i]]);

        if (UNLIKELY((semiJoin() || antiJoin()) && matches->size() == 0))
            matches->push_back(smallNullRow.getPointer());
    }
    else if (LIKELY(!isNull))
    {
        if (UNLIKELY(typelessJoin))
        {
            TypelessData largeKey;
            thIterator it;
            pair<thIterator, thIterator> range;

            largeKey = makeTypelessKey(largeSideRow, largeKeyColumns, keyLength, &tmpKeyAlloc[threadID], smallRG, smallKeyColumns);
            if (largeKey.len > 0)
            {
                it = ht->find(largeKey);
            }
            else
            {
                return;
            }
            
            if (it == ht->end() && !(joinType & (LARGEOUTER | MATCHNULLS)))
                return;

            range = ht->equal_range(largeKey);

            for (; range.first != range.second; ++range.first)
                matches->push_back(range.first->second);
        }
        else if (largeSideRow.getColType(largeKeyColumns[0]) == CalpontSystemCatalog::LONGDOUBLE
           && ld)
        {
            // This is a compare of two long double
            long double largeKey;
            ldIterator it;
            pair<ldIterator, ldIterator> range;
            Row r;

            largeKey = largeSideRow.getLongDoubleField(largeKeyColumns[0]);
            it = ld->find(largeKey);

            if (it == ld->end() && !(joinType & (LARGEOUTER | MATCHNULLS)))
                return;

            range = ld->equal_range(largeKey);

            for (; range.first != range.second; ++range.first)
            {
                matches->push_back(range.first->second);
            }
        }
        else if (!smallRG.usesStringTable())
        {
            int64_t largeKey;
            iterator it;
            pair<iterator, iterator> range;
            Row r;

            if (largeSideRow.getColType(largeKeyColumns[0]) == CalpontSystemCatalog::LONGDOUBLE)
            {
                largeKey = (int64_t)largeSideRow.getLongDoubleField(largeKeyColumns[0]);
            }
            else if (largeSideRow.isUnsigned(largeKeyColumns[0]))
            {
                largeKey = (int64_t)largeSideRow.getUintField(largeKeyColumns[0]);
            }
            else
            {
                largeKey = largeSideRow.getIntField(largeKeyColumns[0]);
            }

            if (ld)
            {
                // Compare against long double
                ldIterator it;
                pair<ldIterator, ldIterator> range;
                long double ldKey = largeKey;
//                ldKey = 6920;
                it = ld->find(ldKey);

                if (it == ld->end() && !(joinType & (LARGEOUTER | MATCHNULLS)))
                    return;

                range = ld->equal_range(ldKey);

                for (; range.first != range.second; ++range.first)
                {
                    matches->push_back(range.first->second);
                }
            }
            else
            {
                it = h->find(largeKey);

                if (it == h->end() && !(joinType & (LARGEOUTER | MATCHNULLS)))
                    return;

                range = h->equal_range(largeKey);

                //smallRG.initRow(&r);
                for (; range.first != range.second; ++range.first)
                {
                    //r.setData(range.first->second);
                    //cerr << "matched small side row: " << r.toString() << endl;
                    matches->push_back(range.first->second);
                }
            }
        }
        else
        {
            int64_t largeKey;
            sthash_t::iterator it;
            pair<sthash_t::iterator, sthash_t::iterator> range;
            Row r;

            largeKey = largeSideRow.getIntField(largeKeyColumns[0]);
            it = sth->find(largeKey);

            if (it == sth->end() && !(joinType & (LARGEOUTER | MATCHNULLS)))
                return;

            range = sth->equal_range(largeKey);

            //smallRG.initRow(&r);
            for (; range.first != range.second; ++range.first)
            {
                //r.setPointer(range.first->second);
                //cerr << "matched small side row: " << r.toString() << endl;
                matches->push_back(range.first->second);
            }
        }
    }

    if (UNLIKELY(largeOuterJoin() && matches->size() == 0))
    {
        //cout << "Matched the NULL row: " << smallNullRow.toString() << endl;
        matches->push_back(smallNullRow.getPointer());
    }

    if (UNLIKELY(inUM() && (joinType & MATCHNULLS) && !isNull && !typelessJoin))
    {
        if (smallRG.getColType(largeKeyColumns[0]) == CalpontSystemCatalog::LONGDOUBLE)
        {
            pair<ldIterator, ldIterator> range = ld->equal_range(joblist::LONGDOUBLENULL);

            for (; range.first != range.second; ++range.first)
                matches->push_back(range.first->second);
        }
        else if (!smallRG.usesStringTable())
        {
            pair<iterator, iterator> range = h->equal_range(getJoinNullValue());

            for (; range.first != range.second; ++range.first)
                matches->push_back(range.first->second);
        }
        else
        {
            pair<sthash_t::iterator, sthash_t::iterator> range = sth->equal_range(getJoinNullValue());

            for (; range.first != range.second; ++range.first)
                matches->push_back(range.first->second);
        }
    }

    /* Bug 3524.  For 'not in' queries this matches everything.
     */
    if (UNLIKELY(inUM() && isNull && antiJoin() && (joinType & MATCHNULLS)))
    {
        if (!typelessJoin)
        {
            if (smallRG.getColType(smallKeyColumns[0]) == CalpontSystemCatalog::LONGDOUBLE)
            {
                ldIterator it;

                for (it = ld->begin(); it != ld->end(); ++it)
                    matches->push_back(it->second);
            }
            else if (!smallRG.usesStringTable())
            {
                iterator it;

                for (it = h->begin(); it != h->end(); ++it)
                    matches->push_back(it->second);
            }
            else
            {
                sthash_t::iterator it;

                for (it = sth->begin(); it != sth->end(); ++it)
                    matches->push_back(it->second);
            }
        }
        else
        {
            thIterator it;

            for (it = ht->begin(); it != ht->end(); ++it)
                matches->push_back(it->second);
        }
    }
}

void TupleJoiner::doneInserting()
{

    // a minor textual cleanup
#ifdef TJ_DEBUG
#define CHECKSIZE \
		if (uniquer.size() > uniqueLimit) { \
			cout << "too many discrete values\n"; \
			return; \
		}
#else
#define CHECKSIZE \
		if (uniquer.size() > uniqueLimit) \
			return;
#endif

    uint32_t col;

    /* Put together the discrete values for the runtime casual partitioning restriction */

    finished = true;

    for (col = 0; col < smallKeyColumns.size(); col++)
    {
        tr1::unordered_set<int64_t> uniquer;
        tr1::unordered_set<int64_t>::iterator uit;
        sthash_t::iterator sthit;
        hash_t::iterator hit;
        ldhash_t::iterator ldit;
        typelesshash_t::iterator thit;
        uint32_t i, pmpos = 0, rowCount;
        Row smallRow;

        smallRG.initRow(&smallRow);

        if (smallRow.isCharType(smallKeyColumns[col]))
            continue;

        rowCount = size();

        if (joinAlg == PM)
            pmpos = 0;
        else if (typelessJoin)
            thit = ht->begin();
        else if (smallRG.getColType(smallKeyColumns[0]) == CalpontSystemCatalog::LONGDOUBLE)
            ldit = ld->begin();
        else if (!smallRG.usesStringTable())
            hit = h->begin();
        else
            sthit = sth->begin();

        for (i = 0; i < rowCount; i++)
        {
            if (joinAlg == PM)
                smallRow.setPointer(rows[pmpos++]);
            else if (typelessJoin)
            {
                smallRow.setPointer(thit->second);
                ++thit;
            }
            else if (smallRG.getColType(smallKeyColumns[col]) == CalpontSystemCatalog::LONGDOUBLE)
            {
                smallRow.setPointer(ldit->second);
                ++ldit;
            }
            else if (!smallRG.usesStringTable())
            {
                smallRow.setPointer(hit->second);
                ++hit;
            }
            else
            {
                smallRow.setPointer(sthit->second);
                ++sthit;
            }

            if (smallRow.getColType(smallKeyColumns[col]) == CalpontSystemCatalog::LONGDOUBLE)
            {
                double dval = (double)roundl(smallRow.getLongDoubleField(smallKeyColumns[col]));
                switch (largeRG.getColType(largeKeyColumns[col]))
                {
                    case CalpontSystemCatalog::DOUBLE:
                    case CalpontSystemCatalog::UDOUBLE:
                    case CalpontSystemCatalog::FLOAT:
                    case CalpontSystemCatalog::UFLOAT:
                    {
                        uniquer.insert(*(int64_t*)&dval);
                    }
                    default:
                    {   
                        uniquer.insert((int64_t)dval);
                    }
                }
            }
            else
            if (smallRow.isUnsigned(smallKeyColumns[col]))
            {
                uniquer.insert((int64_t)smallRow.getUintField(smallKeyColumns[col]));
            }
            else
            {
                uniquer.insert(smallRow.getIntField(smallKeyColumns[col]));
            }

            CHECKSIZE;
        }

        discreteValues[col] = true;
        cpValues[col].clear();
#ifdef TJ_DEBUG
        cout << "inserting " << uniquer.size() << " discrete values\n";
#endif

        for (uit = uniquer.begin(); uit != uniquer.end(); ++uit)
            cpValues[col].push_back(*uit);
    }
}

void TupleJoiner::setInPM()
{
    joinAlg = PM;
}

void TupleJoiner::setInUM()
{
    vector<Row::Pointer> empty;
    Row smallRow;
    uint32_t i, size;

    if (joinAlg == UM)
        return;

    joinAlg = UM;
    size = rows.size();
    smallRG.initRow(&smallRow);
#ifdef TJ_DEBUG
    cout << "converting array to hash, size = " << size << "\n";
#endif

    for (i = 0; i < size; i++)
    {
        smallRow.setPointer(rows[i]);
        insert(smallRow);
    }

#ifdef TJ_DEBUG
    cout << "done\n";
#endif
    rows.swap(empty);

    if (typelessJoin)
    {
        tmpKeyAlloc.reset(new FixedAllocator[threadCount]);

        for (i = 0; i < threadCount; i++)
            tmpKeyAlloc[i] = FixedAllocator(keyLength, true);
    }
}

void TupleJoiner::setPMJoinResults(boost::shared_array<vector<uint32_t> > jr,
                                   uint32_t threadID)
{
    pmJoinResults[threadID] = jr;
}

void TupleJoiner::markMatches(uint32_t threadID, uint32_t rowCount)
{
    boost::shared_array<vector<uint32_t> > matches = pmJoinResults[threadID];
    uint32_t i, j;

    for (i = 0; i < rowCount; i++)
        for (j = 0; j < matches[i].size(); j++)
        {
            if (matches[i][j] < rows.size())
            {
                smallRow[threadID].setPointer(rows[matches[i][j]]);
                smallRow[threadID].markRow();
            }
        }
}

void TupleJoiner::markMatches(uint32_t threadID, const vector<Row::Pointer>& matches)
{
    uint32_t rowCount = matches.size();
    uint32_t i;

    for (i = 0; i < rowCount; i++)
    {
        smallRow[threadID].setPointer(matches[i]);
        smallRow[threadID].markRow();
    }
}

boost::shared_array<std::vector<uint32_t> > TupleJoiner::getPMJoinArrays(uint32_t threadID)
{
    return pmJoinResults[threadID];
}

void TupleJoiner::setThreadCount(uint32_t cnt)
{
    threadCount = cnt;
    pmJoinResults.reset(new boost::shared_array<vector<uint32_t> >[cnt]);
    smallRow.reset(new Row[cnt]);

    for (uint32_t i = 0; i < cnt; i++)
        smallRG.initRow(&smallRow[i]);

    if (typelessJoin)
    {
        tmpKeyAlloc.reset(new FixedAllocator[threadCount]);

        for (uint32_t i = 0; i < threadCount; i++)
            tmpKeyAlloc[i] = FixedAllocator(keyLength, true);
    }

    if (fe)
    {
        fes.reset(new funcexp::FuncExpWrapper[cnt]);

        for (uint32_t i = 0; i < cnt; i++)
            fes[i] = *fe;
    }
}

void TupleJoiner::getUnmarkedRows(vector<Row::Pointer>* out)
{
    Row smallR;

    smallRG.initRow(&smallR);
    out->clear();

    if (inPM())
    {
        uint32_t i, size;

        size = rows.size();

        for (i = 0; i < size; i++)
        {
            smallR.setPointer(rows[i]);

            if (!smallR.isMarked())
                out->push_back(rows[i]);
        }
    }
    else
    {
        if (typelessJoin)
        {
            typelesshash_t::iterator it;

            for (it = ht->begin(); it != ht->end(); ++it)
            {
                smallR.setPointer(it->second);

                if (!smallR.isMarked())
                    out->push_back(it->second);
            }
        }
        else if (smallRG.getColType(smallKeyColumns[0]) == CalpontSystemCatalog::LONGDOUBLE)
        {
            ldIterator it;

            for (it = ld->begin(); it != ld->end(); ++it)
            {
                smallR.setPointer(it->second);

                if (!smallR.isMarked())
                    out->push_back(it->second);
            }
        }
        else if (!smallRG.usesStringTable())
        {
            iterator it;

            for (it = h->begin(); it != h->end(); ++it)
            {
                smallR.setPointer(it->second);

                if (!smallR.isMarked())
                    out->push_back(it->second);
            }
        }
        else
        {
            sthash_t::iterator it;

            for (it = sth->begin(); it != sth->end(); ++it)
            {
                smallR.setPointer(it->second);

                if (!smallR.isMarked())
                    out->push_back(it->second);
            }
        }
    }
}

uint64_t TupleJoiner::getMemUsage() const
{
    if (inUM() && typelessJoin)
        return _pool->getMemUsage() + storedKeyAlloc.getMemUsage();
    else if (inUM())
        return _pool->getMemUsage();
    else
        return (rows.size() * sizeof(Row::Pointer));
}

void TupleJoiner::setFcnExpFilter(boost::shared_ptr<funcexp::FuncExpWrapper> pt)
{
    fe = pt;

    if (fe)
        joinType |= WITHFCNEXP;
    else
        joinType &= ~WITHFCNEXP;
}

void TupleJoiner::updateCPData(const Row& r)
{
    uint32_t col;

    if (antiJoin() || largeOuterJoin())
        return;

    for (col = 0; col < smallKeyColumns.size(); col++)
    {
//        if (r.getColumnWidth(smallKeyColumns[col]) > 8)
        if (r.isLongString(smallKeyColumns[col]))
             continue;

        int64_t& min = cpValues[col][0], &max = cpValues[col][1];

        if (r.isCharType(smallKeyColumns[col]))
        {
            int64_t val = r.getIntField(smallKeyColumns[col]);

            if (order_swap(val) < order_swap(min) ||
                    min == numeric_limits<int64_t>::max())
            {
                min = val;
            }

            if (order_swap(val) > order_swap(max) ||
                    max == numeric_limits<int64_t>::min())
            {
                max = val;
            }
        }
        else if (r.isUnsigned(smallKeyColumns[col]))
        {
            uint64_t uval;
            if (r.getColType(smallKeyColumns[col]) == CalpontSystemCatalog::LONGDOUBLE)
            {
                double dval = (double)roundl(r.getLongDoubleField(smallKeyColumns[col]));
                switch (largeRG.getColType(largeKeyColumns[col]))
                {
                    case CalpontSystemCatalog::DOUBLE:
                    case CalpontSystemCatalog::UDOUBLE:
                    case CalpontSystemCatalog::FLOAT:
                    case CalpontSystemCatalog::UFLOAT:
                    {
                        uval = *(uint64_t*)&dval;
                    }
                    default:
                    {   
                        uval = (uint64_t)dval;
                    }
                }
            }
            else
            {
                uval = r.getUintField(smallKeyColumns[col]);
            }

            if (uval > static_cast<uint64_t>(max))
                max = static_cast<int64_t>(uval);

            if (uval < static_cast<uint64_t>(min))
                min = static_cast<int64_t>(uval);
        }
        else
        {
            int64_t val;
            if (r.getColType(smallKeyColumns[col]) == CalpontSystemCatalog::LONGDOUBLE)
            {
                double dval = (double)roundl(r.getLongDoubleField(smallKeyColumns[col]));
                switch (largeRG.getColType(largeKeyColumns[col]))
                {
                    case CalpontSystemCatalog::DOUBLE:
                    case CalpontSystemCatalog::UDOUBLE:
                    case CalpontSystemCatalog::FLOAT:
                    case CalpontSystemCatalog::UFLOAT:
                    {
                        val = *(int64_t*)&dval;
                    }
                    default:
                    {   
                        val = (int64_t)dval;
                    }
                }
            }
            else
            {
                val = r.getIntField(smallKeyColumns[col]);
            }

            if (val > max)
                max = val;

            if (val < min)
                min = val;
        }
    }
}

size_t TupleJoiner::size() const
{
    if (joinAlg == UM || joinAlg == INSERTING)
    {
        if (UNLIKELY(typelessJoin))
            return ht->size();
        else if (smallRG.getColType(smallKeyColumns[0]) == CalpontSystemCatalog::LONGDOUBLE)
            return ld->size();
        else if (!smallRG.usesStringTable())
            return h->size();
        else
            return sth->size();
    }

    return rows.size();
}

TypelessData makeTypelessKey(const Row& r, const vector<uint32_t>& keyCols,
                             uint32_t keylen, FixedAllocator* fa)
{
    TypelessData ret;
    uint32_t off = 0, i, j;
    execplan::CalpontSystemCatalog::ColDataType type;

    ret.data = (uint8_t*) fa->allocate();

    for (i = 0; i < keyCols.size(); i++)
    {
        type = r.getColTypes()[keyCols[i]];

        if (type == CalpontSystemCatalog::VARCHAR ||
                type == CalpontSystemCatalog::CHAR ||
                type == CalpontSystemCatalog::TEXT)
        {
            // this is a string, copy a normalized version
            const uint8_t* str = r.getStringPointer(keyCols[i]);
            uint32_t width = r.getStringLength(keyCols[i]);

            if (width > 65536)
            {
                throw runtime_error("Cannot join strings greater than 64KB");
            }

            for (j = 0; j < width && str[j] != 0; j++)
            {
                if (off >= keylen)
                    goto toolong;

                ret.data[off++] = str[j];
            }

            if (off >= keylen)
                goto toolong;

            ret.data[off++] = 0;
        }
        else if (r.isUnsigned(keyCols[i]))
        {
            if (off + 8 > keylen)
                goto toolong;
            *((uint64_t*) &ret.data[off]) = r.getUintField(keyCols[i]);
            off += 8;
        }
        else
        {
            if (off + 8 > keylen)
                goto toolong;
            *((int64_t*) &ret.data[off]) = r.getIntField(keyCols[i]);
            off += 8;
        }
    }

    ret.len = off;
    fa->truncateBy(keylen - off);
    return ret;
toolong:
    fa->truncateBy(keylen);
    ret.len = 0;
    return ret;
}

TypelessData makeTypelessKey(const Row& r, const vector<uint32_t>& keyCols,
                             uint32_t keylen, FixedAllocator* fa,
                             const rowgroup::RowGroup& otherSideRG, const std::vector<uint32_t>& otherKeyCols)
{
    TypelessData ret;
    uint32_t off = 0, i, j;
    execplan::CalpontSystemCatalog::ColDataType type;

    ret.data = (uint8_t*) fa->allocate();

    for (i = 0; i < keyCols.size(); i++)
    {
        type = r.getColTypes()[keyCols[i]];

        if (type == CalpontSystemCatalog::VARCHAR ||
                type == CalpontSystemCatalog::CHAR ||
                type == CalpontSystemCatalog::TEXT)
        {
            // this is a string, copy a normalized version
            const uint8_t* str = r.getStringPointer(keyCols[i]);
            uint32_t width = r.getStringLength(keyCols[i]);

            if (width > 65536)
            {
                throw runtime_error("Cannot join strings greater than 64KB");
            }

            for (j = 0; j < width && str[j] != 0; j++)
            {
                if (off >= keylen)
                    goto toolong;

                ret.data[off++] = str[j];
            }

            if (off >= keylen)
                goto toolong;

            ret.data[off++] = 0;
        }
        else if (r.getColType(keyCols[i]) == CalpontSystemCatalog::LONGDOUBLE)
        {
            if (off + sizeof(long double) > keylen)
                goto toolong;
            // Small side is a long double. Since CS can't store larger than DOUBLE,
            // we need to convert to whatever type large side is -- double or int64
            long double keyld = r.getLongDoubleField(keyCols[i]);
            switch (otherSideRG.getColType(otherKeyCols[i]))
            {
                case CalpontSystemCatalog::DOUBLE:
                case CalpontSystemCatalog::UDOUBLE:
                case CalpontSystemCatalog::FLOAT:
                case CalpontSystemCatalog::UFLOAT:
                {
                    if (off + 8 > keylen)
                        goto toolong;
                    if (keyld > MAX_DOUBLE || keyld < MIN_DOUBLE)
                    {
                        ret.len = 0;
                        return ret;
                    }
                    else
                    {
                        double d = (double)keyld;
                        *((int64_t*) &ret.data[off]) = *(int64_t*)&d;
                    }
                    break;
                }
                case CalpontSystemCatalog::LONGDOUBLE:
                {
                    if (off + sizeof(long double) > keylen)
                        goto toolong;
                    *((long double*) &ret.data[off]) = keyld;
                    off += sizeof(long double);
                    break;
                }
                default:
                {   
                    if (off + 8 > keylen)
                        goto toolong;
                    if (r.isUnsigned(keyCols[i]) && keyld > MAX_UBIGINT)
                    {
                        ret.len = 0;
                        return ret;
                    }
                    else if (keyld > MAX_BIGINT || keyld < MIN_BIGINT)
                    {
                        ret.len = 0;
                        return ret;
                    }
                    else
                    {
                        *((int64_t*) &ret.data[off]) = (int64_t)keyld;
                        off += 8;
                    }
                    break;
                }
            }
        }
        else if (r.isUnsigned(keyCols[i]))
        {
            if (off + 8 > keylen)
                goto toolong;
            *((uint64_t*) &ret.data[off]) = r.getUintField(keyCols[i]);
            off += 8;
        }
        else
        {
            if (off + 8 > keylen)
                goto toolong;
            *((int64_t*) &ret.data[off]) = r.getIntField(keyCols[i]);
            off += 8;
        }
    }

    ret.len = off;
    fa->truncateBy(keylen - off);
    return ret;
toolong:
    fa->truncateBy(keylen);
    ret.len = 0;
    return ret;
}

TypelessData makeTypelessKey(const Row& r, const vector<uint32_t>& keyCols, PoolAllocator* fa,
                             const rowgroup::RowGroup& otherSideRG, const std::vector<uint32_t>& otherKeyCols)
{
    TypelessData ret;
    uint32_t off = 0, i, j;
    execplan::CalpontSystemCatalog::ColDataType type;

    uint32_t keylen = 0;

    /* get the length of the normalized key... */
    for (i = 0; i < keyCols.size(); i++)
    {
        type = r.getColTypes()[keyCols[i]];

        if (r.getColType(keyCols[i]) == CalpontSystemCatalog::LONGDOUBLE
         && otherSideRG.getColType(otherKeyCols[i]) == CalpontSystemCatalog::LONGDOUBLE)
        {
            keylen += sizeof(long double);
        }
        else if (r.isCharType(keyCols[i]))
            keylen += r.getStringLength(keyCols[i]) + 1;
        else
            keylen += 8;
    }

    ret.data = (uint8_t*) fa->allocate(keylen);

    for (i = 0; i < keyCols.size(); i++)
    {
        type = r.getColTypes()[keyCols[i]];

        if (type == CalpontSystemCatalog::VARCHAR ||
                type == CalpontSystemCatalog::CHAR ||
                type == CalpontSystemCatalog::TEXT)
        {
            // this is a string, copy a normalized version
            const uint8_t* str = r.getStringPointer(keyCols[i]);
            uint32_t width = r.getStringLength(keyCols[i]);

            if (width > 65536)
            {
                throw runtime_error("Cannot join strings greater than 64KB");
            }

            for (j = 0; j < width && str[j] != 0; j++)
                ret.data[off++] = str[j];

            ret.data[off++] = 0;
        }
        else if (type == CalpontSystemCatalog::LONGDOUBLE)
        {
            // Small side is a long double. Since CS can't store larger than DOUBLE,
            // we need to convert to whatever type large side is -- double or int64
            long double keyld = r.getLongDoubleField(keyCols[i]);
            switch (otherSideRG.getColType(otherKeyCols[i]))
            {
                case CalpontSystemCatalog::DOUBLE:
                case CalpontSystemCatalog::UDOUBLE:
                case CalpontSystemCatalog::FLOAT:
                case CalpontSystemCatalog::UFLOAT:
                {
                    if (keyld > MAX_DOUBLE || keyld < MIN_DOUBLE)
                    {
                        ret.len = 0;
                        return ret;
                    }
                    else
                    {
                        double d = (double)keyld;
                        *((int64_t*) &ret.data[off]) = *(int64_t*)&d;
                        off += 8;
                    }
                    break;
                }
                case CalpontSystemCatalog::LONGDOUBLE:
                {
                    *((long double*) &ret.data[off]) = keyld;
                    off += sizeof(long double);
                    break;
                }
                default:
                {   
                    if (r.isUnsigned(keyCols[i]) && keyld > MAX_UBIGINT)
                    {
                        ret.len = 0;
                        return ret;
                    }
                    else if (keyld > MAX_BIGINT || keyld < MIN_BIGINT)
                    {
                        ret.len = 0;
                        return ret;
                    }
                    else
                    {
                        *((int64_t*) &ret.data[off]) = (int64_t)keyld;
                        off += 8;
                    }
                    break;
                }
            }
        }
        else if (r.isUnsigned(keyCols[i]))
        {
            *((uint64_t*)&ret.data[off]) = r.getUintField(keyCols[i]);
            off += 8;
        }
        else
        {
            *((int64_t*)&ret.data[off]) = r.getIntField(keyCols[i]);
            off += 8;
        }
    }

    assert(off == keylen);
    ret.len = off;
    return ret;
}

uint64_t getHashOfTypelessKey(const Row& r, const vector<uint32_t>& keyCols, uint32_t seed)
{
    Hasher_r hasher;
    uint64_t ret = seed, tmp;
    uint32_t i;
    uint32_t width = 0;
    char nullChar = '\0';
    execplan::CalpontSystemCatalog::ColDataType type;

    for (i = 0; i < keyCols.size(); i++)
    {
        type = r.getColTypes()[keyCols[i]];

        if (type == CalpontSystemCatalog::VARCHAR ||
                type == CalpontSystemCatalog::CHAR ||
                type == CalpontSystemCatalog::TEXT)
        {
            // this is a string, copy a normalized version
            const uint8_t* str = r.getStringPointer(keyCols[i]);
            uint32_t len = r.getStringLength(keyCols[i]);
            ret = hasher((const char*) str, len, ret);
            /*
            for (uint32_t j = 0; j < width && str[j] != 0; j++)
                ret.data[off++] = str[j];
            */
            ret = hasher(&nullChar, 1, ret);
            width += len + 1;
        }
        else if (r.getColType(keyCols[i]) == CalpontSystemCatalog::LONGDOUBLE)
        {
            long double tmp = r.getLongDoubleField(keyCols[i]);
            ret = hasher((char*) &tmp, sizeof(long double), ret);
            width += sizeof(long double);
        }
        else
        if (r.isUnsigned(keyCols[i]))
        {
            tmp = r.getUintField(keyCols[i]);
            ret = hasher((char*) &tmp, 8, ret);
            width += 8;
        }
        else
        {
            tmp = r.getIntField(keyCols[i]);
            ret = hasher((char*) &tmp, 8, ret);
            width += 8;
        }
    }

    ret = hasher.finalize(ret, width);
    return ret;
}

string TypelessData::toString() const
{
    uint32_t i;
    ostringstream os;

    os << hex;

    for (i = 0; i < len; i++)
    {
        os << (uint32_t) data[i] << " ";
    }

    os << dec;
    return os.str();
}

void TypelessData::serialize(messageqcpp::ByteStream& b) const
{
    b << len;
    b.append(data, len);
}

void TypelessData::deserialize(messageqcpp::ByteStream& b, utils::FixedAllocator& fa)
{
    b >> len;
    data = (uint8_t*) fa.allocate(len);
    memcpy(data, b.buf(), len);
    b.advance(len);
}

void TypelessData::deserialize(messageqcpp::ByteStream& b, utils::PoolAllocator& fa)
{
    b >> len;
    data = (uint8_t*) fa.allocate(len);
    memcpy(data, b.buf(), len);
    b.advance(len);
}

bool TupleJoiner::hasNullJoinColumn(const Row& r) const
{
    uint64_t key;

    for (uint32_t i = 0; i < largeKeyColumns.size(); i++)
    {
        if (r.isNullValue(largeKeyColumns[i]))
            return true;

        if (UNLIKELY(bSignedUnsignedJoin))
        {
            // BUG 5628 If this is a signed/unsigned join column and the sign bit is set on either
            // side, then this row should not compare. Treat as NULL to prevent compare, even if
            // the bit patterns match.
            if (smallRG.isUnsigned(smallKeyColumns[i]) != largeRG.isUnsigned(largeKeyColumns[i]))
            {
                if (r.isUnsigned(largeKeyColumns[i]))
                    key = r.getUintField(largeKeyColumns[i]); // Does not propogate sign bit
                else
                    key = r.getIntField(largeKeyColumns[i]);  // Propogates sign bit

                if (key & 0x8000000000000000ULL)
                {
                    return true;
                }
            }
        }
    }

    return false;
}

string TupleJoiner::getTableName() const
{
    return tableName;
}

void TupleJoiner::setTableName(const string& tname)
{
    tableName = tname;
}

/* Disk based join support */

void TupleJoiner::clearData()
{
    STLPoolAllocator<pair<const TypelessData, Row::Pointer> > alloc(64 * 1024 * 1024 + 1);
    _pool = alloc.getPoolAllocator();

    if (typelessJoin)
        ht.reset(new typelesshash_t(10, hasher(), typelesshash_t::key_equal(), alloc));
    else if (smallRG.usesStringTable())
        sth.reset(new sthash_t(10, hasher(), sthash_t::key_equal(), alloc));
    else
        h.reset(new hash_t(10, hasher(), hash_t::key_equal(), alloc));

    std::vector<rowgroup::Row::Pointer> empty;
    rows.swap(empty);
    finished = false;
}

boost::shared_ptr<TupleJoiner> TupleJoiner::copyForDiskJoin()
{
    boost::shared_ptr<TupleJoiner> ret(new TupleJoiner());

    ret->smallRG = smallRG;
    ret->largeRG = largeRG;
    ret->smallNullMemory = smallNullMemory;
    ret->smallNullRow = smallNullRow;
    ret->joinType = joinType;
    ret->tableName = tableName;
    ret->typelessJoin = typelessJoin;
    ret->smallKeyColumns = smallKeyColumns;
    ret->largeKeyColumns = largeKeyColumns;
    ret->keyLength = keyLength;
    ret->bSignedUnsignedJoin = bSignedUnsignedJoin;
    ret->fe = fe;

    ret->nullValueForJoinColumn = nullValueForJoinColumn;
    ret->uniqueLimit = uniqueLimit;

    ret->discreteValues.reset(new bool[smallKeyColumns.size()]);
    ret->cpValues.reset(new vector<int64_t>[smallKeyColumns.size()]);

    for (uint32_t i = 0; i < smallKeyColumns.size(); i++)
    {
        ret->discreteValues[i] = false;
        if (isUnsigned(smallRG.getColTypes()[smallKeyColumns[i]]))
        {
            ret->cpValues[i].push_back(static_cast<int64_t>(numeric_limits<uint64_t>::max()));
            ret->cpValues[i].push_back(0);
        }
        else
        {
            ret->cpValues[i].push_back(numeric_limits<int64_t>::max());
            ret->cpValues[i].push_back(numeric_limits<int64_t>::min());
        }
    }

    if (typelessJoin)
        ret->storedKeyAlloc = FixedAllocator(keyLength);

    ret->setThreadCount(1);
    ret->clearData();
    ret->setInUM();
    return ret;
}






};
