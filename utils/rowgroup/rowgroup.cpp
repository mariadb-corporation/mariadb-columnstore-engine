/*
   Copyright (C) 2014 InfiniDB, Inc.
   Copyright (c) 2019 MariaDB Corporation

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

//
// C++ Implementation: rowgroup
//
// Description:
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008
//

#include "rowgroup.h"

#include <boost/shared_array.hpp>
#include <iterator>
#include <numeric>
#include <sstream>

#include "bytestream.h"
#include "calpontsystemcatalog.h"
#include "columnwidth.h"
#include "dataconvert.h"
#include "helpers.h"
#include "nullvaluemanip.h"

namespace rowgroup
{
using cscType = execplan::CalpontSystemCatalog::ColDataType;

RowGroup::RowGroup()
{
    // 1024 is too generous to waste.
    oldOffsets.reserve(10);
    oids.reserve(10);
    keys.reserve(10);
    types.reserve(10);
    charsetNumbers.reserve(10);
    charsets.reserve(10);
    scale.reserve(10);
    precision.reserve(10);
}

RowGroup::RowGroup(uint32_t colCount, const std::vector<uint32_t>& positions,
                   const std::vector<uint32_t>& roids, const vector<uint32_t>& tkeys,
                   const std::vector<execplan::CalpontSystemCatalog::ColDataType>& colTypes,
                   const std::vector<uint32_t>& csNumbers, const vector<uint32_t>& cscale,
                   const std::vector<uint32_t>& cprecision, uint32_t stringTableThreshold,
                   bool stringTable, const std::vector<bool>& forceInlineData)
  : columnCount(colCount)
  , oldOffsets(positions)
  , oids(roids)
  , keys(tkeys)
  , types(colTypes)
  , charsetNumbers(csNumbers)
  , scale(cscale)
  , precision(cprecision)
  , sTableThreshold(stringTableThreshold)
{
    uint32_t i;

    forceInline.reset(new bool[columnCount]);

    if (forceInlineData.empty())
        for (i = 0; i < columnCount; i++)
            forceInline[i] = false;
    else
        for (i = 0; i < columnCount; i++)
            forceInline[i] = forceInlineData[i];

    colWidths.resize(columnCount);
    stOffsets.resize(columnCount + 1);
    stOffsets[0] = 2;  // 2-byte rid
    hasLongStringField = false;
    hasCollation = false;

    for (i = 0; i < columnCount; i++)
    {
        colWidths[i] = positions[i + 1] - positions[i];

        if (colWidths[i] >= sTableThreshold && !forceInline[i])
        {
            hasLongStringField = true;
            stOffsets[i + 1] = stOffsets[i] + 8;
        }
        else
            stOffsets[i + 1] = stOffsets[i] + colWidths[i];

        if (colHasCollation(i))
        {
            hasCollation = true;
        }
    }

    useStringTable = (stringTable && hasLongStringField);
    offsets = (useStringTable ? &stOffsets[0] : &oldOffsets[0]);

    // Set all the charsets to NULL for jit initialization.
    charsets.insert(charsets.begin(), charsetNumbers.size(), NULL);
}

RowGroup::RowGroup(const RowGroup& r)
  : columnCount(r.columnCount)
  , data(r.data)
  , oldOffsets(r.oldOffsets)
  , stOffsets(r.stOffsets)
  , colWidths(r.colWidths)
  , oids(r.oids)
  , keys(r.keys)
  , types(r.types)
  , charsetNumbers(r.charsetNumbers)
  , charsets(r.charsets)
  , scale(r.scale)
  , precision(r.precision)
  , rgData(r.rgData)
  , strings(r.strings)
  , useStringTable(r.useStringTable)
  , hasCollation(r.hasCollation)
  , hasLongStringField(r.hasLongStringField)
  , sTableThreshold(r.sTableThreshold)
  , forceInline(r.forceInline)
{
    // stOffsets and oldOffsets are sometimes empty...
    // offsets = (useStringTable ? &stOffsets[0] : &oldOffsets[0]);
    offsets = 0;

    if (useStringTable && !stOffsets.empty())
        offsets = &stOffsets[0];
    else if (!useStringTable && !oldOffsets.empty())
        offsets = &oldOffsets[0];
}

RowGroup& RowGroup::operator=(const RowGroup& r)
{
    columnCount = r.columnCount;
    oldOffsets = r.oldOffsets;
    stOffsets = r.stOffsets;
    colWidths = r.colWidths;
    oids = r.oids;
    keys = r.keys;
    types = r.types;
    charsetNumbers = r.charsetNumbers;
    charsets = r.charsets;
    data = r.data;
    scale = r.scale;
    precision = r.precision;
    rgData = r.rgData;
    strings = r.strings;
    useStringTable = r.useStringTable;
    hasCollation = r.hasCollation;
    hasLongStringField = r.hasLongStringField;
    sTableThreshold = r.sTableThreshold;
    forceInline = r.forceInline;
    // offsets = (useStringTable ? &stOffsets[0] : &oldOffsets[0]);
    offsets = 0;

    if (useStringTable && !stOffsets.empty())
        offsets = &stOffsets[0];
    else if (!useStringTable && !oldOffsets.empty())
        offsets = &oldOffsets[0];

    return *this;
}

RowGroup::RowGroup(messageqcpp::ByteStream& bs)
{
    this->deserialize(bs);
}

RowGroup::~RowGroup()
{
}

void RowGroup::resetRowGroup(uint64_t rid)
{
    rgData->setRowCount(0);
    rgData->setBaseRid(rid);
    rgData->setDbRoot(0);
    rgData->setStatus(0);

    if (strings)
        strings->clear();
}

void RowGroup::serialize(messageqcpp::ByteStream& bs) const
{
    bs << columnCount;
    serializeInlineVector(bs, oldOffsets);
    serializeInlineVector(bs, stOffsets);
    serializeInlineVector(bs, colWidths);
    serializeInlineVector(bs, oids);
    serializeInlineVector(bs, keys);
    serializeInlineVector(bs, types);
    serializeInlineVector(bs, charsetNumbers);
    serializeInlineVector(bs, scale);
    serializeInlineVector(bs, precision);
    bs << (uint8_t)useStringTable;
    bs << (uint8_t)hasCollation;
    bs << (uint8_t)hasLongStringField;
    bs << sTableThreshold;
    bs.append((uint8_t*)&forceInline[0], sizeof(bool) * columnCount);
}

void RowGroup::deserialize(messageqcpp::ByteStream& bs)
{
    uint8_t tmp8;

    bs >> columnCount;
    deserializeInlineVector(bs, oldOffsets);
    deserializeInlineVector(bs, stOffsets);
    deserializeInlineVector(bs, colWidths);
    deserializeInlineVector(bs, oids);
    deserializeInlineVector(bs, keys);
    deserializeInlineVector(bs, types);
    deserializeInlineVector(bs, charsetNumbers);
    deserializeInlineVector(bs, scale);
    deserializeInlineVector(bs, precision);
    bs >> tmp8;
    useStringTable = (bool)tmp8;
    bs >> tmp8;
    hasCollation = (bool)tmp8;
    bs >> tmp8;
    hasLongStringField = (bool)tmp8;
    bs >> sTableThreshold;
    forceInline.reset(new bool[columnCount]);
    memcpy(&forceInline[0], bs.buf(), sizeof(bool) * columnCount);
    bs.advance(sizeof(bool) * columnCount);
    // offsets = (useStringTable ? &stOffsets[0] : &oldOffsets[0]);
    offsets = 0;

    if (useStringTable && !stOffsets.empty())
        offsets = &stOffsets[0];
    else if (!useStringTable && !oldOffsets.empty())
        offsets = &oldOffsets[0];

    // Set all the charsets to NULL for jit initialization.
    charsets.insert(charsets.begin(), charsetNumbers.size(), NULL);
}

void RowGroup::serializeRGData(messageqcpp::ByteStream& bs) const
{
    // cout << "****** serializing\n" << toString() << en
    //	if (useStringTable || !hasLongStringField)
    rgData->serialize(bs, getDataSize());
    //	else {
    //		uint64_t size;
    //		RGData *compressed = convertToStringTable(&size);
    //		compressed->serialize(bs, size);
    //		if (compressed != rgData)
    //			delete compressed;
    //	}
}

void RowGroup::serializeColumnData(messageqcpp::ByteStream& bs) const
{
    uint32_t rgSize = getDataSize();
    bs << rgSize;
    bs.append(data, rgSize);
}

uint32_t RowGroup::getDataSize() const
{
    return getRowCount() * offsets[columnCount];
}

uint32_t RowGroup::getDataSize(uint64_t n) const
{
    return n * offsets[columnCount];
}

uint32_t RowGroup::getMaxDataSize() const
{
    return rgCommonSize * offsets[columnCount];
}

uint32_t RowGroup::getMaxDataSizeWithStrings() const
{
    return rgCommonSize * oldOffsets[columnCount];
}

uint32_t RowGroup::getStatus() const
{
    return rgData->getStatus();
}

void RowGroup::setStatus(uint16_t err)
{
    rgData->setStatus(err);
}

uint32_t RowGroup::getColumnWidth(uint32_t col) const
{
    return colWidths[col];
}

uint32_t RowGroup::getColumnCount() const
{
    return columnCount;
}

string RowGroup::toString(const std::vector<uint64_t>& used) const
{
    ostringstream os;
    ostream_iterator<int> oIter1(os, "\t");

    os << "columncount = " << columnCount << endl;
    os << "oids:\t\t";
    copy(oids.begin(), oids.end(), oIter1);
    os << endl;
    os << "keys:\t\t";
    copy(keys.begin(), keys.end(), oIter1);
    os << endl;
    os << "offsets:\t";
    copy(&offsets[0], &offsets[columnCount + 1], oIter1);
    os << endl;
    os << "colWidths:\t";
    copy(colWidths.begin(), colWidths.end(), oIter1);
    os << endl;
    os << "types:\t\t";
    copy(types.begin(), types.end(), oIter1);
    os << endl;
    os << "scales:\t\t";
    copy(scale.begin(), scale.end(), oIter1);
    os << endl;
    os << "precisions:\t";
    copy(precision.begin(), precision.end(), oIter1);
    os << endl;

    if (useStringTable)
        os << "uses a string table\n";
    else
        os << "doesn't use a string table\n";
    if (!used.empty())
        os << "sparse\n";

    // os << "strings = " << hex << (int64_t) strings << "\n";
    // os << "data = " << (int64_t) data << "\n" << dec;
    if (data != NULL)
    {
        Row r;
        initRow(&r);
        getRow(0, &r);
        os << "rowcount = " << getRowCount() << endl;
        if (!used.empty())
        {
            uint64_t cnt = std::accumulate(used.begin(), used.end(), 0ULL,
                                           [](uint64_t a, uint64_t bits)
                                           { return a + __builtin_popcountll(bits); });
            os << "sparse row count = " << cnt << endl;
        }
        os << "base rid = " << getBaseRid() << endl;
        os << "status = " << getStatus() << endl;
        os << "dbroot = " << getDBRoot() << endl;
        os << "row data...\n";

        uint32_t max_cnt = used.empty() ? getRowCount() : (used.size() * 64);
        for (uint32_t i = 0; i < max_cnt; i++)
        {
            if (!used.empty() && !(used[i / 64] & (1ULL << (i % 64))))
                continue;
            os << r.toString(i) << endl;
            r.nextRow();
        }
    }

    return os.str();
}

boost::shared_array<int> makeMapping(const RowGroup& r1, const RowGroup& r2)
{
    boost::shared_array<int> ret(new int[r1.getColumnCount()]);
    // bool reserved[r2.getColumnCount()];
    bool* reserved = (bool*)alloca(r2.getColumnCount() * sizeof(bool));
    uint32_t i, j;

    for (i = 0; i < r2.getColumnCount(); i++)
        reserved[i] = false;

    for (i = 0; i < r1.getColumnCount(); i++)
    {
        for (j = 0; j < r2.getColumnCount(); j++)
            if ((r1.getKeys()[i] == r2.getKeys()[j]) && !reserved[j])
            {
                ret[i] = j;
                reserved[j] = true;
                break;
            }

        if (j == r2.getColumnCount())
            ret[i] = -1;
    }

    return ret;
}

void applyMapping(const boost::shared_array<int>& mapping, const Row& in, Row* out)
{
    applyMapping(mapping.get(), in, out);
}

void applyMapping(const std::vector<int>& mapping, const Row& in, Row* out)
{
    applyMapping((int*)&mapping[0], in, out);
}

void applyMapping(const int* mapping, const Row& in, Row* out)
{
    uint32_t i;

    for (i = 0; i < in.getColumnCount(); i++)
        if (mapping[i] != -1)
        {
            if (UNLIKELY(in.getColTypes()[i] == execplan::CalpontSystemCatalog::VARBINARY ||
                         in.getColTypes()[i] == execplan::CalpontSystemCatalog::BLOB ||
                         in.getColTypes()[i] == execplan::CalpontSystemCatalog::TEXT))
                out->setVarBinaryField(in.getVarBinaryField(i), in.getVarBinaryLength(i),
                                       mapping[i]);
            else if (UNLIKELY(in.isLongString(i)))
                out->setStringField(in.getConstString(i), mapping[i]);
            else if (UNLIKELY(in.isShortString(i)))
                out->setUintField(in.getUintField(i), mapping[i]);
            else if (UNLIKELY(in.getColTypes()[i] == execplan::CalpontSystemCatalog::LONGDOUBLE))
                out->setLongDoubleField(in.getLongDoubleField(i), mapping[i]);
            // WIP this doesn't look right b/c we can pushdown colType
            // Migrate to offset based methods here
            // code precision 2 width convertor
            else if (UNLIKELY(
                         datatypes::isWideDecimalType(in.getColTypes()[i], in.getColumnWidth(i))))
                out->setBinaryField_offset(in.getBinaryField<int128_t>(i), 16,
                                           out->getOffset(mapping[i]));
            else if (in.isUnsigned(i))
                out->setUintField(in.getUintField(i), mapping[i]);
            else
                out->setIntField(in.getIntField(i), mapping[i]);
        }
}

RowGroup& RowGroup::operator+=(const RowGroup& rhs)
{
    boost::shared_array<bool> tmp;
    uint32_t i, j;
    // not appendable if data is set
    assert(!data);

    tmp.reset(new bool[columnCount + rhs.columnCount]);

    for (i = 0; i < columnCount; i++)
        tmp[i] = forceInline[i];

    for (j = 0; j < rhs.columnCount; i++, j++)
        tmp[i] = rhs.forceInline[j];

    forceInline.swap(tmp);

    columnCount += rhs.columnCount;
    oids.insert(oids.end(), rhs.oids.begin(), rhs.oids.end());
    keys.insert(keys.end(), rhs.keys.begin(), rhs.keys.end());
    types.insert(types.end(), rhs.types.begin(), rhs.types.end());
    charsetNumbers.insert(charsetNumbers.end(), rhs.charsetNumbers.begin(),
                          rhs.charsetNumbers.end());
    charsets.insert(charsets.end(), rhs.charsets.begin(), rhs.charsets.end());
    scale.insert(scale.end(), rhs.scale.begin(), rhs.scale.end());
    precision.insert(precision.end(), rhs.precision.begin(), rhs.precision.end());
    colWidths.insert(colWidths.end(), rhs.colWidths.begin(), rhs.colWidths.end());

    //    +4  +4  +8       +2 +4  +8
    // (2, 6, 10, 18) + (2, 4, 8, 16) = (2, 6, 10, 18, 20, 24, 32)
    for (i = 1; i < rhs.stOffsets.size(); i++)
    {
        stOffsets.push_back(stOffsets.back() + rhs.stOffsets[i] - rhs.stOffsets[i - 1]);
        oldOffsets.push_back(oldOffsets.back() + rhs.oldOffsets[i] - rhs.oldOffsets[i - 1]);
    }

    hasLongStringField = rhs.hasLongStringField || hasLongStringField;
    useStringTable = rhs.useStringTable || useStringTable;
    hasCollation = rhs.hasCollation || hasCollation;
    offsets = (useStringTable ? &stOffsets[0] : &oldOffsets[0]);

    return *this;
}

RowGroup operator+(const RowGroup& lhs, const RowGroup& rhs)
{
    RowGroup temp(lhs);
    return temp += rhs;
}

uint32_t RowGroup::getDBRoot() const
{
    return rgData->getDbRoot();
}

void RowGroup::addToSysDataList(execplan::CalpontSystemCatalog::NJLSysDataList& sysDataList)
{
    execplan::ColumnResult* cr;

    rowgroup::Row row;
    initRow(&row);
    uint32_t rowCount = getRowCount();
    uint32_t columnCount = getColumnCount();

    for (uint32_t i = 0; i < rowCount; i++)
    {
        getRow(i, &row);

        for (uint32_t j = 0; j < columnCount; j++)
        {
            int idx = sysDataList.findColumn(getOIDs()[j]);

            if (idx >= 0)
            {
                cr = sysDataList.sysDataVec[idx];
            }
            else
            {
                cr = new execplan::ColumnResult();
                cr->SetColumnOID(getOIDs()[j]);
                sysDataList.push_back(cr);
            }

            // @todo more data type checking. for now only check string, midint and bigint
            switch ((getColTypes()[j]))
            {
            case execplan::CalpontSystemCatalog::CHAR:
            case execplan::CalpontSystemCatalog::VARCHAR:
            {
                switch (getColumnWidth(j))
                {
                case 1: cr->PutData(row.getUintField<1>(j)); break;

                case 2: cr->PutData(row.getUintField<2>(j)); break;

                case 4: cr->PutData(row.getUintField<4>(j)); break;

                case 8: cr->PutData(row.getUintField<8>(j)); break;
                case 16:

                default:
                {
                    string s = row.getStringField(j);
                    cr->PutStringData(string(s.c_str(), strlen(s.c_str())));
                }
                }

                break;
            }

            case execplan::CalpontSystemCatalog::MEDINT:
            case execplan::CalpontSystemCatalog::INT:
            case execplan::CalpontSystemCatalog::UINT: cr->PutData(row.getIntField<4>(j)); break;

            case execplan::CalpontSystemCatalog::DATE: cr->PutData(row.getUintField<4>(j)); break;

            default: cr->PutData(row.getIntField<8>(j));
            }

            cr->PutRid(row.getFileRelativeRid());
        }
    }
}

const CHARSET_INFO* RowGroup::getCharset(uint32_t col)
{
    if (charsets[col] == NULL)
    {
        charsets[col] = &datatypes::Charset(charsetNumbers[col]).getCharset();
    }
    return charsets[col];
}

void RowGroup::setDBRoot(uint32_t dbroot)
{
    rgData->setDbRoot(dbroot);
}

RGData RowGroup::duplicate()
{
    RGData ret(*this, getRowCount());

    if (useStringTable)
    {
        // this isn't a straight memcpy of everything b/c it might be remapping strings.
        // think about a big memcpy + a remap operation; might be faster.
        Row r1, r2;
        RowGroup rg(*this);
        rg.setData(&ret);
        rg.resetRowGroup(getBaseRid());
        rg.setStatus(getStatus());
        rg.setRowCount(getRowCount());
        rg.setDBRoot(getDBRoot());
        initRow(&r1);
        initRow(&r2);
        getRow(0, &r1);
        rg.getRow(0, &r2);

        for (uint32_t i = 0; i < getRowCount(); i++)
        {
            copyRow(r1, &r2);
            r1.nextRow();
            r2.nextRow();
        }
    }
    else
    {
        ret.setRowCount(getRowCount());
        ret.setBaseRid(getBaseRid());
        ret.setDbRoot(getDBRoot());
        ret.setStatus(getStatus());
        memcpy(ret.rowData.get(), data, getDataSize());
    }
    return ret;
}

void RowGroup::append(RGData& rgd)
{
    RowGroup tmp(*this);
    Row src, dest;

    tmp.setData(&rgd);
    initRow(&src);
    initRow(&dest);
    tmp.getRow(0, &src);
    getRow(getRowCount(), &dest);

    for (uint32_t i = 0; i < tmp.getRowCount(); i++, src.nextRow(), dest.nextRow())
    {
        // cerr << "appending row: " << src.toString() << endl;
        copyRow(src, &dest);
    }

    setRowCount(getRowCount() + tmp.getRowCount());
}

void RowGroup::append(RowGroup& rg)
{
    append(*rg.getRGData());
}

void RowGroup::append(RGData& rgd, uint32_t startPos)
{
    RowGroup tmp(*this);
    Row src, dest;

    tmp.setData(&rgd);
    initRow(&src);
    initRow(&dest);
    tmp.getRow(0, &src);
    getRow(startPos, &dest);

    for (uint32_t i = 0; i < tmp.getRowCount(); i++, src.nextRow(), dest.nextRow())
    {
        // cerr << "appending row: " << src.toString() << endl;
        copyRow(src, &dest);
    }

    setRowCount(getRowCount() + tmp.getRowCount());
}

void RowGroup::append(RowGroup& rg, uint32_t startPos)
{
    append(*rg.getRGData(), startPos);
}

RowGroup RowGroup::truncate(uint32_t cols)
{
    idbassert(cols <= columnCount);

    RowGroup ret(*this);
    ret.columnCount = cols;
    ret.oldOffsets.resize(cols + 1);
    ret.stOffsets.resize(cols + 1);
    ret.colWidths.resize(cols);
    ret.oids.resize(cols);
    ret.keys.resize(cols);
    ret.types.resize(cols);
    ret.charsetNumbers.resize(cols);
    ret.charsets.resize(cols);
    ret.scale.resize(cols);
    ret.precision.resize(cols);
    ret.forceInline.reset(new bool[cols]);
    memcpy(ret.forceInline.get(), forceInline.get(), cols * sizeof(bool));

    ret.hasLongStringField = false;
    ret.hasCollation = false;

    for (uint32_t i = 0; i < columnCount && (!ret.hasLongStringField || !ret.hasCollation); i++)
    {
        if (colWidths[i] >= sTableThreshold && !forceInline[i])
        {
            ret.hasLongStringField = true;
        }

        if (colHasCollation(i))
        {
            ret.hasCollation = true;
        }
    }

    ret.useStringTable = (ret.useStringTable && ret.hasLongStringField);
    ret.offsets = (ret.useStringTable ? &ret.stOffsets[0] : &ret.oldOffsets[0]);
    return ret;
}

/* PL 8/10/09: commented the asserts for now b/c for the fcns that are called
every row, they're a measurable performance penalty */
uint32_t RowGroup::getRowCount() const
{
    return rgData->getRowCount();
}

void RowGroup::incRowCount()
{
    int32_t num = rgData->getRowCount() + 1;
    rgData->setRowCount(num);
}

void RowGroup::setRowCount(uint32_t num)
{
    rgData->setRowCount(num);
}

void RowGroup::getRow(uint32_t rowNum, Row* r) const
{
    // 	idbassert(data);
    if (useStringTable != r->usesStringTable())
        initRow(r);

    r->baseRid = getBaseRid();
    r->data = &(data[rowNum * offsets[columnCount]]);
    r->strings = strings;
    r->userDataStore = rgData->userDataStore.get();
}

void RowGroup::setData(RGData* rgd)
{
    data = rgd->rowData.get();
    strings = rgd->strings.get();
    rgData = rgd;
}

RGData* RowGroup::getRGData() const
{
    return rgData;
}

void RowGroup::setUseStringTable(bool b)
{
    useStringTable = (b && hasLongStringField);
    // offsets = (useStringTable ? &stOffsets[0] : &oldOffsets[0]);
    offsets = 0;

    if (useStringTable && !stOffsets.empty())
        offsets = &stOffsets[0];
    else if (!useStringTable && !oldOffsets.empty())
        offsets = &oldOffsets[0];

    if (!useStringTable)
        strings = NULL;
}

uint64_t RowGroup::getBaseRid() const
{
    return rgData->getBaseRid();
}

bool RowGroup::operator<(const RowGroup& rhs) const
{
    return getBaseRid() < rhs.getBaseRid();
}

void RowGroup::initRow(Row* r, bool forceInlineData) const
{
    r->columnCount = columnCount;

    if (LIKELY(!types.empty()))
    {
        r->colWidths = (uint32_t*)&colWidths[0];
        r->types = (execplan::CalpontSystemCatalog::ColDataType*)&(types[0]);
        r->charsetNumbers = (uint32_t*)&(charsetNumbers[0]);
        r->charsets = (CHARSET_INFO**)&(charsets[0]);
        r->scale = (uint32_t*)&(scale[0]);
        r->precision = (uint32_t*)&(precision[0]);
    }

    if (forceInlineData)
    {
        r->useStringTable = false;
        r->oldOffsets = (uint32_t*)&(oldOffsets[0]);
        r->stOffsets = (uint32_t*)&(stOffsets[0]);
        r->offsets = (uint32_t*)&(oldOffsets[0]);
    }
    else
    {
        r->useStringTable = useStringTable;
        r->oldOffsets = (uint32_t*)&(oldOffsets[0]);
        r->stOffsets = (uint32_t*)&(stOffsets[0]);
        r->offsets = offsets;
    }

    r->hasLongStringField = hasLongStringField;
    r->sTableThreshold = sTableThreshold;
    r->forceInline = forceInline;
    r->hasCollation = hasCollation;
}

uint32_t RowGroup::getRowSize() const
{
    return offsets[columnCount];
}

uint32_t RowGroup::getRowSizeWithStrings() const
{
    return oldOffsets[columnCount];
}

uint64_t RowGroup::getSizeWithStrings(uint64_t n) const
{
    if (strings == NULL)
        return getDataSize(n);
    else
        return getDataSize(n) + strings->getSize();
}

uint64_t RowGroup::getSizeWithStrings() const
{
    return getSizeWithStrings(getRowCount());
}

bool RowGroup::isCharType(uint32_t colIndex) const
{
    return datatypes::isCharType(types[colIndex]);
}

bool RowGroup::isUnsigned(uint32_t colIndex) const
{
    return datatypes::isUnsigned(types[colIndex]);
}

bool RowGroup::isShortString(uint32_t colIndex) const
{
    return (
        (getColumnWidth(colIndex) <= 7 &&
         types[colIndex] == execplan::CalpontSystemCatalog::VARCHAR) ||
        (getColumnWidth(colIndex) <= 8 && types[colIndex] == execplan::CalpontSystemCatalog::CHAR));
}

bool RowGroup::isLongString(uint32_t colIndex) const
{
    return (
        (getColumnWidth(colIndex) > 7 &&
         types[colIndex] == execplan::CalpontSystemCatalog::VARCHAR) ||
        (getColumnWidth(colIndex) > 8 && types[colIndex] == execplan::CalpontSystemCatalog::CHAR) ||
        types[colIndex] == execplan::CalpontSystemCatalog::VARBINARY ||
        types[colIndex] == execplan::CalpontSystemCatalog::BLOB ||
        types[colIndex] == execplan::CalpontSystemCatalog::TEXT);
}

bool RowGroup::usesStringTable() const
{
    return useStringTable;
}

const std::vector<uint32_t>& RowGroup::getOffsets() const
{
    return oldOffsets;
}

const std::vector<uint32_t>& RowGroup::getOIDs() const
{
    return oids;
}

const std::vector<uint32_t>& RowGroup::getKeys() const
{
    return keys;
}

execplan::CalpontSystemCatalog::ColDataType RowGroup::getColType(uint32_t colIndex) const
{
    return types[colIndex];
}

const std::vector<execplan::CalpontSystemCatalog::ColDataType>& RowGroup::getColTypes() const
{
    return types;
}

std::vector<execplan::CalpontSystemCatalog::ColDataType>& RowGroup::getColTypes()
{
    return types;
}

const std::vector<uint32_t>& RowGroup::getCharsetNumbers() const
{
    return charsetNumbers;
}

uint32_t RowGroup::getCharsetNumber(uint32_t colIndex) const
{
    return charsetNumbers[colIndex];
}

const std::vector<uint32_t>& RowGroup::getScale() const
{
    return scale;
}

const std::vector<uint32_t>& RowGroup::getPrecision() const
{
    return precision;
}

const std::vector<uint32_t>& RowGroup::getColWidths() const
{
    return colWidths;
}

boost::shared_array<bool>& RowGroup::getForceInline()
{
    return forceInline;
}

uint64_t convertToRid(const uint32_t& partitionNum, const uint16_t& segmentNum,
                      const uint8_t& exNum, const uint16_t& blNum)
{
    uint64_t partNum = partitionNum, segNum = segmentNum, extentNum = exNum, blockNum = blNum;

    // extentNum gets trunc'd to 6 bits, blockNums to 10 bits
    extentNum &= 0x3f;
    blockNum &= 0x3ff;

    return (partNum << 32) | (segNum << 16) | (extentNum << 10) | blockNum;
}

void copyRow(const Row& in, Row* out, uint32_t colCount)
{
    if (&in == out)
        return;

    out->setRid(in.getRelRid());

    if (!in.usesStringTable() && !out->usesStringTable())
    {
        memcpy(out->getData(), in.getData(),
               std::min(in.getOffset(colCount), out->getOffset(colCount)));
        return;
    }

    for (uint32_t i = 0; i < colCount; i++)
    {
        if (UNLIKELY(in.getColTypes()[i] == execplan::CalpontSystemCatalog::VARBINARY ||
                     in.getColTypes()[i] == execplan::CalpontSystemCatalog::BLOB ||
                     in.getColTypes()[i] == execplan::CalpontSystemCatalog::TEXT ||
                     in.getColTypes()[i] == execplan::CalpontSystemCatalog::CLOB))
        {
            out->setVarBinaryField(in.getVarBinaryStringField(i), i);
        }
        else if (UNLIKELY(in.isLongString(i)))
        {
            out->setStringField(in.getConstString(i), i);
        }
        else if (UNLIKELY(in.isShortString(i)))
        {
            out->setUintField(in.getUintField(i), i);
        }
        else if (UNLIKELY(in.getColTypes()[i] == execplan::CalpontSystemCatalog::LONGDOUBLE))
        {
            out->setLongDoubleField(in.getLongDoubleField(i), i);
        }
        else if (UNLIKELY(datatypes::isWideDecimalType(in.getColType(i), in.getColumnWidth(i))))
        {
            in.copyBinaryField<int128_t>(*out, i, i);
        }
        else
        {
            out->setIntField(in.getIntField(i), i);
        }
    }
}

void copyRow(const Row& in, Row* out)
{
    copyRow(in, out, std::min(in.getColumnCount(), out->getColumnCount()));
}

void RowGroup::setBaseRid(const uint32_t& partNum, const uint16_t& segNum, const uint8_t& extentNum,
                          const uint16_t& blockNum)
{
    rgData->setBaseRid(convertToRid(partNum, segNum, extentNum, blockNum));
}

uint32_t RowGroup::getStringTableThreshold() const
{
    return sTableThreshold;
}

void RowGroup::setStringStore(boost::shared_ptr<StringStore> ss)
{
    if (useStringTable)
    {
        rgData->setStringStore(ss);
        strings = rgData->strings.get();
    }
}

void RowGroup::getLocation(uint32_t* partNum, uint16_t* segNum, uint8_t* extentNum,
                           uint16_t* blockNum)
{
    getLocationFromRid(getBaseRid(), partNum, segNum, extentNum, blockNum);
}

}  // namespace rowgroup

// vim:ts=4 sw=4:
