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
// C++ Interface: rowgroup
//
// Description:
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008

#pragma once

#include <stdexcept>
#include <string>
#include <vector>
//#define NDEBUG
#include <execinfo.h>

#include <boost/shared_array.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <cassert>
#include <cfloat>
#include <cmath>

#include "branchpred.h"
#include "bytestream.h"
#include "calpontsystemcatalog.h"
#include "collation.h"
#include "common/hashfamily.h"
#include "datatypes/mcs_int128.h"
#include "exceptclasses.h"
#include "hasher.h"
#include "joblisttypes.h"
#include "mcsv1_udaf.h"
#include "rgdata.h"
#include "row.h"
#include "stringstore.h"
#include "userdatastore.h"

namespace rowgroup
{
/*
    The RowGroup family of classes encapsulate the data moved through the
    system.

     - RowGroup specifies the format of the data primarily (+ some other metadata),
     - RGData (aka RowGroup Data) encapsulates the data,
     - Row is used to extract fields from the data and iterate.

    JobListFactory instantiates the RowGroups to be used by each stage of processing.
    RGDatas are passed between stages, and their RowGroup instances are used
    to interpret them.

    Historically, row data was just a chunk of contiguous memory, a uint8_t *.
    Every field had a fixed width, which allowed for quick offset
    calculation when assigning or retrieving individual fields.  That worked
    well for a few years, but at some point it became common to declare
    all strings as max-length, and to manipulate them in queries.

    Having fixed-width fields, even for strings, required an unreasonable
    amount of memory.  RGData & StringStore were introduced to handle strings
    more efficiently, at least with respect to memory.  The row data would
    still be a uint8_t *, and columns would be fixed-width, but string fields
    above a certain width would contain a 'Pointer' that referenced a string in
    StringStore.  Strings are stored efficiently in StringStore, so there is
    no longer wasted space.

    StringStore comes with a different inefficiency however.  When a value
    is overwritten, the original string cannot be freed independently of the
    others, so it continues to use space.  If values are only set once, as is
    the typical case, then StringStore is efficient.  When it is necessary
    to overwrite string fields, it is possible to configure these classes
    to use the original data format so that old string fields do not accumulate
    in memory.  Of course, be careful, because blobs and text fields in CS are
    declared as 2GB strings!

    A single RGData contains up to one 'logical block' worth of data,
    which is 8192 rows.  One RGData is usually treated as one unit of work by
    PrimProc and the JobSteps, but the rows an RGData contains and how many are
    treated as a work unit depend on the operation being done.

    For example, PrimProc works in units of 8192 contiguous rows
    that come from disk.  If half of the rows were filtered out, then the
    RGData it passes to the next stage would only contain 4096 rows.

    Others build results incrementally before passing them along, such as
    group-by.  If one group contains 11111 values, then group-by will
    return 2 RGDatas for that group, one with 8192 rows, and one with 2919.

    Note: There is no synchronization in any of these classes for obvious
    performance reasons.  Likewise, although it's technically safe for many
    readers to access an RGData simultaneously, that would not be an
    efficient thing to do.  Try to stick to designs where a single RGData
    is used by a single thread at a time.
*/

/* TODO: OO the rowgroup data to the extent there's no measurable performance hit. */

/** @brief RowGroup is a lightweight interface for processing packed row data

        A RowGroup is an interface for parsing and/or modifying row data as described at the top
        of this file.  Its lifecycle can be tied to a producer or consumer's lifecycle.
        Only one instance is required to process any number of blocks with a
        given column configuration.  The column configuration is specified in the
        constructor, and the block data to process is specified through the
        setData() function.	 It will not copy or take ownership of the data it processes;
        the caller should do that.

        Row and RowGroup share some bits.  RowGroup owns the memory they share.
*/

const int16_t rgCommonSize = 8192;
class RowGroup : public messageqcpp::Serializeable
{
  public:
    /** @brief The default ctor.  It does nothing.  Need to init by assignment or deserialization */
    RowGroup();

    /** @brief The RowGroup ctor, which specifies the column config to process

    @param colCount The number of columns
    @param positions An array specifying the offsets within the packed data
                of a row where each column begins.  It should have colCount + 1
                entries.  The first offset is 2, because a row begins with a 2-byte
                RID.  The last entry should be the offset of the last column +
                its length, which is also the size of the entire row including the rid.
    @param coids An array of oids for each column.
    @param tkeys An array of unique id for each column.
    @param colTypes An array of COLTYPEs for each column.
    @param charsetNumbers an Array of the lookup numbers for the charset/collation object.
    @param scale An array specifying the scale of DECIMAL types (0 for non-decimal)
    @param precision An array specifying the precision of DECIMAL types (0 for non-decimal)
    */

    RowGroup(uint32_t colCount, const std::vector<uint32_t>& positions,
             const std::vector<uint32_t>& cOids, const std::vector<uint32_t>& tkeys,
             const std::vector<execplan::CalpontSystemCatalog::ColDataType>& colTypes,
             const std::vector<uint32_t>& charsetNumbers, const std::vector<uint32_t>& scale,
             const std::vector<uint32_t>& precision, uint32_t stringTableThreshold,
             bool useStringTable = true,
             const std::vector<bool>& forceInlineData = std::vector<bool>());

    /** @brief The copiers.  It copies metadata, not the row data */
    RowGroup(const RowGroup&);

    /** @brief Assignment operator.  It copies metadata, not the row data */
    RowGroup& operator=(const RowGroup&);

    explicit RowGroup(messageqcpp::ByteStream& bs);

    ~RowGroup();

    void initRow(Row*, bool forceInlineData = false) const;
    uint32_t getRowCount() const;
    void incRowCount();
    void setRowCount(uint32_t num);
    void getRow(uint32_t rowNum, Row*) const;
    uint32_t getRowSize() const;
    uint32_t getRowSizeWithStrings() const;
    uint64_t getBaseRid() const;
    void setData(RGData* rgd);

    RGData* getRGData() const;

    uint32_t getStatus() const;
    void setStatus(uint16_t);

    uint32_t getDBRoot() const;
    void setDBRoot(uint32_t);

    uint32_t getDataSize() const;
    uint32_t getDataSize(uint64_t n) const;
    uint32_t getMaxDataSize() const;
    uint32_t getMaxDataSizeWithStrings() const;
    uint32_t getEmptySize() const;

    // this returns the size of the row data with the string table
    uint64_t getSizeWithStrings() const;
    uint64_t getSizeWithStrings(uint64_t n) const;

    // sets the row count to 0 and the baseRid to something
    // effectively initializing whatever chunk of memory
    // data points to
    void resetRowGroup(uint64_t baseRid);

    /* The Serializeable interface */
    void serialize(messageqcpp::ByteStream&) const;
    void deserialize(messageqcpp::ByteStream&);

    uint32_t getColumnWidth(uint32_t col) const;
    uint32_t getColumnCount() const;
    const std::vector<uint32_t>& getOffsets() const;
    const std::vector<uint32_t>& getOIDs() const;
    const std::vector<uint32_t>& getKeys() const;
    const std::vector<uint32_t>& getColWidths() const;
    execplan::CalpontSystemCatalog::ColDataType getColType(uint32_t colIndex) const;
    const std::vector<execplan::CalpontSystemCatalog::ColDataType>& getColTypes() const;
    std::vector<execplan::CalpontSystemCatalog::ColDataType>& getColTypes();
    const std::vector<uint32_t>& getCharsetNumbers() const;
    uint32_t getCharsetNumber(uint32_t colIndex) const;
    boost::shared_array<bool>& getForceInline();
    static inline uint32_t getHeaderSize()
    {
        return headerSize;
    }

    // this returns true if the type is CHAR or VARCHAR
    bool isCharType(uint32_t colIndex) const;
    bool isUnsigned(uint32_t colIndex) const;
    bool isShortString(uint32_t colIndex) const;
    bool isLongString(uint32_t colIndex) const;

    bool colHasCollation(uint32_t colIndex) const
    {
        return datatypes::typeHasCollation(getColType(colIndex));
    }

    const std::vector<uint32_t>& getScale() const;
    const std::vector<uint32_t>& getPrecision() const;

    bool usesStringTable() const;
    void setUseStringTable(bool);

    //	RGData *convertToInlineData(uint64_t *size = NULL) const;  // caller manages the memory
    // returned by this 	void convertToInlineDataInPlace(); 	RGData
    // *convertToStringTable(uint64_t *size = NULL) const; 	void convertToStringTableInPlace();
    void serializeRGData(messageqcpp::ByteStream&) const;
    void serializeColumnData(messageqcpp::ByteStream&) const;
    uint32_t getStringTableThreshold() const;

    void append(RGData&);
    void append(RowGroup&);
    void append(RGData&, uint pos);  // insert starting at position 'pos'
    void append(RowGroup&, uint pos);

    RGData duplicate();  // returns a copy of the attached RGData

    std::string toString(const std::vector<uint64_t>& used = {}) const;

    /** operator+=
     *
     * append the metadata of another RowGroup to this RowGroup
     */
    RowGroup& operator+=(const RowGroup& rhs);

    // returns a RowGroup with only the first cols columns.  Useful for generating a
    // RowGroup where the first cols make up a key of some kind, and the rest is irrelevant.
    RowGroup truncate(uint32_t cols);

    /** operator<
     *
     * Orders RG's based on baseRid
     */
    bool operator<(const RowGroup& rhs) const;

    void addToSysDataList(execplan::CalpontSystemCatalog::NJLSysDataList& sysDataList);

    /* Base RIDs are now a combination of partition#, segment#, extent#, and block#. */
    void setBaseRid(const uint32_t& partNum, const uint16_t& segNum, const uint8_t& extentNum,
                    const uint16_t& blockNum);
    void getLocation(uint32_t* partNum, uint16_t* segNum, uint8_t* extentNum, uint16_t* blockNum);

    void setStringStore(boost::shared_ptr<StringStore>);

    const CHARSET_INFO* getCharset(uint32_t col);

  private:
    uint32_t columnCount = 0;
    uint8_t* data = nullptr;

    std::vector<uint32_t> oldOffsets;  // inline data offsets
    std::vector<uint32_t> stOffsets;   // string table offsets
    uint32_t* offsets = nullptr;       // offsets either points to oldOffsets or stOffsets
    std::vector<uint32_t> colWidths;
    // oids: the real oid of the column, may have duplicates with alias.
    // This oid is necessary for front-end to decide the real column width.
    std::vector<uint32_t> oids;
    // keys: the unique id for pair(oid, alias). bug 1632.
    // Used to map the projected column and rowgroup index
    std::vector<uint32_t> keys;
    std::vector<execplan::CalpontSystemCatalog::ColDataType> types;
    // For string collation
    std::vector<uint32_t> charsetNumbers;
    std::vector<CHARSET_INFO*> charsets;

    // DECIMAL support.  For non-decimal fields, the values are 0.
    std::vector<uint32_t> scale;
    std::vector<uint32_t> precision;

    // string table impl
    RGData* rgData = nullptr;
    StringStore* strings = nullptr;  // note, strings and data belong to rgData
    bool useStringTable = false;
    bool hasCollation = false;
    bool hasLongStringField = false;
    uint32_t sTableThreshold = 20;
    boost::shared_array<bool> forceInline;

    static const uint32_t headerSize = 18;
    static const uint32_t rowCountOffset = 0;
    static const uint32_t baseRidOffset = 4;
    static const uint32_t statusOffset = 12;
    static const uint32_t dbRootOffset = 14;
};

uint64_t convertToRid(const uint32_t& partNum, const uint16_t& segNum, const uint8_t& extentNum,
                      const uint16_t& blockNum);
void getLocationFromRid(uint64_t rid, uint32_t* partNum, uint16_t* segNum, uint8_t* extentNum,
                        uint16_t* blockNum);

// returns the first rid of the logical block specified by baseRid
uint64_t getExtentRelativeRid(uint64_t baseRid);

// returns the first rid of the logical block specified by baseRid
uint64_t getFileRelativeRid(uint64_t baseRid);

/** operator+
 *
 * add the metadata of 2 RowGroups together and return a new RowGroup
 */
RowGroup operator+(const RowGroup& lhs, const RowGroup& rhs);

boost::shared_array<int> makeMapping(const RowGroup& r1, const RowGroup& r2);
void applyMapping(const boost::shared_array<int>& mapping, const Row& in, Row* out);
void applyMapping(const std::vector<int>& mapping, const Row& in, Row* out);
void applyMapping(const int* mapping, const Row& in, Row* out);

uint64_t convertToRid(const uint32_t& partitionNum, const uint16_t& segmentNum,
                      const uint8_t& exNum, const uint16_t& blNum);

void copyRow(const Row& in, Row* out, uint32_t colCount);
void copyRow(const Row& in, Row* out);

}  // namespace rowgroup

// vim:ts=4 sw=4:
