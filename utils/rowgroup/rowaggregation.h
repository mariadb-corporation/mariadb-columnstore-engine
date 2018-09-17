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

#ifndef ROWAGGREGATION_H
#define ROWAGGREGATION_H

/** @file rowaggregation.h
 * Classes in this file are used to aggregate Rows in RowGroups.
 * RowAggregation is the class that performs the aggregation.
 * RowAggGroupByCol and RowAggFunctionCol are support classes used to describe
 * the columns involved in the aggregation.
 * @endcode
 */

#include <cstring>
#include <stdint.h>
#include <vector>
#ifdef _MSC_VER
#include <unordered_map>
#include <unordered_set>
#else
#include <tr1/unordered_map>
#include <tr1/unordered_set>
#endif
#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>
#include <boost/scoped_array.hpp>

#include "serializeable.h"
#include "bytestream.h"
#include "rowgroup.h"
#include "hasher.h"
#include "stlpoolallocator.h"
#include "returnedcolumn.h"
#include "mcsv1_udaf.h"
#include "constantcolumn.h"

// To do: move code that depends on joblist to a proper subsystem.
namespace joblist
{
class ResourceManager;
}

namespace rowgroup
{


struct RowPosition
{
    uint64_t group: 48;
    uint64_t row: 16;

    static const uint64_t MSB = 0x800000000000ULL;   //48th bit is set
    inline RowPosition(uint64_t g, uint64_t r) : group(g), row(r) { }
    inline RowPosition() { }
};

/** @brief Enumerates aggregate functions supported by RowAggregation
 */
enum RowAggFunctionType
{
    ROWAGG_FUNCT_UNDEFINE, // default
    ROWAGG_COUNT_ASTERISK, // COUNT(*) counts all rows including nulls
    ROWAGG_COUNT_COL_NAME, // COUNT(column_name) only counts non-null rows
    ROWAGG_SUM,
    ROWAGG_AVG,
    ROWAGG_MIN,
    ROWAGG_MAX,

    // Statistics Function, ROWAGG_STATS is the generic name.
    ROWAGG_STATS,
    ROWAGG_STDDEV_POP,
    ROWAGG_STDDEV_SAMP,
    ROWAGG_VAR_POP,
    ROWAGG_VAR_SAMP,

    // BIT Function, ROWAGG_BIT_OP is the generic name.
    ROWAGG_BIT_OP,
    ROWAGG_BIT_AND,
    ROWAGG_BIT_OR,
    ROWAGG_BIT_XOR,

    // GROUP_CONCAT
    ROWAGG_GROUP_CONCAT,

    // DISTINCT: performed on UM only
    ROWAGG_COUNT_DISTINCT_COL_NAME, // COUNT(distinct column_name) only counts non-null rows
    ROWAGG_DISTINCT_SUM,
    ROWAGG_DISTINCT_AVG,

    // Constant
    ROWAGG_CONSTANT,

    // User Defined Aggregate Function
    ROWAGG_UDAF,

    // If an Aggregate has more than one parameter, this will be used for parameters after the first
    ROWAGG_MULTI_PARM,

    // internal function type to avoid duplicate the work
    // handling ROWAGG_COUNT_NO_OP, ROWAGG_DUP_FUNCT and ROWAGG_DUP_AVG is a little different
    // ROWAGG_COUNT_NO_OP  :  count done by AVG, no need to copy
    // ROWAGG_DUP_FUNCT    :  copy data before AVG calculation, because SUM may share by AVG
    // ROWAGG_DUP_AVG      :  copy data after AVG calculation
    ROWAGG_COUNT_NO_OP,    // COUNT(column_name), but leave count() to AVG
    ROWAGG_DUP_FUNCT,      // duplicate aggregate Function(), except AVG and UDAF, in select
    ROWAGG_DUP_AVG,        // duplicate AVG(column_name) in select
    ROWAGG_DUP_STATS,      // duplicate statistics functions in select
    ROWAGG_DUP_UDAF        // duplicate UDAF function in select
};


//------------------------------------------------------------------------------
/** @brief Specifies a column in a RowGroup that is part of the aggregation
 *   "GROUP BY" clause.
 */
//------------------------------------------------------------------------------
struct RowAggGroupByCol
{
    /** @brief RowAggGroupByCol constructor
     *
     * @param inputColIndex(in) column index into input row
     * @param outputColIndex(in) column index into output row
     *    outputColIndex argument should be omitted if this GroupBy
     *    column is not to be included in the output.
     */
    RowAggGroupByCol(int32_t inputColIndex, int32_t outputColIndex = -1) :
        fInputColumnIndex(inputColIndex), fOutputColumnIndex(outputColIndex) {}
    ~RowAggGroupByCol() {}

    uint32_t	fInputColumnIndex;
    uint32_t	fOutputColumnIndex;
};

inline messageqcpp::ByteStream& operator<<(messageqcpp::ByteStream& b, RowAggGroupByCol& o)
{
    return (b << o.fInputColumnIndex << o.fOutputColumnIndex);
}
inline messageqcpp::ByteStream& operator>>(messageqcpp::ByteStream& b, RowAggGroupByCol& o)
{
    return (b >> o.fInputColumnIndex >> o.fOutputColumnIndex);
}

//------------------------------------------------------------------------------
/** @brief Specifies a column in a RowGroup that is to be aggregated, and what
 *   aggregation function is to be performed.
 *
 *   If a column is aggregated more than once(ex: SELECT MIN(l_shipdate),
 *   MAX(l_shipdate)...), then 2 RowAggFunctionCol objects should be created
 *   with the same inputColIndex, one for the MIN function, and one for the
 *   MAX function.
 */
//------------------------------------------------------------------------------
struct RowAggFunctionCol
{
    /** @brief RowAggFunctionCol constructor
     *
     * @param aggFunction(in)    aggregation function to be performed
     * @param inputColIndex(in)  column index into input row
     * @param outputColIndex(in) column index into output row
     * @param auxColIndex(in)    auxiliary index into output row for avg/count
     * @param stats(in)          real statistics function where generic name in aggFunction
     */
    RowAggFunctionCol(RowAggFunctionType aggFunction, RowAggFunctionType stats,
                      int32_t inputColIndex, int32_t outputColIndex, int32_t auxColIndex = -1) :
        fAggFunction(aggFunction), fStatsFunction(stats), fInputColumnIndex(inputColIndex),
        fOutputColumnIndex(outputColIndex), fAuxColumnIndex(auxColIndex) {}
    virtual ~RowAggFunctionCol() {}

    virtual void serialize(messageqcpp::ByteStream& bs) const;
    virtual void deserialize(messageqcpp::ByteStream& bs);

    RowAggFunctionType  fAggFunction;      // aggregate function
    // statistics function stores ROWAGG_STATS in fAggFunction and real function in fStatsFunction
    RowAggFunctionType  fStatsFunction;

    uint32_t            fInputColumnIndex;
    uint32_t            fOutputColumnIndex;

    // fAuxColumnIndex is used in 4 cases:
    // 1. for AVG - point to the count column, the fInputColumnIndex is for sum
    // 2. for statistics function - point to sum(x), +1 is sum(x**2)
    // 3. for UDAF - contain the context user data as binary
    // 4. for duplicate - point to the real aggretate column to be copied from
    // Set only on UM, the fAuxColumnIndex is defaulted to fOutputColumnIndex+1 on PM.
    uint32_t            fAuxColumnIndex;

    // For UDAF that have more than one parameter and some parameters are constant.
    // There will be a series of RowAggFunctionCol created, one for each parameter.
    // The first will be a RowUDAFFunctionCol. Subsequent ones will be RowAggFunctionCol
    // with fAggFunction == ROWAGG_MULTI_PARM. Order is important.
    // If this parameter is constant, that value is here.
    SRCP fpConstCol;
};


struct RowUDAFFunctionCol : public RowAggFunctionCol
{
    RowUDAFFunctionCol(mcsv1sdk::mcsv1Context& context, int32_t inputColIndex,
                       int32_t outputColIndex, int32_t auxColIndex = -1) :
        RowAggFunctionCol(ROWAGG_UDAF, ROWAGG_FUNCT_UNDEFINE,
                          inputColIndex, outputColIndex, auxColIndex),
        fUDAFContext(context), bInterrupted(false)
    {
        fUDAFContext.setInterrupted(&bInterrupted);
    }

    RowUDAFFunctionCol(int32_t inputColIndex,
                       int32_t outputColIndex, int32_t auxColIndex = -1) :
        RowAggFunctionCol(ROWAGG_UDAF, ROWAGG_FUNCT_UNDEFINE,
                          inputColIndex, outputColIndex, auxColIndex),
        bInterrupted(false)
    {}
    RowUDAFFunctionCol(const RowUDAFFunctionCol& rhs) :
        RowAggFunctionCol(ROWAGG_UDAF, ROWAGG_FUNCT_UNDEFINE, rhs.fInputColumnIndex,
                          rhs.fOutputColumnIndex, rhs.fAuxColumnIndex),
        fUDAFContext(rhs.fUDAFContext),
        bInterrupted(false)
    {}

    virtual ~RowUDAFFunctionCol() {}

    virtual void serialize(messageqcpp::ByteStream& bs) const;
    virtual void deserialize(messageqcpp::ByteStream& bs);

    mcsv1sdk::mcsv1Context fUDAFContext;  // The UDAF context
    bool bInterrupted;                    // Shared by all the threads
};

inline void RowAggFunctionCol::serialize(messageqcpp::ByteStream& bs) const
{
    bs << (uint8_t)fAggFunction;
    bs << fInputColumnIndex;
    bs << fOutputColumnIndex;

    if (fpConstCol)
    {
        bs << (uint8_t)1;
        fpConstCol.get()->serialize(bs);
    }
    else
    {
        bs << (uint8_t)0;
    }

}

inline void RowAggFunctionCol::deserialize(messageqcpp::ByteStream& bs)
{
    bs >> (uint8_t&)fAggFunction;
    bs >> fInputColumnIndex;
    bs >> fOutputColumnIndex;
    uint8_t t;
    bs >> t;

    if (t)
    {
        fpConstCol.reset(new ConstantColumn);
        fpConstCol.get()->unserialize(bs);
    }
}

inline void RowUDAFFunctionCol::serialize(messageqcpp::ByteStream& bs) const
{
    RowAggFunctionCol::serialize(bs);
    fUDAFContext.serialize(bs);
}

inline void RowUDAFFunctionCol::deserialize(messageqcpp::ByteStream& bs)
{
    // This deserialize is called when the function gets to PrimProc.
    // reset is called because we're starting a new sub-evaluate cycle.
    RowAggFunctionCol::deserialize(bs);
    fUDAFContext.unserialize(bs);
    fUDAFContext.setInterrupted(&bInterrupted);
    mcsv1sdk::mcsv1_UDAF::ReturnCode rc;
    rc = fUDAFContext.getFunction()->reset(&fUDAFContext);

    if (rc == mcsv1sdk::mcsv1_UDAF::ERROR)
    {
        bInterrupted = true;
        throw logging::QueryDataExcept(fUDAFContext.getErrorMessage(), logging::aggregateFuncErr);
    }
}

struct ConstantAggData
{
    std::string        fConstValue;
    std::string        fUDAFName;     // If a UDAF is called with constant.
    RowAggFunctionType fOp;
    bool               fIsNull;

    ConstantAggData() : fOp(ROWAGG_FUNCT_UNDEFINE), fIsNull(false)
    {}

    ConstantAggData(const std::string& v, RowAggFunctionType f, bool n) :
        fConstValue(v), fOp(f), fIsNull(n)
    {}

    ConstantAggData(const std::string& v, const std::string u, RowAggFunctionType f, bool n) :
        fConstValue(v), fUDAFName(u), fOp(f), fIsNull(n)
    {}
};

typedef boost::shared_ptr<RowAggGroupByCol>  SP_ROWAGG_GRPBY_t;
typedef boost::shared_ptr<RowAggFunctionCol> SP_ROWAGG_FUNC_t;

class RowAggregation;

class AggHasher
{
public:
    AggHasher(const Row& row, Row** tRow, uint32_t keyCount, RowAggregation* ra);
    inline uint64_t operator()(const RowPosition& p) const;

private:
    explicit AggHasher();
    RowAggregation* agg;
    Row** tmpRow;
    mutable Row r;
    uint32_t lastKeyCol;
};

class AggComparator
{
public:
    AggComparator(const Row& row, Row** tRow, uint32_t keyCount, RowAggregation* ra);
    inline bool operator()(const RowPosition&, const RowPosition&) const;

private:
    explicit AggComparator();
    RowAggregation* agg;
    Row** tmpRow;
    mutable Row r1, r2;
    uint32_t lastKeyCol;
};

class KeyStorage
{
public:
    KeyStorage(const RowGroup& keyRG, Row** tRow);

    inline RowPosition addKey();
    inline uint64_t getMemUsage();

private:
    Row row;
    Row** tmpRow;
    RowGroup rg;
    std::vector<RGData> storage;
    uint64_t memUsage;

    friend class ExternalKeyEq;
    friend class ExternalKeyHasher;
};

class ExternalKeyHasher
{
public:
    ExternalKeyHasher(const RowGroup& keyRG, KeyStorage* ks, uint32_t keyColCount, Row** tRow);
    inline uint64_t operator()(const RowPosition& pos) const;

private:
    mutable Row row;
    mutable Row** tmpRow;
    uint32_t lastKeyCol;
    KeyStorage* ks;
};

class ExternalKeyEq
{
public:
    ExternalKeyEq(const RowGroup& keyRG, KeyStorage* ks, uint32_t keyColCount, Row** tRow);
    inline bool operator()(const RowPosition& pos1, const RowPosition& pos2) const;

private:
    mutable Row row1, row2;
    mutable Row** tmpRow;
    uint32_t lastKeyCol;
    KeyStorage* ks;
};

typedef std::tr1::unordered_set<RowPosition, AggHasher, AggComparator, utils::STLPoolAllocator<RowPosition> >
RowAggMap_t;

#if defined(__GNUC__) && (__GNUC__ == 4 && __GNUC_MINOR__ < 5)
typedef std::tr1::unordered_map<RowPosition, RowPosition, ExternalKeyHasher, ExternalKeyEq,
        utils::STLPoolAllocator<std::pair<const RowPosition, RowPosition> > > ExtKeyMap_t;
#else
typedef std::tr1::unordered_map<RowPosition, RowPosition, ExternalKeyHasher, ExternalKeyEq,
        utils::STLPoolAllocator<std::pair<RowPosition, RowPosition> > > ExtKeyMap_t;
#endif

struct GroupConcat
{
    // GROUP_CONCAT(DISTINCT col1, 'const', col2 ORDER BY col3 desc SEPARATOR 'sep')
    std::vector<std::pair<uint32_t, uint32_t> > fGroupCols;    // columns to concatenate, and position
    std::vector<std::pair<uint32_t, bool> > fOrderCols;    // columns to order by [asc/desc]
    std::string                         fSeparator;
    std::vector<std::pair<std::string, uint32_t> >  fConstCols; // constant columns in group
    bool                                fDistinct;
    uint64_t                            fSize;

    RowGroup                            fRowGroup;
    boost::shared_array<int>            fMapping;
    std::vector<std::pair<int, bool> >  fOrderCond;    // position to order by [asc/desc]
    joblist::ResourceManager*           fRm;           // resource manager
    boost::shared_ptr<int64_t>			fSessionMemLimit;

    GroupConcat() : fRm(NULL) {}
};

typedef boost::shared_ptr<GroupConcat>  SP_GroupConcat;


class GroupConcatAg
{
public:
    GroupConcatAg(SP_GroupConcat&);
    virtual ~GroupConcatAg();

    virtual void initialize() {};
    virtual void processRow(const rowgroup::Row&) {};
    virtual void merge(const rowgroup::Row&, uint64_t) {};

    void getResult(uint8_t*) {};
    uint8_t* getResult()
    {
        return NULL;
    }

protected:
    rowgroup::SP_GroupConcat              fGroupConcat;
};

typedef boost::shared_ptr<GroupConcatAg>  SP_GroupConcatAg;





//------------------------------------------------------------------------------
/** @brief Class that aggregates RowGroups.
 */
//------------------------------------------------------------------------------
class RowAggregation : public messageqcpp::Serializeable
{
public:
    /** @brief RowAggregation default constructor
     *
     * @param rowAggGroupByCols(in) specify GroupBy columns and their
     *    mapping from input to output.  If vector is empty, then all the
     *    rows will be aggregated into a single implied group.  Order is
     *    important here. The primary GroupBy column should be first, the
     *    secondary GroupBy column should be second, etc.
     * @param rowAggFunctionCols(in) specify function columns and their
     *    mapping from input to output.
     */
    RowAggregation();
    RowAggregation(const std::vector<SP_ROWAGG_GRPBY_t>& rowAggGroupByCols,
                   const std::vector<SP_ROWAGG_FUNC_t>&  rowAggFunctionCols);
    RowAggregation(const RowAggregation& rhs);

    /** @brief RowAggregation default destructor
     */
    virtual ~RowAggregation();

    /** @brief clone this object for multi-thread use
     */
    inline virtual RowAggregation* clone() const
    {
        return new RowAggregation (*this);
    }

    /** @brief Denotes end of data insertion following multiple calls to addRowGroup().
     */
    virtual void endOfInput();

    /** @brief reset RowAggregation outputRowGroup and hashMap
     */
    virtual void aggReset();

    /** @brief Define content of data to be aggregated and its aggregated output.
     *
     * @param pRowGroupIn(in)   contains definition of the input data.
     * @param pRowGroupOut(out) contains definition of the output data.
     */
    virtual void setInputOutput(const RowGroup& pRowGroupIn, RowGroup* pRowGroupOut)
    {
        fRowGroupIn = pRowGroupIn;
        fRowGroupOut = pRowGroupOut;
        initialize();
    }


    /** @brief Define content of data to be joined
     *
     *    This method must be call after setInputOutput() for PM hashjoin case.
     *
     * @param pSmallSideRG(in) contains definition of the small side data.
     * @param pLargeSideRG(in) contains definition of the large side data.
     */
    void setJoinRowGroups(std::vector<RowGroup>* pSmallSideRG, RowGroup* pLargeSideRG);

    /** @brief Returns group by column vector
     *
     * This function is used to duplicate the RowAggregation object
     *
     * @returns a reference of the group by vector
     */
    std::vector<SP_ROWAGG_GRPBY_t>&	getGroupByCols()
    {
        return fGroupByCols;
    }

    /** @brief Returns aggregate function vector
     *
     * This function is used to duplicate the RowAggregation object
     *
     * @returns a reference of the aggregation function vector
     */
    std::vector<SP_ROWAGG_FUNC_t>&	getAggFunctions()
    {
        return fFunctionCols;
    }

    /** @brief Add a group of rows to be aggregated.
     *
     * This function can be called to iteratively add RowGroups for aggregation.
     *
     * @parm pRowGroupIn(in) RowGroup to be added to aggregation.
     */
    virtual void addRowGroup(const RowGroup* pRowGroupIn);
    virtual void addRowGroup(const RowGroup* pRowGroupIn, std::vector<Row::Pointer>& inRows);

    /** @brief Serialize RowAggregation object into a ByteStream.
     *
     * @parm bs(out) BytesStream that is to be written to.
     */
    void serialize(messageqcpp::ByteStream& bs) const;

    /** @brief Unserialize RowAggregation object from a ByteStream.
     *
     * @parm bs(in) BytesStream that is to be read from.
     */
    void deserialize(messageqcpp::ByteStream& bs);

    /** @brief set the memory limit for RowAggregation
     *
     * @parm limit(in) memory limit for both Map and secondary RowGroups
     */
    void setMaxMemory(uint64_t limit)
    {
        fMaxMemory = limit;
    }

    /** @brief load result set into byte stream
     *
     * @parm bs(out) BytesStream that is to be written to.
     */
    void loadResult(messageqcpp::ByteStream& bs);
    void loadEmptySet(messageqcpp::ByteStream& bs);

    /** @brief get output rowgroup
     *
     * @returns a const pointer of the output rowgroup
     */
    const RowGroup* getOutputRowGroup() const
    {
        return fRowGroupOut;
    }
    RowGroup* getOutputRowGroup()
    {
        return fRowGroupOut;
    }

    RowAggMap_t* mapPtr()
    {
        return fAggMapPtr;
    }
    std::vector<RGData*>& resultDataVec()
    {
        return fResultDataVec;
    }

    virtual void aggregateRow(Row& row);
    inline uint32_t aggMapKeyLength()
    {
        return fAggMapKeyCount;
    }

protected:
    virtual void initialize();
    virtual void initMapData(const Row& row);
    virtual void attachGroupConcatAg();

    virtual void updateEntry(const Row& row);
    virtual void doMinMaxSum(const Row&, int64_t, int64_t, int);
    virtual void doAvg(const Row&, int64_t, int64_t, int64_t);
    virtual void doStatistics(const Row&, int64_t, int64_t, int64_t);
    virtual void doBitOp(const Row&, int64_t, int64_t, int);
    virtual void doUDAF(const Row&, int64_t, int64_t, int64_t, uint64_t& funcColsIdx);
    virtual bool countSpecial(const RowGroup* pRG)
    {
        fRow.setIntField<8>(fRow.getIntField<8>(0) + pRG->getRowCount(), 0);
        return true;
    }

    virtual bool newRowGroup();
    virtual void clearAggMap()
    {
        if (fAggMapPtr) fAggMapPtr->clear();
    }

    void resetUDAF(uint64_t funcColID);

    inline bool isNull(const RowGroup* pRowGroup, const Row& row, int64_t col);
    inline void makeAggFieldsNull(Row& row);
    inline void copyNullRow(Row& row)
    {
        copyRow(fNullRow, &row);
    }

    inline void updateIntMinMax(int64_t val1, int64_t val2, int64_t col, int func);
    inline void updateUintMinMax(uint64_t val1, uint64_t val2, int64_t col, int func);
    inline void updateCharMinMax(uint64_t val1, uint64_t val2, int64_t col, int func);
    inline void updateDoubleMinMax(double val1, double val2, int64_t col, int func);
    inline void updateFloatMinMax(float val1, float val2, int64_t col, int func);
    inline void updateStringMinMax(std::string val1, std::string val2, int64_t col, int func);
    inline void updateIntSum(int64_t val1, int64_t val2, int64_t col);
    inline void updateUintSum(uint64_t val1, uint64_t val2, int64_t col);
    inline void updateDoubleSum(double val1, double val2, int64_t col);
    inline void updateFloatSum(float val1, float val2, int64_t col);

    std::vector<SP_ROWAGG_GRPBY_t>                  fGroupByCols;
    std::vector<SP_ROWAGG_FUNC_t>                   fFunctionCols;
    RowAggMap_t*                                    fAggMapPtr;
    uint32_t                                        fAggMapKeyCount;   // the number of columns that make up the key
    RowGroup                                        fRowGroupIn;
    RowGroup*                                       fRowGroupOut;

    Row                                             fRow;
    Row                                             fNullRow;
    Row*											 tmpRow;   // used by the hashers & eq functors
    boost::scoped_array<uint8_t>                    fNullRowData;
    std::vector<RGData*>                           fResultDataVec;

    uint64_t                                        fTotalRowCount;
    uint64_t                                        fMaxTotalRowCount;
    uint64_t                                        fMaxMemory;

    RGData*                                         fPrimaryRowData;

    std::vector<boost::shared_ptr<RGData> >         fSecondaryRowDataVec;

    // for support PM aggregation after PM hashjoin
    std::vector<RowGroup>*                          fSmallSideRGs;
    RowGroup*                                       fLargeSideRG;
    boost::shared_array<boost::shared_array<int> >  fSmallMappings;
    boost::shared_array<int>                        fLargeMapping;
    uint32_t                                            fSmallSideCount;
    boost::scoped_array<Row> rowSmalls;

    // for hashmap
    boost::shared_ptr<utils::STLPoolAllocator<RowPosition> > fAlloc;

    // for 8k poc
    RowGroup                                        fEmptyRowGroup;
    RGData                                          fEmptyRowData;
    Row                                             fEmptyRow;

    boost::scoped_ptr<AggHasher> fHasher;
    boost::scoped_ptr<AggComparator> fEq;

    //TODO: try to get rid of these friend decl's.  AggHasher & Comparator
    //need access to rowgroup storage holding the rows to hash & ==.
    friend class AggHasher;
    friend class AggComparator;

    // We need a separate copy for each thread.
    mcsv1sdk::mcsv1Context fRGContext;

    // These are handy for testing the actual type of static_any for UDAF
    static const static_any::any& charTypeId;
    static const static_any::any& scharTypeId;
    static const static_any::any& shortTypeId;
    static const static_any::any& intTypeId;
    static const static_any::any& longTypeId;
    static const static_any::any& llTypeId;
    static const static_any::any& ucharTypeId;
    static const static_any::any& ushortTypeId;
    static const static_any::any& uintTypeId;
    static const static_any::any& ulongTypeId;
    static const static_any::any& ullTypeId;
    static const static_any::any& floatTypeId;
    static const static_any::any& doubleTypeId;
    static const static_any::any& strTypeId;
};

//------------------------------------------------------------------------------
/** @brief derived Class that aggregates multi-rowgroups on UM
 *    One-phase case: aggregate from projected RG to final aggregated RG.
 */
//------------------------------------------------------------------------------
class RowAggregationUM : public RowAggregation
{
public:
    /** @brief RowAggregationUM constructor
     */
    RowAggregationUM() {}
    RowAggregationUM(
        const std::vector<SP_ROWAGG_GRPBY_t>& rowAggGroupByCols,
        const std::vector<SP_ROWAGG_FUNC_t>&  rowAggFunctionCols,
        joblist::ResourceManager*,
        boost::shared_ptr<int64_t> sessionMemLimit);
    RowAggregationUM(const RowAggregationUM& rhs);

    /** @brief RowAggregationUM default destructor
     */
    ~RowAggregationUM();

    /** @brief Denotes end of data insertion following multiple calls to addRowGroup().
     */
    void endOfInput();

    /** @brief Finializes the result set before sending back to the front end.
     */
    void finalize();

    /** @brief Returns aggregated rows in a RowGroup.
     *
     * This function should be called repeatedly until false is returned (meaning end of data).
     *
     * @returns true if more data, else false if no more data.
     */
    bool nextRowGroup();

    /** @brief Add an aggregator for DISTINCT aggregation
     */
    void distinctAggregator(const boost::shared_ptr<RowAggregation>& da)
    {
        fDistinctAggregator = da;
    }

    /** @brief expressions to be evaluated after aggregation
     */
    void expression(const std::vector<execplan::SRCP>& exp)
    {
        fExpression = exp;
    }
    const std::vector<execplan::SRCP>& expression()
    {
        return fExpression;
    }

    // for multi threaded
    joblist::ResourceManager* getRm()
    {
        return fRm;
    }
    inline virtual RowAggregationUM* clone() const
    {
        return new RowAggregationUM (*this);
    }

    /** @brief access the aggregate(constant) columns
     */
    void constantAggregate(const std::vector<ConstantAggData>& v)
    {
        fConstantAggregate = v;
    }
    const std::vector<ConstantAggData>& constantAggregate() const
    {
        return fConstantAggregate;
    }

    /** @brief access the group_concat
     */
    void groupConcat(const std::vector<SP_GroupConcat>& v)
    {
        fGroupConcat = v;
    }
    const std::vector<SP_GroupConcat>& groupConcat() const
    {
        return fGroupConcat;
    }

    void aggregateRow(Row&);
    //void initialize();
    virtual void aggReset();

    void setInputOutput(const RowGroup& pRowGroupIn, RowGroup* pRowGroupOut);

protected:
    // virtual methods from base
    void initialize();
    void aggregateRowWithRemap(Row&);

    void attachGroupConcatAg();
    void updateEntry(const Row& row);
    bool countSpecial(const RowGroup* pRG)
    {
        fRow.setIntField<8>(
            fRow.getIntField<8>(
                fFunctionCols[0]->fOutputColumnIndex) + pRG->getRowCount(),
            fFunctionCols[0]->fOutputColumnIndex);
        return true;
    }

    bool newRowGroup();

    // calculate the average after all rows received. UM only function.
    void calculateAvgColumns();

    // calculate the statistics function all rows received. UM only function.
    void calculateStatisticsFunctions();

    // Sets the value from valOut into column colOut, performing any conversions.
    void SetUDAFValue(static_any::any& valOut, int64_t colOut);

    // If the datatype returned by evaluate isn't what we expect, convert.
    void SetUDAFAnyValue(static_any::any& valOut, int64_t colOut);

    // calculate the UDAF function all rows received. UM only function.
    void calculateUDAFColumns();

    // fix duplicates. UM only function.
    void fixDuplicates(RowAggFunctionType funct);

    // evaluate expressions
    virtual void evaluateExpression();

    // fix the aggregate(constant)
    virtual void fixConstantAggregate();
    virtual void doNullConstantAggregate(const ConstantAggData&, uint64_t);
    virtual void doNotNullConstantAggregate(const ConstantAggData&, uint64_t);

    // @bug3362, group_concat
    virtual void doGroupConcat(const Row&, int64_t, int64_t);
    virtual void setGroupConcatString();

    bool fHasAvg;
    bool fKeyOnHeap;
    bool fHasStatsFunc;
    bool fHasUDAF;

    boost::shared_ptr<RowAggregation> fDistinctAggregator;

    // for function on aggregation
    std::vector<execplan::SRCP>       fExpression;

    /* Derived classes that use a lot of memory need to update totalMemUsage and request
     * the memory from rm in that order. */
    uint64_t                          fTotalMemUsage;

    joblist::ResourceManager*         fRm;

    // @bug3475, aggregate(constant), sum(0), count(null), etc
    std::vector<ConstantAggData>      fConstantAggregate;

    // @bug3362, group_concat
    std::vector<SP_GroupConcat>       fGroupConcat;
    std::vector<SP_GroupConcatAg>     fGroupConcatAg;
    std::vector<SP_ROWAGG_FUNC_t>     fFunctionColGc;

    // for when the group by & distinct keys are not stored in the output rows
    rowgroup::RowGroup fKeyRG;
    boost::scoped_ptr<ExternalKeyEq> fExtEq;
    boost::scoped_ptr<ExternalKeyHasher> fExtHash;
    boost::scoped_ptr<KeyStorage> fKeyStore;
    boost::scoped_ptr<utils::STLPoolAllocator<std::pair<RowPosition, RowPosition> > > fExtKeyMapAlloc;
    boost::scoped_ptr<ExtKeyMap_t> fExtKeyMap;

    boost::shared_ptr<int64_t> fSessionMemLimit;
private:
    uint64_t fLastMemUsage;
    uint32_t fNextRGIndex;
};


//------------------------------------------------------------------------------
/** @brief derived Class that aggregates PM partially aggregated RowGroups on UM
 *    Two-phase case:
 *      phase 1 - aggregate from projected RG to partial aggregated RG on PM
 *                The base RowAggregation handles the 1st phase.
 *      phase 2 - aggregate from partially aggregated RG to final RG on UM
 *                This class handles the 2nd phase.
 */
//------------------------------------------------------------------------------
class RowAggregationUMP2 : public RowAggregationUM
{
public:
    /** @brief RowAggregationUM constructor
     */
    RowAggregationUMP2() {}
    RowAggregationUMP2(
        const std::vector<SP_ROWAGG_GRPBY_t>& rowAggGroupByCols,
        const std::vector<SP_ROWAGG_FUNC_t>&  rowAggFunctionCols,
        joblist::ResourceManager*,
        boost::shared_ptr<int64_t> sessionMemLimit);
    RowAggregationUMP2(const RowAggregationUMP2& rhs);

    /** @brief RowAggregationUMP2 default destructor
     */
    ~RowAggregationUMP2();
    inline virtual RowAggregationUMP2* clone() const
    {
        return new RowAggregationUMP2 (*this);
    }

protected:
    // virtual methods from base
    void updateEntry(const Row& row);
    void doAvg(const Row&, int64_t, int64_t, int64_t);
    void doStatistics(const Row&, int64_t, int64_t, int64_t);
    void doGroupConcat(const Row&, int64_t, int64_t);
    void doBitOp(const Row&, int64_t, int64_t, int);
    void doUDAF(const Row&, int64_t, int64_t, int64_t, uint64_t& funcColsIdx);
    bool countSpecial(const RowGroup* pRG)
    {
        return false;
    }
};



//------------------------------------------------------------------------------
/** @brief derived Class that aggregates on distinct columns on UM
 *    The internal aggregator will handle one or two phases aggregation
 */
//------------------------------------------------------------------------------
class RowAggregationDistinct : public RowAggregationUMP2
{
public:
    /** @brief RowAggregationDistinct constructor
     */
    RowAggregationDistinct() {}
    RowAggregationDistinct(
        const std::vector<SP_ROWAGG_GRPBY_t>& rowAggGroupByCols,
        const std::vector<SP_ROWAGG_FUNC_t>&  rowAggFunctionCols,
        joblist::ResourceManager*,
        boost::shared_ptr<int64_t> sessionMemLimit);

    /** @brief Copy Constructor for multi-threaded aggregation
     */
    RowAggregationDistinct(const RowAggregationDistinct& rhs);

    /** @brief RowAggregationDistinct default destructor
     */
    ~RowAggregationDistinct();

    /** @brief Add an aggregator for pre-DISTINCT aggregation
     */
    void addAggregator(const boost::shared_ptr<RowAggregation>& agg, const RowGroup& rg);

    void setInputOutput(const RowGroup& pRowGroupIn, RowGroup* pRowGroupOut);

    virtual void doDistinctAggregation();
    virtual void doDistinctAggregation_rowVec(std::vector<Row::Pointer>& inRows);
    void addRowGroup(const RowGroup* pRowGroupIn);
    void addRowGroup(const RowGroup* pRowGroupIn, std::vector<Row::Pointer>& inRows);

    // multi-threade debug
    boost::shared_ptr<RowAggregation>& aggregator()
    {
        return fAggregator;
    }
    void aggregator(boost::shared_ptr<RowAggregation> aggregator)
    {
        fAggregator = aggregator;
    }
    RowGroup& rowGroupDist()
    {
        return fRowGroupDist;
    }
    void rowGroupDist(RowGroup& rowGroupDist)
    {
        fRowGroupDist = rowGroupDist;
    }
    inline virtual RowAggregationDistinct* clone() const
    {
        return new RowAggregationDistinct (*this);
    }

protected:
    // virtual methods from base
    void updateEntry(const Row& row);

    boost::shared_ptr<RowAggregation>   fAggregator;
    RowGroup                            fRowGroupDist;
    RGData                              fDataForDist;
};


//------------------------------------------------------------------------------
/** @brief derived Class for aggregates multiple columns with distinct key word
 *    Get distinct values of the column per group by entry
 */
//------------------------------------------------------------------------------
class RowAggregationSubDistinct : public RowAggregationUM
{
public:
    /** @brief RowAggregationSubDistinct constructor
     */
    RowAggregationSubDistinct() {}
    RowAggregationSubDistinct(
        const std::vector<SP_ROWAGG_GRPBY_t>& rowAggGroupByCols,
        const std::vector<SP_ROWAGG_FUNC_t>&  rowAggFunctionCols,
        joblist::ResourceManager*,
        boost::shared_ptr<int64_t> sessionMemLimit);
    RowAggregationSubDistinct(const RowAggregationSubDistinct& rhs);

    /** @brief RowAggregationSubDistinct default destructor
     */
    ~RowAggregationSubDistinct();

    void setInputOutput(const RowGroup& pRowGroupIn, RowGroup* pRowGroupOut);
    void addRowGroup(const RowGroup* pRowGroupIn);
    inline virtual RowAggregationSubDistinct* clone() const
    {
        return new RowAggregationSubDistinct (*this);
    }

    void addRowGroup(const RowGroup* pRowGroupIn, std::vector<Row::Pointer>& inRow);

protected:
    // virtual methods from RowAggregationUM
    void doGroupConcat(const Row&, int64_t, int64_t);

    // for groupby columns and the aggregated distinct column
    Row                                             fDistRow;
    boost::scoped_array<uint8_t>                    fDistRowData;
};


//------------------------------------------------------------------------------
/** @brief derived Class that aggregates multiple columns with distinct key word
 *    Each distinct column will have its own aggregator
 */
//------------------------------------------------------------------------------
class RowAggregationMultiDistinct : public RowAggregationDistinct
{
public:
    /** @brief RowAggregationMultiDistinct constructor
     */
    RowAggregationMultiDistinct() {}
    RowAggregationMultiDistinct(
        const std::vector<SP_ROWAGG_GRPBY_t>& rowAggGroupByCols,
        const std::vector<SP_ROWAGG_FUNC_t>&  rowAggFunctionCols,
        joblist::ResourceManager*,
        boost::shared_ptr<int64_t> sessionMemLimit);
    RowAggregationMultiDistinct(const RowAggregationMultiDistinct& rhs);

    /** @brief RowAggregationMultiDistinct default destructor
     */
    ~RowAggregationMultiDistinct();

    /** @brief Add sub aggregators
     */
    void addSubAggregator(const boost::shared_ptr<RowAggregationUM>& agg,
                          const RowGroup& rg,
                          const std::vector<SP_ROWAGG_FUNC_t>& funct);

    void setInputOutput(const RowGroup& pRowGroupIn, RowGroup* pRowGroupOut);
    void addRowGroup(const RowGroup* pRowGroupIn);

    virtual void doDistinctAggregation();
    virtual void doDistinctAggregation_rowVec(std::vector<std::vector<Row::Pointer> >& inRows);

    inline virtual RowAggregationMultiDistinct* clone() const
    {
        return new RowAggregationMultiDistinct (*this);
    }

    void addRowGroup(const RowGroup* pRowGroupIn, std::vector<std::vector<Row::Pointer> >& inRows);

    std::vector<boost::shared_ptr<RowAggregationUM> >& subAggregators()
    {
        return fSubAggregators;
    }

    void subAggregators(std::vector<boost::shared_ptr<RowAggregationUM> >& subAggregators)
    {
        fSubAggregators = subAggregators;
    }

protected:
    // virtual methods from base
    std::vector<boost::shared_ptr<RowAggregationUM> > fSubAggregators;
    std::vector<RowGroup>                             fSubRowGroups;
    std::vector<boost::shared_ptr<RGData> >           fSubRowData;
    std::vector<std::vector<SP_ROWAGG_FUNC_t> >       fSubFunctions;
};



typedef boost::shared_ptr<RowAggregation>           SP_ROWAGG_t;
typedef boost::shared_ptr<RowAggregation>           SP_ROWAGG_PM_t;
typedef boost::shared_ptr<RowAggregationUM>         SP_ROWAGG_UM_t;
typedef boost::shared_ptr<RowAggregationDistinct>   SP_ROWAGG_DIST;



} // end of rowgroup namespace

#endif  // ROWAGGREGATION_H

