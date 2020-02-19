#include <gtest/gtest.h> // googletest header file
#include "rowgroup.h"
#include "columnwidth.h"
#include "joblisttypes.h"

#define WIDE_DEC_PRECISION 38
#define INITIAL_ROW_OFFSET 2

using int128_t = __int128;
using uint128_t = unsigned __int128;

class RowTest : public ::testing::Test {
 protected:
  void SetUp() override {
    uint8_t width = utils::widthByPrecision(WIDE_DEC_PRECISION);
    uint32_t oid =3001;

    std::vector<uint32_t> offsets, roids, tkeys, cscale, cprecision;
    std::vector<execplan::CalpontSystemCatalog::ColDataType> types;
    offsets.push_back(INITIAL_ROW_OFFSET);
    offsets.push_back(width+INITIAL_ROW_OFFSET); 
    offsets.push_back(width*2+INITIAL_ROW_OFFSET); 
    roids.push_back(oid); roids.push_back(oid+1);
    tkeys.push_back(1); tkeys.push_back(1);
    types.push_back(execplan::CalpontSystemCatalog::DECIMAL);
    types.push_back(execplan::CalpontSystemCatalog::UDECIMAL);
    cscale.push_back(0); cscale.push_back(0);
    cprecision.push_back(WIDE_DEC_PRECISION);
    cprecision.push_back(WIDE_DEC_PRECISION);
    rowgroup::RowGroup inRG(roids.size(), //column count
        offsets, //oldOffset
        roids, // column oids
        tkeys, //keys
        types, // types
        cscale, //scale
        cprecision, // precision
        20, // sTableThreshold
        false //useStringTable
    );
    rg = inRG;
    rgD.reinit(rg);
    rg.setData(&rgD);

    rg.initRow(&r);
    rowSize = r.getSize();
    rg.getRow(0, &r);

    std::vector<int128_t> sValueVector;
    std::vector<uint128_t> uValueVector;
    int128_t nullValue = 0;
    uint64_t* uint128_pod = reinterpret_cast<uint64_t*>(&nullValue);
    uint128_pod[0] = joblist::BINARYEMPTYROW;
    uint128_pod[1] = joblist::BINARYNULL;

    sValueVector.push_back(nullValue);
    sValueVector.push_back(-42);
    sValueVector.push_back(-42*0xFFFFFFFFFFFFFFFFLL);
    sValueVector.push_back(0);
    sValueVector.push_back(nullValue-1);

    uValueVector.push_back(nullValue);
    uValueVector.push_back(42);
    uValueVector.push_back(42*0xFFFFFFFFFFFFFFFFLL);
    uValueVector.push_back(0);
    uValueVector.push_back(nullValue);
    uValueVector.push_back(nullValue-1);

    for(size_t i = 0; i < sValueVector.size(); i++)
    {
        r.setBinaryField_offset(&sValueVector[i], 
            sizeof(sValueVector[0]), INITIAL_ROW_OFFSET);
        r.setBinaryField_offset(&uValueVector[i], 
            sizeof(uValueVector[0]), INITIAL_ROW_OFFSET+width);
        r.nextRow(rowSize);
    }
    rowCount = sValueVector.size();
  }
  // void TearDown() override {}

  rowgroup::Row r;
  rowgroup::RowGroup rg;
  rowgroup::RGData rgD;
  uint32_t rowSize;
  size_t rowCount;
};

TEST_F(RowTest, NonNULLValuesCheck) {
    rg.getRow(1, &r);
    for (size_t i = 0; i <= rg.getRowCount(); i++)
    {
        EXPECT_FALSE(r.isNullValue(0));
        EXPECT_FALSE(r.isNullValue(1));
        r.nextRow(rowSize);
    }
}

TEST_F(RowTest, NULLValuesCheck) {
    rg.getRow(0, &r);
    EXPECT_TRUE(r.isNullValue(0));
    EXPECT_TRUE(r.isNullValue(1));
}

//Row::isNullValue_offset
//toString
//initToNull
//toCSV
//applyMapping
//setBinaryField remove Field1 and combine setBinaryField
//getBinaryField
//Remove from set/getIntFields Varbinary
