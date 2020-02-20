#include <gtest/gtest.h> // googletest header file
#include "rowgroup.h"
#include "columnwidth.h"
#include "joblisttypes.h"
#include "iostream"

#define WIDE_DEC_PRECISION 38U
#define INITIAL_ROW_OFFSET 2

using int128_t = __int128;
using uint128_t = unsigned __int128;
using CSCDataType = execplan::CalpontSystemCatalog::ColDataType;

class RowDecimalTest : public ::testing::Test {
 protected:
  void SetUp() override {
    uint32_t precision = WIDE_DEC_PRECISION;
    uint32_t oid =3001;

    std::vector<CSCDataType>types;
    std::vector<decltype(precision)>precisionVec;
    std::vector<uint32_t> roids, tkeys, cscale;
    types.push_back(execplan::CalpontSystemCatalog::DECIMAL);
    types.push_back(execplan::CalpontSystemCatalog::UDECIMAL);
    for (size_t i=0; i <= 3; i++) {
        types.push_back(execplan::CalpontSystemCatalog::DECIMAL);
    }
    precisionVec.push_back(precision);
    precisionVec.push_back(precision);
    precisionVec.push_back(18);
    precisionVec.push_back(9);
    precisionVec.push_back(4);
    precisionVec.push_back(2);
    std::vector<uint32_t>widthVec;
    uint32_t offset = INITIAL_ROW_OFFSET;
    offsets.push_back(offset);
    for (size_t i=0; i < types.size(); i++) {
      uint8_t width = utils::widthByPrecision(precisionVec[i]);
      widthVec.push_back(width);
      offset += width;
      offsets.push_back(offset);
      roids.push_back(oid+i);
      tkeys.push_back(i+1); cscale.push_back(0);
    }
    /*offsets.push_back(INITIAL_ROW_OFFSET);
    offsets.push_back(16+INITIAL_ROW_OFFSET); 
    offsets.push_back(16*2+INITIAL_ROW_OFFSET);
    roids.push_back(oid); roids.push_back(oid+1);
    tkeys.push_back(1); tkeys.push_back(1);
    cscale.push_back(0); cscale.push_back(0);*/
    
    rowgroup::RowGroup inRG(roids.size(), //column count
        offsets, //oldOffset
        roids, // column oids
        tkeys, //keys
        types, // types
        cscale, //scale
        precisionVec, // precision
        20, // sTableThreshold
        false //useStringTable
    );

    //std::cout << inRG.toString() << std::endl;
    rg = inRG;
    rgD.reinit(rg);
    rg.setData(&rgD);

    rg.initRow(&r);
    rowSize = r.getSize();
    rg.getRow(0, &r);

    int128_t nullValue = 0;
    int128_t bigValue = 0;
    uint64_t* uint128_pod = reinterpret_cast<uint64_t*>(&nullValue);
    uint128_pod[0] = joblist::BINARYEMPTYROW;
    uint128_pod[1] = joblist::BINARYNULL;
    bigValue = 42*0xFFFFFFFFFFFFFFFFLL;

    sValueVector.push_back(nullValue);
    sValueVector.push_back(-42);
    sValueVector.push_back(bigValue);
    sValueVector.push_back(0);
    sValueVector.push_back(nullValue-1);

    uValueVector.push_back(nullValue);
    uValueVector.push_back(42);
    uValueVector.push_back(bigValue);
    uValueVector.push_back(0);
    uValueVector.push_back(nullValue-1);

    s8ValueVector.push_back(joblist::TINYINTNULL);
    s8ValueVector.push_back(-0x79);
    s8ValueVector.push_back(0);
    s8ValueVector.push_back(0x81);
    s8ValueVector.push_back(joblist::TINYINTNULL-1);

    s16ValueVector.push_back(joblist::SMALLINTNULL);
    s16ValueVector.push_back(-0x79);
    s16ValueVector.push_back(0);
    s16ValueVector.push_back(0x81);
    s16ValueVector.push_back(joblist::SMALLINTNULL-1);

    s32ValueVector.push_back(joblist::INTNULL);
    s32ValueVector.push_back(-0x79);
    s32ValueVector.push_back(0);
    s32ValueVector.push_back(0x81);
    s32ValueVector.push_back(joblist::INTNULL-1);

    s64ValueVector.push_back(joblist::INTNULL);
    s64ValueVector.push_back(-0x79);
    s64ValueVector.push_back(0);
    s64ValueVector.push_back(0x81);
    s64ValueVector.push_back(joblist::INTNULL-1);

    r.initToNull();
    r.nextRow(rowSize);
    for(size_t i = 1; i < sValueVector.size(); i++) {
        r.setBinaryField_offset(&sValueVector[i],
            sizeof(sValueVector[0]), offsets[0]);
        r.setBinaryField_offset(&uValueVector[i],
            sizeof(uValueVector[0]), offsets[1]);
        r.setIntField(s64ValueVector[i], 2);
        r.setIntField(s32ValueVector[i], 3);
        r.setIntField(s16ValueVector[i], 4);
        r.setIntField(s8ValueVector[i], 5);
        //std::cout << r.toString() << std::endl;
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
  std::vector<int128_t> sValueVector;
  std::vector<uint128_t> uValueVector;
  std::vector<int64_t> s8ValueVector;
  std::vector<int64_t> s16ValueVector;
  std::vector<int64_t> s32ValueVector;
  std::vector<int64_t> s64ValueVector;
  std::vector<uint32_t> offsets; 
};

TEST_F(RowDecimalTest, NonNULLValuesCheck) {
    rg.getRow(1, &r);
    for (size_t i = 1; i <= sValueVector.size(); i++) {
        EXPECT_FALSE(r.isNullValue(0));
        EXPECT_FALSE(r.isNullValue(1));
        EXPECT_FALSE(r.isNullValue(2));
        EXPECT_FALSE(r.isNullValue(3));
        EXPECT_FALSE(r.isNullValue(4));
        EXPECT_FALSE(r.isNullValue(5));
        r.nextRow(rowSize);
    }
}

TEST_F(RowDecimalTest, initToNullANDisNullValueValueCheck) {
    rg.getRow(0, &r);
    EXPECT_TRUE(r.isNullValue(0));
    EXPECT_TRUE(r.isNullValue(1));
    EXPECT_TRUE(r.isNullValue(2));
    EXPECT_TRUE(r.isNullValue(3));
    EXPECT_TRUE(r.isNullValue(4));
    EXPECT_TRUE(r.isNullValue(5));
}

TEST_F(RowDecimalTest, getBinaryFieldCheck) {
    rg.getRow(0, &r);
    uint128_t* u128Value;
    int128_t* s128Value;
//    std::remove_reference<decltype(*u128Value)>::type uType;
//    std::remove_reference<decltype(*s128Value)>::type sType;

    for (size_t i = 0; i < sValueVector.size(); i++) {
        s128Value = r.getBinaryField<int128_t>(0);
        EXPECT_EQ(*s128Value, sValueVector[i]);
        u128Value = r.getBinaryField<uint128_t>(1);
        EXPECT_EQ(*u128Value, uValueVector[i]);
        //EXPECT_EQ(r.getIntField(2),s64ValueVector[i]);
        //EXPECT_EQ(r.getIntField(3),s32ValueVector[i]);
        //EXPECT_EQ(r.getIntField(4),s16ValueVector[i]);
        //EXPECT_EQ(r.getIntField(5),s8ValueVector[i]);
        r.nextRow(rowSize);
    }
}
TEST_F(RowDecimalTest, toStringCheck) {
    std::string exemplar1("0: NULL NULL NULL NULL NULL NULL ");
    rg.getRow(0, &r);
    EXPECT_EQ(r.toString(), exemplar1);
    r.nextRow(rowSize);
    std::cout << r.toString() << std::endl;
    r.nextRow(rowSize);
    std::cout << r.toString() << std::endl;
    r.nextRow(rowSize);
    std::cout << r.toString() << std::endl;
}

//toString
//toCSV
//applyMapping
//equals
