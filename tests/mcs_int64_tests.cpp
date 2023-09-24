#include <cstdint>

#include "gtest/gtest.h"
#include "joblisttypes.h"
#include "mcs_int64.h"

TEST(TUInt64, checkMCSIntLimits)
{
  // MCS us
  {
    datatypes::TUInt64 nullConst(joblist::BIGINTNULL);
    datatypes::TUInt64 emptyRowConst(joblist::BIGINTEMPTYROW);
    EXPECT_EQ(nullConst.toMCSInt64(), datatype::MIN_BIGINT);
    EXPECT_EQ(emptyRowConst.toMCSInt64(), datatype::MIN_BIGINT);
  }
}
