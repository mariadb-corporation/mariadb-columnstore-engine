#include "mariadb_charset/collation.h"
#include "mariadb_my_sys.h"
#include "mcs_datatype.h"

namespace datatypes
{
static inline CHARSET_INFO& get_charset_or_bin(int32_t charsetNumber)
{
  CHARSET_INFO* cs = get_charset(charsetNumber, MYF(MY_WME));
  return cs ? *cs : my_charset_bin;
}

Charset::Charset(uint32_t charsetNumber) : mCharset(&get_charset_or_bin(charsetNumber))
{
}

void Charset::setCharset(uint32_t charsetNumber)
{
  mCharset = &get_charset_or_bin(charsetNumber);
}

}  // namespace datatypes
