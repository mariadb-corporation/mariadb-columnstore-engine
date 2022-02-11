#ifndef _SQL_CRYPT_H_
#define _SQL_CRYPT_H_

//#include "my_global.h"
/* Macros to make switching between C and C++ mode easier */
#ifdef __cplusplus
#define C_MODE_START \
  extern "C"         \
  {
#define C_MODE_END }
#else
#define C_MODE_START
#define C_MODE_END
#endif
#include "my_rnd.h"

namespace funcexp
{
class SQL_CRYPT
{
  struct my_rnd_struct rand, org_rand;
  char decode_buff[256], encode_buff[256];
  uint shift;

 public:
  SQL_CRYPT()
  {
  }
  SQL_CRYPT(ulong* seed)
  {
    init(seed);
  }
  ~SQL_CRYPT()
  {
  }
  void init(ulong* seed);
  void reinit()
  {
    shift = 0;
    rand = org_rand;
  }
  void encode(char* str, uint length);
  void decode(char* str, uint length);
};

}  // namespace funcexp

#endif /* _SQL_CRYPT_H_ */
