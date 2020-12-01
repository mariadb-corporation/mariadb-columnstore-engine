#include <cstdlib>
#include <string>
using namespace std;

#include "functor_str.h"
#include "funchelpers.h"
#include "functioncolumn.h"
using namespace execplan;

namespace funcexp
{

void Func_decode::hash_password(ulong *result, const char *password, uint password_len)
{
  ulong nr=1345345333L, add=7, nr2=0x12345671L;
  ulong tmp;
  const char *password_end= password + password_len;
  for (; password < password_end; password++)
  {
    if (*password == ' ' || *password == '\t')
      continue;
    tmp= (ulong) (unsigned char) *password;
    nr^= (((nr & 63)+add)*tmp)+ (nr << 8);
    nr2+=(nr2 << 8) ^ nr;
    add+=tmp;
  }
  result[0]=nr & (((ulong) 1L << 31) -1L);
  result[1]=nr2 & (((ulong) 1L << 31) -1L);
}

CalpontSystemCatalog::ColType Func_decode::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
    return resultType;
}

string Func_decode::getStrVal(rowgroup::Row& row,
                           FunctionParm& parm,
                           bool& isNull,
                           CalpontSystemCatalog::ColType&)
{
    const string& str = parm[0]->data()->getStrVal(row, isNull);
    if (isNull)
    {
        return "";
    }
    const string& password = parm[1]->data()->getStrVal(row, isNull);
    if (isNull)
    {
        return "";
    }

    int nStrLen = str.length();
    int nPassLen = password.length();
    char res[nStrLen+1];
    memset(res,0,nStrLen+1);

    if (!fSeeded)
    {
        hash_password(fSeeds, password.c_str(), nPassLen);
        sql_crypt.init(fSeeds);
        fSeeded = true;
    }

    memcpy(res,str.c_str(),nStrLen);
    sql_crypt.decode(res,nStrLen);
    sql_crypt.reinit();

    return res;
}

}
