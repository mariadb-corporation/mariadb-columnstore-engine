/* Copyright (C) 2014 InfiniDB, Inc.

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

/*******************************************************************************
 * $Id: we_convertor.cpp 4726 2013-08-07 03:38:36Z bwilkinson $
 *
 *******************************************************************************/
/** @file */

#include <unistd.h>
#include <fnmatch.h>
#include <limits>
#include <cstring>
#include <vector>
#ifdef _MSC_VER
#include <cstdio>
#endif
#include "mcsconfig.h"

#include "we_convertor.h"
#include "numericliteral.h"

using namespace std;
using namespace execplan;
namespace
{
const char DATE_TIME_FORMAT[] = "%04d-%02d-%02d %02d:%02d:%02d";
// ColumnStore file `full file name` format.
const char CS_FULL_FILENAME_FORMAT[] =
    "*[0-9][0-9][0-9].dir/[0-9][0-9][0-9].dir/[[0-9][0-9][0-9].dir/"
    "[0-9][0-9][0-9].dir/[0-9][0-9][0-9].dir/FILE[0-9][0-9][0-9].cdf";
// ColumnStore file `directory name` format.
const char CS_DIR_FORMAT[] = "[0-9][0-9][0-9].dir";
// ColumnStore file `file name` format.
const char CS_FILE_FORMAT[] = "FILE[0-9][0-9][0-9].cdf";

/*******************************************************************************
 * DESCRIPTION:
 *    Takes an 8-bit value and converts it into a directory name.
 * PARAMETERS:
 *    pBuffer(output) - a pointer to the output buffer
 *    blen   (input)  - the length of the output buffer (in bytes)
 *    val    (input)  - value to be used in the formatted name
 * RETURN:
 *    the number of characters printed in the output buffer (not including
 *    the null terminator).  -1 is returned if the input buffer pointer is NULL.
 ******************************************************************************/
int _doDir(char* pBuffer, int blen, unsigned int val)
{
  int rc;

  if (!pBuffer)
  {
    rc = -1;
  }
  else
  {
    rc = snprintf(pBuffer, blen, "%03u.dir", val);
    pBuffer[blen - 1] = (char)0;
  }

  return rc;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Takes an 8-bit value and converts it into a file name.
 * PARAMETERS:
 *    pBuffer(output) - a pointer to the output buffer
 *    blen   (input)  - the length of the output buffer (in bytes)
 *    val    (input)  - value to be used in the formatted name
 * RETURN:
 *    the number of characters printed in the output buffer (not including
 *    the null terminator).  -1 is returned if the input buffer pointer is NULL.
 ******************************************************************************/
int _doFile(char* pBuffer, int blen, unsigned char val)
{
  int rc;

  if (!pBuffer)
  {
    rc = -1;
  }
  else
  {
    rc = snprintf(pBuffer, blen, "FILE%03d.cdf", val);
    pBuffer[blen - 1] = (char)0;
  }

  return rc;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Takes a buffer in ColumnStore `text` format and converts it to an
 *    integer.
 * PARAMETERS:
 *    buffer(input)  - a pointer to the input buffer.
 *    fmt   (input)  - a pointer to the format buffer.
 *    offset(input)  - an offset.
 *    val   (output) - converted integer.
 * RETURN:
 *   0 is returned on success, -1 is returned on error.
 ******************************************************************************/
int32_t _fromText(const char* buffer, const char* fmt, uint32_t offset, uint32_t& val)
{
  int32_t rc = -1;
  // Number length in characters.
  const uint32_t numberLen = 3;

  // Check that buffer is in the correct `directory` format.
  if (buffer && (fnmatch(fmt, buffer, 0) == 0))
  {
    datatypes::DataCondition error;
    val = literal::UnsignedInteger(buffer + offset, numberLen).toXIntPositive<uint32_t>(error);
    // The number cannot exceed 0xff.
    if (!error && val < 256)
      rc = 0;
  }

  return rc;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Takes a buffer in ColumnStore `directory` format and converts it to an
 *    integer.
 * PARAMETERS:
 *    buffer(input)  - a pointer to the input buffer.
      val   (output) - converted integer.
 * RETURN:
 *   0 is returned on success, -1 is returned on error.
 ******************************************************************************/
int32_t _fromDir(const char* buffer, uint32_t& val)
{
  return _fromText(buffer, CS_DIR_FORMAT, 0, val);
}

/*******************************************************************************
 * DESCRIPTION:
 *    Takes a buffer in ColumnStore `file` format and converts it to an
 *    integer.
 * PARAMETERS:
 *    buffer(input)  - a pointer to the input buffer.
      val   (output) - converted integer.
 * RETURN:
 *   0 is returned on success, -1 is returned on error.
 ******************************************************************************/
int32_t _fromFile(const char* buffer, uint32_t& val)
{
  return _fromText(buffer, CS_FILE_FORMAT, 4, val);
}

}  // namespace

namespace WriteEngine
{
// This struct represents a path to a segment file.
struct Convertor::dmFilePathArgs_t
{
  struct dmFilePathPart_t
  {
    char* pName;  // Name buffer.
    int32_t len;  // Size in bytes of a buffer.
  };
  // A, B, C, D, E and file name.
  dmFilePathPart_t pathPart[6];

  // Initialize `dmFilePathArgs_t` from the given buffer.
  void initialize(char buff[6][MAX_DB_DIR_NAME_SIZE])
  {
    // A, B, C, D, E directories.
    for (uint32_t i = 0; i < 6; ++i)
    {
      pathPart[i].pName = buff[i];
      pathPart[i].len = sizeof(buff[i]);
    }
  }
};

/*******************************************************************************
 * DESCRIPTION:
 *    Get time string
 * PARAMETERS:
 *    none
 * RETURN:
 *    time string
 ******************************************************************************/
/* static */
const std::string Convertor::getTimeStr()
{
  char buf[sizeof(DATE_TIME_FORMAT) + 10] = {0};
  time_t curTime = time(NULL);
  struct tm pTime;
  localtime_r(&curTime, &pTime);
  string timeStr;

  snprintf(buf, sizeof(buf), DATE_TIME_FORMAT, pTime.tm_year + 1900, pTime.tm_mon + 1, pTime.tm_mday,
           pTime.tm_hour, pTime.tm_min, pTime.tm_sec);

  timeStr = buf;

  return timeStr;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Convert int value to string
 * PARAMETERS:
 *    val - value
 * RETURN:
 *    string
 ******************************************************************************/
/* static */
const std::string Convertor::int2Str(int val)
{
  char buf[12];
  string myStr;

  snprintf(buf, sizeof(buf), "%d", val);
  myStr = buf;

  return myStr;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Convert a numeric string to a decimal long long, given the specified
 *    scale.  errno should be checked upon return from this function to see
 *    if it is set to ERANGE, meaning the value was out of range.
 * PARAMETERS:
 *    field       - string to be interpreted
 *    fieldLength - length of "field"
 *    scale       - decimal scale value to be used to parse "field"
 * RETURN:
 *    converted long long for specified "field"
 ******************************************************************************/
/* static */

long long Convertor::convertDecimalString(const char* field, int fieldLength, int scale)
{
  long double dval = strtold(field, NULL);
  long long ret = 0;

  // move scale digits to the left of the decimal point
  for (int i = 0; i < scale; i++)
    dval *= 10;

  // range check against int64
  if (dval > LLONG_MAX)
  {
    errno = ERANGE;
    return LLONG_MAX;
  }
  if (dval < LLONG_MIN)
  {
    errno = ERANGE;
    return LLONG_MIN;
  }
  errno = 0;

  ret = dval;

  // get the fractional part of what's left & round ret up or down.
  dval -= ret;
  if (dval >= 0.5 && ret < LLONG_MAX)
    ++ret;
  else if (dval <= -0.5 && ret > LLONG_MIN)
    --ret;
  return ret;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Convert an oid to a filename (with partition and segment number
 *    in the filepath.
 * PARAMETERS:
 *    fid - fid
 *    fullFileName - file name
 *    dbDirName - components of fullFileName
 *    partition - partition number to be in used in filepath
 *    segment   - segment number to be used in filename
 * RETURN:
 *    NO_ERROR if success, other if fail
 ******************************************************************************/
/* static */
int Convertor::oid2FileName(FID fid, char* fullFileName, char dbDirName[][MAX_DB_DIR_NAME_SIZE],
                            uint32_t partition, uint16_t segment)
{
  dmFilePathArgs_t args;
  int rc;

  // Initialize.
  char buff[6][MAX_DB_DIR_NAME_SIZE];
  args.initialize(buff);

  RETURN_ON_WE_ERROR((rc = dmOid2FPath(fid, partition, segment, &args)), ERR_DM_CONVERT_OID);
  sprintf(fullFileName, "%s/%s/%s/%s/%s/%s", args.pathPart[0].pName, args.pathPart[1].pName,
          args.pathPart[2].pName, args.pathPart[3].pName, args.pathPart[4].pName, args.pathPart[5].pName);

  for (uint32_t i = 0; i < 6; ++i)
  {
    strcpy(dbDirName[i], args.pathPart[i].pName);
  }

  //  std::cout << "OID: " << fid <<
  //       " mapping to file: " << fullFileName <<std::endl;

  return NO_ERROR;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Convert the given filename to an oid, segment and partition.
 * PARAMETERS:
 *    fullFileName INPUT  -- filename.
 *    oid          OUTPUT -- oid number from the given filename.
 *    partition    OUTPUT -- partition number from the given filename.
 *    segment      OUTPUT -- segment number from the given fielname.
 * RETURN:
 *    NO_ERROR if success, other if fail.
 ******************************************************************************/
int Convertor::fileName2Oid(const std::string& fullFileName, uint32_t& oid, uint32_t& partition,
                            uint32_t& segment)
{
  // ColumnStore file directory separator.
  const char dirSep = '/';
  // The number of the directories in the ColumnStore file name.
  // Note: without `DBRoot` directory.
  const uint32_t dirNamesMaxSize = 6;
  const uint32_t fullFileNameLen = fullFileName.size();

  // Verify the given `fullFileName`.
  if (!fullFileNameLen ||
      // If not match `fnmatch` returns a result code which is not equal to
      // zero.
      // TODO: Probably we should use `std::regex_match`, but currently
      // there are still parts of code which use legacy `char *`, so
      // `std::regex_match` is not applicable there without creating
      // additional `std::string` from `char *`.
      fnmatch(CS_FULL_FILENAME_FORMAT, fullFileName.c_str(), 0))
  {
    return -1;
  }

  std::vector<std::string> dirNames;
  // We need exact 6 instances.
  dirNames.reserve(6);

  uint32_t end = fullFileNameLen;
  // Signed integer for `index` since it could be less than zero.
  int32_t index = fullFileNameLen - 1;

  // Iterate over `fullFileName` starting from the end and split it by
  // directory separator. Since we starting from the end we need just 6
  // instances to match ColumnStore file name format specification.
  while (index >= 0 && dirNames.size() < dirNamesMaxSize)
  {
    while (index >= 0 && fullFileName[index] != dirSep)
    {
      --index;
    }

    // Begin is a `dirSep` index + 1.
    uint32_t begin = index + 1;
    const uint32_t dirNameLen = end - begin;
    // We already checked the `fullFileName` format,
    // but this check is only intended to make sure that this algo works
    // correctly on any input, if something changes.
    if (dirNameLen > 0 && dirNameLen < MAX_DB_DIR_NAME_SIZE)
    {
      dirNames.push_back(fullFileName.substr(begin, dirNameLen));
    }
    else
    {
      // Something wrong with filename, just return an error.
      return -1;
    }
    // Set `end` to the last directory separator index.
    end = index;
    // Skip current directory separator.
    --index;
  }

  // Make sure we parsed 6 instances.
  if (dirNames.size() != 6)
  {
    return -1;
  }

  // Initialize `dmFilePathArgs_t` struct.
  char buff[6][MAX_DB_DIR_NAME_SIZE];

  dmFilePathArgs_t args;
  args.initialize(buff);

  // Populate `dmFilePathArgs_t` struct with the given names.
  // Starting from the E directory.
  for (uint32_t i = 0, dirCount = 5; i <= dirCount; ++i)
  {
    strcpy(args.pathPart[dirCount - i].pName, dirNames[i].c_str());
  }

  // FIXME: Currently used ERR_DM_CONVERT_OID, should we introduce new error
  // code?
  RETURN_ON_WE_ERROR(dmFPath2Oid(args, oid, partition, segment), ERR_DM_CONVERT_OID);

  return NO_ERROR;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Map specified errno to the associated error message string.
 * PARAMETERS:
 *    errNum  - errno to be converted
 *    errString-(output) error message string associated with errNum
 * RETURN:
 *    none
 ******************************************************************************/
/* static */
void Convertor::mapErrnoToString(int errNum, std::string& errString)
{
  char errnoMsgBuf[1024];
#if STRERROR_R_CHAR_P
  char* errnoMsg = strerror_r(errNum, errnoMsgBuf, sizeof(errnoMsgBuf));

  if (errnoMsg)
    errString = errnoMsg;
  else
    errString.clear();

#else
  int errnoMsg = strerror_r(errNum, errnoMsgBuf, sizeof(errnoMsgBuf));

  if (errnoMsg == 0)
    errString = errnoMsgBuf;
  else
    errString.clear();

#endif
}

/*******************************************************************************
 * DESCRIPTION:
 *    Convert specified ColDataType to internal storage type (ColType).
 * PARAMETERS:
 *    dataType     - Interface data-type
 *    internalType - Internal data-type used for storing
 * RETURN:
 *    none
 ******************************************************************************/
/* static */
void Convertor::convertColType(CalpontSystemCatalog::ColDataType dataType, int colWidth,
                               ColType& internalType, bool isToken)
{
  if (isToken)
  {
    internalType = WriteEngine::WR_TOKEN;
    return;
  }

  switch (dataType)
  {
    // Map BIT and TINYINT to WR_BYTE
    case CalpontSystemCatalog::BIT:
    case CalpontSystemCatalog::TINYINT: internalType = WriteEngine::WR_BYTE; break;

    // Map SMALLINT to WR_SHORT
    case CalpontSystemCatalog::SMALLINT: internalType = WriteEngine::WR_SHORT; break;

    // Map MEDINT to WR_MEDINT
    case CalpontSystemCatalog::MEDINT: internalType = WriteEngine::WR_MEDINT; break;

    // Map INT, and DATE to WR_INT
    case CalpontSystemCatalog::INT:
    case CalpontSystemCatalog::DATE: internalType = WriteEngine::WR_INT; break;

    // Map FLOAT and UFLOAT to WR_FLOAT
    case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT: internalType = WriteEngine::WR_FLOAT; break;

    // Map BIGINT, DATETIME, TIMESTAMP, and TIME to WR_LONGLONG
    case CalpontSystemCatalog::BIGINT:
    case CalpontSystemCatalog::DATETIME:
    case CalpontSystemCatalog::TIMESTAMP:
    case CalpontSystemCatalog::TIME: internalType = WriteEngine::WR_LONGLONG; break;

    // Map DOUBLE and UDOUBLE to WR_DOUBLE
    case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE: internalType = WriteEngine::WR_DOUBLE; break;

    // Map DECIMAL to applicable integer type
    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL:
    {
      switch (colWidth)
      {
        case 1: internalType = WriteEngine::WR_BYTE; break;

        case 2: internalType = WriteEngine::WR_SHORT; break;

        case 4: internalType = WriteEngine::WR_INT; break;

        case 8: internalType = WriteEngine::WR_LONGLONG; break;

        default: internalType = WriteEngine::WR_BINARY; break;
      }

      break;
    }

    // Map BLOB to WR_BLOB
    case CalpontSystemCatalog::BLOB: internalType = WriteEngine::WR_BLOB; break;

    // Map TEXT to WR_TEXT
    case CalpontSystemCatalog::TEXT: internalType = WriteEngine::WR_TEXT; break;

    // Map VARBINARY to WR_VARBINARY
    case CalpontSystemCatalog::VARBINARY: internalType = WriteEngine::WR_VARBINARY; break;

    // Map CHAR, VARCHAR, and CLOB to WR_CHAR
    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::VARCHAR:
    case CalpontSystemCatalog::CLOB: internalType = WriteEngine::WR_CHAR; break;

    // Map UTINYINT to WR_UBYTE
    case CalpontSystemCatalog::UTINYINT: internalType = WriteEngine::WR_UBYTE; break;

    // Map USMALLINT to WR_USHORT
    case CalpontSystemCatalog::USMALLINT: internalType = WriteEngine::WR_USHORT; break;

    // Map UMEDINT to WR_UMEDINT
    case CalpontSystemCatalog::UMEDINT: internalType = WriteEngine::WR_UMEDINT; break;

    // Map UINT to WR_UINT
    case CalpontSystemCatalog::UINT: internalType = WriteEngine::WR_UINT; break;

    // Map UBIGINT, TIMESTAMP to WR_ULONGLONG
    case CalpontSystemCatalog::UBIGINT: internalType = WriteEngine::WR_ULONGLONG; break;

    default: internalType = WriteEngine::WR_CHAR; break;
  }
}

/*******************************************************************************
 * DESCRIPTION:
 *    Convert specified internal storage type (ColType) to ColDataType. Since
 *    there is a one to many relationship, we choose the most general.
 * PARAMETERS:
 *    internalType - Internal data-type used for storing
 *    dataType     - Interface data-type
 * RETURN:
 *    none
 ******************************************************************************/
/* static */
void Convertor::convertWEColType(ColType internalType, CalpontSystemCatalog::ColDataType& dataType)
{
  switch (internalType)
  {
    // Map BIT and TINYINT to WR_BYTE
    case WriteEngine::WR_BYTE: dataType = CalpontSystemCatalog::TINYINT; break;

    // Map SMALLINT to WR_SHORT
    case WriteEngine::WR_SHORT: dataType = CalpontSystemCatalog::SMALLINT; break;

    // Map MEDINT to WR_MEDINT
    case WriteEngine::WR_MEDINT: dataType = CalpontSystemCatalog::MEDINT; break;

    // Map INT, and DATE to WR_INT
    case WriteEngine::WR_INT: dataType = CalpontSystemCatalog::INT; break;

    // Map FLOAT and UFLOAT to WR_FLOAT
    case WriteEngine::WR_FLOAT: dataType = CalpontSystemCatalog::FLOAT; break;

    // Map BIGINT, DATETIME, TIME, and TIMESTAMP to WR_LONGLONG
    case WriteEngine::WR_LONGLONG: dataType = CalpontSystemCatalog::BIGINT; break;

    // Map DOUBLE and UDOUBLE to WR_DOUBLE
    case WriteEngine::WR_DOUBLE: dataType = CalpontSystemCatalog::DOUBLE; break;

    // Map BLOB to WR_BLOB
    case WriteEngine::WR_BLOB: dataType = CalpontSystemCatalog::BLOB; break;

    // Map TEXT to WR_TEXT
    case WriteEngine::WR_TEXT: dataType = CalpontSystemCatalog::TEXT; break;

    // Map VARBINARY to WR_VARBINARY
    case WriteEngine::WR_VARBINARY: dataType = CalpontSystemCatalog::VARBINARY; break;

    // Map CHAR, VARCHAR, and CLOB to WR_CHAR
    case WriteEngine::WR_CHAR: dataType = CalpontSystemCatalog::CHAR; break;

    // Map UTINYINT to WR_UBYTE
    case WriteEngine::WR_UBYTE: dataType = CalpontSystemCatalog::UTINYINT; break;

    // Map USMALLINT to WR_USHORT
    case WriteEngine::WR_USHORT: dataType = CalpontSystemCatalog::USMALLINT; break;

    // Map UMEDINT to WR_UMEDINT
    case WriteEngine::WR_UMEDINT: dataType = CalpontSystemCatalog::UMEDINT; break;

    // Map UINT to WR_UINT
    case WriteEngine::WR_UINT: dataType = CalpontSystemCatalog::UINT; break;

    // Map UBIGINT and TIMESTAMP to WR_ULONGLONG
    case WriteEngine::WR_ULONGLONG: dataType = CalpontSystemCatalog::UBIGINT; break;

    default: dataType = CalpontSystemCatalog::CHAR; break;
  }
}

/*******************************************************************************
 * DESCRIPTION:
 *    Convert curStruct from an interface-type struct to an internal-type
 *    struct.  curStruct is handled as a ColStruct.
 * PARAMETERS:
 *    curStruct - column struct to be initialized
 * RETURN:
 *    none
 ******************************************************************************/
/* static */
void Convertor::convertColType(ColStruct* curStruct)
{
  CalpontSystemCatalog::ColDataType dataType  // This will be updated later,
      = CalpontSystemCatalog::CHAR;           // CHAR used only for initialization.
  ColType* internalType = NULL;
  bool bTokenFlag = false;
  int* width = NULL;

  dataType = curStruct->colDataType;
  internalType = &(curStruct->colType);
  bTokenFlag = curStruct->tokenFlag;
  width = &(curStruct->colWidth);

  switch (dataType)
  {
    // Map BIT and TINYINT to WR_BYTE
    case CalpontSystemCatalog::BIT:
    case CalpontSystemCatalog::TINYINT: *internalType = WriteEngine::WR_BYTE; break;

    // Map SMALLINT to WR_SHORT
    case CalpontSystemCatalog::SMALLINT: *internalType = WriteEngine::WR_SHORT; break;

    // Map MEDINT to WR_MEDINT
    case CalpontSystemCatalog::MEDINT: *internalType = WriteEngine::WR_MEDINT; break;

    // Map INT, and DATE to WR_INT
    case CalpontSystemCatalog::INT:
    case CalpontSystemCatalog::DATE: *internalType = WriteEngine::WR_INT; break;

    // Map FLOAT and UFLOAT to WR_FLOAT
    case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT: *internalType = WriteEngine::WR_FLOAT; break;

    // Map BIGINT, DATETIME, TIME, and TIMESTAMP to WR_LONGLONG
    case CalpontSystemCatalog::BIGINT:
    case CalpontSystemCatalog::DATETIME:
    case CalpontSystemCatalog::TIMESTAMP:
    case CalpontSystemCatalog::TIME: *internalType = WriteEngine::WR_LONGLONG; break;

    // Map DOUBLE and UDOUBLE to WR_DOUBLE
    case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE: *internalType = WriteEngine::WR_DOUBLE; break;

    // Map DECIMAL to applicable integer type
    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL:
    {
      switch (*width)
      {
        case 1: *internalType = WriteEngine::WR_BYTE; break;

        case 2: *internalType = WriteEngine::WR_SHORT; break;

        case 4: *internalType = WriteEngine::WR_INT; break;

        case 8: *internalType = WriteEngine::WR_LONGLONG; break;

        default: *internalType = WriteEngine::WR_BINARY; break;
      }

      break;
    }

    // Map BLOB to WR_BLOB
    case CalpontSystemCatalog::BLOB: *internalType = WriteEngine::WR_BLOB; break;

    // Map TEXT to WR_TEXT
    case CalpontSystemCatalog::TEXT: *internalType = WriteEngine::WR_TEXT; break;

    // Map VARBINARY to WR_VARBINARY
    case CalpontSystemCatalog::VARBINARY: *internalType = WriteEngine::WR_VARBINARY; break;

    // Map CHAR, VARCHAR, and CLOB to WR_CHAR
    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::VARCHAR:
    case CalpontSystemCatalog::CLOB: *internalType = WriteEngine::WR_CHAR; break;

    // Map UTINYINT to WR_UBYTE
    case CalpontSystemCatalog::UTINYINT: *internalType = WriteEngine::WR_UBYTE; break;

    // Map USMALLINT to WR_USHORT
    case CalpontSystemCatalog::USMALLINT: *internalType = WriteEngine::WR_USHORT; break;

    // Map UMEDINT to WR_UMEDINT
    case CalpontSystemCatalog::UMEDINT: *internalType = WriteEngine::WR_UMEDINT; break;

    // Map UINT to WR_UINT
    case CalpontSystemCatalog::UINT: *internalType = WriteEngine::WR_UINT; break;

    // Map UBIGINT to WR_ULONGLONG
    case CalpontSystemCatalog::UBIGINT: *internalType = WriteEngine::WR_ULONGLONG; break;

    default: *internalType = WriteEngine::WR_CHAR; break;
  }

  if (bTokenFlag)  // token overwrite any other types
    *internalType = WriteEngine::WR_TOKEN;

  // check whether width is in sync with the requirement
  *width = getCorrectRowWidth(dataType, *width);
}

/*******************************************************************************
 * DESCRIPTION:
 *    Get the correct width for a row
 * PARAMETERS:
 *    dataType - data type
 *    width - data width in byte
 * RETURN:
 *    emptyVal - the value of empty row
 ******************************************************************************/
/* static */
int Convertor::getCorrectRowWidth(CalpontSystemCatalog::ColDataType dataType, int width)
{
  int offset, newWidth = 4;

  switch (dataType)
  {
    case CalpontSystemCatalog::TINYINT:
    case CalpontSystemCatalog::UTINYINT: newWidth = 1; break;

    case CalpontSystemCatalog::SMALLINT:
    case CalpontSystemCatalog::USMALLINT: newWidth = 2; break;

    case CalpontSystemCatalog::MEDINT:
    case CalpontSystemCatalog::INT:
    case CalpontSystemCatalog::UMEDINT:
    case CalpontSystemCatalog::UINT: newWidth = 4; break;

    case CalpontSystemCatalog::BIGINT:
    case CalpontSystemCatalog::UBIGINT: newWidth = 8; break;

    case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT: newWidth = 4; break;

    case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE: newWidth = 8; break;

    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL:
      if (width == 1)
        newWidth = 1;
      else if (width == 2)
        newWidth = 2;
      else if (width <= 4)
        newWidth = 4;
      else if (width <= 8)
        newWidth = 8;
      else
        newWidth = 16;
      break;

    case CalpontSystemCatalog::DATE: newWidth = 4; break;

    case CalpontSystemCatalog::DATETIME:
    case CalpontSystemCatalog::TIME:
    case CalpontSystemCatalog::TIMESTAMP: newWidth = 8; break;

    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::VARCHAR:
    case CalpontSystemCatalog::VARBINARY:  // treat same as varchar for now
    default:
      offset = (dataType == CalpontSystemCatalog::VARCHAR) ? -1 : 0;
      newWidth = 1;

      if (width == (2 + offset))
        newWidth = 2;
      else if (width >= (3 + offset) && width <= (4 + offset))
        newWidth = 4;
      else if (width >= (5 + offset))
        newWidth = 8;

      break;
  }

  return newWidth;
}

/*******************************************************************************
 * DESCRIPTION:
 * Converts an OID into a group if directories and a filename.
 *
 * This function takes a 32-bit Object ID (OID).  If DLen is 0, then the
 * OID is converted into 3 hierarchical directory names and a filename.
 * If DLen is >0 then the OID is converted into 5 hierarchical directory
 * names and a filename (using the partition and segment as additional
 * input into the filepath.  The actual location
 * of the file is <DBRoot>/<DirA>/<DirB>/<DirC>/<FName>, or
 * <DBRoot>/<DirA>/<DirB>/<DirC>/<DirD>/<part#>/<segFName>.  The <DBRoot>
 * entry must be pre-pended by the calling application after calling
 * this function.  The value for <DBRoot> is stored in the Columnstore.xml
 * configuration file.
 *
 * PARAMETERS:
 *    oid       INPUT -- The Object Id.
 *    partition INPUT -- partition to be included in filepath.
 *    segment   INPUT -- segment to be included in filepath.
 *    dmFilePathArgs* INPUT/OUTPUT -- Points to a buffer structure
 *
 * RETURN:
 *    return 0 if everything went OK.  -1 if an error occured.  Two
 *    kinds of errors are possible:
 *
 *        - a null pointer was passed in
 *        - truncation occured.
 *
 *    If a null buffer pointer is passed in, a return code
 *    of -1 will be returned FOR THAT BUFFER.
 *
 *    Truncation can occur if the buffer length specified in
 *    dmFilePathArgs is too small.
 *
 *    If a buffer's return code is not zero, the appropriate
 *    return code in dmfilePathArgs can be examined.  If a
 *    buffer's return code is be less than zero, the
 *    corresponding buffer pointer was NULL.  If it is greater
 *    or equal to the buffer's length argument, length is too small
 ******************************************************************************/
/*static*/
int Convertor::dmOid2FPath(uint32_t oid, uint32_t partition, uint32_t segment, dmFilePathArgs_t* pArgs)
{
  int32_t retCodes[6];
  // A, B, C directories.
  for (uint32_t i = 0, shift = 24, mask = 0xff000000; i < 3; ++i, shift -= 8, mask >>= 8)
  {
    retCodes[i] = _doDir(pArgs->pathPart[i].pName, pArgs->pathPart[i].len, (uint32_t)((oid & mask) >> shift));
  }

  // include partition and seg num in the file path if they are present
  if (pArgs->pathPart[3].len > 0)
  {
    // D directory.
    retCodes[3] = _doDir(pArgs->pathPart[3].pName, pArgs->pathPart[3].len, (uint32_t)(oid & 0x000000ff));

    // E directory - partition.
    retCodes[4] = _doDir(pArgs->pathPart[4].pName, pArgs->pathPart[4].len, partition);
    // File - segment.
    retCodes[5] = _doFile(pArgs->pathPart[5].pName, pArgs->pathPart[5].len, segment);

    // D.rc < 0 || E.rc < 0
    if ((retCodes[3] < 0) || (retCodes[4] < 0))
      return -1;

    // D.rc >= A.len || E.rc >= A.len
    if ((retCodes[3] >= pArgs->pathPart[0].len) || (retCodes[4] >= pArgs->pathPart[0].len))
      return -1;
  }
  else
  {
    retCodes[5] = _doFile(pArgs->pathPart[5].pName, pArgs->pathPart[5].len, (uint32_t)(oid & 0x000000ff));
  }

  // A.rc < 0 || B.rc < 0 || C.rc < 0
  // A.rc >= A.len || B.rc >= B.len || C.rc >= C.len
  for (uint32_t i = 0; i < 3; ++i)
  {
    if ((retCodes[i] < 0) || (retCodes[i] >= pArgs->pathPart[i].len))
      return -1;
  }
  // F.rc < 0 || F.rc >= F.len
  if ((retCodes[5] < 0) || (retCodes[5] >= pArgs->pathPart[5].len))
    return -1;

  return 0;
}

/*******************************************************************************
 * DESCRIPTION:
 * Converts populated `dmFilePathArgs_t` struct to an oid, partition,
 * and segment.
 *
 * PARAMETERS:
 *    pArgs     INPUT --  a const reference to `dmFilePathArgs_t` struct.
 *    oid       OUTPUT -- oid for the given file name.
 *    partition OUTPUT -- partition for the given file name.
 *    segment   OUTPUT -- segment for the given filename.
 *
 * RETURN:
 *    return 0 if everything went OK.  -1 if an error occured.
 ******************************************************************************/
int32_t Convertor::dmFPath2Oid(const dmFilePathArgs_t& pArgs, uint32_t& oid, uint32_t& partition,
                               uint32_t& segment)
{
  oid = 0;
  // A, B, C, D - directories.
  for (uint32_t i = 0, shift = 24; i < 4; ++i, shift -= 8)
  {
    uint32_t val = 0;
    auto rc = _fromDir(pArgs.pathPart[i].pName, val);
    if (rc == -1)
      return rc;

    oid |= val << shift;
  }

  // Partition.
  auto rc = _fromDir(pArgs.pathPart[4].pName, partition);
  if (rc == -1)
    return rc;

  // Segment.
  rc = _fromFile(pArgs.pathPart[5].pName, segment);
  if (rc == -1)
    return rc;

  return 0;
}
}  // namespace WriteEngine
