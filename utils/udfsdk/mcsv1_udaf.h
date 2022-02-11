/* Copyright (C) 2017 MariaDB Corporation

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

/***********************************************************************
 *   $Id$
 *
 *   mcsv1_UDAF.h
 ***********************************************************************/

/**
 * Columnstore interface for writing a User Defined Aggregate
 * Functions (UDAF) and User Defined Analytic Functions (UDAnF)
 * or a function that can act as either - UDA(n)F
 *
 * The basic steps are:
 *
 * 1. Create a the UDA(n)F function interface in some .h file.
 * 2. Create the UDF function implementation in some .cpp file
 * 3. Create the connector stub (MariaDB UDAF definition) for
 * this UDF function.
 * 4. build the dynamic librarys using all of the source.
 * 5  Put the library in $COLUMNSTORE_INSTALL/lib of
 * all modules
 * 6. Put the connector stub in $COLUMNSTORE_INSTALL/mysql/lib
 * of all UMs
 * 7. restart the Columnstore system. 7. notify mysqld about the
 * new functions with commands like:
 *
 *    CREATE AGGREGATE FUNCTION all_null returns BOOL soname
 *    'libudf_mysql.so';
 *
 *    // An example that only makes sense as a UDAnF
 *    CREATE AGGREGATE FUNCTION mcs_interpolate returns REAL
 *    soname 'libudf_mysql.so';
 *
 * Use the name of the connector stub library in CREATE
 * AGGREGATE FUNCTION
 *
 * The UDAF functions may run distributed in the Columnstore
 * engine. UDAnF do not run distributed.
 *
 * UDAF is User Defined Aggregate Function.
 * UDAnF is User Defined Analytic Function.
 * UDA(n)F is an acronym for a function that could be either. It
 * is also used to describe the interface that is used for
 * either.
 */

#ifndef HEADER_mcsv1_udaf
#define HEADER_mcsv1_udaf

#include <cstdlib>
#include <string>
#include <vector>
#include <map>
#include <boost/shared_ptr.hpp>
#ifdef _MSC_VER
#include <unordered_map>
#else
#include <tr1/unordered_map>
#endif
#include "any.hpp"
#include "calpontsystemcatalog.h"
#include "wf_frame.h"
#include "my_decimal_limits.h"

#if defined(_MSC_VER) && defined(xxxRGNODE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace mcsv1sdk
{
// The return type of the CREATE AGGREGATE statement
enum enum_mariadb_return_type
{
  MYSQL_TYPE_DOUBLE = 5,
  MYSQL_TYPE_LONGLONG = 8,
  MYSQL_TYPE_VARCHAR = 15,
  MYSQL_TYPE_NEWDECIMAL = 246
};

/**
 * A map from name to function object.
 *
 * This is temporary until we get the library loading task
 * complete
 *
 * TODO: Remove when library loading is enabled.
 */
class mcsv1_UDAF;
typedef std::tr1::unordered_map<std::string, mcsv1_UDAF*> UDAF_MAP;

class UDAFMap
{
 public:
  EXPORT UDAFMap(){};

  EXPORT ~UDAFMap(){};

  static EXPORT UDAF_MAP& getMap();

 private:
  static UDAF_MAP& fm();
};

/**
 * A class to hold your user data
 *
 * If your UDAF only needs a fixed sized data struct, you need
 * do nothing with this. Call setUserDataSize in your init
 * function with the required size and the framework will take
 * care of it.
 *
 * If you need something more or just want to control things,
 * then override UserData with your data structure and
 * implement createUserData in your function object to create
 * your data structure. Your UserData destuctor should take care
 * of any cleanup you may need (Simple containers clean
 * themselves up).
 */
class mcsv1Context;

struct UserData
{
  UserData() : size(0), data(NULL){};
  UserData(size_t sz)
  {
    size = sz;
    data = new uint8_t[sz];
  }

  virtual ~UserData()
  {
    if (data)
      delete[] data;
  }

  /**
   * serialize()
   *
   * User data is passed between processes. In order to do so, it
   * must be serialized. Since user data can have sub objects,
   * containers and the like, it is up to the UDAF to provide the
   * serialize function. The streaming functionality of
   * messageqcpp::ByteStream must be used.
   *
   * The default streams the size and data buffer to the
   * ByteStream
   */
  virtual void serialize(messageqcpp::ByteStream& bs) const;

  /**
   * unserialize()
   *
   * User data is passed between processes. In order to do so, it
   * must be unserialized. Since user data can have sub objects,
   * containers and the like, it is up to the UDAF to provide the
   * unserialize function. The streaming functionality of
   * messageqcpp::ByteStream must be used.
   *
   * data is the datablock returned by createUserData.
   *
   * The default creates the data array and streams into data.
   */
  virtual void unserialize(messageqcpp::ByteStream& bs);

  // The default data store. You may or may not wish to use these fields.
  uint32_t size;
  uint8_t* data;

 private:
  // For now, copy construction is unwanted
  UserData(UserData&);
};

// Flags to define the type and limitations of a UDA(n)F
// Used in context->fRunFlags
static uint64_t UDAF_OVER_REQUIRED __attribute__((unused)) = 1;      // May only be used as UDAnF
static uint64_t UDAF_OVER_ALLOWED __attribute__((unused)) = 1 << 1;  // May be used as UDAF or UDAnF
static uint64_t UDAF_ORDER_REQUIRED __attribute__((unused)) = 1
                                                              << 2;  // If used as UDAnF, ORDER BY is required
static uint64_t UDAF_ORDER_ALLOWED __attribute__((unused)) = 1
                                                             << 3;  // If used as UDAnF, ORDER BY is optional
static uint64_t UDAF_WINDOWFRAME_REQUIRED __attribute__((unused)) =
    1 << 4;  // If used as UDAnF, a WINDOW FRAME is required
static uint64_t UDAF_WINDOWFRAME_ALLOWED __attribute__((unused)) =
    1 << 5;  // If used as UDAnF, a WINDOW FRAME is optional
static uint64_t UDAF_MAYBE_NULL __attribute__((unused)) = 1 << 6;    // If UDA(n)F might return NULL.
static uint64_t UDAF_IGNORE_NULLS __attribute__((unused)) = 1 << 7;  // If UDA(n)F wants NULL rows suppressed.
static uint64_t UDAF_DISTINCT __attribute__((unused)) = 1
                                                        << 8;  // Force UDA(n)F to be distinct on first param.

// Flags set by the framework to define the context of the call.
// User code shouldn't use these directly
// used in context->fContextFlags
static uint64_t CONTEXT_IS_ANALYTIC __attribute__((unused)) = 1;  // If called using OVER
static uint64_t CONTEXT_HAS_CURRENT_ROW __attribute__((unused)) =
    1 << 1;  // The current window contains the current row.
static uint64_t CONTEXT_IS_PM __attribute__((unused)) = 1 << 2;  // The call was made by the PM

// Flags that describe the contents of a specific input parameter
// These will be set in context->dataFlags for each method call by the framework.
// User code shouldn't use these directly
static uint32_t PARAM_IS_NULL __attribute__((unused)) = 1;
static uint32_t PARAM_IS_CONSTANT __attribute__((unused)) = 1 << 1;

// This is the context class that is passed to all API callbacks
// The framework potentially sets data here for each invocation of
// mcsv1_UDAF methods. Access methods are given for data useful to UDA(n)F.
// Don't modify anything directly except the struct retrieved with getUserData().

// UDA(n)F devlopers should not modify this class. The framework and other UDA(n)F
// rely on it being as it was when they were compiled.
//
// It's probable that future versions of Columnstore will add functionality to
// the context. UDA(n)F may need to be re-compiled in this case.
class mcsv1Context
{
 public:
  EXPORT mcsv1Context();
  EXPORT mcsv1Context(const mcsv1Context& rhs);
  // The destructor is virtual only in case a version 2 is made derived from v1
  // to promote backward compatibility.
  // mcsv1Context should never be subclassed by UDA(n)F developers
  EXPORT virtual ~mcsv1Context();

  // Set an error message if something goes wrong
  EXPORT void setErrorMessage(std::string errmsg);

  // Get the previously set error message
  EXPORT const std::string& getErrorMessage() const;

  // Set the flags as a set. Return the previous flags.
  EXPORT uint64_t setRunFlags(uint64_t flags);
  // return the flags
  EXPORT uint64_t getRunFlags() const;

  // The following set, get, clear and toggle methods can be used to manipulate
  // multiple flags by ORing them together in the call sequence.
  // Ex setRunFlag(UDAF_OVER_REQUIRED | UDAF_ORDER_REQUIRED);
  // sets both flags and returns true if BOTH flags are already set.
  //
  // Set a specific flag and return its previous setting
  EXPORT bool setRunFlag(uint64_t flag);
  // Get a specific flag
  EXPORT bool getRunFlag(uint64_t flag);
  // clear a specific flag and return its previous setting
  EXPORT bool clearRunFlag(uint64_t flag);
  // toggle a specific flag and return its previous setting
  EXPORT bool toggleRunFlag(uint64_t flag);

  // Use these to determine the way your UDA(n)F was called
  // Valid in all method calls
  EXPORT bool isAnalytic();
  EXPORT bool isWindowHasCurrentRow();

  // Determine if the call is made by the UM
  // This could be because the UDA(n)F is not being distributed
  // Or it could be during setup or during consolodation of PM values.
  // valid in all calls
  EXPORT bool isUM();

  // Determine if the call is made by the PM
  // This will be during partial aggregation performed on the PM
  // valid in all calls
  EXPORT bool isPM();

  // Parameter refinement description accessors

  // How many actual parameters were entered.
  // valid in all calls
  size_t getParameterCount() const;

  // Determine if an input parameter is NULL
  // valid in nextValue and dropValue
  EXPORT bool isParamNull(int paramIdx);

  // If a parameter is a constant, the UDA(n)F could presumably optimize its workings
  // during the first call to nextValue().
  // Is there a better way to determine this?
  // valid in nextValue
  EXPORT bool isParamConstant(int paramIdx);

  // For getting the result type.
  EXPORT execplan::CalpontSystemCatalog::ColDataType getResultType() const;

  // For getting the decimal characteristics for the return type.
  // These will be set to the default before init().
  EXPORT int32_t getScale() const;
  EXPORT int32_t getPrecision() const;

  // If you want to change the result type
  // valid in init()
  EXPORT bool setResultType(execplan::CalpontSystemCatalog::ColDataType resultType);

  // For setting the decimal characteristics for the return value.
  // This only makes sense if the return type is decimal, but should be set
  // to (0, -1) for other types if the inout is decimal.
  // valid in init()
  // Set the scale to DECIMAL_NOT_SPECIFIED if you want a floating decimal.
  EXPORT bool setScale(int32_t scale);
  EXPORT bool setPrecision(int32_t precision);

  // For all types, get the return column width in bytes. Ex. INT will return 4.
  EXPORT int32_t getColWidth();

  // For non-numric return types, set the return column width. This defaults
  // to a length determined by the input datatype.
  // valid in init()
  EXPORT bool setColWidth(int32_t colWidth);

  // If a method is known to take a while, call this periodically to see if something
  // interupted the processing. If getInterrupted() returns true, then the executing
  // method should clean up and exit.
  EXPORT bool getInterrupted() const;

  // Allocate instance specific memory. This should be type cast to a structure overlay
  // defined by the function. The actual allocatoin occurs in the various modules that
  // do the aggregation. If the UDAF is being calculated in a distributed fashion, then
  // multiple instances of this data may be allocated. Calls to the subaggregate functions
  // do not share a context.
  // You do not need to worry about freeing this memory. The framework handles all management.
  // Call this during init()
  EXPORT void setUserDataSize(int bytes);

  // Call this everywhere except init()
  EXPORT UserData* getUserData();

  // Many UDAnF need a default Window Frame. If none is set here, the default is
  // UNBOUNDED PRECEDING to CURRENT ROW.
  // It's possible to not allow the the WINDOW FRAME phrase in the UDAnF by setting
  // the UDAF_WINDOWFRAME_REQUIRED and UDAF_WINDOWFRAME_ALLOWED both to false. Columnstore
  // requires a Window Frame in order to process UDAnF. In this case, the default will
  // be used for all calls.
  // Possible values for start frame are
  // WF_UNBOUNDED_PRECEDING, WF_CURRENT_ROW, WF_PRECEDING or WF_FOLLOWING
  // possible values for end frame are
  // WF_CURRENT_ROW, WF_UNBOUNDED_FOLLOWING, WF_PRECEDING or WF_FOLLOWING
  // If WF_PRECEEdING and/or WF_FOLLOWING, a start or end constant should
  // be included to say how many preceeding or following is the default
  // Set this during init()
  EXPORT bool setDefaultWindowFrame(execplan::WF_FRAME defaultStartFrame, execplan::WF_FRAME defaultEndFrame,
                                    int32_t startConstant = 0,  // For WF_PRECEEDING or WF_FOLLOWING
                                    int32_t endConstant = 0);   // For WF_PRECEEDING or WF_FOLLOWING

  // There may be times you want to know the actual frame set by the caller
  EXPORT void getStartFrame(execplan::WF_FRAME& startFrame, int32_t& startConstant) const;
  EXPORT void getEndFrame(execplan::WF_FRAME& endFrame, int32_t& endConstant) const;

  // Deep Equivalence
  bool operator==(const mcsv1Context& c) const;
  bool operator!=(const mcsv1Context& c) const;

  // stream operator for debugging
  EXPORT const std::string toString() const;

  // Get the name of the function
  EXPORT const std::string& getName() const;

  // Get the return type as set by CREATE AGGREGATE FUNCTION
  EXPORT enum_mariadb_return_type getMariaDBReturnType() const;

  EXPORT mcsv1Context& operator=(const mcsv1Context& rhs);
  EXPORT mcsv1Context& copy(const mcsv1Context& rhs);

  // Character collation support
  EXPORT void setCharsetNumber(uint32_t csNum);
  EXPORT uint32_t getCharsetNumber();  // Returns the unique ID for the language/collation
  EXPORT CHARSET_INFO* getCharset();

 private:
  uint64_t fRunFlags;      // Set by the user to define the type of UDA(n)F
  uint64_t fContextFlags;  // Set by the framework to define this specific call.
  int32_t fUserDataSize;
  boost::shared_ptr<UserData> fUserData;
  execplan::CalpontSystemCatalog::ColDataType fResultType;
  int32_t fColWidth;         // The length in bytes of the return type
  int32_t fResultscale;      // For scale, the number of digits to the right of the decimal
  int32_t fResultPrecision;  // The max number of digits allowed in the decimal value
  std::string errorMsg;
  uint32_t* dataFlags;  // an integer array wirh one entry for each parameter
  bool* bInterrupted;   // Gets set to true by the Framework if something happens
  execplan::WF_FRAME
      fStartFrame;               // Is set to default to start, then modified by the actual frame in the call
  execplan::WF_FRAME fEndFrame;  // Is set to default to start, then modified by the actual frame in the call
  int32_t fStartConstant;        // for start frame WF_PRECEEDIMG or WF_FOLLOWING
  int32_t fEndConstant;          // for end frame WF_PRECEEDIMG or WF_FOLLOWING
  std::string functionName;
  mcsv1sdk::mcsv1_UDAF* func;
  int32_t fParamCount;
  std::vector<uint32_t> paramKeys;
  enum_mariadb_return_type mariadbReturnType;
  uint32_t fCharsetNumber;

 public:
  // For use by the framework
  EXPORT void serialize(messageqcpp::ByteStream& b) const;
  EXPORT void unserialize(messageqcpp::ByteStream& b);
  EXPORT void createUserData();
  EXPORT void setUserData(boost::shared_ptr<UserData> userData);
  EXPORT void setUserData(UserData* userData);
  EXPORT void setName(std::string name);
  EXPORT void setContextFlags(uint64_t flags);
  EXPORT void setContextFlag(uint64_t flag);
  EXPORT void clearContextFlag(uint64_t flag);
  EXPORT uint64_t getContextFlags() const;
  EXPORT uint32_t getUserDataSize() const;
  EXPORT uint32_t* getDataFlags();
  EXPORT void setDataFlags(uint32_t* flags);
  EXPORT void setInterrupted(bool interrupted);
  EXPORT void setInterrupted(bool* interrupted);
  EXPORT mcsv1sdk::mcsv1_UDAF* getFunction();
  EXPORT mcsv1sdk::mcsv1_UDAF* getFunction() const;
  EXPORT boost::shared_ptr<UserData> getUserDataSP();
  EXPORT void setParamCount(int32_t paramCount);
  std::vector<uint32_t>* getParamKeys();
  EXPORT void setMariaDBReturnType(enum_mariadb_return_type rt);
};

// Since aggregate functions can operate on any data type, we use the following structure
// to define the input row data. To be type insensiteve, data is stored in type static_any::any.
//
// To access the data it must be type cast to the correct type using static_any::cast.
// example for int data:
//
// if (valIn.compatible(intTypeId)
//     int myint = valIn.cast<int>();
//
// For multi-paramter aggregations, the colsIn vector of next_value()
// contains the ordered set of row parameters.
//
// For char, varchar, text, varbinary and blob types, columnData will be std::string.
struct ColumnDatum
{
  execplan::CalpontSystemCatalog::ColDataType dataType;  // defined in calpontsystemcatalog.h
  static_any::any columnData;                            // Not valid in init()
  uint32_t scale;                                        // If dataType is a DECIMAL type
  uint32_t precision;                                    // If dataType is a DECIMAL type
  std::string alias;                                     // Only filled in for init()
  uint32_t charsetNumber;                                // For string collations
  ColumnDatum()
   : dataType(execplan::CalpontSystemCatalog::UNDEFINED), scale(0), precision(-1), charsetNumber(8){};
};

// Override mcsv1_UDAF to build your User Defined Aggregate (UDAF) and/or
// User Defined Analytic Function (UDAnF).
// These will be singleton classes, so don't put any instance
// specific data in here. All instance data is stored in mcsv1Context
// passed to each user function and retrieved by the getUserData() method.
//
// Each API function returns a ReturnCode. If ERROR is returned at any time,
// the query is aborted, getInterrupted() will begin to return true and the
// message set in config->setErrorMessage() is returned to MariaDB.
class mcsv1_UDAF
{
 public:
  enum ReturnCode
  {
    ERROR = 0,
    SUCCESS = 1,
    NOT_IMPLEMENTED = 2  // User UDA(n)F shouldn't return this
  };
  // Defaults OK
  mcsv1_UDAF(){};
  virtual ~mcsv1_UDAF(){};

  /**
   * init()
   *
   * Mandatory. Implement this to initialize flags and instance
   * data. Called once per SQL statement. You can do any sanity
   * checks here.
   *
   * colTypes (in) - A vector of ColDataType defining the
   * parameters of the UDA(n)F call. These can be used to decide
   * to override the default return type. If desired, the new
   * return type can be set by context->setReturnType() and
   * decimal scale and precision can be set by context->setScale
   * and context->setPrecision respectively.
   *
   * Return mcsv1_UDAF::ERROR on any error, such as non-compatible
   * colTypes or wrong number of arguments. Else return
   * mcsv1_UDAF::SUCCESS.
   */
  virtual ReturnCode init(mcsv1Context* context, ColumnDatum* colTypes) = 0;

  /**
   * reset()
   *
   * Mandatory. Reset the UDA(n)F for a new group, partition or,
   * in some cases, new Window Frame. Do not free any memory
   * allocated by createUserData(). The SDK Framework owns
   * that memory and will handle that. Use this opportunity to
   * reset any variables in context->getUserData() needed for the
   * next aggregation. May be called multiple times if running in
   * a ditributed fashion.
   *
   * Use this opportunity to initialize the userData.
   */
  virtual ReturnCode reset(mcsv1Context* context) = 0;

  /**
   * nextValue()
   *
   * Mandatory. Handle a single row.
   *
   * colsIn - A vector of data structure describing the input
   * data.
   *
   * This function is called once for every row in the filtered
   * result set (before aggregation). It is very important that
   * this function is efficient.
   *
   * If the UDAF is running in a distributed fashion, nextValue
   * cannot depend on order, as it will only be called for each
   * row found on the specific PM.
   *
   * valsIn (in) - a vector of the parameters from the row.
   */
  virtual ReturnCode nextValue(mcsv1Context* context, ColumnDatum* valsIn) = 0;

  /**
   * subEvaluate()
   *
   * Mandatory -- Called if the UDAF is running in a distributed
   * fashion. Columnstore tries to run all aggregate functions
   * distributed, depending on context.
   *
   * Perform an aggregation on rows partially aggregated by
   * nextValue. Columnstore calls nextValue for each row on a
   * given PM for a group (GROUP BY). subEvaluate is called on the
   * UM to consolodate those values into a single instance of
   * userData. Keep your aggregated totals in context's userData.
   * The first time this is called for a group, reset() would have
   * been called with this version of userData.
   *
   * Called for every partial data set in each group in GROUP BY.
   *
   * When subEvaluate has been called for all subAggregated data
   * sets, Evaluate will be called with the same context as here.
   *
   * valIn (In) - This is a pointer to a UserData class with the
   * partially aggregated values. It will contain the value of
   * userData as seen in the last call to NextValue for a given
   * PM.
   *
   */
  virtual ReturnCode subEvaluate(mcsv1Context* context, const UserData* userDataIn) = 0;

  /**
   * evaluate()
   *
   * Mandatory. Get the aggregated value.
   *
   * Called for every new group if UDAF GROUP BY, UDAnF partition
   * or, in some cases, new Window Frame.
   *
   * Set the aggregated value into valOut. The datatype is assumed
   * to be the same as that set in the init() function;
   *
   * If the UDAF is running in a distributed fashion, evaluate is
   * called after a series of subEvaluate calls.
   *
   * valOut (out) - Set the aggregated value here. The datatype is
   * assumed to be the same as that set in the init() function;
   *
   * To return a NULL value, don't assign to valOut.
   */
  virtual ReturnCode evaluate(mcsv1Context* context, static_any::any& valOut) = 0;

  /**
   * dropValue()
   *
   * Optional -- If defined, the server will call this instead of
   * reset for UDAnF.
   *
   * Don't implement if a UDAnF has one or more of the following:
   * The UDAnF can't be used with a Window Frame
   * The UDAnF is not reversable in some way
   * The UDAnF is not interested in optimal performance
   *
   * If not implemented, reset() followed by a series of
   * nextValue() will be called for each movement of the Window
   * Frame.
   *
   * If implemented, then each movement of the Window Frame will
   * result in dropValue() being called for each row falling out
   * of the Frame and nextValue() being called for each new row
   * coming into the Frame.
   *
   * valsDropped (in) - a vector of the parameters from the row
   * leaving the Frame
   *
   * dropValue() will not be called for unbounded/current row type
   * frames, as those are already optimized.
   */
  virtual ReturnCode dropValue(mcsv1Context* context, ColumnDatum* valsDropped);

  /**
   * createUserData()
   *
   * Optional -- The default is to create a data byte array of
   * size as set in context->setUserDataSize()
   *
   * Create your variable length data structure via
   * userData = new <UserData_type>
   *
   * The data structure may contain references to containers or
   * pointers to other objects. Remember that for distributed
   * processing, this may be called multiple times for variaous
   * computing blocks. At the least, it will be called once per PM
   * that processes the data, and once more for the UM. For UDAnF,
   * it may only be called once.
   *
   * Set length to the base length of the data structure you
   * create.
   *
   */
  virtual ReturnCode createUserData(UserData*& userdata, int32_t& length);

 protected:
  double toDouble(ColumnDatum& datum) const
  {
    double val = convertAnyTo<double>(datum.columnData);
    if (val != 0 && datum.scale > 0)
      val /= datatypes::scaleDivisor<double>(datum.scale);
    return val;
  }

  // some handy conversion routines
  template <typename T>
  T convertAnyTo(static_any::any&) const;
  // These are handy for testing the actual type of static_any
  static const static_any::any& charTypeId;
  static const static_any::any& scharTypeId;
  static const static_any::any& shortTypeId;
  static const static_any::any& intTypeId;
  static const static_any::any& longTypeId;
  static const static_any::any& llTypeId;
  static const static_any::any& int128TypeId;
  static const static_any::any& ucharTypeId;
  static const static_any::any& ushortTypeId;
  static const static_any::any& uintTypeId;
  static const static_any::any& ulongTypeId;
  static const static_any::any& ullTypeId;
  static const static_any::any& floatTypeId;
  static const static_any::any& doubleTypeId;
  static const static_any::any& strTypeId;
};

/***********************************************************************
 * There is no user modifiable code past this point
 ***********************************************************************/
// Function definitions for mcsv1Context
inline mcsv1Context::mcsv1Context()
 : fRunFlags(UDAF_OVER_ALLOWED | UDAF_ORDER_ALLOWED | UDAF_WINDOWFRAME_ALLOWED)
 , fContextFlags(0)
 , fUserDataSize(0)
 , fResultType(execplan::CalpontSystemCatalog::UNDEFINED)
 , fColWidth(0)
 , fResultscale(0)
 , fResultPrecision(18)
 , dataFlags(NULL)
 , bInterrupted(NULL)
 , fStartFrame(execplan::WF_UNBOUNDED_PRECEDING)
 , fEndFrame(execplan::WF_CURRENT_ROW)
 , fStartConstant(0)
 , fEndConstant(0)
 , func(NULL)
 , fParamCount(0)
 , fCharsetNumber(8)  // Latin1
{
}

inline mcsv1Context::mcsv1Context(const mcsv1Context& rhs) : dataFlags(NULL)
{
  copy(rhs);
}

inline mcsv1Context& mcsv1Context::copy(const mcsv1Context& rhs)
{
  fRunFlags = rhs.fRunFlags;
  fContextFlags = rhs.fContextFlags;
  fResultType = rhs.fResultType;
  fUserDataSize = rhs.fUserDataSize;
  fColWidth = rhs.fColWidth;
  fResultscale = rhs.fResultscale;
  fResultPrecision = rhs.fResultPrecision;
  rhs.getStartFrame(fStartFrame, fStartConstant);
  rhs.getEndFrame(fEndFrame, fEndConstant);
  functionName = rhs.functionName;
  bInterrupted = rhs.bInterrupted;  // Multiple threads will use the same reference
  func = rhs.func;
  fParamCount = rhs.fParamCount;
  fCharsetNumber = rhs.fCharsetNumber;
  return *this;
}

inline mcsv1Context::~mcsv1Context()
{
}

inline mcsv1Context& mcsv1Context::operator=(const mcsv1Context& rhs)
{
  dataFlags = NULL;
  return copy(rhs);
}

inline void mcsv1Context::setErrorMessage(std::string errmsg)
{
  errorMsg = errmsg;
}

inline const std::string& mcsv1Context::getErrorMessage() const
{
  return errorMsg;
}

inline uint64_t mcsv1Context::setRunFlags(uint64_t flags)
{
  uint64_t f = fRunFlags;
  fRunFlags = flags;
  return f;
}

inline uint64_t mcsv1Context::getRunFlags() const
{
  return fRunFlags;
}

inline bool mcsv1Context::setRunFlag(uint64_t flag)
{
  bool b = fRunFlags & flag;
  fRunFlags |= flag;
  return b;
}

inline bool mcsv1Context::getRunFlag(uint64_t flag)
{
  return fRunFlags & flag;
}

inline bool mcsv1Context::clearRunFlag(uint64_t flag)
{
  bool b = fRunFlags & flag;
  fRunFlags &= ~flag;
  return b;
}

inline bool mcsv1Context::toggleRunFlag(uint64_t flag)
{
  bool b = fRunFlags & flag;
  fRunFlags ^= flag;
  return b;
}

inline bool mcsv1Context::isAnalytic()
{
  return fContextFlags & CONTEXT_IS_ANALYTIC;
}

inline bool mcsv1Context::isWindowHasCurrentRow()
{
  return fContextFlags & CONTEXT_HAS_CURRENT_ROW;
}

inline bool mcsv1Context::isUM()
{
  return !(fContextFlags & CONTEXT_IS_PM);
}

inline bool mcsv1Context::isPM()
{
  return fContextFlags & CONTEXT_IS_PM;
}

inline size_t mcsv1Context::getParameterCount() const
{
  return fParamCount;
}

inline bool mcsv1Context::isParamNull(int paramIdx)
{
  if (dataFlags)
    return dataFlags[paramIdx] & PARAM_IS_NULL;

  return false;
}

inline bool mcsv1Context::isParamConstant(int paramIdx)
{
  if (dataFlags)
    return dataFlags[paramIdx] & PARAM_IS_CONSTANT;

  return false;
}

inline execplan::CalpontSystemCatalog::ColDataType mcsv1Context::getResultType() const
{
  return fResultType;
}

inline bool mcsv1Context::setResultType(execplan::CalpontSystemCatalog::ColDataType resultType)
{
  fResultType = resultType;
  return true;  // We may want to sanity check here.
}

inline int32_t mcsv1Context::getScale() const
{
  return fResultscale;
}

inline int32_t mcsv1Context::getPrecision() const
{
  return fResultPrecision;
}

inline bool mcsv1Context::setScale(int32_t scale)
{
  fResultscale = scale;
  return true;
}

inline bool mcsv1Context::setPrecision(int32_t precision)
{
  fResultPrecision = precision;
  return true;
}

inline bool mcsv1Context::setColWidth(int32_t colWidth)
{
  fColWidth = colWidth;
  return true;
}

inline void mcsv1Context::setInterrupted(bool interrupted)
{
  if (bInterrupted)
  {
    *bInterrupted = interrupted;
  }
}

inline void mcsv1Context::setInterrupted(bool* interrupted)
{
  bInterrupted = interrupted;
}

inline bool mcsv1Context::getInterrupted() const
{
  return bInterrupted && *bInterrupted;
}

inline void mcsv1Context::setUserDataSize(int bytes)
{
  fUserDataSize = bytes;
}

inline UserData* mcsv1Context::getUserData()
{
  if (!fUserData)
  {
    createUserData();
  }

  return fUserData.get();
}

inline boost::shared_ptr<UserData> mcsv1Context::getUserDataSP()
{
  if (!fUserData)
  {
    createUserData();
  }

  return fUserData;
}

inline void mcsv1Context::setUserData(boost::shared_ptr<UserData> userData)
{
  fUserData = userData;
}

inline void mcsv1Context::setUserData(UserData* userData)
{
  if (userData)
  {
    fUserData.reset(userData);
  }
  else
  {
    fUserData.reset();
  }
}

inline bool mcsv1Context::setDefaultWindowFrame(execplan::WF_FRAME defaultStartFrame,
                                                execplan::WF_FRAME defaultEndFrame, int32_t startConstant,
                                                int32_t endConstant)
{
  // TODO: Add sanity checks
  fStartFrame = defaultStartFrame;
  fEndFrame = defaultEndFrame;
  fStartConstant = startConstant;
  fEndConstant = endConstant;
  return true;
}

inline void mcsv1Context::getStartFrame(execplan::WF_FRAME& startFrame, int32_t& startConstant) const
{
  startFrame = fStartFrame;
  startConstant = fStartConstant;
}

inline void mcsv1Context::getEndFrame(execplan::WF_FRAME& endFrame, int32_t& endConstant) const
{
  endFrame = fEndFrame;
  endConstant = fEndConstant;
}

inline const std::string& mcsv1Context::getName() const
{
  return functionName;
}

inline void mcsv1Context::setName(std::string name)
{
  functionName = name;
}

inline uint64_t mcsv1Context::getContextFlags() const
{
  return fContextFlags;
}

inline void mcsv1Context::setContextFlags(uint64_t flags)
{
  fContextFlags = flags;
}

inline void mcsv1Context::setContextFlag(uint64_t flag)
{
  fContextFlags |= flag;
}

inline void mcsv1Context::clearContextFlag(uint64_t flag)
{
  fContextFlags &= ~flag;
}

inline uint32_t mcsv1Context::getUserDataSize() const
{
  return fUserDataSize;
}

inline uint32_t* mcsv1Context::getDataFlags()
{
  return dataFlags;
}

inline void mcsv1Context::setDataFlags(uint32_t* flags)
{
  dataFlags = flags;
}

inline void mcsv1Context::setParamCount(int32_t paramCount)
{
  fParamCount = paramCount;
}

inline std::vector<uint32_t>* mcsv1Context::getParamKeys()
{
  return &paramKeys;
}

inline enum_mariadb_return_type mcsv1Context::getMariaDBReturnType() const
{
  return mariadbReturnType;
}

inline void mcsv1Context::setMariaDBReturnType(enum_mariadb_return_type rt)
{
  mariadbReturnType = rt;
}

inline void mcsv1Context::setCharsetNumber(uint32_t csNum)
{
  fCharsetNumber = csNum;
}

inline uint32_t mcsv1Context::getCharsetNumber()
{
  return fCharsetNumber;
}

inline mcsv1_UDAF::ReturnCode mcsv1_UDAF::dropValue(mcsv1Context* context, ColumnDatum* valsDropped)
{
  return NOT_IMPLEMENTED;
}

inline mcsv1_UDAF::ReturnCode mcsv1_UDAF::createUserData(UserData*& userData, int32_t& length)
{
  userData = new UserData(length);
  userData->size = length;
  return SUCCESS;
}

// Handy helper functions
template <typename T>
inline T mcsv1_UDAF::convertAnyTo(static_any::any& valIn) const
{
  T val = 0;
  if (valIn.compatible(longTypeId))
  {
    val = valIn.cast<long>();
  }
  else if (valIn.compatible(charTypeId))
  {
    val = valIn.cast<char>();
  }
  else if (valIn.compatible(scharTypeId))
  {
    val = valIn.cast<signed char>();
  }
  else if (valIn.compatible(shortTypeId))
  {
    val = valIn.cast<short>();
  }
  else if (valIn.compatible(intTypeId))
  {
    val = valIn.cast<int>();
  }
  else if (valIn.compatible(llTypeId))
  {
    val = valIn.cast<long long>();
  }
  else if (valIn.compatible(ucharTypeId))
  {
    val = valIn.cast<unsigned char>();
  }
  else if (valIn.compatible(ushortTypeId))
  {
    val = valIn.cast<unsigned short>();
  }
  else if (valIn.compatible(uintTypeId))
  {
    val = valIn.cast<unsigned int>();
  }
  else if (valIn.compatible(ulongTypeId))
  {
    val = valIn.cast<unsigned long>();
  }
  else if (valIn.compatible(ullTypeId))
  {
    val = valIn.cast<unsigned long long>();
  }
  else if (valIn.compatible(floatTypeId))
  {
    val = valIn.cast<float>();
  }
  else if (valIn.compatible(doubleTypeId))
  {
    val = valIn.cast<double>();
  }
  else if (valIn.compatible(int128TypeId))
  {
    val = valIn.cast<int128_t>();
  }
  else
  {
    throw std::runtime_error("mcsv1_UDAF::convertAnyTo(): input param has unrecognized type");
  }
  return val;
}

};  // namespace mcsv1sdk

#undef EXPORT

#endif  // HEADER_mcsv1_udaf.h
