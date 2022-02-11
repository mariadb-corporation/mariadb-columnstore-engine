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
 * 4. build the dynamic library using all of the source.
 * 5  Put the library in $COLUMNSTORE_INSTALL/lib of
 * all modules
 * 6. restart the Columnstore system.
 * 7. notify mysqld about the new function:
 *
 *    CREATE AGGREGATE FUNCTION ssq returns REAL soname
 *    'libudf_mysql.so';
 *
 * The UDAF function will run distributed in the Columnstore
 * engine. UDAnF do not run distributed.
 *
 * UDAF is User Defined Aggregate Function.
 * UDAnF is User Defined Analytic Function.
 * UDA(n)F is an acronym for a function that could be either. It
 * is also used to describe the interface that is used for
 * either.
 */
#ifndef HEADER_ssq
#define HEADER_ssq

#include <cstdlib>
#include <string>
#include <vector>
#ifdef _MSC_VER
#include <unordered_map>
#else
#include <tr1/unordered_map>
#endif

#include "mcsv1_udaf.h"
#include "calpontsystemcatalog.h"
#include "windowfunctioncolumn.h"

#if defined(_MSC_VER) && defined(xxxRGNODE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace mcsv1sdk
{
// Override mcsv1_UDAF to build your User Defined Aggregate (UDAF) and/or
// User Defined Analytic Function (UDAnF).
// These will be singleton classes, so don't put any instance
// specific data in here. All instance data is stored in mcsv1Context
// passed to each user function and retrieved by the getUserData() method.
//
// Each API function returns a ReturnCode. If ERROR is returned at any time,
// the query is aborted, getInterrupted() will begin to return true and the
// message set in config->setErrorMessage() is returned to MariaDB.

// A simple aggregate to return the sum of squares
class ssq : public mcsv1_UDAF
{
 public:
  // Defaults OK
  ssq() : mcsv1_UDAF(){};
  virtual ~ssq(){};

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
  virtual ReturnCode init(mcsv1Context* context, ColumnDatum* colTypes);

  /**
   * reset()
   *
   * Mandatory. Reset the UDA(n)F for a new group, partition or,
   * in some cases, new Window Frame. Do not free any memory
   * allocated by context->createUserData(). The SDK Framework
   * owns that memory and will handle that. Use this opportunity
   * to reset any variables in context->getUserData() needed for
   * the next aggregation. May be called multiple times on
   * different modules.
   */
  virtual ReturnCode reset(mcsv1Context* context);

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
  virtual ReturnCode nextValue(mcsv1Context* context, ColumnDatum* valsIn);

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
   * sets, Evaluate will be called.
   *
   * valIn (In) - This is a pointer to a memory block of the size
   * set in setUserDataSize. It will contain the value of userData
   * as seen in the last call to NextValue for a given PM.
   *
   */
  virtual ReturnCode subEvaluate(mcsv1Context* context, const UserData* userDataIn);

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
  virtual ReturnCode evaluate(mcsv1Context* context, static_any::any& valOut);

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

 protected:
};

};  // namespace mcsv1sdk

#undef EXPORT

#endif  // HEADER_ssq.h
