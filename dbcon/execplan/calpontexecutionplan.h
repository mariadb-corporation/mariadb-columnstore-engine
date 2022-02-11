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

/***********************************************************************
 *   $Id: calpontexecutionplan.h 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/
/** @file */

#ifndef CALPONTEXECUTIONPLAN_H
#define CALPONTEXECUTIONPLAN_H
#include <string>
#include <boost/shared_ptr.hpp>

namespace messageqcpp
{
class ByteStream;
}

/**
 * Namespace
 */
namespace execplan
{
/**
 * ******************************* Abstract Class ****************************
 * CalpontExecutionPlan does not have any pure virtual methods, but its author
 *   defined it as an abstract class, so you should not use it directly.
 *   Inherit from it instead and create only objects from the derived classes
 * *****************************************************************************
 */
class CalpontExecutionPlan
{
  /**
   * Public stuff
   */
 public:
  /**
   * Flags that can be passed to caltraceon().
   */
  enum TRACE_FLAGS
  {
    TRACE_NONE = 0x0000,             /*!< No tracing */
    TRACE_LOG = 0x0001,              /*!< Full return of rows, extra debug about query in log */
    TRACE_NO_ROWS1 = 0x0002,         /*!< Same as above, but rows not given to OCI layer */
    TRACE_NO_ROWS2 = 0x0004,         /*!< Same as above, but rows not converted from stream */
    TRACE_NO_ROWS3 = 0x0008,         /*!< Same as above, but rows not sent to DM from UM */
    TRACE_NO_ROWS4 = 0x0010,         /*!< Same as above, but rows not sent to DeliveryStep */
    TRACE_LBIDS = 0x0020,            /*!< Enable LBID tracing in PrimProc */
    TRACE_PLAN_ONLY = 0x0040,        /*!< Only generate a serialized CSEP */
    PM_PROFILE = 0x0080,             /*!< Enable PM profiling in PrimProc */
    IGNORE_CP = 0x0100,              /*!< Ignore casual partitioning metadata */
    WRITE_TO_FILE = 0x0200,          /*!< writes table rows out to a file from the Oracle connector */
    NOWRITE_TO_FILE = 0x0400,        /*!< does not write table rows out to a file from the Oracle connector */
    TRACE_DISKIO_UM = 0x0800,        /*!< Enable UM disk I/O logging */
    TRACE_RESRCMGR = 0x1000,         /*!< Trace Resource Manager Usage */
    TRACE_TUPLE_AUTOSWITCH = 0x4000, /*!< Enable MySQL tuple-to-table auto switch */
    TRACE_TUPLE_OFF = 0x8000,        /*!< Enable MySQL table interface */
  };

  /**
   * Constructors
   */
  CalpontExecutionPlan();
  /**
   * Destructors
   */
  virtual ~CalpontExecutionPlan();
  /**
   * Accessor Methods
   */
  /**
   * Operations
   */
  /*
   * The serialization interface
   */
  /** @brief Convert *this to a stream of bytes
   *
   * Convert *this to a stream of bytes.
   * @param b The ByteStream to write the bytes to.
   */
  virtual void serialize(messageqcpp::ByteStream& b) const = 0;

  /** @brief Construct a CalpontExecutionPlan from a stream of bytes
   *
   * Construct a CalpontExecutionPlan from a stream of bytes.
   * @param b The ByteStream to read from.
   */
  virtual void unserialize(messageqcpp::ByteStream& b) = 0;

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return true iff every member of t is a duplicate copy of every member of this; false otherwise
   */
  virtual bool operator==(const CalpontExecutionPlan* t) const = 0;

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return false iff every member of t is a duplicate copy of every member of this; true otherwise
   */
  virtual bool operator!=(const CalpontExecutionPlan* t) const = 0;

  /**
   * Protected stuff
   */
 protected:
  /**
   * Fields
   */
  /**
   *
   */
  /**
   * Constructors
   */
  /**
   * Accessor Methods
   */
  /**
   * Operations
   */
  /**
   * Private stuff
   */
 private:
  /**
   * Fields
   */
  /**
   *
   */
  /**
   * Constructors
   */
  /**
   * Accessor Methods
   */
  /**
   * Operations
   */
};

typedef boost::shared_ptr<CalpontExecutionPlan> SCEP;

}  // namespace execplan
#endif  // CALPONTEXECUTIONPLAN_H
