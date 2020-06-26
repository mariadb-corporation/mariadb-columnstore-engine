/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2019 MariaDB Corporation

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

//  $Id: functor.h 3495 2013-01-21 14:09:51Z rdempsey $

/** @file */

#ifndef FUNCTOR_H
#define FUNCTOR_H

#include <cstdlib>
#include <string>
#include <sstream>
#include <string>
#include <mutex>

#include "parsetree.h"
#include "exceptclasses.h"
#include "errorids.h"
#include "idberrorinfo.h"

#include "calpontsystemcatalog.h"

#include "dataconvert.h"

namespace rowgroup
{
class Row;
}

namespace execplan
{
extern const std::string colDataTypeToString(CalpontSystemCatalog::ColDataType cdt);
}

namespace funcexp
{
//typedef std::vector<execplan::STNP> FunctionParm;
typedef std::vector<execplan::SPTP> FunctionParm;


/** @brief Func class
  */
class Func
{
public:
    Func();
    Func(const std::string& funcName);
    virtual ~Func() {}

    const std::string funcName() const
    {
        return fFuncName;
    }
    void funcName(const std::string funcName)
    {
        fFuncName = funcName;
    }

    const std::string timeZone() const
    {
        std::unique_lock<std::mutex> l(tzMutex);
        return fTimeZone;
    }
    void timeZone(const std::string timeZone)
    {
        std::unique_lock<std::mutex> l(tzMutex);
        fTimeZone = timeZone;
    }

    virtual execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) = 0;

    virtual int64_t getIntVal(rowgroup::Row& row,
                              FunctionParm& fp,
                              bool& isNull,
                              execplan::CalpontSystemCatalog::ColType& op_ct) = 0;

    virtual uint64_t getUintVal(rowgroup::Row& row,
                                FunctionParm& fp,
                                bool& isNull,
                                execplan::CalpontSystemCatalog::ColType& op_ct)
    {
        return static_cast<uint64_t>(getIntVal(row, fp, isNull, op_ct));
    }

    virtual double getDoubleVal(rowgroup::Row& row,
                                FunctionParm& fp,
                                bool& isNull,
                                execplan::CalpontSystemCatalog::ColType& op_ct) = 0;

    virtual long double getLongDoubleVal(rowgroup::Row& row,
                                FunctionParm& fp,
                                bool& isNull,
                                execplan::CalpontSystemCatalog::ColType& op_ct) = 0;

    virtual std::string getStrVal(rowgroup::Row& row,
                                  FunctionParm& fp,
                                  bool& isNull,
                                  execplan::CalpontSystemCatalog::ColType& op_ct) = 0;

    virtual execplan::IDB_Decimal getDecimalVal(rowgroup::Row& row,
            FunctionParm& fp,
            bool& isNull,
            execplan::CalpontSystemCatalog::ColType& op_ct)
    {
        return execplan::IDB_Decimal(getIntVal(row, fp, isNull, op_ct), 0, 0);
    }

    virtual int32_t getDateIntVal(rowgroup::Row& row,
                                  FunctionParm& fp,
                                  bool& isNull,
                                  execplan::CalpontSystemCatalog::ColType& op_ct)
    {
        return intToDate(getIntVal(row, fp, isNull, op_ct));
    }

    virtual int64_t getDatetimeIntVal(rowgroup::Row& row,
                                      FunctionParm& fp,
                                      bool& isNull,
                                      execplan::CalpontSystemCatalog::ColType& op_ct)
    {
        return intToDatetime(getIntVal(row, fp, isNull, op_ct));
    }

    virtual int64_t getTimestampIntVal(rowgroup::Row& row,
                                       FunctionParm& fp,
                                       bool& isNull,
                                       execplan::CalpontSystemCatalog::ColType& op_ct)
    {
        return intToTimestamp(getIntVal(row, fp, isNull, op_ct));
    }

    virtual int64_t getTimeIntVal(rowgroup::Row& row,
                                  FunctionParm& fp,
                                  bool& isNull,
                                  execplan::CalpontSystemCatalog::ColType& op_ct)
    {
        return intToTime(getIntVal(row, fp, isNull, op_ct));
    }

    virtual bool getBoolVal(rowgroup::Row& row,
                            FunctionParm& fp,
                            bool& isNull,
                            execplan::CalpontSystemCatalog::ColType& op_ct)
    {
        int64_t intVal = getIntVal(row, fp, isNull, op_ct);
        return (!isNull && intVal);
    }

    virtual float getFloatVal(rowgroup::Row& row,
                              FunctionParm& fp,
                              bool& isNull,
                              execplan::CalpontSystemCatalog::ColType& op_ct)
    {
        return getDoubleVal(row, fp, isNull, op_ct);
    }

    float floatNullVal() const
    {
        return fFloatNullVal;
    }
    double doubleNullVal() const
    {
        return fDoubleNullVal;
    }
    long double longDoubleNullVal() const
    {
        return fLongDoubleNullVal;
    }


protected:
    virtual uint32_t stringToDate(std::string);
    virtual uint64_t stringToDatetime(std::string);
    virtual uint64_t stringToTimestamp(std::string);
    virtual int64_t stringToTime(std::string);

    virtual uint32_t intToDate(int64_t);
    virtual uint64_t intToDatetime(int64_t);
    virtual uint64_t intToTimestamp(int64_t);
    virtual int64_t intToTime(int64_t);

    virtual std::string intToString(int64_t);
    virtual std::string doubleToString(double);
    virtual std::string longDoubleToString(long double);

    virtual int64_t nowDatetime();
    virtual int64_t addTime(dataconvert::DateTime& dt1, dataconvert::Time& dt2);
    virtual int64_t addTime(dataconvert::Time& dt1, dataconvert::Time& dt2);

    std::string fFuncName;

private:
    //defaults okay
    //Func(const Func& rhs);
    //Func& operator=(const Func& rhs);

    void init();

    float fFloatNullVal;
    double fDoubleNullVal;
    long double fLongDoubleNullVal;

    std::string fTimeZone;
    mutable std::mutex tzMutex;
};


}

#endif



