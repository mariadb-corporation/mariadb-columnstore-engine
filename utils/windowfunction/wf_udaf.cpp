/************************************************************************************
  Copyright (C) 2017 MariaDB Corporation AB

  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Library General Public
  License as published by the Free Software Foundation; either
  version 2 of the License, or (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Library General Public License for more details.

  You should have received a copy of the GNU Library General Public
  License along with this library; if not see <http://www.gnu.org/licenses>
  or write to the Free Software Foundation, Inc.,
  51 Franklin St., Fifth Floor, Boston, MA 02110, USA
 *************************************************************************************/


//#define NDEBUG
#include <cassert>
#include <cmath>
#include <sstream>
#include <iomanip>
using namespace std;

#include <boost/shared_ptr.hpp>
using namespace boost;

#include "loggingid.h"
#include "errorcodes.h"
#include "idberrorinfo.h"
using namespace logging;

#include "rowgroup.h"
using namespace rowgroup;

#include "idborderby.h"
using namespace ordering;

#include "joblisttypes.h"
#include "calpontsystemcatalog.h"
#include "constantcolumn.h"
using namespace execplan;

#include "windowfunctionstep.h"
using namespace joblist;

#include "wf_udaf.h"


namespace windowfunction
{
template<typename T>
boost::shared_ptr<WindowFunctionType> WF_udaf<T>::makeFunction(int id, const string& name, int ct, mcsv1sdk::mcsv1Context& context)
{
	boost::shared_ptr<WindowFunctionType> func;
	switch (ct)
	{
		case CalpontSystemCatalog::TINYINT:
		case CalpontSystemCatalog::SMALLINT:
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::INT:
		case CalpontSystemCatalog::BIGINT:
		case CalpontSystemCatalog::DECIMAL:
		{
			func.reset(new WF_udaf<int64_t>(id, name, context));
			break;
		}
		case CalpontSystemCatalog::UTINYINT:
		case CalpontSystemCatalog::USMALLINT:
		case CalpontSystemCatalog::UMEDINT:
		case CalpontSystemCatalog::UINT:
		case CalpontSystemCatalog::UBIGINT:
		case CalpontSystemCatalog::UDECIMAL:
		{
			func.reset(new WF_udaf<uint64_t>(id, name, context));
			break;
		}
		case CalpontSystemCatalog::DOUBLE:
		case CalpontSystemCatalog::UDOUBLE:
		{
			func.reset(new WF_udaf<double>(id, name, context));
			break;
		}
		case CalpontSystemCatalog::FLOAT:
		case CalpontSystemCatalog::UFLOAT:
		{
			func.reset(new WF_udaf<float>(id, name, context));
			break;
		}
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:
		case CalpontSystemCatalog::VARBINARY:
		case CalpontSystemCatalog::TEXT:
		case CalpontSystemCatalog::BLOB:
		{
			func.reset(new WF_udaf<string>(id, name, context));
			break;
		}
		default:
		{
			string errStr = name + "(" + colType2String[ct] + ")";
			errStr = IDBErrorInfo::instance()->errorMsg(ERR_WF_INVALID_PARM_TYPE, errStr);
			cerr << errStr << endl;
			throw IDBExcept(errStr, ERR_WF_INVALID_PARM_TYPE);

			break;
		}
	}

	// Get the UDAnF function object
	WF_udaf* wfUDAF = (WF_udaf*)func.get();
	mcsv1sdk::mcsv1Context& udafContext = wfUDAF->getContext();
	udafContext.setInterrupted(wfUDAF->getInterruptedPtr());
	wfUDAF->resetData();
	return func;
}

template<typename T>
WF_udaf<T>::WF_udaf(WF_udaf& rhs) : fUDAFContext(rhs.getContext()),
	bInterrupted(rhs.getInterrupted()),
	fDistinct(rhs.getDistinct())
{
	getContext().setInterrupted(getInterruptedPtr());
}

template<typename T>
WindowFunctionType* WF_udaf<T>::clone() const
{
	return new WF_udaf(*const_cast<WF_udaf*>(this));
}

template<typename T>
void WF_udaf<T>::resetData()
{
	getContext().getFunction()->reset(&getContext());
	fSet.clear();
	WindowFunctionType::resetData();
}

template<typename T>
void WF_udaf<T>::parseParms(const std::vector<execplan::SRCP>& parms)
{
    bRespectNulls = true;
	// parms[1]: respect null | ignore null
	ConstantColumn* cc = dynamic_cast<ConstantColumn*>(parms[1].get());
	idbassert(cc != NULL);
	bool isNull = false;  // dummy, harded coded
	bRespectNulls = (cc->getIntVal(fRow, isNull) > 0);
}

template<typename T>
bool WF_udaf<T>::dropValues(int64_t b, int64_t e)
{
	if (!bHasDropValue)
	{
		// Save work if we discovered dropValue is not implemented in the UDAnF
		return false;
	}

	mcsv1sdk::mcsv1_UDAF::ReturnCode rc;
	uint64_t colOut = fFieldIndex[0];
	uint64_t colIn = fFieldIndex[1];

	mcsv1sdk::ColumnDatum datum;
	datum.dataType = fRow.getColType(colIn);
	datum.scale = fRow.getScale(colIn);
	datum.precision = fRow.getPrecision(colOut);

	for (int64_t i = b; i < e; i++)
	{
		if (i % 1000 == 0 && fStep->cancelled())
			break;

		fRow.setData(getPointer(fRowData->at(i)));
		// Turn on NULL flags
		std::vector<uint32_t> flags;
		uint32_t flag = 0;
		if (fRow.isNullValue(colIn) == true)
		{
			if (!bRespectNulls)
			{
				continue;
			}
			flag |= mcsv1sdk::PARAM_IS_NULL;
		}
		flags.push_back(flag);
		getContext().setDataFlags(&flags);

		T valIn;
		getValue(colIn, valIn, &datum.dataType);

		// Check for distinct, if turned on.
		// TODO: when we impliment distinct, we need to revist this.
		if ((fDistinct) || (fSet.find(valIn) != fSet.end()))
		{
			continue;
		}

		datum.columnData = valIn;

		std::vector<mcsv1sdk::ColumnDatum> valsIn;
		valsIn.push_back(datum);

		rc = getContext().getFunction()->dropValue(&getContext(), valsIn);
		if (rc == mcsv1sdk::mcsv1_UDAF::NOT_IMPLEMENTED)
		{
			bHasDropValue = false;
			return false;
		}
		if (rc == mcsv1sdk::mcsv1_UDAF::ERROR)
		{
			bInterrupted = true;
			string errStr = IDBErrorInfo::instance()->errorMsg(ERR_WF_UDANF_ERROR, getContext().getErrorMessage());
			cerr << errStr << endl;
			throw IDBExcept(errStr, ERR_WF_UDANF_ERROR);
		}
	}

	return true;
}

// Sets the value from valOut into column colOut, performing any conversions.
template<typename T>
void WF_udaf<T>::SetUDAFValue(static_any::any& valOut, int64_t colOut,
							  int64_t b, int64_t e, int64_t c)
{
	static const static_any::any& charTypeId = (char)1;
	static const static_any::any& scharTypeId = (signed char)1;
	static const static_any::any& shortTypeId = (short)1;
	static const static_any::any& intTypeId = (int)1;
	static const static_any::any& longTypeId = (long)1;
	static const static_any::any& llTypeId = (long long)1;
	static const static_any::any& ucharTypeId = (unsigned char)1;
	static const static_any::any& ushortTypeId = (unsigned short)1;
	static const static_any::any& uintTypeId = (unsigned int)1;
	static const static_any::any& ulongTypeId = (unsigned long)1;
	static const static_any::any& ullTypeId = (unsigned long long)1;
	static const static_any::any& floatTypeId = (float)1;
	static const static_any::any& doubleTypeId = (double)1;
	static const std::string typeStr("");
	static const static_any::any& strTypeId = typeStr;

	CDT colDataType = fRow.getColType(colOut);
	if (valOut.empty())
	{
		// If valOut is empty, we return NULL
		T* pv = NULL;
		setValue(colDataType, b, e, c, pv);
		fPrev = c;
		return;
	}

	// This may seem a bit convoluted. Users shouldn't return a type
	// that they didn't set in mcsv1_UDAF::init(), but this
	// handles whatever return type is given and casts
	// it to whatever they said to return.
	int64_t intOut = 0;
	uint64_t uintOut = 0;
	float floatOut = 0.0;
	double doubleOut = 0.0;
	ostringstream oss;
	std::string strOut;

	if (valOut.compatible(charTypeId))
	{
		uintOut = intOut  = valOut.cast<char>();
		floatOut = intOut;
		oss << intOut;
	}
	else if (valOut.compatible(scharTypeId))
	{
		uintOut = intOut = valOut.cast<signed char>();
		floatOut = intOut;
		oss << intOut;
	}
	else if (valOut.compatible(shortTypeId))
	{
		uintOut = intOut = valOut.cast<short>();
		floatOut = intOut;
		oss << intOut;
	}
	else if (valOut.compatible(intTypeId))
	{
		uintOut = intOut = valOut.cast<int>();
		floatOut = intOut;
		oss << intOut;
	}
	else if (valOut.compatible(longTypeId))
	{
		uintOut = intOut = valOut.cast<long>();
		floatOut = intOut;
		oss << intOut;
	}
	else if (valOut.compatible(llTypeId))
	{
		uintOut = intOut = valOut.cast<long long>();
		floatOut = intOut;
		oss << intOut;
	}
	else if (valOut.compatible(ucharTypeId))
	{
		intOut = uintOut = valOut.cast<unsigned char>();
		floatOut = uintOut;
		oss << uintOut;
	}
	else if (valOut.compatible(ushortTypeId))
	{
		intOut = uintOut = valOut.cast<unsigned short>();
		floatOut = uintOut;
		oss << uintOut;
	}
	else if (valOut.compatible(uintTypeId))
	{
		intOut = uintOut = valOut.cast<unsigned int>();
		floatOut = uintOut;
		oss << uintOut;
	}
	else if (valOut.compatible(ulongTypeId))
	{
		intOut = uintOut = valOut.cast<unsigned long>();
		floatOut = uintOut;
		oss << uintOut;
	}
	else if (valOut.compatible(ullTypeId))
	{
		intOut = uintOut = valOut.cast<unsigned long long>();
		floatOut = uintOut;
		oss << uintOut;
	}
	else if (valOut.compatible(floatTypeId))
	{
		floatOut = valOut.cast<float>();
		doubleOut = floatOut;
		intOut = uintOut = floatOut;
		oss << floatOut;
	}
	else if (valOut.compatible(doubleTypeId))
	{
		doubleOut = valOut.cast<double>();
		floatOut = (float)doubleOut;
		uintOut = (uint64_t)doubleOut;
		intOut = (int64_t)doubleOut;
		oss << doubleOut;
	}

	if (valOut.compatible(strTypeId))
	{
		std::string strOut = valOut.cast<std::string>();
		// Convert the string to numeric type, just in case.
		intOut = atol(strOut.c_str());
		uintOut = strtoul(strOut.c_str(), NULL, 10);
		doubleOut = strtod(strOut.c_str(), NULL);
		floatOut = (float)doubleOut;
	}
	else
	{
		strOut = oss.str();
	}

	switch (colDataType)
	{
		case execplan::CalpontSystemCatalog::BIT:
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::DECIMAL:
		case execplan::CalpontSystemCatalog::UDECIMAL:
			setValue(colDataType, b, e, c, &intOut);
			break;
		case execplan::CalpontSystemCatalog::UTINYINT:
		case execplan::CalpontSystemCatalog::USMALLINT:
		case execplan::CalpontSystemCatalog::UMEDINT:
		case execplan::CalpontSystemCatalog::UINT:
		case execplan::CalpontSystemCatalog::UBIGINT:
		case execplan::CalpontSystemCatalog::DATE:
		case execplan::CalpontSystemCatalog::DATETIME:
			setValue(colDataType, b, e, c, &uintOut);
			break;
		case execplan::CalpontSystemCatalog::FLOAT:
		case execplan::CalpontSystemCatalog::UFLOAT:
			setValue(colDataType, b, e, c, &floatOut);
			break;
		case execplan::CalpontSystemCatalog::DOUBLE:
		case execplan::CalpontSystemCatalog::UDOUBLE:
			setValue(colDataType, b, e, c, &doubleOut);
			break;
		case execplan::CalpontSystemCatalog::CHAR:
		case execplan::CalpontSystemCatalog::VARCHAR:
		case execplan::CalpontSystemCatalog::TEXT:
		case execplan::CalpontSystemCatalog::VARBINARY:
		case execplan::CalpontSystemCatalog::CLOB:
		case execplan::CalpontSystemCatalog::BLOB:
			setValue(colDataType, b, e, c, &strOut);
			break;
		default:
		{
			std::ostringstream errmsg;
			errmsg << "WF_udaf: No logic for data type: " << colDataType;
			cerr << errmsg.str() << endl;
			throw runtime_error(errmsg.str().c_str());
			break;
		}
	}
}

template<typename T>
void WF_udaf<T>::operator()(int64_t b, int64_t e, int64_t c)
{
	mcsv1sdk::mcsv1_UDAF::ReturnCode rc;
	uint64_t colOut = fFieldIndex[0];

	if ((fFrameUnit == WF__FRAME_ROWS) ||
		(fPrev == -1) ||
		(!fPeer->operator()(getPointer(fRowData->at(c)), getPointer(fRowData->at(fPrev)))))
	{
		// for unbounded - current row special handling
		if (fPrev >= b && fPrev < c)
			b = c;
		else if (fPrev <= e && fPrev > c)
			e = c;

		uint64_t colIn = fFieldIndex[1];

		mcsv1sdk::ColumnDatum datum;
		datum.dataType = fRow.getColType(colIn);
		datum.scale = fRow.getScale(colIn);
		datum.precision = fRow.getPrecision(colOut);

		if (b<=c && c<=e)
			getContext().setContextFlag(mcsv1sdk::CONTEXT_HAS_CURRENT_ROW);
		else
			getContext().clearContextFlag(mcsv1sdk::CONTEXT_HAS_CURRENT_ROW);


		for (int64_t i = b; i <= e; i++)
		{
			if (i % 1000 == 0 && fStep->cancelled())
				break;

			fRow.setData(getPointer(fRowData->at(i)));
			// Turn on NULL flags
			std::vector<uint32_t> flags;
			uint32_t flag = 0;
			if (fRow.isNullValue(colIn) == true)
			{
				if (!bRespectNulls)
				{
					continue;
				}
				flag |= mcsv1sdk::PARAM_IS_NULL;
			}
			flags.push_back(flag);
			getContext().setDataFlags(&flags);

			T valIn;
			getValue(colIn, valIn, &datum.dataType);

			// Check for distinct, if turned on.
			if ((fDistinct) || (fSet.find(valIn) != fSet.end()))
			{
				continue;
			}

			if (fDistinct)
				fSet.insert(valIn);

			datum.columnData = valIn;

			std::vector<mcsv1sdk::ColumnDatum> valsIn;
			valsIn.push_back(datum);

			rc = getContext().getFunction()->nextValue(&getContext(), valsIn);
			if (rc == mcsv1sdk::mcsv1_UDAF::ERROR)
			{
				bInterrupted = true;
				string errStr = IDBErrorInfo::instance()->errorMsg(ERR_WF_UDANF_ERROR, getContext().getErrorMessage());
				cerr << errStr << endl;
				throw IDBExcept(errStr, ERR_WF_UDANF_ERROR);
			}
		}

		rc = getContext().getFunction()->evaluate(&getContext(), fValOut);
		if (rc == mcsv1sdk::mcsv1_UDAF::ERROR)
		{
			bInterrupted = true;
			string errStr = IDBErrorInfo::instance()->errorMsg(ERR_WF_UDANF_ERROR, getContext().getErrorMessage());
			cerr << errStr << endl;
			throw IDBExcept(errStr, ERR_WF_UDANF_ERROR);
		}
	}

	SetUDAFValue(fValOut, colOut, b, e, c);

	fPrev = c;
}

template
boost::shared_ptr<WindowFunctionType> WF_udaf<int64_t>::makeFunction(int id, const string& name, int ct, mcsv1sdk::mcsv1Context& context);

}   //namespace
// vim:ts=4 sw=4:

