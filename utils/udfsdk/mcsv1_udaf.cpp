/* Copyright (C) 2017 MariaDB Corporaton

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

#include <sstream>
#include <cstring>
#include <stdexcept>
#include "mcsv1_udaf.h"
#include "bytestream.h"
#include "objectreader.h"

using namespace mcsv1sdk;
/**
 * All UDA(n)F functions must be registered in the function map.
 * They will be picked up by the Columnstore modules during 
 * startup. 
 *  
 * This is a temporary kludge until we get the library loader 
 * task complete 
 */
UDAF_MAP UDAFMap::fm;
#include "allnull.h"
#include "ssq.h"
#include "median.h"
#include "avg_mode.h"
UDAF_MAP& UDAFMap::getMap()
{
	if (fm.size() > 0)
	{
		return fm;
	}
	// first: function name
	// second: Function pointer
	// please use lower case for the function name. Because the names might be 
	// case-insensitive in MySQL depending on the setting. In such case, 
	// the function names passed to the interface is always in lower case.
	fm["allnull"] = new allnull();
	fm["ssq"] = new ssq();
	fm["median"] = new median();
	fm["avg_mode"] = new avg_mode();
	
	return fm;
}

int32_t mcsv1Context::getColWidth()
{
	if (fColWidth > 0)
	{
		return fColWidth;
	}
	// JIT initialization for types that have a defined size.
	switch (fResultType)
	{
	case CalpontSystemCatalog::BIT:
	case CalpontSystemCatalog::TINYINT:
	case CalpontSystemCatalog::UTINYINT:
	case CalpontSystemCatalog::CHAR:
		fColWidth = 1;
		break;
	case CalpontSystemCatalog::SMALLINT:
	case CalpontSystemCatalog::USMALLINT:
		fColWidth = 2;
		break;
	case CalpontSystemCatalog::MEDINT:
	case CalpontSystemCatalog::INT:
	case CalpontSystemCatalog::UMEDINT:
	case CalpontSystemCatalog::UINT:
	case CalpontSystemCatalog::FLOAT:
	case CalpontSystemCatalog::UFLOAT:
	case CalpontSystemCatalog::DATE:
		fColWidth = 4;
		break;
	case CalpontSystemCatalog::BIGINT:
	case CalpontSystemCatalog::UBIGINT:
	case CalpontSystemCatalog::DECIMAL:
	case CalpontSystemCatalog::UDECIMAL:
	case CalpontSystemCatalog::DOUBLE:
	case CalpontSystemCatalog::UDOUBLE:
	case CalpontSystemCatalog::DATETIME:
	case CalpontSystemCatalog::STRINT:
		fColWidth = 8;
		break;
	case CalpontSystemCatalog::LONGDOUBLE:
		fColWidth = sizeof(long double);
		break;
	default:
		break;
	}
	return fColWidth;
}

bool mcsv1Context::operator==(const mcsv1Context& c) const
{
	// We don't test the per row data fields. They don't determine
	// if it's the same Context.
	if (getName()        != c.getName()
	 ||	fRunFlags        != c.fRunFlags
	 || fContextFlags    != c.fContextFlags
	 || fUserDataSize    != c.fUserDataSize
	 || fResultType      != c.fResultType
	 || fResultscale  != c.fResultscale
	 || fResultPrecision != c.fResultPrecision
	 || fStartFrame      != c.fStartFrame
	 || fEndFrame        != c.fEndFrame
	 || fStartConstant   != c.fStartConstant
	 || fEndConstant     != c.fEndConstant)
		return false;
	return true;
}

bool mcsv1Context::operator!=(const mcsv1Context& c) const
{
	return (!(*this == c));
}

const std::string mcsv1Context::toString() const
{
	std::ostringstream output;
	output << "mcsv1Context: " << getName() << std::endl;
	output << "  RunFlags=" << fRunFlags << " ContextFlags=" << fContextFlags << std::endl;
	output << "  UserDataSize=" << fUserDataSize << " ResultType=" << colDataTypeToString(fResultType) << std::endl;
	output << "  Resultscale=" << fResultscale << " ResultPrecision=" << fResultPrecision << std::endl;
	output << "  ErrorMsg=" << errorMsg << std::endl;
	output << "  bInterrupted=" << bInterrupted << std::endl;
	output << "  StartFrame=" << fStartFrame << " EndFrame=" << fEndFrame << std::endl;
	output << "  StartConstant=" << fStartConstant << " EndConstant=" << fEndConstant << std::endl;
	return output.str();
}

mcsv1sdk::mcsv1_UDAF* mcsv1Context::getFunction()
{
	if (func)
	{
		return func;
	}

	// Just in time initialization
	if (functionName.length() == 0)
	{
		std::ostringstream errmsg;
		errmsg << "mcsv1Context::getFunction: " << functionName << " is empty";
		throw std::logic_error(errmsg.str());
	}
	mcsv1sdk::UDAF_MAP::iterator funcIter = mcsv1sdk::UDAFMap::getMap().find(functionName);
	if (funcIter == mcsv1sdk::UDAFMap::getMap().end())
	{
		 std::ostringstream errmsg;
		 errmsg << "mcsv1Context::getFunction: " << functionName << " is undefined";
		 throw std::logic_error(errmsg.str());
	}
	func = funcIter->second;
	return func;
}

mcsv1sdk::mcsv1_UDAF* mcsv1Context::getFunction() const
{
	return const_cast<mcsv1Context*>(this)->getFunction();
}

void mcsv1Context::createUserData() 
{
	// Try the function. If not implemented, create a byte array.
	UserData* userData = NULL;
	mcsv1_UDAF::ReturnCode rc = getFunction()->createUserData(userData, fUserDataSize);
	if (rc == mcsv1_UDAF::ERROR)
	{
		std::ostringstream errmsg;
		errmsg << "mcsv1Context::createUserData: " << functionName << errorMsg.c_str();
		throw std::logic_error(errmsg.str());
	}
	setUserData(userData);
}

void mcsv1Context::serialize(messageqcpp::ByteStream& b) const
{
	b.needAtLeast(sizeof(mcsv1Context));
	b << (ObjectReader::id_t) ObjectReader::MCSV1_CONTEXT;
	b << functionName;
	b << fRunFlags;
	// Dont send context flags, These are set for each call
	b << fUserDataSize;
	b << (uint32_t)fResultType;
	b << fResultscale;
	b << fResultPrecision;
	b << errorMsg;
	// Don't send dataflags. These are set for each call
	// bInterrupted is set internally.
	b << (uint32_t)fStartFrame;
	b << (uint32_t)fEndFrame;
	b << fStartConstant;
	b << fEndConstant;
}

void mcsv1Context::unserialize(messageqcpp::ByteStream& b)
{
	ObjectReader::checkType(b, ObjectReader::MCSV1_CONTEXT);
	b >> functionName;
	b >> fRunFlags;
	b >> fUserDataSize;
	uint32_t iResultType;
	b >> iResultType;
	fResultType = (CalpontSystemCatalog::ColDataType)iResultType;
	b >> fResultscale;
	b >> fResultPrecision;
	b >> errorMsg;
	uint32_t frame;
	b >> frame;
	fStartFrame = (WF_FRAME)frame;
	b >> frame;
	fEndFrame = (WF_FRAME)frame;
	b >> fStartConstant;
	b >> fEndConstant;
}

void UserData::serialize(messageqcpp::ByteStream& bs) const
{
	bs << size;
	bs.append(data, size);
}

void UserData::unserialize(messageqcpp::ByteStream& bs)
{
	bs >> size;
	memcpy(data, bs.buf(), size);
	bs.advance(size);
}

const std::string typeStr("");
const static_any::any& mcsv1_UDAF::charTypeId((char)1);
const static_any::any& mcsv1_UDAF::scharTypeId((signed char)1);
const static_any::any& mcsv1_UDAF::shortTypeId((short)1);
const static_any::any& mcsv1_UDAF::intTypeId((int)1);
const static_any::any& mcsv1_UDAF::longTypeId((long)1);
const static_any::any& mcsv1_UDAF::llTypeId((long long)1);
const static_any::any& mcsv1_UDAF::ucharTypeId((unsigned char)1);
const static_any::any& mcsv1_UDAF::ushortTypeId((unsigned short)1);
const static_any::any& mcsv1_UDAF::uintTypeId((unsigned int)1);
const static_any::any& mcsv1_UDAF::ulongTypeId((unsigned long)1);
const static_any::any& mcsv1_UDAF::ullTypeId((unsigned long long)1);
const static_any::any& mcsv1_UDAF::floatTypeId((float)1);
const static_any::any& mcsv1_UDAF::doubleTypeId((double)1);
const static_any::any& mcsv1_UDAF::strTypeId(typeStr);


