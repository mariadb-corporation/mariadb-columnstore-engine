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
*   $Id: predicateoperator.cpp 9306 2013-03-12 15:49:11Z rdempsey $
*
*
***********************************************************************/
#include <iostream>

#include "bytestream.h"
#include "predicateoperator.h"
#include "objectreader.h"

#include "liboamcpp.h"
using namespace oam;

using namespace std;

bool futf8 = true;

namespace {
    
/**@brief util struct for converting string to lower case */
struct to_lower
{
    char operator() (char c) const { return tolower(c); }
};

//Trim any leading/trailing ws
const string lrtrim(const string& in)
{
	string::size_type p1;
	p1 = in.find_first_not_of(" \t\n");
	if (p1 == string::npos) p1 = 0;
	string::size_type p2;
	p2 = in.find_last_not_of(" \t\n");
	if (p2 == string::npos) p2 = in.size() - 1;
	return string(in, p1, (p2 - p1 + 1));
}

}

namespace execplan {
    
/**
 * Constructors/Destructors
 */
PredicateOperator::PredicateOperator()
{
	Oam oam;
    // get and set locale language    
	string systemLang = "C";

	try{
        oam.getSystemConfig("SystemLang", systemLang);
    }
    catch(...)
    {}

    if ( systemLang != "en_US.UTF-8" &&
        systemLang.find("UTF") != string::npos )
        futf8 = true;
}

PredicateOperator::PredicateOperator(const string& operatorName)
{
	Oam oam;
    // get and set locale language    
	string systemLang = "C";

	try{
        oam.getSystemConfig("SystemLang", systemLang);
    }
    catch(...)
    {}

    if ( systemLang != "en_US.UTF-8" &&
        systemLang.find("UTF") != string::npos )
        futf8 = true;

	data(operatorName);
}					

PredicateOperator::PredicateOperator(const PredicateOperator& rhs) : Operator(rhs)
{
	Oam oam;
    // get and set locale language    
	string systemLang = "C";

	try{
        oam.getSystemConfig("SystemLang", systemLang);
    }
    catch(...)
    {}

    if ( systemLang != "en_US.UTF-8" &&
        systemLang.find("UTF") != string::npos )
        futf8 = true;

	data(rhs.data());
}

PredicateOperator:: ~PredicateOperator()
{
}

/**
 * Operations
 */

/**
 * friend function
 */
ostream& operator<<(ostream &output, const PredicateOperator& rhs) 
{
	output << rhs.toString() << endl;
	output << "OpType=" << rhs.operationType().colDataType << endl;
	return output;
} 

/**
 * The serialization interface
 */
void PredicateOperator::serialize(messageqcpp::ByteStream& b) const
{
	b << (ObjectReader::id_t) ObjectReader::PREDICATEOPERATOR;
	//b << fData;
	Operator::serialize(b);
}

void PredicateOperator::unserialize(messageqcpp::ByteStream& b)
{
	ObjectReader::checkType(b, ObjectReader::PREDICATEOPERATOR);
	//b >> fData;
	Operator::unserialize(b);
}

bool PredicateOperator::operator==(const PredicateOperator& t) const
{
	if (data() == t.data())
		return true;
	return false;
}

bool PredicateOperator::operator==(const TreeNode* t) const
{
	const PredicateOperator *o;

	o = dynamic_cast<const PredicateOperator*>(t);
	if (o == NULL)
		return false;
	return *this == *o;
}

bool PredicateOperator::operator!=(const PredicateOperator& t) const
{
	return (!(*this == t));
}

bool PredicateOperator::operator!=(const TreeNode* t) const
{
	return (!(*this == t));
}

//FIXME: VARBINARY???
void PredicateOperator::setOpType(Type& l, Type& r)
{
	if ( l.colDataType == execplan::CalpontSystemCatalog::DATETIME ||
		   l.colDataType == execplan::CalpontSystemCatalog::DATE )
	{
		switch (r.colDataType)
		{
			case execplan::CalpontSystemCatalog::CHAR:
			case execplan::CalpontSystemCatalog::VARCHAR:	
				fOperationType = l;
				break;
			case execplan::CalpontSystemCatalog::DATETIME:
				fOperationType.colDataType = execplan::CalpontSystemCatalog::DATETIME;
				fOperationType.colWidth = 8;
				break;
			case execplan::CalpontSystemCatalog::DATE:
				fOperationType = l;
				break;
			default:
				fOperationType.colDataType = execplan::CalpontSystemCatalog::DOUBLE;
				fOperationType.colWidth = 8;
				break;
		} 
	}
	else if ( r.colDataType == execplan::CalpontSystemCatalog::DATETIME ||
		   r.colDataType == execplan::CalpontSystemCatalog::DATE )
	{
		switch (l.colDataType)
		{
			case execplan::CalpontSystemCatalog::CHAR:
			case execplan::CalpontSystemCatalog::VARCHAR:	
				fOperationType.colDataType = execplan::CalpontSystemCatalog::VARCHAR;
				fOperationType.colWidth = 255;
				break;
			case execplan::CalpontSystemCatalog::DATETIME:
				fOperationType.colDataType = execplan::CalpontSystemCatalog::DATETIME;
				fOperationType.colWidth = 8;
				break;
			case execplan::CalpontSystemCatalog::DATE:
				fOperationType = r;
				break;
			default:
				fOperationType.colDataType = execplan::CalpontSystemCatalog::DOUBLE;
				fOperationType.colWidth = 8;
				break;
		} 
	}
	else if (l.colDataType == execplan::CalpontSystemCatalog::DECIMAL ||
             l.colDataType == execplan::CalpontSystemCatalog::UDECIMAL)
	{
		switch (r.colDataType)
		{			
			case execplan::CalpontSystemCatalog::DECIMAL:
            case execplan::CalpontSystemCatalog::UDECIMAL:
			{
				// should following the result type that MySQL gives
				fOperationType = l;
				fOperationType.scale = (l.scale > r.scale ? l.scale : r.scale);
				break;
			}
			case execplan::CalpontSystemCatalog::INT:
			case execplan::CalpontSystemCatalog::MEDINT:
			case execplan::CalpontSystemCatalog::TINYINT:
			case execplan::CalpontSystemCatalog::BIGINT:
            case execplan::CalpontSystemCatalog::UINT:
            case execplan::CalpontSystemCatalog::UMEDINT:
            case execplan::CalpontSystemCatalog::UTINYINT:
            case execplan::CalpontSystemCatalog::UBIGINT:
				fOperationType.colDataType = execplan::CalpontSystemCatalog::DECIMAL;
				fOperationType.scale = l.scale;
				fOperationType.colWidth = 8;
				break;
			default:
				fOperationType.colDataType = execplan::CalpontSystemCatalog::DOUBLE;
				fOperationType.colWidth = 8;							
		}
	}
	else if (r.colDataType == execplan::CalpontSystemCatalog::DECIMAL ||
             r.colDataType == execplan::CalpontSystemCatalog::UDECIMAL)
	{
		switch (l.colDataType)
		{
			case execplan::CalpontSystemCatalog::DECIMAL:
            case execplan::CalpontSystemCatalog::UDECIMAL:
			{
				// should following the result type that MySQL gives based on the following logic?
				// @NOTE is this trustable?
				fOperationType = fResultType;
				break;
			}
			case execplan::CalpontSystemCatalog::INT:
			case execplan::CalpontSystemCatalog::MEDINT:
			case execplan::CalpontSystemCatalog::TINYINT:
			case execplan::CalpontSystemCatalog::BIGINT:
            case execplan::CalpontSystemCatalog::UINT:
            case execplan::CalpontSystemCatalog::UMEDINT:
            case execplan::CalpontSystemCatalog::UTINYINT:
            case execplan::CalpontSystemCatalog::UBIGINT:
				fOperationType.colDataType = execplan::CalpontSystemCatalog::DECIMAL;
				fOperationType.scale = r.scale;
				fOperationType.colWidth = 8;
				break;
			default:
				fOperationType.colDataType = execplan::CalpontSystemCatalog::DOUBLE;			
				fOperationType.colWidth = 8;				
		}
	}
    // If both sides are unsigned, use UBIGINT as result type, otherwise
    // "promote" to BIGINT.
    else if (isUnsigned(l.colDataType) && isUnsigned(r.colDataType))
    {
        fOperationType.colDataType = execplan::CalpontSystemCatalog::UBIGINT;
        fOperationType.colWidth = 8;
    }
    else if ((isSignedInteger(l.colDataType) && isUnsigned(r.colDataType)) ||
             (isUnsigned(l.colDataType) && isSignedInteger(r.colDataType)) ||
             (isSignedInteger(l.colDataType) && isSignedInteger(r.colDataType)))
    {
        fOperationType.colDataType = execplan::CalpontSystemCatalog::BIGINT;
        fOperationType.colWidth = 8;
    }
	else if ((l.colDataType == execplan::CalpontSystemCatalog::CHAR ||
		       l.colDataType == execplan::CalpontSystemCatalog::VARCHAR ||
               l.colDataType == execplan::CalpontSystemCatalog::TEXT) &&
		       (r.colDataType == execplan::CalpontSystemCatalog::CHAR ||
		       r.colDataType == execplan::CalpontSystemCatalog::VARCHAR ||
               r.colDataType == execplan::CalpontSystemCatalog::TEXT))
	{
		if ( ( (l.colDataType == execplan::CalpontSystemCatalog::CHAR && l.colWidth <= 8) ||
			  (l.colDataType == execplan::CalpontSystemCatalog::VARCHAR && l.colWidth < 8) ) &&
			  ( (r.colDataType == execplan::CalpontSystemCatalog::CHAR && r.colWidth <= 8) ||
			  (r.colDataType == execplan::CalpontSystemCatalog::VARCHAR && r.colWidth < 8) ) )
		{
			if ( futf8 )
			{
				fOperationType.colDataType = execplan::CalpontSystemCatalog::VARCHAR;
				fOperationType.colWidth = 255;
			}
			else
			{
				fOperationType.colDataType = execplan::CalpontSystemCatalog::BIGINT;
				fOperationType.scale = 0;
				fOperationType.colWidth = 8;

				// @bug3532, char[] as network order int for fast comparison.
				l.colDataType = execplan::CalpontSystemCatalog::STRINT;
				r.colDataType = execplan::CalpontSystemCatalog::STRINT;
			}
		}
		else
		{
			fOperationType.colDataType = execplan::CalpontSystemCatalog::VARCHAR;
			fOperationType.colWidth = 255;
		}
	}
	else
	{
		fOperationType.colDataType = execplan::CalpontSystemCatalog::DOUBLE;	
		fOperationType.colWidth = 8;
	}
}

}  // namespace
