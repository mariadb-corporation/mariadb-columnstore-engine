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
*   $Id: constantfilter.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
#include <exception>
#include <stdexcept>
#include <string>
#include <iostream>
#include <sstream>
using namespace std;

#include "constantfilter.h"
#include "returnedcolumn.h"
#include "operator.h"
#include "simplecolumn.h"
#include "simplefilter.h"
#include "bytestream.h"
#include "objectreader.h"
#include "aggregatecolumn.h"
#include "windowfunctioncolumn.h"

namespace
{
template<class T> struct deleter : public unary_function<T&, void>
{
    void operator()(T& x)
    {
        delete x;
    }
};
}

namespace execplan
{

/**
 * Constructors/Destructors
 */
ConstantFilter::ConstantFilter()
{}

ConstantFilter::ConstantFilter(const SOP& op, ReturnedColumn* lhs, ReturnedColumn* rhs)
{
    SSFP ssfp(new SimpleFilter(op, lhs, rhs));
    fFilterList.push_back(ssfp);
    SimpleColumn* sc = dynamic_cast<SimpleColumn*>(lhs);
    fCol.reset(sc->clone());
}

ConstantFilter::ConstantFilter(SimpleFilter* sf)
{
    SSFP ssfp(sf);
    fFilterList.push_back(ssfp);
    const SimpleColumn* sc = dynamic_cast<const SimpleColumn*>(sf->lhs());
    fCol.reset(sc->clone());
}

ConstantFilter::ConstantFilter(const ConstantFilter& rhs):
    fOp(rhs.fOp),
    fCol(rhs.fCol)
{
    fFilterList.clear();
    fSimpleColumnList.clear();
    fWindowFunctionColumnList.clear();
    SSFP ssfp;

    for (uint32_t i = 0; i < rhs.fFilterList.size(); i++)
    {
        ssfp.reset(rhs.fFilterList[i]->clone());
        fFilterList.push_back(ssfp);
        fSimpleColumnList.insert(fSimpleColumnList.end(),
                                 ssfp->simpleColumnList().begin(),
                                 ssfp->simpleColumnList().end());
        fAggColumnList.insert(fAggColumnList.end(),
                              ssfp->aggColumnList().begin(),
                              ssfp->aggColumnList().end());
        fWindowFunctionColumnList.insert(fWindowFunctionColumnList.end(),
                                         ssfp->windowfunctionColumnList().begin(),
                                         ssfp->windowfunctionColumnList().end());
    }
}

ConstantFilter::~ConstantFilter()
{
}

/**
 * Methods
 */

const string ConstantFilter::toString() const
{
    ostringstream output;
    output << "ConstantFilter" << endl;

    if (fOp) output << "  " << *fOp << endl;

    if (!fFunctionName.empty()) output << "  Func: " << fFunctionName << endl;

    if (fCol) output << "   " << *fCol << endl;

    for (unsigned int i = 0; i < fFilterList.size(); i++)
        output << "  " << *fFilterList[i] << endl;

    return output.str();
}

ostream& operator<<(ostream& output, const ConstantFilter& rhs)
{
    output << rhs.toString();
    return output;
}

bool ConstantFilter::hasAggregate()
{
    if (fAggColumnList.empty())
    {
        for (uint32_t i = 0; i < fFilterList.size(); i++)
        {
            if (fFilterList[i]->hasAggregate())
            {
                fAggColumnList.insert(fAggColumnList.end(),
                                      fFilterList[i]->aggColumnList().begin(),
                                      fFilterList[i]->aggColumnList().end());
            }
        }
    }

    if (!fAggColumnList.empty())
    {
        return true;
    }

    return false;
}

void ConstantFilter::serialize(messageqcpp::ByteStream& b) const
{
    FilterList::const_iterator it;
    b << static_cast<ObjectReader::id_t>(ObjectReader::CONSTANTFILTER);
    Filter::serialize(b);

    if (fOp != NULL)
        fOp->serialize(b);
    else
        b << static_cast<ObjectReader::id_t>(ObjectReader::NULL_CLASS);

    if (fCol != NULL)
        fCol->serialize(b);
    else
        b << static_cast<ObjectReader::id_t>(ObjectReader::NULL_CLASS);

    b << static_cast<uint32_t>(fFilterList.size());

    for (it = fFilterList.begin(); it != fFilterList.end(); it++)
        (*it)->serialize(b);

    b << fFunctionName;
}

void ConstantFilter::unserialize(messageqcpp::ByteStream& b)
{
    uint32_t size, i;
    ObjectReader::checkType(b, ObjectReader::CONSTANTFILTER);
    SimpleFilter* sf;

    Filter::unserialize(b);
    fOp.reset(dynamic_cast<Operator*>(ObjectReader::createTreeNode(b)));
    fCol.reset(dynamic_cast<ReturnedColumn*>(ObjectReader::createTreeNode(b)));
    b >> size;
    fFilterList.clear();
    fSimpleColumnList.clear();
    fAggColumnList.clear();
    fWindowFunctionColumnList.clear();

    for (i = 0; i < size; i++)
    {
        sf = dynamic_cast<SimpleFilter*>(ObjectReader::createTreeNode(b));
        SSFP ssfp(sf);
        fFilterList.push_back(ssfp);
        fSimpleColumnList.insert(fSimpleColumnList.end(),
                                 ssfp->simpleColumnList().begin(),
                                 ssfp->simpleColumnList().end());
        fAggColumnList.insert(fAggColumnList.end(),
                              ssfp->aggColumnList().begin(),
                              ssfp->aggColumnList().end());
        fWindowFunctionColumnList.insert(fWindowFunctionColumnList.end(),
                                         ssfp->windowfunctionColumnList().begin(),
                                         ssfp->windowfunctionColumnList().end());
    }

    b >> fFunctionName;
}

bool ConstantFilter::operator==(const ConstantFilter& t) const
{
    const Filter* f1, *f2;
    FilterList::const_iterator it, it2;

    f1 = static_cast<const Filter*>(this);
    f2 = static_cast<const Filter*>(&t);

    if (*f1 != *f2)
        return false;

    if (fOp != NULL)
    {
        if (*fOp != *t.fOp)
            return false;
    }
    else if (t.fOp != NULL)
        return false;

    //fFilterList
    if (fFilterList.size() != t.fFilterList.size())
        return false;

    for	(it = fFilterList.begin(), it2 = t.fFilterList.begin();
            it != fFilterList.end(); ++it, ++it2)
    {
        if (**it != **it2)
            return false;
    }

    return true;
}

bool ConstantFilter::operator==(const TreeNode* t) const
{
    const ConstantFilter* o;

    o = dynamic_cast<const ConstantFilter*>(t);

    if (o == NULL)
        return false;

    return *this == *o;
}

bool ConstantFilter::operator!=(const ConstantFilter& t) const
{
    return (!(*this == t));
}

bool ConstantFilter::operator!=(const TreeNode* t) const
{
    return (!(*this == t));
}

void ConstantFilter::setDerivedTable()
{
    if (fCol->hasAggregate())
    {
        fDerivedTable = "";
        return;
    }

    fDerivedTable = fCol->derivedTable();
}

void ConstantFilter::replaceRealCol(std::vector<SRCP>& derivedColList)
{
    ReturnedColumn* tmp = derivedColList[fCol->colPosition()]->clone();
    fCol.reset(tmp);

    for (unsigned i = 0; i < fFilterList.size(); i++)
        fFilterList[i]->replaceRealCol(derivedColList);
}

const std::vector<SimpleColumn*>& ConstantFilter::simpleColumnList()
{
    return fSimpleColumnList;
}

void ConstantFilter::setSimpleColumnList()
{
    fSimpleColumnList.clear();

    for (uint32_t i = 0; i < fFilterList.size(); i++)
    {
        fFilterList[i]->setSimpleColumnList();
        fSimpleColumnList.insert(fSimpleColumnList.end(),
                                 fFilterList[i]->simpleColumnList().begin(),
                                 fFilterList[i]->simpleColumnList().end());
    }
}


} // namespace execplan
// vim:ts=4 sw=4:
