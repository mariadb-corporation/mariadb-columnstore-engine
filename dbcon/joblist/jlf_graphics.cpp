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

// $Id: jlf_graphics.cpp 9550 2013-05-17 23:58:07Z xlou $

// Cross engine at the top due to MySQL includes
#define PREFER_MY_CONFIG_H
#include "crossenginestep.h"
#include <iostream>
using namespace std;

#include "joblist.h"
#include "primitivestep.h"
#include "subquerystep.h"
#include "windowfunctionstep.h"
#include "tupleaggregatestep.h"
#include "tupleannexstep.h"
#include "tuplehashjoin.h"
#include "tupleunion.h"
using namespace joblist;

#include "jlf_graphics.h"

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wpotentially-evaluated-expression"
// for warnings on typeid :expression with side effects will be evaluated despite being used as an operand to
// 'typeid'
#endif

namespace jlf_graphics
{
ostream& writeDotCmds(ostream& dotFile, const JobStepVector& query, const JobStepVector& project)
{
  // Graphic view draw
  dotFile << "digraph G {" << endl;
  JobStepVector::iterator qsi;
  JobStepVector::iterator psi;
  int ctn = 0;

  // merge in the subquery steps
  JobStepVector querySteps = query;
  JobStepVector projectSteps = project;
  {
    SubQueryStep* subquery = NULL;
    qsi = querySteps.begin();

    while (qsi != querySteps.end())
    {
      if ((subquery = dynamic_cast<SubQueryStep*>(qsi->get())) != NULL)
      {
        querySteps.erase(qsi);
        JobStepVector subSteps = subquery->subJoblist()->querySteps();
        querySteps.insert(querySteps.end(), subSteps.begin(), subSteps.end());
        qsi = querySteps.begin();
      }
      else
      {
        qsi++;
      }
    }
  }

  for (qsi = querySteps.begin(); qsi != querySteps.end(); ctn++, qsi++)
  {
    //		if (dynamic_cast<OrDelimiter*>(qsi->get()) != NULL)
    //			continue;

    uint16_t stepidIn = qsi->get()->stepId();
    dotFile << stepidIn << " [label=\"st_" << stepidIn << " ";

    if (typeid(*(qsi->get())) == typeid(pColStep))
    {
      dotFile << "(" << qsi->get()->tableOid() << "/" << qsi->get()->oid() << ")"
              << "\"";
      dotFile << " shape=ellipse";
    }
    else if (typeid(*(qsi->get())) == typeid(pColScanStep))
    {
      dotFile << "(" << qsi->get()->tableOid() << "/" << qsi->get()->oid() << ")"
              << "\"";
      dotFile << " shape=box";
    }
    //		else if (typeid(*(qsi->get())) == typeid(HashJoinStep) ||
    //				 typeid(*(qsi->get())) == typeid(StringHashJoinStep))
    //		{
    //			dotFile << "\"";
    //			dotFile << " shape=diamond";
    //		}
    else if (typeid(*(qsi->get())) == typeid(TupleHashJoinStep))
    {
      dotFile << "\"";
      dotFile << " shape=diamond peripheries=2";
    }
    //		else if (typeid(*(qsi->get())) == typeid(UnionStep) ||
    //				 typeid(*(qsi->get())) == typeid(TupleUnion) )
    else if (typeid(*(qsi->get())) == typeid(TupleUnion))
    {
      dotFile << "\"";
      dotFile << " shape=triangle";
    }
    else if (typeid(*(qsi->get())) == typeid(pDictionaryStep))
    {
      dotFile << "\"";
      dotFile << " shape=trapezium";
    }
    else if (typeid(*(qsi->get())) == typeid(FilterStep))
    {
      dotFile << "\"";
      dotFile << " shape=house orientation=180";
    }
    //		else if (typeid(*(qsi->get())) == typeid(ReduceStep))
    //		{
    //			dotFile << "\"";
    //			dotFile << " shape=triangle orientation=180";
    //		}
    //		else if (typeid(*(qsi->get())) == typeid(BatchPrimitiveStep) || typeid(*(qsi->get())) ==
    //typeid(TupleBPS))
    else if (typeid(*(qsi->get())) == typeid(TupleBPS))
    {
      bool isTuple = false;
      BatchPrimitive* bps = dynamic_cast<BatchPrimitive*>(qsi->get());

      if (dynamic_cast<TupleBPS*>(bps) != 0)
        isTuple = true;

      dotFile << "(" << bps->tableOid() << "/" << bps->oid();
      OIDVector projectOids = bps->getProjectOids();

      if (projectOids.size() > 0)
      {
        dotFile << "\\l";
        dotFile << "PC: ";
      }

      for (unsigned int i = 0; i < projectOids.size(); i++)
      {
        dotFile << projectOids[i] << " ";

        if ((i + 1) % 3 == 0)
          dotFile << "\\l";
      }

      dotFile << ")\"";
      dotFile << " shape=box style=bold";

      if (isTuple)
        dotFile << " peripheries=2";
    }
    else if (typeid(*(qsi->get())) == typeid(CrossEngineStep))
    {
      BatchPrimitive* bps = dynamic_cast<BatchPrimitive*>(qsi->get());
      dotFile << "(" << bps->alias() << ")\"";
      dotFile << " shape=box style=bold";
      dotFile << " peripheries=2";
    }
    else if (typeid(*(qsi->get())) == typeid(TupleAggregateStep))
    {
      dotFile << "\"";
      dotFile << " shape=triangle orientation=180";
    }
    else if (typeid(*(qsi->get())) == typeid(TupleAnnexStep))
    {
      dotFile << "\"";
      dotFile << " shape=star";
    }
    else if (typeid(*(qsi->get())) == typeid(WindowFunctionStep))
    {
      dotFile << "\"";
      dotFile << " shape=triangle orientation=180";
      dotFile << " peripheries=2";
    }
    //		else if (typeid(*(qsi->get())) == typeid(AggregateFilterStep))
    //		{
    //			dotFile << "\"";
    //			dotFile << " shape=hexagon peripheries=2 style=bold";
    //		}
    //		else if (typeid(*(qsi->get())) == typeid(BucketReuseStep))
    //		{
    //			dotFile << "(" << qsi->get()->tableOid() << "/" << qsi->get()->oid() << ")" << "\"";
    //			dotFile << " shape=box style=dashed";
    //		}
    else
      dotFile << "\"";

    dotFile << "]" << endl;

    for (unsigned int i = 0; i < qsi->get()->outputAssociation().outSize(); i++)
    {
      RowGroupDL* dlout = qsi->get()->outputAssociation().outAt(i)->rowGroupDL();
      ptrdiff_t dloutptr = (ptrdiff_t)dlout;

      for (unsigned int k = 0; k < querySteps.size(); k++)
      {
        uint16_t stepidOut = querySteps[k].get()->stepId();
        JobStepAssociation queryInputSA = querySteps[k].get()->inputAssociation();

        for (unsigned int j = 0; j < queryInputSA.outSize(); j++)
        {
          RowGroupDL* dlin = queryInputSA.outAt(j)->rowGroupDL();
          ptrdiff_t dlinptr = (ptrdiff_t)dlin;;

          if ((ptrdiff_t)dloutptr == (ptrdiff_t)dlinptr)
          {
            dotFile << stepidIn << " -> " << stepidOut;
          }
        }
      }

      for (psi = projectSteps.begin(); psi < projectSteps.end(); psi++)
      {
        uint16_t stepidOut = psi->get()->stepId();
        JobStepAssociation projectInputSA = psi->get()->inputAssociation();

        for (unsigned int j = 0; j < projectInputSA.outSize(); j++)
        {
          RowGroupDL* dlin = projectInputSA.outAt(j)->rowGroupDL();
          ptrdiff_t dlinptr = (ptrdiff_t)dlin;;

          if (dloutptr == dlinptr)
          {
            dotFile << stepidIn << " -> " << stepidOut;
          }
        }
      }
    }
  }

  for (psi = projectSteps.begin(), ctn = 0; psi != projectSteps.end(); ctn++, psi++)
  {
    uint16_t stepidIn = psi->get()->stepId();
    dotFile << stepidIn << " [label=\"st_" << stepidIn << " ";

    if (typeid(*(psi->get())) == typeid(pColStep))
    {
      dotFile << "(" << psi->get()->tableOid() << "/" << psi->get()->oid() << ")"
              << "\"";
      dotFile << " shape=ellipse";
    }
    else if (typeid(*(psi->get())) == typeid(pColScanStep))
    {
      dotFile << "(" << psi->get()->tableOid() << "/" << psi->get()->oid() << ")"
              << "\"";
      dotFile << " shape=box";
    }
    else if (typeid(*(psi->get())) == typeid(pDictionaryStep))
    {
      dotFile << "\"";
      dotFile << " shape=trapezium";
    }
    else if (typeid(*(psi->get())) == typeid(PassThruStep))
    {
      dotFile << "(" << psi->get()->tableOid() << "/" << psi->get()->oid() << ")"
              << "\"";
      dotFile << " shape=octagon";
    }
    //		else if (typeid(*(psi->get())) == typeid(BatchPrimitiveStep) || typeid(*(psi->get())) ==
    //typeid(TupleBPS))
    else if (typeid(*(psi->get())) == typeid(TupleBPS))
    {
      bool isTuple = false;
      BatchPrimitive* bps = dynamic_cast<BatchPrimitive*>(psi->get());

      if (dynamic_cast<TupleBPS*>(bps) != 0)
        isTuple = true;

      dotFile << "(" << bps->tableOid() << ":\\l";
      OIDVector projectOids = bps->getProjectOids();

      for (unsigned int i = 0; i < projectOids.size(); i++)
      {
        dotFile << projectOids[i] << " ";

        if ((i + 1) % 3 == 0)
          dotFile << "\\l";
      }

      dotFile << ")\"";
      dotFile << " shape=box style=bold";

      if (isTuple)
        dotFile << " peripheries=2";
    }
    else if (typeid(*(qsi->get())) == typeid(CrossEngineStep))
    {
      BatchPrimitive* bps = dynamic_cast<BatchPrimitive*>(qsi->get());
      dotFile << "(" << bps->alias() << ")\"";
      dotFile << " shape=box style=bold";
      dotFile << " peripheries=2";
    }
    else
      dotFile << "\"";

    dotFile << "]" << endl;

    for (unsigned int i = 0; i < psi->get()->outputAssociation().outSize(); i++)
    {
      RowGroupDL* dlout = psi->get()->outputAssociation().outAt(i)->rowGroupDL();
      ptrdiff_t dloutptr = (ptrdiff_t)dlout;

      for (unsigned int k = ctn + 1; k < projectSteps.size(); k++)
      {
        uint16_t stepidOut = projectSteps[k].get()->stepId();
        JobStepAssociation projectInputSA = projectSteps[k].get()->inputAssociation();

        for (unsigned int j = 0; j < projectInputSA.outSize(); j++)
        {
          RowGroupDL* dlin = projectInputSA.outAt(j)->rowGroupDL();
          ptrdiff_t dlinptr = (ptrdiff_t)dlin;

          if ((ptrdiff_t)dloutptr == (ptrdiff_t)dlinptr)
          {
            dotFile << stepidIn << " -> " << stepidOut;

          }
        }
      }
    }
  }

  dotFile << "}" << endl;

  return dotFile;
}

}  // end namespace jlf_graphics


#ifdef __clang__
#pragma clang diagnostic pop
#endif
