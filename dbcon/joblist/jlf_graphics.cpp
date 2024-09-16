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

#include <cstddef>
#include <iterator>
#include <sstream>

#include "joblist.h"
#include "jobstep.h"
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
std::string generateDotFileName(const std::string& prefix)
{
  ostringstream oss;
  struct timeval tvbuf;
  gettimeofday(&tvbuf, 0);
  struct tm tmbuf;
  localtime_r(reinterpret_cast<time_t*>(&tvbuf.tv_sec), &tmbuf);
  oss << prefix << setfill('0') << setw(4) << (tmbuf.tm_year + 1900) << setw(2) << (tmbuf.tm_mon + 1)
      << setw(2) << (tmbuf.tm_mday) << setw(2) << (tmbuf.tm_hour) << setw(2) << (tmbuf.tm_min) << setw(2)
      << (tmbuf.tm_sec) << setw(6) << (tvbuf.tv_usec) << ".dot";
  return oss.str();
}

JobStepVector GraphGeneratorInterface::extractSubquerySteps(const SJSTEP& sqs)
{
  JobStepVector res;
  auto* subQueryStep = dynamic_cast<SubQueryStep*>(sqs.get());

  if (subQueryStep)
  {
    JobStepVector subQuerySteps;
    auto& stepsBeforeRecursion = subQueryStep->subJoblist()->querySteps();
    for (auto& step : stepsBeforeRecursion)
    {
      auto steps = extractJobSteps(step);
      subQuerySteps.insert(subQuerySteps.end(), steps.begin(), steps.end());
    }
    res.insert(res.end(), subQuerySteps.begin(), subQuerySteps.end());
  }
  res.push_back(sqs);
  return res;
}

// This f() is recursive to handle nested subqueries
JobStepVector GraphGeneratorInterface::extractJobSteps(const SJSTEP& umbrella)
{
  JobStepVector res;
  if (typeid(*umbrella) == typeid(SubAdapterStep))
  {
    auto* subAdapterStep = dynamic_cast<SubAdapterStep*>(umbrella.get());
    assert(subAdapterStep);
    auto subQuerySteps = extractSubquerySteps(subAdapterStep->subStep());
    res.insert(res.end(), subQuerySteps.begin(), subQuerySteps.end());
    res.push_back(umbrella);
  }

  else if (typeid(*umbrella) == typeid(SubQueryStep))
  {
    auto subQuerySteps = extractSubquerySteps(umbrella);
    res.insert(res.end(), subQuerySteps.begin(), subQuerySteps.end());
  }
  else
  {
    res.push_back(umbrella);
  }
  return res;
}

std::string getLadderRepr(const JobStepVector& steps, const std::vector<size_t>& tabsToPretify)
{
  std::ostringstream oss;
  assert(tabsToPretify.size() == steps.size());
  // Tabs are in the reverse order of the steps
  auto tabsIt = tabsToPretify.begin();
  // Reverse the order of the steps to draw the graph from top to bottom
  for (auto s = steps.rbegin(); s != steps.rend(); ++s, ++tabsIt)
  {
    oss << std::string(*tabsIt, '\t');
    oss << (*s)->extendedInfo() << std::endl;
  }
  return oss.str();
}

std::string GraphGeneratorInterface::getGraphNode(const SJSTEP& stepPtr)
{
  auto& step = *stepPtr;
  uint16_t stepidIn = step.stepId();
  std::ostringstream oss;
  oss << stepidIn << " [label=\"st_" << stepidIn << " ";

  if (typeid(step) == typeid(pColStep))
  {
    oss << "(" << step.tableOid() << "/" << step.oid() << ")" << "\"";
    oss << " shape=ellipse";
  }
  else if (typeid(step) == typeid(pColScanStep))
  {
    oss << "(" << step.tableOid() << "/" << step.oid() << ")" << "\"";
    oss << " shape=box";
  }
  else if (typeid(step) == typeid(TupleBPS))
  {
    bool isTuple = false;
    BatchPrimitive* bps = dynamic_cast<BatchPrimitive*>(stepPtr.get());

    if (dynamic_cast<TupleBPS*>(bps) != 0)
      isTuple = true;

    oss << "(" << bps->tableOid() << "/" << bps->oid() << "/" << bps->alias();
    OIDVector projectOids = bps->getProjectOids();

    if (projectOids.size() > 0)
    {
      oss << "\\l";
      oss << "PC: ";
    }

    for (unsigned int i = 0; i < projectOids.size(); i++)
    {
      oss << projectOids[i] << " ";

      if ((i + 1) % 3 == 0)
        oss << "\\l";
    }

    oss << ")\"";
    oss << " shape=box style=bold";

    if (isTuple)
      oss << " peripheries=2";
  }
  else if (typeid(step) == typeid(CrossEngineStep))
  {
    CrossEngineStep* cej = dynamic_cast<CrossEngineStep*>(stepPtr.get());
    oss << "(" << cej->tableName() << "/" << cej->tableAlias() << ")\"";
    oss << " shape=cylinder style=bold";
  }
  else if (typeid(step) == typeid(TupleHashJoinStep))
  {
    oss << "\"";
    oss << " shape=diamond peripheries=2";
  }
  else if (typeid(step) == typeid(TupleUnion))
  {
    oss << "\"";
    oss << " shape=triangle";
  }
  else if (typeid(step) == typeid(pDictionaryStep))
  {
    oss << "\"";
    oss << " shape=trapezium";
  }
  else if (typeid(step) == typeid(FilterStep))
  {
    oss << "\"";
    oss << " shape=invhouse";
  }

  else if (typeid(step) == typeid(TupleAggregateStep))
  {
    oss << "\"";
    oss << " shape=invtriangle";
  }
  else if (typeid(step) == typeid(TupleAnnexStep))
  {
    oss << "\"";
    oss << " shape=star";
  }
  else if (typeid(step) == typeid(WindowFunctionStep))
  {
    oss << "\"";
    oss << " shape=invtriangle";
    oss << " peripheries=2";
  }
  else if (typeid(step) == typeid(SubAdapterStep))
  {
    oss << "\"";
    oss << " shape=polygon";
    oss << " peripheries=2";
  }
  else if (typeid(step) == typeid(SubQueryStep))
  {
    oss << "\"";
    oss << " shape=polygon";
  }
  else
  {
    oss << "\"";
  }

  oss << "]" << endl;
  return oss.str();
}

std::pair<size_t, std::string> GraphGeneratorInterface::getTabsAndEdges(
    const JobStepVector& querySteps, const JobStepVector& projectSteps, const SJSTEP& stepPtr,
    const std::vector<size_t>& tabsToPretify)
{
  auto& step = *stepPtr;
  uint16_t stepidIn = step.stepId();
  std::ostringstream oss;
  size_t tab = 0;

  for (unsigned int i = 0; i < step.outputAssociation().outSize(); i++)
  {
    ptrdiff_t dloutptr = 0;
    auto* dlout = step.outputAssociation().outAt(i)->rowGroupDL();
    uint32_t numConsumers = step.outputAssociation().outAt(i)->getNumConsumers();

    if (dlout)
    {
      dloutptr = (ptrdiff_t)dlout;
    }

    for (auto it = querySteps.rbegin(); it != querySteps.rend(); ++it)
    {
      auto& otherStep = *it;
      // Reverse order idx
      auto otherIdx = std::distance(querySteps.rbegin(), it);
      uint16_t stepidOut = otherStep.get()->stepId();
      JobStepAssociation queryInputSA = otherStep.get()->inputAssociation();

      for (unsigned int j = 0; j < queryInputSA.outSize(); j++)
      {
        ptrdiff_t dlinptr = 0;
        auto* dlin = queryInputSA.outAt(j)->rowGroupDL();

        if (dlin)
        {
          dlinptr = (ptrdiff_t)dlin;
        }

        if ((ptrdiff_t)dloutptr == (ptrdiff_t)dlinptr)
        {
          oss << stepidIn << " -> " << stepidOut;

          if (dlin)
          {
            oss << " [label=\"[" << numConsumers << "]\"]" << endl;
            tab = tabsToPretify[otherIdx] + 1;
          }
        }
      }
    }

    for (auto& otherProjectStep : projectSteps)
    {
      uint16_t stepidOut = otherProjectStep->stepId();
      JobStepAssociation projectInputSA = otherProjectStep->inputAssociation();

      for (unsigned int j = 0; j < projectInputSA.outSize(); j++)
      {
        ptrdiff_t dlinptr = 0;
        auto* dlin = projectInputSA.outAt(j)->rowGroupDL();

        if (dlin)
          dlinptr = (ptrdiff_t)dlin;

        if (dloutptr == dlinptr)
        {
          oss << stepidIn << " -> " << stepidOut;

          if (dlin)
          {
            oss << " [label=\"[" << numConsumers << "]\"]" << endl;
          }
        }
      }
    }
  }
  return {tab, oss.str()};
}

std::string GraphGeneratorInterface::getGraphProjectionNode(SJSTEP& step)
{
  std::ostringstream oss;
  uint16_t stepidIn = step->stepId();
  oss << stepidIn << " [label=\"st_" << stepidIn << " ";

  if (typeid(*(step)) == typeid(pColStep))
  {
    oss << "(" << step->tableOid() << "/" << step->oid() << ")" << "\"";
    oss << " shape=ellipse";
  }
  else if (typeid(*(step)) == typeid(pColScanStep))
  {
    oss << "(" << step->tableOid() << "/" << step->oid() << ")" << "\"";
    oss << " shape=box";
  }
  else if (typeid(*(step)) == typeid(pDictionaryStep))
  {
    oss << "\"";
    oss << " shape=trapezium";
  }
  else if (typeid(*(step)) == typeid(PassThruStep))
  {
    oss << "(" << step->tableOid() << "/" << step->oid() << ")" << "\"";
    oss << " shape=octagon";
  }
  else if (typeid(*(step)) == typeid(TupleBPS))
  {
    bool isTuple = false;
    BatchPrimitive* bps = dynamic_cast<BatchPrimitive*>(step.get());

    if (dynamic_cast<TupleBPS*>(bps) != 0)
      isTuple = true;

    oss << "(" << bps->tableOid() << ":\\l";
    OIDVector projectOids = bps->getProjectOids();

    for (unsigned int i = 0; i < projectOids.size(); i++)
    {
      oss << projectOids[i] << " ";

      if ((i + 1) % 3 == 0)
        oss << "\\l";
    }

    oss << ")\"";
    oss << " shape=box style=bold";

    if (isTuple)
      oss << " peripheries=2";
  }
  else if (typeid(*(step)) == typeid(CrossEngineStep))
  {
    CrossEngineStep* cej = dynamic_cast<CrossEngineStep*>(step.get());
    oss << "(" << cej->tableName() << "/" << cej->tableAlias() << ")\"";
    oss << " shape=cylinder style=bold";
  }
  else
    oss << "\"";

  oss << "]" << endl;
  return oss.str();
}

std::string GraphGeneratorInterface::getProjectionEdges(JobStepVector& steps, SJSTEP& step,
                                                        const std::size_t ctn)
{
  uint16_t stepidIn = step->stepId();
  std::ostringstream oss;

  for (unsigned int i = 0; i < step->outputAssociation().outSize(); i++)
  {
    ptrdiff_t dloutptr = 0;
    auto* dlout = step->outputAssociation().outAt(i)->rowGroupDL();
    uint32_t numConsumers = step->outputAssociation().outAt(i)->getNumConsumers();

    if (dlout)
    {
      dloutptr = (ptrdiff_t)dlout;
    }

    for (auto k = ctn + 1; k < steps.size(); k++)
    {
      uint16_t stepidOut = steps[k].get()->stepId();
      JobStepAssociation projectInputSA = steps[k].get()->inputAssociation();

      for (unsigned int j = 0; j < projectInputSA.outSize(); j++)
      {
        ptrdiff_t dlinptr = 0;
        auto* dlin = projectInputSA.outAt(j)->rowGroupDL();

        if (dlin)
          dlinptr = (ptrdiff_t)dlin;

        if ((ptrdiff_t)dloutptr == (ptrdiff_t)dlinptr)
        {
          oss << stepidIn << " -> " << stepidOut;

          if (dlin)
          {
            oss << " [label=\"[" << numConsumers << "]\"]" << endl;
          }
        }
      }
    }
  }
  return oss.str();
}

std::string GraphGeneratorInterface::writeDotCmds()
{
  // Graphic view draw
  std::ostringstream oss;
  oss << "digraph G {" << endl;

  // merge in the subquery steps
  JobStepVector querySteps;
  for (auto& step : query)
  {
    auto steps = extractJobSteps(step);
    querySteps.insert(querySteps.end(), steps.begin(), steps.end());
  }

  JobStepVector projectSteps = project;
  std::vector<size_t> tabsToPretify;
  // Reverse the order of the steps to draw the graph from top to bottom
  for (auto it = querySteps.rbegin(); it != querySteps.rend(); ++it)
  {
    auto& step = *it;
    oss << getGraphNode(step);
    auto [tab, graphEdges] = getTabsAndEdges(querySteps, projectSteps, step, tabsToPretify);
    tabsToPretify.push_back(tab);
    oss << graphEdges;
  }

  for (auto psi = projectSteps.begin(); psi != projectSteps.end(); ++psi)
  {
    auto& step = *psi;
    oss << getGraphProjectionNode(step);
    auto idx = std::distance(projectSteps.begin(), psi);
    oss << getProjectionEdges(projectSteps, step, idx);
  }

  oss << "}" << endl;

  auto ladderRepr = getLadderRepr(querySteps, tabsToPretify);
  cout << endl;
  cout << ladderRepr;

  return oss.str();
}

}  // end namespace jlf_graphics

#ifdef __clang__
#pragma clang diagnostic pop
#endif
