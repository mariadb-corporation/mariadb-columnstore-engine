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

// $Id: jlf_graphics.h 9210 2013-01-21 14:10:42Z rdempsey $

/** @file */

#pragma once

#include "joblist.h"

namespace jlf_graphics
{
/** Format a stream of dot commands
 * Format a stream of dot commands
 */

std::string getLadderRepr(const joblist::JobStepVector& steps, const std::vector<size_t>& tabsToPretify);
std::string generateDotFileName(const std::string& prefix);

class GraphGeneratorInterface
{
 public:
  GraphGeneratorInterface(const joblist::JobStepVector& query, const joblist::JobStepVector& project)
   : query(query), project(project){};

  virtual ~GraphGeneratorInterface(){};

  virtual std::string writeDotCmds();

 private:
  virtual joblist::JobStepVector extractSubquerySteps(const joblist::SJSTEP& sqs);
  virtual joblist::JobStepVector extractJobSteps(const joblist::SJSTEP& umbrella);

  virtual std::string getGraphNode(const joblist::SJSTEP& stepPtr);
  virtual std::pair<size_t, std::string> getTabsAndEdges(const joblist::JobStepVector& querySteps,
                                                         const joblist::JobStepVector& projectSteps,
                                                         const joblist::SJSTEP& stepPtr,
                                                         const std::vector<size_t>& tabsToPretify);
  virtual std::string getGraphProjectionNode(joblist::SJSTEP& step);
  virtual std::string getProjectionEdges(joblist::JobStepVector& steps, joblist::SJSTEP& step,
                                         const std::size_t ctn);

  const joblist::JobStepVector& query;
  const joblist::JobStepVector& project;
};

class GraphGeneratorNoStats : public GraphGeneratorInterface
{
 public:
  GraphGeneratorNoStats(const joblist::JobStepVector& query, const joblist::JobStepVector& project)
   : GraphGeneratorInterface(query, project){};
  ~GraphGeneratorNoStats(){};
};

class GraphGeneratorWStats : public GraphGeneratorInterface
{
 public:
  GraphGeneratorWStats(const joblist::JobStepVector& query, const joblist::JobStepVector& project)
   : GraphGeneratorInterface(query, project){};
  ~GraphGeneratorWStats(){};

 private:
};

}  // namespace jlf_graphics
