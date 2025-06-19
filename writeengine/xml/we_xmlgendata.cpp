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

/*******************************************************************************
 * $Id: we_xmlgendata.cpp 4450 2013-01-21 14:13:24Z rdempsey $
 *
 *******************************************************************************/

#include "we_xmlgendata.h"

#include <iostream>
#include <boost/filesystem/path.hpp>
#include "we_config.h"

namespace
{
const std::string JOBDIR("job");
}

namespace WriteEngine
{
/* static */ const std::string XMLGenData::DELIMITER("-d");
/* static */ const std::string XMLGenData::DESCRIPTION("-s");
/* static */ const std::string XMLGenData::ENCLOSED_BY_CHAR("-E");
/* static */ const std::string XMLGenData::ESCAPE_CHAR("-C");
/* static */ const std::string XMLGenData::JOBID("-j");
/* static */ const std::string XMLGenData::MAXERROR("-e");
/* static */ const std::string XMLGenData::NAME("-n");
/* static */ const std::string XMLGenData::PATH("-p");
/* static */ const std::string XMLGenData::RPT_DEBUG("-b");
/* static */ const std::string XMLGenData::USER("-u");
/* static */ const std::string XMLGenData::NO_OF_READ_BUFFER("-r");
/* static */ const std::string XMLGenData::READ_BUFFER_CAPACITY("-c");
/* static */ const std::string XMLGenData::WRITE_BUFFER_SIZE("-w");
/* static */ const std::string XMLGenData::EXT("-x");
/* static */ const std::string XMLGenData::SKIP_ROWS("-O");

//------------------------------------------------------------------------------
// XMLGenData constructor
// Omit inserting JOBID; derived class is required to insert
//------------------------------------------------------------------------------
XMLGenData::XMLGenData()
{
  fParms.emplace(DELIMITER, "|");
  fParms.emplace(DESCRIPTION, "");
  fParms.emplace(ENCLOSED_BY_CHAR, "");
  fParms.emplace(ESCAPE_CHAR, "\\");
  fParms.emplace(JOBID, "299");
  fParms.emplace(MAXERROR, "10");
  fParms.emplace(NAME, "");
  boost::filesystem::path p{std::string(Config::getBulkRoot())};
  p /= JOBDIR;
  fParms.emplace(PATH, p.string());

  fParms.emplace(RPT_DEBUG, "0");
  fParms.emplace(USER, "");
  fParms.emplace(NO_OF_READ_BUFFER, "5");
  fParms.emplace(READ_BUFFER_CAPACITY, "1048576");
  fParms.emplace(WRITE_BUFFER_SIZE, "10485760");
  fParms.emplace(EXT, "tbl");
  fParms.emplace(SKIP_ROWS, "0");
}

//------------------------------------------------------------------------------
// XMLGenData destructor
//------------------------------------------------------------------------------
/* virtual */
XMLGenData::~XMLGenData() = default;

//------------------------------------------------------------------------------
// Return value for the specified parm.
//------------------------------------------------------------------------------
std::string XMLGenData::getParm(const std::string& key) const
{
  auto p = fParms.find(key);

  if (fParms.end() != p)
    return p->second;
  else
    return "";
}

//------------------------------------------------------------------------------
// print contents to specified stream
//------------------------------------------------------------------------------
/* virtual */
void XMLGenData::print(std::ostream& /* os */) const
{
}

}  // namespace WriteEngine
