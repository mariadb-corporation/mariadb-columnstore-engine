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
 * $Id$
 *
 *******************************************************************************/

/*
 * we_xmlgetter.h
 *
 *  Created on: Feb 7, 2012
 *      Author: bpaul
 */

#pragma once

#include <libxml/parser.h>

namespace WriteEngine
{
class WEXmlgetter
{
 public:
  explicit WEXmlgetter(const std::string& ConfigName);
  ~WEXmlgetter();

 public:
  //..Public methods
  std::string getValue(const std::vector<std::string>& sections) const;
  std::string getAttribute(const std::vector<std::string>& sections, const std::string& Tag) const;
  void getConfig(const std::string& section, const std::string& name, std::vector<std::string>& values) const;
  void getAttributeListForAllChildren(const std::vector<std::string>& sections,
                                      const std::string& attributeTag,
                                      std::vector<std::string>& attributeValues) const;

 private:
  //..Private methods
  static const xmlNode* getNode(const xmlNode* pParent, const std::string& section);
  static bool getNodeAttribute(const xmlNode* pNode, const char* pTag, std::string& strVal);
  static bool getNodeContent(const xmlNode* pNode, std::string& strVal);

  //..Private data members
  std::string fConfigName;  // xml filename
  xmlDocPtr fDoc;           // xml document pointer
  xmlNode* fpRoot;          // root element
};

} /* namespace WriteEngine */
