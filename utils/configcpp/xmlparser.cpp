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

/******************************************************************************************
 * $Id$
 *
 ******************************************************************************************/
#include "mcsconfig.h"

#include <string>
#include <stdexcept>
#include <libxml/xmlmemory.h>
#include <libxml/parser.h>
#include <vector>

#include "collation.h"
using namespace std;

#include "xmlparser.h"

namespace config
{
const string XMLParser::getConfig(const xmlDocPtr doc, const string& section, const string& name) const
{
  string res;
  xmlNodePtr cur1 = xmlDocGetRootElement(doc);

  if (cur1 == NULL)
    throw runtime_error("XMLParser::getConfig: error accessing XML root");

  cur1 = cur1->xmlChildrenNode;

  while (cur1 != NULL)
  {
    string cur1name = (const char*)cur1->name;

    if ((datatypes::ASCIIStringCaseInsensetiveEquals(cur1name, section)))
    {
      xmlNodePtr cur2 = cur1->xmlChildrenNode;

      while (cur2 != NULL)
      {
        string cur2name = (const char*)cur2->name;

        if ((datatypes::ASCIIStringCaseInsensetiveEquals(cur2name, name)))
        {
          xmlNodePtr cur3 = cur2->xmlChildrenNode;

          if (cur3)
            res = (const char*)cur3->content;

          return res;
        }

        cur2 = cur2->next;
      }
    }

    cur1 = cur1->next;
  }

  // maybe nullstr if not found
  return res;
}

void XMLParser::getConfig(const xmlDocPtr doc, const string& section, const string& name,
                          vector<string>& values) const
{
  string res;

  xmlNodePtr cur1 = xmlDocGetRootElement(doc);

  if (cur1 == NULL)
    throw runtime_error("XMLParser::getConfig: error accessing XML root");

  cur1 = cur1->xmlChildrenNode;

  while (cur1 != NULL)
  {
    string cur1name = (const char*)cur1->name;

    if (datatypes::ASCIIStringCaseInsensetiveEquals(cur1name, section))
    {
      xmlNodePtr cur2 = cur1->xmlChildrenNode;

      while (cur2 != NULL)
      {
        string cur2name = (const char*)cur2->name;

        if (datatypes::ASCIIStringCaseInsensetiveEquals(cur2name, name))
        {
          res.clear();
          xmlNodePtr cur3 = cur2->xmlChildrenNode;

          if (cur3)
            res = (const char*)cur3->content;

          values.push_back(res);
        }

        cur2 = cur2->next;
      }
    }

    cur1 = cur1->next;
  }
}

void XMLParser::setConfig(xmlDocPtr doc, const string& section, const string& name, const string& value)
{
  xmlNodePtr cur1 = xmlDocGetRootElement(doc);

  if (cur1 == NULL)
    throw runtime_error("XMLParser::setConfig: error accessing XML root");

  xmlNodePtr cur2;

  cur1 = cur1->xmlChildrenNode;

  while (cur1 != NULL)
  {
    string cur1name = (const char*)cur1->name;

    if (datatypes::ASCIIStringCaseInsensetiveEquals(cur1name, section))
    {
      cur2 = cur1->xmlChildrenNode;

      while (cur2 != NULL)
      {
        string cur2name = (const char*)cur2->name;

        if (datatypes::ASCIIStringCaseInsensetiveEquals(cur2name, name))
        {
          xmlNodePtr cur3 = cur2->xmlChildrenNode;

          if (cur3 == NULL)
          {
            xmlAddChild(cur2, xmlNewText((const xmlChar*)"\t"));
            cur3 = cur2->xmlChildrenNode;
            xmlFree(cur3->content);
          }
          else
          {
            xmlFree(cur3->content);
          }

          cur3->content = xmlStrdup((const xmlChar*)value.c_str());
          return;
        }

        cur2 = cur2->next;
      }

      // We found the section, but not the name, so we need to add a new node here
      xmlAddChild(cur1, xmlNewText((const xmlChar*)"\t"));
      xmlNewTextChild(cur1, NULL, (const xmlChar*)name.c_str(), (const xmlChar*)value.c_str());
      xmlAddChild(cur1, xmlNewText((const xmlChar*)"\n\t"));
      return;
    }

    cur1 = cur1->next;
  }

  // We did not find the section, so we need to add it and the name here
  cur1 = xmlDocGetRootElement(doc);
  xmlAddChild(cur1, xmlNewText((const xmlChar*)"\t"));
  cur2 = xmlNewChild(cur1, NULL, (const xmlChar*)section.c_str(), NULL);
  xmlAddChild(cur2, xmlNewText((const xmlChar*)"\n\t\t"));
  xmlNewTextChild(cur2, NULL, (const xmlChar*)name.c_str(), (const xmlChar*)value.c_str());
  xmlAddChild(cur2, xmlNewText((const xmlChar*)"\n\t"));
  xmlAddChild(cur1, xmlNewText((const xmlChar*)"\n"));

  return;
}

void XMLParser::delConfig(xmlDocPtr doc, const string& section, const string& name)
{
  string res;

  xmlNodePtr cur1 = xmlDocGetRootElement(doc);

  if (cur1 == NULL)
    throw runtime_error("XMLParser::delConfig: error accessing XML root");

  cur1 = cur1->xmlChildrenNode;

  while (cur1 != NULL)
  {
    string cur1name = (const char*)cur1->name;

    if (datatypes::ASCIIStringCaseInsensetiveEquals(cur1name, section))
    {
      xmlNodePtr cur2 = cur1->xmlChildrenNode;

      while (cur2 != NULL)
      {
        xmlNodePtr tmp = cur2;
        cur2 = cur2->next;
        string tmpname = (const char*)tmp->name;

        if (datatypes::ASCIIStringCaseInsensetiveEquals(tmpname, name))
        {
          xmlUnlinkNode(tmp);
          xmlFreeNode(tmp);
        }
      }
    }

    cur1 = cur1->next;
  }

  return;
}

const vector<string> XMLParser::enumConfig(const xmlDocPtr doc) const
{
  vector<string> resv;
  string res;

  xmlNodePtr cur1 = xmlDocGetRootElement(doc);

  if (cur1 == NULL)
    throw runtime_error("XMLParser::getConfig: error accessing XML root");

  cur1 = cur1->xmlChildrenNode;

  while (cur1 != NULL)
  {
    res = reinterpret_cast<const char*>(cur1->name);

    if (res != "text" && res != "comment")
      resv.push_back(res);

    cur1 = cur1->next;
  }

  return resv;
}

const vector<string> XMLParser::enumSection(const xmlDocPtr doc, const string& section) const
{
  vector<string> resv;
  string res;

  xmlNodePtr cur1 = xmlDocGetRootElement(doc);

  if (cur1 == NULL)
    throw runtime_error("XMLParser::getConfig: error accessing XML root");

  cur1 = cur1->xmlChildrenNode;

  while (cur1 != NULL)
  {
    if ((!xmlStrcmp(cur1->name, (const xmlChar*)section.c_str())))
    {
      xmlNodePtr cur2 = cur1->xmlChildrenNode;

      while (cur2 != NULL)
      {
        res = reinterpret_cast<const char*>(cur2->name);

        if (res != "text" && res != "comment")
          resv.push_back(res);

        cur2 = cur2->next;
      }
    }

    cur1 = cur1->next;
  }

  return resv;
}

}  // namespace config
