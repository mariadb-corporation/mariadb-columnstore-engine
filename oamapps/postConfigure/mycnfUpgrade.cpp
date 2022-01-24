/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016 MariaDB Corporation

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
 * $Id: mycnfUpgrade.cpp 64 2006-10-12 22:21:51Z dhill $
 *
 *
 *
 ******************************************************************************************/
/**
 * @file
 */

#include <unistd.h>
#include <iterator>
#include <numeric>
#include <deque>
#include <iostream>
#include <ostream>
#include <fstream>
#include <cstdlib>
#include <string>
#include <limits.h>
#include <sstream>
#include <exception>
#include <stdexcept>
#include <vector>
#include <stdio.h>
#include <ctype.h>
#include <netdb.h>
#include <sys/sysinfo.h>
#include <climits>
#include <cstring>
#include <glob.h>
#include <boost/regex.hpp>
#include "liboamcpp.h"
#include "installdir.h"
#include "mcsconfig.h"

using namespace std;
using namespace oam;

/* MCOL-1844.  On an upgrade, the user may have customized options in their old
 * myCnf-include-args.text file.  Merge it with the packaged version, and then process as we
 * have before.
 */
string rtrim(const string& in)
{
  string::const_reverse_iterator rbegin = in.rbegin();
  while (rbegin != in.rend() && isspace(*rbegin))
    ++rbegin;
  return string(in.begin(), rbegin.base());
}

void mergeMycnfIncludeArgs()
{
  string userArgsFilename = std::string(MCSSUPPORTDIR) + "/myCnf-include-args.text.rpmsave";
  string packagedArgsFilename = std::string(MCSSUPPORTDIR) + "/myCnf-include-args.text";
  ifstream userArgs(userArgsFilename.c_str());
  fstream packagedArgs(packagedArgsFilename.c_str(), ios::in);

  if (!userArgs || !packagedArgs)
    return;

  // de-dup the args and comments in both files
  set<string> argMerger;
  set<string> comments;
  string line;
  while (getline(packagedArgs, line))
  {
    line = rtrim(line);
    if (line[0] == '#')
      comments.insert(line);
    else if (line.size() > 0)
      argMerger.insert(line);
  }
  while (getline(userArgs, line))
  {
    line = rtrim(line);
    if (line[0] == '#')
      comments.insert(line);
    else if (line.size() > 0)
      argMerger.insert(line);
  }
  userArgs.close();
  packagedArgs.close();

  // write the merged version, comments first.  They'll get ordered
  // alphabetically but, meh.
  packagedArgs.open(packagedArgsFilename.c_str(), ios::out | ios::trunc);
  for (set<string>::iterator it = comments.begin(); it != comments.end(); it++)
    packagedArgs << *it << endl;
  for (set<string>::iterator it = argMerger.begin(); it != argMerger.end(); it++)
    packagedArgs << *it << endl;
  packagedArgs.close();
}

int main(int argc, char* argv[])
{
  Oam oam;

  // check for port argument
  string mysqlPort;

  if (argc > 1)
  {
    mysqlPort = argv[1];

    // set mysql password
    oam.changeMyCnf("port", mysqlPort);
    exit(0);
  }

  // my.cnf file
  string mycnfFile = std::string(MCSMYCNFDIR) + "/columnstore.cnf";
  ifstream mycnffile(mycnfFile.c_str());

  if (!mycnffile)
  {
    cerr << "mycnfUpgrade - columnstore.cnf file not found: " << mycnfFile << endl;
    exit(1);
  }

  // my.cnf.rpmsave file
  string mycnfsaveFile = std::string(MCSMYCNFDIR) + "/columnstore.cnf.rpmsave";
  ifstream mycnfsavefile(mycnfsaveFile.c_str());

  if (!mycnfsavefile)
  {
    cerr << "mycnfUpgrade - columnstore.cnf.rpmsave file not found: " << mycnfsaveFile << endl;
    exit(1);
  }

  // MCOL-1844.  The user may have added options to their myCnf-include-args file.  Merge
  // myCnf-include-args.text with myCnf-include-args.text.rpmsave, save in myCnf-include-args.text
  mergeMycnfIncludeArgs();

  // include arguments file
  string includeFile = std::string(MCSSUPPORTDIR) + "/myCnf-include-args.text";
  ifstream includefile(includeFile.c_str());

  if (!includefile)
  {
    cerr << "mycnfUpgrade - columnstore.cnf include argument file not found: " << includeFile << endl;
    exit(1);
  }

  // exclude arguments file
  string excludeFile = std::string(MCSSUPPORTDIR) + "/myCnf-exclude-args.text";
  ifstream excludefile(excludeFile.c_str());

  if (!excludefile)
  {
    cerr << "mycnfUpgrade - columnstore.cnf exclude argument file not found: " << endl;
    exit(1);
  }

  // go though include list
  char line[200];
  string includeArg;

  while (includefile.getline(line, 200))
  {
    includeArg = line;

    boost::regex icludeArgRegEx("^#*\\s*" + includeArg + "\\s*=");
    // see if in columnstore.cnf.rpmsave
    ifstream mycnfsavefile(mycnfsaveFile.c_str());
    char line[200];
    string oldbuf;

    while (mycnfsavefile.getline(line, 200))
    {
      oldbuf = line;

      if (boost::regex_search(oldbuf.begin(), oldbuf.end(), icludeArgRegEx))
      {
        // found in columnstore.cnf.rpmsave, check if this is commented out
        if (line[0] != '#')
        {
          // no, check in columnstore.cnf and replace if exist or add if it doesn't

          ifstream mycnffile(mycnfFile.c_str());
          vector<string> lines;
          char line1[200];
          string newbuf;
          bool updated = false;

          while (mycnffile.getline(line1, 200))
          {
            newbuf = line1;

            if (boost::regex_search(newbuf.begin(), newbuf.end(), icludeArgRegEx))
            {
              newbuf = oldbuf;
              cout << "Updated argument: " << includeArg << endl;
              updated = true;
            }

            // output to temp file
            lines.push_back(newbuf);
          }

          // write out a new columnstore.cnf
          mycnffile.close();
          unlink(mycnfFile.c_str());
          ofstream newFile(mycnfFile.c_str());

          // create new file
          int fd = open(mycnfFile.c_str(), O_RDWR | O_CREAT, 0644);

          copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
          newFile.close();

          close(fd);

          if (!updated)
          {
            // not found, so add
            ifstream mycnffile(mycnfFile.c_str());
            vector<string> lines;
            char line1[200];
            string newbuf;

            while (mycnffile.getline(line1, 200))
            {
              newbuf = line1;
              boost::regex mysqldSectionRegEx("\\[mysqld\\]");

              if (boost::regex_search(newbuf.begin(), newbuf.end(), mysqldSectionRegEx))
              {
                lines.push_back(newbuf);
                newbuf = oldbuf;
                cout << "Added argument: " << includeArg << endl;
              }

              // output to temp file
              lines.push_back(newbuf);
            }

            // write out a new columnstore.cnf
            mycnffile.close();
            unlink(mycnfFile.c_str());
            ofstream newFile(mycnfFile.c_str());

            // create new file
            int fd = open(mycnfFile.c_str(), O_RDWR | O_CREAT, 0666);

            copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
            newFile.close();

            close(fd);
            break;
          }
        }
      }
    }
  }

  string USER = "mysql";

  char* p = getenv("USER");

  if (p && *p)
    USER = p;

  string cmd = "chown " + USER + ":" + USER + " " + mycnfFile;
  system(cmd.c_str());

  exit(0);
}
// vim:ts=4 sw=4:
