/* Copyright (C) 2021 MariaDB Corporation

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

#include <iostream>
#include <string>
#include <ftw.h>
#include <boost/filesystem.hpp>

#include "configcpp.h"
#include "rebuildEM.h"
#include "IDBPolicy.h"
#include "IDBFileSystem.h"

using namespace idbdatafile;
using namespace RebuildExtentMap;

static void usage(const string& pname)
{
    std::cout << "usage: " << pname << " [-vdhs]" << std::endl;
    std::cout
        << "rebuilds the extent map from the contents of the database file "
           "system."
        << std::endl;
    std::cout
        << "   -v display verbose progress information. More v's, more debug"
        << std::endl;
    std::cout << "   -d display what would be done--don't do it" << std::endl;
    std::cout << "   -h display this help text" << std::endl;
    std::cout << "   -s show extent map and quit" << std::endl;
}

int main(int argc, char** argv)
{
    int32_t option;
    std::string pname(argv[0]);
    bool showExtentMap = false;
    bool verbose = false;
    bool display = false;

    while ((option = getopt(argc, argv, "vdhs")) != EOF)
    {
        switch (option)
        {
        case 'v':
            verbose = true;
            break;

        case 'd':
            display = true;
            break;

        case 's':
            showExtentMap = true;
            break;

        case 'h':
        case '?':
        default:
            usage(pname);
            return (option == 'h' ? 0 : 1);
            break;
        }
    }

    EMReBuilder emReBuilder(verbose, display);
    // Just show EM and quit.
    if (showExtentMap)
    {
        emReBuilder.showExtentMap();
        return 0;
    }

    auto* config = config::Config::makeConfig();
    // The part `ActiveNodes` is not present in single node config.
    bool isMultiNode =
        config->getConfig("ActiveNodes", "Node") == "" ? false : true;
    if (isMultiNode)
    {
        std::cout << "Rebuild extent map for multi-node is not supported. "
                  << std::endl;
        return 0;
    }

    const auto BRMSavesEM =
        config->getConfig("SystemConfig", "DBRMRoot") + "_em";
    // Check for `BRM_saves_em` file presents.
    if (boost::filesystem::exists(BRMSavesEM))
    {
        std::cout << BRMSavesEM << " file exists. " << std::endl;
        std::cout << "Please note: this tool is only suitable in situations "
                     "where there is no `BRM_saves_em` file. "
                  << std::endl;
        std::cout << "If `BRM_saves_em` "
                     "exists extent map will be restored from it. "
                  << std::endl;
        return 0;
    }

    // Initialize system extents from the binary blob.
    auto rc = emReBuilder.initializeSystemExtents();
    if (rc == -1)
    {
        return 1;
    }

    // Make config from default path.
    std::string count = config->getConfig("SystemConfig", "DBRootCount");
    // Read the number of DBRoots.
    uint32_t dbRootCount = config->uFromText(count);

    // Iterate over DBRoots starting from the first one.
    for (uint32_t dbRootNumber = 1; dbRootNumber <= dbRootCount;
         ++dbRootNumber)
    {
        std::string dbRootName = "DBRoot" + std::to_string(dbRootNumber);
        auto dbRootPath = config->getConfig("SystemConfig", dbRootName);
        emReBuilder.setDBRoot(dbRootNumber);
        emReBuilder.collectExtents(dbRootPath.c_str());
        emReBuilder.rebuildExtentMap();
        emReBuilder.clear();
    }

    return 0;
}
