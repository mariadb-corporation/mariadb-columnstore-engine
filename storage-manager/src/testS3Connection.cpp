/* Copyright (C) 2020 MariaDB Corporation

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

#include "S3Storage.h"

#include <iostream>
#include <stdlib.h>
#include <unistd.h>
#include <boost/program_options.hpp>

using namespace storagemanager;
using namespace std;
namespace po = boost::program_options;

void printUsage(const po::options_description& desc)
{
  cout << "MariaDB Columnstore Storage Manager Test Configuration Connectivity.\n" << endl;
  cout << "Usage: testS3Connection \n" << endl;
  cout<< desc <<endl;
  cout << "Returns Success=0 Failure=1\n" << endl;
}
int s3TestConnection(bool verbose)
{
  S3Storage* s3 = NULL;
  int ret = 0;
  try
  {
    S3Storage* s3 = new S3Storage(true, verbose);
    cout << "S3 Storage Manager Configuration OK" << endl;
    delete s3;
  }
  catch (exception& e)
  {
    cout << "S3 Storage Manager Configuration Error:" << endl;
    cout << e.what() << endl;
    if (s3)
      delete s3;
    ret = 1;
  }
  return ret;
}

int main(int argc, char* argv[])
{
  try
  {
    //define options
    po::options_description desc("Options");
    desc.add_options ()
    ("help,h", "Print help message")
    ("verbose,v", po::value<bool>()->default_value(false), "Enable verbose output");

    // Parse command line
    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if(vm.count("help"))
    {
      printUsage (desc);
      return 0;
    }
    // Run the test with verbose flag
    return s3TestConnection(vm["verbose"].as<bool>());
  }
  catch(const std::exception& e)
  {
    std::cerr << e.what() << '\n';
  }
  
}
