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

using namespace storagemanager;
using namespace std;

void printUsage()
{
  cout << "MariaDB Columnstore Storage Manager Test Configuration Connectivity.\n" << endl;
  cout << "Usage: testS3Connection \n" << endl;
  cout << "Returns Success=0 Failure=1\n" << endl;
}
int s3TestConnection()
{
  S3Storage* s3 = NULL;
  int ret = 0;
  try
  {
    S3Storage* s3 = new S3Storage(true);
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
  int option;
  while ((option = getopt(argc, argv, "h")) != EOF)
  {
    switch (option)
    {
      case 'h':
      default:
        printUsage();
        return 0;
        break;
    }
  }
  return s3TestConnection();
}
