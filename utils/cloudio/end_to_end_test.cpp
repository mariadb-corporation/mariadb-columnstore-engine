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

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <boost/thread.hpp>
#include <unistd.h>
#include <list>
#include <string>

#include "SMFileSystem.h"
#include "SMDataFile.h"
#include "SMFileFactory.h"
#include "sm_exceptions.h"
#include "messageFormat.h"

/* The purpose of this is to test the idbdatafile cloud classes using a dummy
SessionManager defined in this file. */

#undef NDEBUG
#include <assert.h>

using namespace idbdatafile;
using namespace std;

volatile bool die = false;
int errCode = 0;

void printResultPASS(int result)
{
  if (result == 0)
    cout << " PASS" << endl;
  else
    cout << " FAIL" << endl;
}

void printResultFAIL(int result)
{
  if (result == 0)
    cout << " FAIL" << endl;
  else
    cout << " PASS" << endl;
}

void printResultPASS(bool result)
{
  if (result)
    cout << " PASS" << endl;
  else
    cout << " FAIL" << endl;
}

void printResultFAIL(bool result)
{
  if (result)
    cout << " FAIL" << endl;
  else
    cout << " PASS" << endl;
}

void printResult(int result, int expected)
{
  if (result == expected)
    cout << " PASS" << endl;
  else
    cout << " FAIL" << endl;
}

int testAll()
{
  /* Instantiate each of SM subclasses, call each function, and verify the expected error response */
  int err;
  SMFileFactory factory;
  list<string> filenames;
  SMFileSystem filesystem;

  cout << "compressedSize --- ";
  bool gotException = false;
  try
  {
    filesystem.compressedSize("./testData/dummy");
  }
  catch (NotImplementedYet&)
  {
    gotException = true;
  }
  assert(gotException && !die);
  cout << " N/A" << endl;

  cout << "copyFile dummy1->dummy2 --- ";
  err = filesystem.copyFile("./testData/dummy1", "./testData/dummy2");
  printResultFAIL(err);

  cout << "copyFile end_to_end_test_file.start->readTest --- ";
  err = filesystem.copyFile("./testData/end_to_end_test_file.start", "./testData/readTest");
  printResultPASS(err);

  cout << "copyFile end_to_end_test_file.start->dummy --- ";
  err = filesystem.copyFile("./testData/end_to_end_test_file.start", "./testData/dummy");
  printResultPASS(err);

  cout << "rename dummy->dummyrename --- ";
  err = filesystem.rename("./testData/dummy", "./testData/dummyrename");
  printResultPASS(err);

  cout << "exists dummyrename --- ";
  bool exists = filesystem.exists("./testData/dummyrename");
  printResultPASS(exists);
  cout << "exists DNE --- ";
  bool doesNotExist = filesystem.exists("./testData/DNE");
  printResultFAIL(doesNotExist);

  cout << "filesystemisup --- ";
  bool fsIsUp = filesystem.filesystemIsUp();
  printResultPASS(fsIsUp);

  cout << "isdir testData --- ";
  bool isDir = filesystem.isDir("testData");
  printResultPASS(isDir);

  cout << "isdir readTest --- ";
  bool isNotDir = filesystem.isDir("./testData/readTest");
  printResultFAIL(isNotDir);

  cout << "isdir DNE --- ";
  isNotDir = filesystem.isDir("./testData/DNE");
  printResultFAIL(isNotDir);

  cout << "listdirectory testData --- ";
  err = filesystem.listDirectory("testData", filenames);
  printResultPASS(err);

  cout << "listdirectory dummyrename --- ";
  err = filesystem.listDirectory("./testData/dummyrename", filenames);
  printResultFAIL(err);

  cout << "listdirectory DNE --- ";
  err = filesystem.listDirectory("./testData/DNE", filenames);
  printResultFAIL(err);

  cout << "remove dummyrename --- ";
  err = filesystem.remove("./testData/dummyrename");
  printResultPASS(err);

  cout << "size readTest --- ";
  err = filesystem.size("./testData/readTest");
  printResult(err, 10940);

  cout << "size DNE --- ";
  err = filesystem.size("./testData/DNE");
  printResultFAIL(err);

  cout << "open readTest r --- ";
  IDBDataFile* fileR = factory.open("./testData/readTest", "r", 0, 0);
  if (fileR)
    cout << " PASS" << endl;
  else
    cout << " FAIL" << endl;

  cout << "open readTest a --- ";
  IDBDataFile* fileW = factory.open("./testData/readTest", "a", 0, 0);
  if (fileW)
    cout << " PASS" << endl;
  else
    cout << " FAIL" << endl;

  cout << "open DNE --- ";
  IDBDataFile* file2 = factory.open("./testData/DNE", "r", 0, 0);
  if (file2)
    cout << " FAIL" << endl;
  else
    cout << " PASS" << endl;

  cout << "pread --- ";
  uint8_t buf[2048];
  err = fileR->pread(buf, 0, 1024);
  printResult(err, 1024);

  cout << "read --- ";
  err = fileR->read(buf, 1024);
  printResult(err, 1024);

  int newSize = 10940 + 1024;
  cout << "write --- ";
  err = fileW->write(buf, 1024);
  printResult(err, 1024);
  cout << "size " << newSize << " --- ";
  err = fileW->size();
  cout << err;
  printResult(err, newSize);

  cout << "seek SEEK_SET --- ";
  err = fileR->seek(1234, SEEK_SET);
  printResultPASS(err);
  cout << "tell 1234 --- ";
  err = fileR->tell();
  cout << err;
  printResult(err, 1234);
  cout << "seek SEEK_CUR --- ";
  err = fileR->seek(1234, SEEK_CUR);
  printResultPASS(err);
  cout << "tell 2468 --- ";
  err = fileR->tell();
  cout << err;
  printResult(err, 2468);
  cout << "seek SEEK_END --- ";
  err = fileR->seek(1234, SEEK_END);
  printResultPASS(err);
  cout << "tell 12174 --- ";
  err = fileR->tell();
  cout << err;
  printResult(err, 13198);

  cout << "truncate  1234 --- ";
  err = fileR->truncate(1234);
  printResultPASS(err);

  cout << "size 1234 --- ";
  err = fileR->size();
  printResult(err, 1234);

  cout << "mtime --- ";
  err = fileR->mtime();
  if (err > 0)
    cout << " PASS" << endl;
  else
    cout << " FAIL" << endl;

  cout << "remove readTest --- ";
  err = filesystem.remove("./testData/readTest");
  printResultPASS(err);

  cout << "done." << endl;
  return errCode;
}

int main()
{
  int ret;

  ret = testAll();
  // ret = testTest();
  return ret;
}
