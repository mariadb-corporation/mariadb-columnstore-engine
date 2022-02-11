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
 * we_filereadthread.h
 *
 *  Created on: Oct 25, 2011
 *      Author: bpaul
 */

#ifndef WE_FILEREADTHREAD_H_
#define WE_FILEREADTHREAD_H_

#include "we_cmdargs.h"
#include "libmarias3/marias3.h"
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/stream.hpp>

namespace WriteEngine
{
class WESDHandler;
class WEFileReadThread;

class WEReadThreadRunner
{
 public:
  WEReadThreadRunner(WEFileReadThread& Owner) : fRef(Owner)
  {
    // ctor
  }
  ~WEReadThreadRunner()
  {
  }

  void operator()();  // Thread function

 private:
  WEFileReadThread& fRef;
};

//------------------------------------------------------------------------------

class WEFileReadThread
{
 public:
  WEFileReadThread(WESDHandler& aSdh);
  virtual ~WEFileReadThread();

  void reset();
  void setup(std::string FileName);
  void shutdown();
  void feedData();
  unsigned int readDataFile(messageqcpp::SBS& Sbs);
  unsigned int readBinaryDataFile(messageqcpp::SBS& Sbs, unsigned int recLen);
  void openInFile();

  int getNextRow(std::istream& ifs, char* pBuf, int MaxLen);

  boost::thread* getFpThread() const
  {
    return fpThread;
  }
  bool isContinue() const
  {
    return fContinue;
  }
  void setContinue(bool fContinue)
  {
    this->fContinue = fContinue;
  }
  std::string getInFileName() const
  {
    return fInFileName;
  }
  unsigned int getTgtPmId() const
  {
    return fTgtPmId;
  }
  unsigned int getBatchQty() const
  {
    return fBatchQty;
  }
  void setFpThread(boost::thread* fpThread)
  {
    this->fpThread = fpThread;
  }
  void setInFileName(std::string fInFileName)
  {
    if ((0 == fInFileName.compare("STDIN")) || (0 == fInFileName.compare("stdin")))
      this->fInFileName = "/dev/stdin";
    else
      this->fInFileName = fInFileName;
  }
  //@BUG 4326
  const std::istream& getInFile() const
  {
    return fInFile;
  }
  void setBatchQty(unsigned int BatchQty)
  {
    fBatchQty = BatchQty;
  }

  bool chkForListOfFiles(std::string& FileName);
  std::string getNextInputDataFile();
  void add2InputDataFileList(std::string& FileName);

 private:
  enum
  {
    DEFAULTBUFFSIZE = 1024 * 1024
  };

  // don't allow anyone else to set
  void setTgtPmId(unsigned int fTgtPmId)
  {
    this->fTgtPmId = fTgtPmId;
  }

  WESDHandler& fSdh;
  boost::thread* fpThread;
  boost::mutex fFileMutex;
  bool fContinue;
  std::string fInFileName;
  std::istream fInFile;   //@BUG 4326
  std::ifstream fIfFile;  //@BUG 4326

  typedef std::list<std::string> strList;
  strList fInfileList;

  unsigned int fTgtPmId;
  unsigned int fBatchQty;
  bool fEnclEsc;  // Encl/Esc char is set
  char fEncl;     // Encl char
  char fEsc;      // Esc char
  char fDelim;    // Column Delimit char
  char* fBuff;    // main data buffer
  int fBuffSize;

  /* To support mode 1 imports from objects on S3 */
  void initS3Connection(const WECmdArgs&);
  bool doS3Import;
  std::string s3Key;
  std::string s3Secret;
  std::string s3Bucket;
  std::string s3Region;
  std::string s3Host;
  ms3_st* s3Connection;
  uint8_t* buf;
  std::unique_ptr<boost::iostreams::array_source> arrSource;
  std::unique_ptr<boost::iostreams::stream<boost::iostreams::array_source> > s3Stream;
};

} /* namespace WriteEngine */
#endif /* WE_FILEREADTHREAD_H_ */
