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
 * we_filereadthread.cpp
 *
 *  Created on: Oct 25, 2011
 *      Author: bpaul
 */

#include "we_messages.h"
#include "we_sdhandler.h"
#include "we_splitterapp.h"

#include <boost/thread/condition.hpp>
#include <boost/scoped_array.hpp>
#include <boost/thread.hpp>
using namespace boost;

#include "messagequeue.h"
#include "bytestream.h"
using namespace messageqcpp;

#include <fstream>
#include <istream>
#include <list>
using namespace std;

#include "we_filereadthread.h"

namespace WriteEngine
{
void WEReadThreadRunner::operator()()
{
  try
  {
    fRef.feedData();
  }
  catch (std::exception& ex)
  {
    throw runtime_error(ex.what());
  }
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------

WEFileReadThread::WEFileReadThread(WESDHandler& aSdh)
 : fSdh(aSdh)
 , fpThread(0)
 , fFileMutex()
 , fContinue(true)
 , fInFileName()
 , fInFile(std::cin.rdbuf())
 ,  //@BUG 4326
 fTgtPmId(0)
 , fBatchQty(0)
 , fEnclEsc(false)
 , fEncl('\0')
 , fEsc('\\')
 , fDelim('|')
 , fSkipRows(0)
{
  // TODO batch qty to get from config
  fBatchQty = 10000;

  if (fSdh.getReadBufSize() < DEFAULTBUFFSIZE)
  {
    fBuffSize = DEFAULTBUFFSIZE;
  }
  else
  {
    fBuffSize = fSdh.getReadBufSize();
  }

  const WECmdArgs& args = fSdh.fRef.fCmdArgs;
  initS3Connection(args);
}

// WEFileReadThread::WEFileReadThread(const WEFileReadThread& rhs):fSdh(rhs.fSdh)
//{
//	// TODO copy constructor
//}

WEFileReadThread::~WEFileReadThread()
{
  // if(fInFile.is_open()) fInFile.close(); //@BUG 4326
  if (fIfFile.is_open())
    fIfFile.close();

  setTgtPmId(0);

  if (fpThread)
  {
    delete fpThread;
  }

  fpThread = 0;
  // cout << "WEFileReadThread destructor called" << endl;

  if (doS3Import)
  {
    ms3_deinit(s3Connection);
    ms3_library_deinit();
    if (buf)
    {
      s3Stream.reset();
      arrSource.reset();
      free(buf);
    }
  }
}

//------------------------------------------------------------------------------

void WEFileReadThread::reset()
{
  // if(fInFile.is_open()) fInFile.close(); //@BUG 4326
  if (fIfFile.is_open())
    fIfFile.close();

  setTgtPmId(0);

  if (fpThread)
  {
    delete fpThread;
  }

  fpThread = 0;
  // cout << "WEFileReadThread destructor called" << endl;
  this->setContinue(true);

  if (buf)
  {
    arrSource.reset();
    s3Stream.reset();
    free(buf);
    buf = NULL;
  }
}
//------------------------------------------------------------------------------

void WEFileReadThread::setup(std::string FileName)
{
  if (fSdh.getDebugLvl())
    cout << "WEFileReadThread::setup : *** Input files = " << FileName << endl;

  reset();

  try
  {
    char aEncl = fSdh.getEnclChar();
    char aEsc = fSdh.getEscChar();
    char aDelim = fSdh.getDelimChar();

    if (aEncl)
      fEncl = aEncl;

    if (aEsc)
      fEsc = aEsc;

    if (aDelim)
      fDelim = aDelim;

    if (aEncl != 0)
      fEnclEsc = true;

    fSkipRows = fSdh.getSkipRows();

    // BUG 4342 - Need to support "list of infiles"
    // chkForListOfFiles(FileName); - List prepared in sdhandler.

    string aStrName = getNextInputDataFile();

    if (fSdh.getDebugLvl() > 2)
      cout << "Next InFileName = " << aStrName << endl;

    setInFileName(aStrName);
    // setInFileName(FileName);
    openInFile();
    // set the target PM
    fpThread = new boost::thread(WEReadThreadRunner(*this));
  }
  catch (std::exception& ex)
  {
    // cout << ex.what() << endl;
    // throw runtime_error("Exception occurred in WEFileReadThread\n");
    throw runtime_error(ex.what());
  }

  if (fpThread)
  {
    // Need to send a all clear??
  }
}

//------------------------------------------------------------------------------

bool WEFileReadThread::chkForListOfFiles(const std::string& fileName)
{
  // cout << "Inside chkForListOfFiles("<< FileName << ")" << endl;
  istringstream iss(fileName);
  ostringstream oss;
  size_t start = 0, end = 0;
  const char* sep = " ,|";
  ms3_status_st ms3status;

  do
  {
    end = fileName.find_first_of(sep, start);
    std::string aFile = fileName.substr(start, end - start);
    if (aFile == "STDIN" || aFile == "stdin")
      aFile = "/dev/stdin";

    if (fSdh.getDebugLvl() > 1)
      cout << "Next Input File " << aFile << endl;

    if ((!doS3Import && access(aFile.c_str(), O_RDONLY) != 0) ||
        (doS3Import && ms3_status(s3Connection, s3Bucket.c_str(), aFile.c_str(), &ms3status) != 0))
    {
      oss << "Could not access " << aFile;
      throw runtime_error(oss.str());
    }

    fInfileList.push_back(aFile);
    start = end + 1;
  } while (end != string::npos);

  // cout << "Going out chkForListOfFiles("<< FileName << ")" << endl;

  return false;
}
//------------------------------------------------------------------------------

std::string WEFileReadThread::getNextInputDataFile()
{
  std::string aNextFile;

  if (fInfileList.size() > 0)
  {
    aNextFile = fInfileList.front();
    fInfileList.pop_front();
  }

  // cout << "Next Input DataFile = " << aNextFile << endl;

  return aNextFile;
}
//------------------------------------------------------------------------------

void WEFileReadThread::add2InputDataFileList(const std::string& fileName)
{
  fInfileList.push_front(fileName);
}
//------------------------------------------------------------------------------

void WEFileReadThread::shutdown()
{
  this->setContinue(false);
  boost::mutex::scoped_lock aLock(fFileMutex);  // wait till readDataFile() finish

  // if(fInFile.is_open()) fInFile.close(); //@BUG 4326
  if (fIfFile.is_open())
    fIfFile.close();
  if (buf)
  {
    s3Stream.reset();
    arrSource.reset();
    free(buf);
    buf = NULL;
  }
}

//------------------------------------------------------------------------------

void WEFileReadThread::feedData()
{
  unsigned int aRowCnt = 0;
  const unsigned int c10mSec = 10000;

  while (isContinue())
  {
    unsigned int TgtPmId = getTgtPmId();

    if (TgtPmId == 0)
    {
      setTgtPmId(fSdh.getNextPm2Feed());
      TgtPmId = getTgtPmId();
    }

    if ((TgtPmId > 0) && (fInFile.good()))
    {
      try
      {
        messageqcpp::SBS aSbs(new messageqcpp::ByteStream);

        if (fSdh.getImportDataMode() == IMPORT_DATA_TEXT)
          aRowCnt = readDataFile(aSbs);
        else
          aRowCnt = readBinaryDataFile(aSbs, fSdh.getTableRecLen());

        // cout << "Length " << aSbs->length() <<endl;    - for debug
        fSdh.updateRowTx(aRowCnt, TgtPmId);
        boost::mutex::scoped_lock aLock(fSdh.fSendMutex);
        fSdh.send2Pm(aSbs, TgtPmId);
        aLock.unlock();
        setTgtPmId(0);  // reset PmId. Send the data to next least data
      }
      catch (std::exception& ex)
      {
        throw runtime_error(ex.what());
      }
    }
    else
    {
      usleep(c10mSec);
      setTgtPmId(0);
    }

    // Finish reading file and thread can go away once data sent
    if (fInFile.eof())
    {
      if (fInfileList.size() != 0)
      {
        if (fIfFile.is_open())
          fIfFile.close();

        string aStrName = getNextInputDataFile();
        setInFileName(aStrName);
        openInFile();
      }
      else
      {
        // if there is no more files to be read send EOD
        // cout << "Sending EOD message to PM" << endl;
        fSdh.sendEODMsg();
        setContinue(false);
      }
    }
  }
}

// helper to copy line from input file to the stringstream.
// not very performant, yet.
static void copyLine(std::vector<char>& out, std::istream& is)
{
  out.clear();
  while (is.good() && !is.eof())
  {
    char c = is.get();
    out.push_back(c);
    if (c == '\n')
    {
      break;
    }
  }
}
//------------------------------------------------------------------------------
// Read input data as ASCII text
//------------------------------------------------------------------------------
unsigned int WEFileReadThread::readDataFile(messageqcpp::SBS& Sbs)
{
  fBuff.reserve(fBuffSize * 2);	
  boost::mutex::scoped_lock aLock(fFileMutex);

  // For now we are going to send KEEPALIVES
  //*Sbs << (ByteStream::byte)(WE_CLT_SRV_KEEPALIVE);
  if (fInFile.good() && !fInFile.eof())
  {
    // cout << "Inside WEFileReadThread::readDataFile" << endl;
    // char aBuff[1024*1024];			// TODO May have to change it later
    // char*pStart = aBuff;
    unsigned int aIdx = 0;
    *Sbs << static_cast<ByteStream::byte>(WE_CLT_SRV_DATA);

    while (!fInFile.eof() && aIdx < getBatchQty())
    {
      if (fSkipRows > 0)
      {
        fSkipRows--;
	copyLine(fBuff, fInFile);
        if (fSdh.getDebugLvl() > 3)
        {
          if (fBuff.size() > 0)
          {
	    std::string s(fBuff.data(), fBuff.size());
            cout << "Skip header row (" << fSkipRows<< " to go): " << s << endl;
          }
        }
        continue;
      }

      if (fEnclEsc)
      {
	cout << "getting next row" << endl;
        // pStart = aBuff;
        getNextRow(fInFile, fBuff);
      }
      else
      {
	      cout << "copying line" << endl;
        copyLine(fBuff, fInFile);
      }

      if (fBuff.size())
      {
        if (fBuff[fBuff.size() - 1] != '\n')
          fBuff.push_back('\n');

        if (fSdh.getDebugLvl() > 3)
	{
	  std::string s(fBuff.data(), fBuff.size());
          cout << "Data Read " << s << endl;
	}

        (*Sbs).append(reinterpret_cast<ByteStream::byte*>(fBuff.data()), fBuff.size());
        aIdx++;

        if (fSdh.getDebugLvl() > 2)
          cout << "File data line = " << aIdx << endl;
      }

      // for debug
      // if(fSdh.getDebugLvl()>3) cout << aIdx << endl;
    }  // while

    return aIdx;
  }  // if

  return 0;
}

//------------------------------------------------------------------------------
// Read input data as binary data
//------------------------------------------------------------------------------
unsigned int WEFileReadThread::readBinaryDataFile(messageqcpp::SBS& Sbs, unsigned int recLen)
{
  boost::mutex::scoped_lock aLock(fFileMutex);

  if ((fInFile.good()) && (!fInFile.eof()))
  {
    unsigned int aIdx = 0;
    unsigned int aLen = 0;
    *Sbs << (ByteStream::byte)(WE_CLT_SRV_DATA);

    while ((!fInFile.eof()) && (aIdx < getBatchQty()))
    {
      fBuff.resize(recLen);
      fInFile.read(fBuff.data(), recLen);
      aLen = fInFile.gcount();

      if (aLen > 0)
      {
        (*Sbs).append(reinterpret_cast<ByteStream::byte*>(fBuff.data()), aLen);
        aIdx++;

        if (fSdh.getDebugLvl() > 2)
          cout << "Binary input data line = " << aIdx << endl;

        if (aLen != recLen)
        {
          cout << "Binary input data does not end on record boundary;"
                  " Last record is "
               << aLen << " bytes long."
               << " Expected record length is: " << recLen << endl;
        }
      }
    }  // while

    return aIdx;
  }  // if

  return 0;
}

//------------------------------------------------------------------------------

void WEFileReadThread::openInFile()
{
  try
  {
    /*  If an S3 transfer
        use ms3 lib to d/l data into mem
        use boost::iostreams to wrap the mem in a stream interface
        point infile's stream buffer to it.

        MCOL-4576: The options to setup S3 with cpimport have been removed and this
        code is unreachable. However we may need to resurrect it at some point in some form.
        Performance issues with extremely large data files as well as the fact files larger
        than system memory will cause an OOM error. Multipart downloads/uploads need to be
        implemented or more likely a different streaming solution developed with external API tools

        MCOL-4576 work around is to use 3rd party CLI tools and pipe data file from S3 bucket
        into cpimport stdin. 3rd party tooling for large object downloads will be more efficient.
    */

    if (fSdh.getDebugLvl())
      cout << "Input Filename: " << fInFileName << endl;

    if (doS3Import)
    {
      size_t bufLen = 0;
      if (buf)
      {
        s3Stream.reset();
        arrSource.reset();
        free(buf);
        buf = NULL;
      }
      if (fSdh.getDebugLvl())
        cout << "Downloading " << fInFileName << endl;
      int err = ms3_get(s3Connection, s3Bucket.c_str(), fInFileName.c_str(), &buf, &bufLen);
      if (fSdh.getDebugLvl())
        cout << "Download complete." << endl;
      if (err)
      {
        ostringstream os;
        if (ms3_server_error(s3Connection))
          os << "Download of '" << fInFileName
             << "' failed.  Error from the server: " << ms3_server_error(s3Connection);
        else
          os << "Download of '" << fInFileName << "' failed.  Got '" << ms3_error(err) << "'.";
        throw runtime_error(os.str());
      }

      arrSource.reset(new boost::iostreams::array_source((char*)buf, bufLen));
      s3Stream.reset(new boost::iostreams::stream<boost::iostreams::array_source>(*arrSource));
      fInFile.rdbuf(s3Stream->rdbuf());
    }

    else if (fInFileName == "/dev/stdin")
    {
      char aDefCon[16], aGreenCol[16];
      snprintf(aDefCon, sizeof(aDefCon), "\033[0m");
      snprintf(aGreenCol, sizeof(aGreenCol), "\033[0;32m");

      if (fSdh.getDebugLvl())  // BUG 4195
        cout << aGreenCol << "trying to read from STDIN... " << aDefCon << endl;
      fInFile.rdbuf(cin.rdbuf());
    }

    //@BUG 4326
    else if (fInFileName != "/dev/stdin")
    {
      if (!fIfFile.is_open())
      {
        if (fSdh.getImportDataMode() == IMPORT_DATA_TEXT)
          fIfFile.open(fInFileName.c_str());
        else  // @bug 5193: binary import
          fIfFile.open(fInFileName.c_str(), std::ios_base::in | std::ios_base::binary);
      }

      if (!fIfFile.good())
        throw runtime_error("Could not open Input file " + fInFileName);

      fInFile.rdbuf(fIfFile.rdbuf());  //@BUG 4326
    }

    // Got new file, so reset fSkipRows
    fSkipRows = fSdh.getSkipRows();

    //@BUG 4326  -below three lines commented out
    //		if (!fInFile.is_open()) fInFile.open(fInFileName.c_str());
    //		if (!fInFile.good())
    //			throw runtime_error("Could not open Input file "+fInFileName);
  }
  catch (std::exception& ex)
  {
    cout << "Error in Opening input data file " << fInFileName << endl;
    throw runtime_error(ex.what());  // BUG 4201 FIX
  }
}

//------------------------------------------------------------------------------

int WEFileReadThread::getNextRow(istream& ifs, std::vector<char>& buf)
{
  buf.resize(0); // keep allocated capacity.
  // const char ENCL ='\"';		//TODO for time being
  // const char ESC = '\0';		//TODO for time being
  const char ENCL = fEncl;
  const char ESC = fEsc;
  bool aTrailEsc = false;
  int aCh = ifs.get();

  while (ifs.good())
  {
    if (aCh == ENCL)
    {
      // we got the first enclosedBy char.
      buf.push_back(aCh);
      aCh = ifs.get();

      // cout << "aCh 1 = " << aCh << endl;
      while (aCh != ENCL)  // Loop thru till we hit another one
      {
        if (aCh == ESC)  // check spl cond ESC inside ENCL of '\n' here
        {
          buf.push_back(aCh);
          aCh = ifs.get();
          buf.push_back(aCh);
          aCh = ifs.get();  // get the next char for while loop
                            // cout << "aCh 2 = " << aCh << endl;
        }                   // case ESC
        else
        {
          buf.push_back(aCh);
          aCh = ifs.get();
          // cout << "aCh 3 = " << aCh << endl;
        }
      }

      buf.push_back(aCh);     // ENCL char got
      aTrailEsc = true;  //@BUG 4641
    }                    // case ENCL
    else if (aCh == ESC)
    {
      buf.push_back(aCh);
      aCh = ifs.get();
      buf.push_back(aCh);
      // cout << "aCh 4 = " << aCh << endl;
    }  // case ESC
    else
    {
      buf.push_back(aCh);
      // cout << "aCh 5 = " << aCh << endl;
    }

    // cout << "pBuf1 " << pBuf << endl;
    if (aCh == '\n')
      break;  // we got a full row

    aCh = ifs.get();

    // BUG 4641 To avoid seg fault when a wrong/no ESC char provided.
    while (aTrailEsc)
    {
      // BUG 4903  EOF, to handle files ending w/ EOF and w/o '\n'
      if ((aCh == '\n') || (aCh == EOF) || (aCh == fDelim))
      {
        aTrailEsc = false;
        break;
      }
      else
      {
        buf.push_back(aCh);
        aCh = ifs.get();
      }
    }

  }  // end of while loop

  return buf.size();
}

void WEFileReadThread::initS3Connection(const WECmdArgs& args)
{
  doS3Import = args.isS3Import();
  if (doS3Import)
  {
    s3Key = args.getS3Key();
    s3Secret = args.getS3Secret();
    s3Bucket = args.getS3Bucket();
    s3Region = args.getS3Region();
    s3Host = args.getS3Host();
    ms3_library_init();
    s3Connection =
        ms3_init(s3Key.c_str(), s3Secret.c_str(), s3Region.c_str(), (s3Host.empty() ? nullptr : s3Host.c_str()));
    if (!s3Connection)
      throw runtime_error("failed to get an S3 connection");
  }
  else
    s3Connection = nullptr;
  buf = nullptr;
}

//------------------------------------------------------------------------------

} /* namespace WriteEngine */
