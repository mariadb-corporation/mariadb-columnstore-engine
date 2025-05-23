/* Copyright (C) 2019 MariaDB Corporation

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

#include "Replicator.h"
#include "IOCoordinator.h"
#include "SMLogging.h"
#include "Utilities.h"
#include "Cache.h"
#include "KVStorageInitializer.h"
#include "KVPrefixes.h"
#include "fdbcs.hpp"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/sendfile.h>
#include <boost/filesystem.hpp>
#define BOOST_SPIRIT_THREADSAFE
#include <boost/property_tree/json_parser.hpp>

#include <boost/format.hpp>
#include <iostream>

using namespace std;

namespace
{
storagemanager::Replicator* rep = NULL;
boost::mutex m;
}  // namespace
namespace storagemanager
{
Replicator::Replicator()
{
  mpConfig = Config::get();
  mpLogger = SMLogging::get();
  try
  {
    msJournalPath = mpConfig->getValue("ObjectStorage", "journal_path");
    if (msJournalPath.empty())
    {
      mpLogger->log(LOG_CRIT, "ObjectStorage/journal_path is not set");
      throw runtime_error("Please set ObjectStorage/journal_path in the storagemanager.cnf file");
    }
  }
  catch (...)
  {
    mpLogger->log(LOG_CRIT, "Could not load metadata_path from storagemanger.cnf file.");
    throw runtime_error("Please set ObjectStorage/metadata_path in the storagemanager.cnf file");
  }
  try
  {
    boost::filesystem::create_directories(msJournalPath);
  }
  catch (exception& e)
  {
    syslog(LOG_CRIT, "Failed to create %s, got: %s", msJournalPath.c_str(), e.what());
    throw e;
  }
  msCachePath = mpConfig->getValue("Cache", "path");
  if (msCachePath.empty())
  {
    mpLogger->log(LOG_CRIT, "Cache/path is not set");
    throw runtime_error("Please set Cache/path in the storagemanager.cnf file");
  }
  try
  {
    boost::filesystem::create_directories(msCachePath);
  }
  catch (exception& e)
  {
    mpLogger->log(LOG_CRIT, "Failed to create %s, got: %s", msCachePath.c_str(), e.what());
    throw e;
  }
  repUserDataWritten = repHeaderDataWritten = replicatorObjectsCreated = replicatorJournalsCreated = 0;
}

Replicator::~Replicator()
{
}

Replicator* Replicator::get()
{
  if (rep)
    return rep;
  boost::mutex::scoped_lock s(m);
  if (rep)
    return rep;
  rep = new Replicator();
  return rep;
}

void Replicator::printKPIs() const
{
  cout << "Replicator" << endl;
  cout << "\treplicatorUserDataWritten = " << repUserDataWritten << endl;
  cout << "\treplicatorHeaderDataWritten = " << repHeaderDataWritten << endl;

  cout << "\treplicatorObjectsCreated = " << replicatorObjectsCreated << endl;
  cout << "\treplicatorJournalsCreated = " << replicatorJournalsCreated << endl;
}

#define OPEN(name, mode)         \
  fd = ::open(name, mode, 0600); \
  if (fd < 0)                    \
    return fd;                   \
  ScopedCloser sc(fd);

int Replicator::newObject(const boost::filesystem::path& filename, const uint8_t* data, off_t offset,
                          size_t length)
{
  const string objectFilename = msCachePath + "/" + filename.string();
  int fd, err;
  OPEN(objectFilename.c_str(), O_WRONLY | O_CREAT);
  size_t count = 0;
  while (count < length)
  {
    err = ::pwrite(fd, &data[count], length - count, offset + count);
    if (err <= 0)
    {
      if (count > 0)  // return what was successfully written
        return count;
      else
        return err;
    }
    count += err;
  }
  repUserDataWritten += count;
  ++replicatorObjectsCreated;
  return count;
}

int Replicator::newNullObject(const boost::filesystem::path& filename, size_t length)
{
  int fd, err;
  string objectFilename = msCachePath + "/" + filename.string();

  OPEN(objectFilename.c_str(), O_WRONLY | O_CREAT);
  err = ftruncate(fd, length);

  return err;
}

ssize_t Replicator::_pwrite(int fd, const void* data, size_t length, off_t offset)
{
  ssize_t err;
  size_t count = 0;
  uint8_t* bData = (uint8_t*)data;

  do
  {
    err = ::pwrite(fd, &bData[count], length - count, offset + count);
    if (err < 0 || (err == 0 && errno != EINTR))
    {
      if (count > 0)
        return count;
      else
        return err;
    }
    count += err;
  } while (count < length);

  return count;
}

ssize_t Replicator::_write(int fd, const void* data, size_t length)
{
  ssize_t err;
  size_t count = 0;
  uint8_t* bData = (uint8_t*)data;

  do
  {
    err = ::write(fd, &bData[count], length - count);
    if (err < 0 || (err == 0 && errno != EINTR))
    {
      if (count > 0)
        return count;
      else
        return err;
    }
    count += err;
  } while (count < length);

  return count;
}

/* XXXPAT: I think we'll have to rewrite this function some; we'll have to at least clearly define
   what happens in the various error scenarios.

   To be more resilent in the face of hard errors, we may also want to redefine what a journal file is.
   If/when we cannot fix the journal file in the face of an error, there are scenarios that the read code
   will not be able to cope with.  Ex, a journal entry that says it's 200 bytes long, but there are only
   really 100 bytes.  The read code has no way to tell the difference if there is an entry that follows
   the bad entry, and that will cause an unrecoverable error.

   Initial thought on a sol'n.  Make each journal entry its own file in a tmp dir, ordered by a sequence
   number in the filename.  Then, one entry cannot affect the others, and the end of the file is unambiguously
   the end of the data.  On successful write, move the file to where it should be.  This would also prevent
   the readers from ever seeing bad data, and possibly reduce the size of some critical sections.

   Benefits would be data integrity, and possibly add'l parallelism.  The downside is of course, a higher
   number of IO ops for the same operation.
*/

int Replicator::addJournalEntry(const boost::filesystem::path& filename, const uint8_t* data, off_t offset,
                                size_t length)
{
  uint64_t offlen[] = {(uint64_t)offset, length};
  const int version = 1;
  const auto journalName = getJournalName(msJournalPath + "/" + filename.string() + ".journal");
  const auto journalSizeName = getJournalName(msJournalPath + "/" + filename.string() + "_size" + ".journal");
  boost::filesystem::path firstDir = *((filename).begin());
  const uint64_t thisEntryMaxOffset = (offset + length - 1);
  string dataStr;
  size_t dataStrOffset = 0;

  auto kvStorage = KVStorageInitializer::getStorageInstance();
  auto keyGen = std::make_shared<FDBCS::BoostUIDKeyGenerator>();
  FDBCS::BlobHandler journalHandler(keyGen);
  auto resultPair = journalHandler.readBlob(kvStorage, journalName);
  const std::string& journalData = resultPair.second;
  const bool journalExists = resultPair.first;
  if (!journalExists)
  {
    // create new journal file with header
    string header = (boost::format("{ \"version\" : \"%03i\", \"max_offset\" : \"%011u\" }") % version %
                     thisEntryMaxOffset)
                        .str();
    const size_t headerLength = header.length();
    dataStr.resize(headerLength + 1 + JOURNAL_ENTRY_HEADER_SIZE + length);
    std::memcpy(&dataStr[dataStrOffset], header.c_str(), headerLength);
    // Specifies the end of the header.
    dataStr[headerLength] = 0;
    dataStrOffset = headerLength + 1;
    repHeaderDataWritten += headerLength + 1;
    Cache::get()->newJournalEntry(firstDir, headerLength + 1);
    ++replicatorJournalsCreated;
  }
  else
  {
    size_t tmp;
    std::shared_ptr<char[]> headertxt;
    try
    {
      headertxt = seekToEndOfHeader1_(journalData, &tmp);
    }
    catch (std::runtime_error& e)
    {
      mpLogger->log(LOG_CRIT, "%s", e.what());
      errno = EIO;
      return -1;
    }
    catch (...)
    {
      mpLogger->log(LOG_CRIT, "Unknown exception caught during seekToEndOfHeader1.");
      errno = EIO;
      return -1;
    }
    stringstream ss;
    ss << headertxt.get();
    boost::property_tree::ptree header;
    try
    {
      boost::property_tree::json_parser::read_json(ss, header);
    }
    catch (const boost::property_tree::json_parser::json_parser_error& e)
    {
      mpLogger->log(LOG_CRIT, "%s", e.what());
      errno = EIO;
      return -1;
    }
    catch (...)
    {
      mpLogger->log(LOG_CRIT, "Unknown exception caught during read_json.");
      errno = EIO;
      return -1;
    }
    assert(header.get<int>("version") == 1);
    const uint64_t currentMaxOffset = header.get<uint64_t>("max_offset");
    dataStr.resize(journalData.size() + JOURNAL_ENTRY_HEADER_SIZE + length);
    size_t journalOffset = 0;

    if (thisEntryMaxOffset > currentMaxOffset)
    {
      string header = (boost::format("{ \"version\" : \"%03i\", \"max_offset\" : \"%011u\" }") % version %
                       thisEntryMaxOffset)
                          .str();
      const size_t headerLenght = header.length();
      std::memcpy(&dataStr[0], header.c_str(), headerLenght);
      dataStr[headerLenght] = 0;
      dataStrOffset = headerLenght + 1;
      journalOffset = headerLenght + 1;
      repHeaderDataWritten += headerLenght + 1;
    }

    std::memcpy(&dataStr[dataStrOffset], &journalData[journalOffset], journalData.size() - journalOffset);
    dataStrOffset = journalData.size();
  }

  std::memcpy(&dataStr[dataStrOffset], offlen, JOURNAL_ENTRY_HEADER_SIZE);
  dataStrOffset += JOURNAL_ENTRY_HEADER_SIZE;
  repHeaderDataWritten += JOURNAL_ENTRY_HEADER_SIZE;
  std::memcpy(&dataStr[dataStrOffset], data, length);
  dataStrOffset += length;
  assert(dataStr.size() == dataStrOffset);

  if (journalExists && !journalHandler.removeBlob(kvStorage, journalName))
  {
    mpLogger->log(LOG_CRIT, "Cannot remove journal blob.");
    errno = EIO;
    return -1;
  }

  if (!journalHandler.writeBlob(kvStorage, journalName, dataStr))
  {
    mpLogger->log(LOG_CRIT, "Cannot write journal blob.");
    errno = EIO;
    return -1;
  }

  {
    auto tnx = kvStorage->createTransaction();
    tnx->set(journalSizeName, std::to_string(dataStr.size()));
    if (!tnx->commit())
    {
      mpLogger->log(LOG_CRIT, "Cannot write journal size.");
      errno = EIO;
      return -1;
    }
  }

  repUserDataWritten += length;
  return length;
}

int Replicator::remove(const boost::filesystem::path& filename, Flags flags)
{
  int ret = 0;

  if (flags & NO_LOCAL)
    return 0;  // not implemented yet

  try
  {
    //#ifndef NDEBUG
    //    assert(boost::filesystem::remove_all(filename) > 0);
    //#else
    boost::filesystem::remove_all(filename);
    //#endif
  }
  catch (boost::filesystem::filesystem_error& e)
  {
#ifndef NDEBUG
    cout << "Replicator::remove(): caught an execption: " << e.what() << endl;
    assert(0);
#endif
    errno = e.code().value();
    ret = -1;
  }
  return ret;
}

int Replicator::removeJournal(const boost::filesystem::path& filename)
{
  auto kvStorage = KVStorageInitializer::getStorageInstance();
  auto keyGen = std::make_shared<FDBCS::BoostUIDKeyGenerator>();
  FDBCS::BlobHandler journalHandler(keyGen);
  if (!journalHandler.removeBlob(kvStorage, filename.string()))
  {
    return -1;
  }
  return 0;
}

int Replicator::removeJournalSize(const boost::filesystem::path& filename)
{
  auto kvStorage = KVStorageInitializer::getStorageInstance();
  auto tnx = kvStorage->createTransaction();
  tnx->remove(filename.string());
  return tnx->commit();
}


int Replicator::updateMetadata(MetadataFile& meta)
{
  return meta.writeMetadata();
}

}  // namespace storagemanager
