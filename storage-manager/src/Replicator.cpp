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
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/sendfile.h>
#include <boost/filesystem.hpp>
#define BOOST_SPIRIT_THREADSAFE
#include <boost/property_tree/json_parser.hpp>
#include <boost/shared_array.hpp>
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
  int fd, err;
  string objectFilename = msCachePath + "/" + filename.string();

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
  int fd, err;
  uint64_t offlen[] = {(uint64_t)offset, length};
  size_t count = 0;
  int version = 1;
  int l_errno;
  char errbuf[80];
  bool bHeaderChanged = false;
  string headerRollback = "";
  string journalFilename = msJournalPath + "/" + filename.string() + ".journal";
  boost::filesystem::path firstDir = *((filename).begin());
  uint64_t thisEntryMaxOffset = (offset + length - 1);

  uint64_t currentMaxOffset = 0;
  bool exists = boost::filesystem::exists(journalFilename);
  OPEN(journalFilename.c_str(), (exists ? O_RDWR : O_WRONLY | O_CREAT))

  if (!exists)
  {
    bHeaderChanged = true;
    // create new journal file with header
    string header = (boost::format("{ \"version\" : \"%03i\", \"max_offset\" : \"%011u\" }") % version %
                     thisEntryMaxOffset)
                        .str();
    err = _write(fd, header.c_str(), header.length() + 1);
    l_errno = errno;
    repHeaderDataWritten += (header.length() + 1);
    if ((uint)err != (header.length() + 1))
    {
      // return the error because the header for this entry on a new journal file failed
      mpLogger->log(LOG_CRIT, "Replicator::addJournalEntry: Writing journal header failed (%s).",
                    strerror_r(l_errno, errbuf, 80));
      errno = l_errno;
      return err;
    }
    Cache::get()->newJournalEntry(firstDir, header.length() + 1);
    ++replicatorJournalsCreated;
  }
  else
  {
    // read the existing header and check if max_offset needs to be updated
    size_t tmp;
    boost::shared_array<char> headertxt;
    try
    {
      headertxt = seekToEndOfHeader1(fd, &tmp);
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
    headerRollback = headertxt.get();
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
    uint64_t currentMaxOffset = header.get<uint64_t>("max_offset");
    if (thisEntryMaxOffset > currentMaxOffset)
    {
      bHeaderChanged = true;
      string header = (boost::format("{ \"version\" : \"%03i\", \"max_offset\" : \"%011u\" }") % version %
                       thisEntryMaxOffset)
                          .str();
      err = _pwrite(fd, header.c_str(), header.length() + 1, 0);
      l_errno = errno;
      repHeaderDataWritten += (header.length() + 1);
      if ((uint)err != (header.length() + 1))
      {
        // only the header was possibly changed rollback attempt
        mpLogger->log(LOG_CRIT,
                      "Replicator::addJournalEntry: Updating journal header failed. "
                      "Attempting to rollback and continue.");
        int rollbackErr = _pwrite(fd, headerRollback.c_str(), headerRollback.length() + 1, 0);
        if ((uint)rollbackErr == (headerRollback.length() + 1))
          mpLogger->log(LOG_CRIT, "Replicator::addJournalEntry: Rollback of journal header success.");
        else
          mpLogger->log(LOG_CRIT, "Replicator::addJournalEntry: Rollback of journal header failed!");
        errno = l_errno;
        if (err < 0)
          return err;
        else
          return 0;
      }
    }
  }

  off_t entryHeaderOffset = ::lseek(fd, 0, SEEK_END);

  err = _write(fd, offlen, JOURNAL_ENTRY_HEADER_SIZE);
  l_errno = errno;
  repHeaderDataWritten += JOURNAL_ENTRY_HEADER_SIZE;
  if (err != JOURNAL_ENTRY_HEADER_SIZE)
  {
    // this entry failed so if the header was updated roll it back
    if (bHeaderChanged)
    {
      mpLogger->log(LOG_CRIT,
                    "Replicator::addJournalEntry: write journal entry header failed. Attempting to rollback "
                    "and continue.");
      // attempt to rollback top level header
      int rollbackErr = _pwrite(fd, headerRollback.c_str(), headerRollback.length() + 1, 0);
      if ((uint)rollbackErr != (headerRollback.length() + 1))
      {
        mpLogger->log(LOG_CRIT, "Replicator::addJournalEntry: Rollback of journal header failed! (%s)",
                      strerror_r(errno, errbuf, 80));
        errno = l_errno;
        if (err < 0)
          return err;
        else
          return 0;
      }
    }
    int rollbackErr = ::ftruncate(fd, entryHeaderOffset);
    if (rollbackErr != 0)
    {
      mpLogger->log(LOG_CRIT, "Replicator::addJournalEntry: Truncate to previous EOF failed! (%s)",
                    strerror_r(errno, errbuf, 80));
      errno = l_errno;
      if (err < 0)
        return err;
      else
        return 0;
    }
    l_errno = errno;
    return err;
  }
  while (count < length)
  {
    err = _write(fd, &data[count], length - count);
    if (err < 0)
    {
      l_errno = errno;
      /* XXXBEN: Attempt to update entry header with the partial write and write it.
         IF the write fails to update entry header report an error to IOC and logging.
         */
      if (count > 0)  // return what was successfully written
      {
        mpLogger->log(LOG_CRIT,
                      "Replicator::addJournalEntry: Got '%s' writing a journal entry. "
                      "Attempting to update and continue.",
                      strerror_r(l_errno, errbuf, 80));
        // Update the file header max_offset if necessary and possible
        thisEntryMaxOffset = (offset + count - 1);
        if (thisEntryMaxOffset > currentMaxOffset)
        {
          string header = (boost::format("{ \"version\" : \"%03i\", \"max_offset\" : \"%011u\" }") % version %
                           thisEntryMaxOffset)
                              .str();
          int rollbackErr = _pwrite(fd, header.c_str(), header.length() + 1, 0);
          if ((uint)rollbackErr != (header.length() + 1))
          {
            mpLogger->log(LOG_CRIT, "Replicator::addJournalEntry: Update of journal header failed! (%s)",
                          strerror_r(errno, errbuf, 80));
            errno = l_errno;
            return err;
          }
        }
        // Update the journal entry header
        offlen[1] = count;
        int rollbackErr = _pwrite(fd, offlen, JOURNAL_ENTRY_HEADER_SIZE, entryHeaderOffset);
        if ((uint)rollbackErr != JOURNAL_ENTRY_HEADER_SIZE)
        {
          mpLogger->log(LOG_CRIT, "Replicator::addJournalEntry: Update of journal entry header failed! (%s)",
                        strerror_r(errno, errbuf, 80));
          errno = l_errno;
          return err;
        }
        // return back what we did write
        mpLogger->log(LOG_CRIT, "Replicator::addJournalEntry: Partial write success.");
        repUserDataWritten += count;
        return count;
      }
      else
      {
        // If the header was changed rollback and reset EOF
        // Like this never happened
        // Currently since this returns the err from the first write. IOC returns -1 and writeTask returns an
        // error So system is likely broken in some way
        if (bHeaderChanged)
        {
          mpLogger->log(LOG_CRIT,
                        "Replicator::addJournalEntry: write journal entry failed (%s). "
                        "Attempting to rollback and continue.",
                        strerror_r(l_errno, errbuf, 80));
          // attempt to rollback top level header
          string header =
              (boost::format("{ \"version\" : \"%03i\", \"max_offset\" : \"%011u\" }") % version % 0).str();
          int rollbackErr = _pwrite(fd, header.c_str(), header.length() + 1, 0);
          if ((uint)rollbackErr != (header.length() + 1))
          {
            mpLogger->log(LOG_CRIT, "Replicator::addJournalEntry: Rollback of journal header failed (%s)!",
                          strerror_r(errno, errbuf, 80));
            errno = l_errno;
            return err;
          }
        }
        int rollbackErr = ::ftruncate(fd, entryHeaderOffset);
        if (rollbackErr != 0)
        {
          mpLogger->log(LOG_CRIT, "Replicator::addJournalEntry: Remove entry header failed (%s)!",
                        strerror_r(errno, errbuf, 80));
          errno = l_errno;
          return err;
        }
        mpLogger->log(LOG_CRIT, "Replicator::addJournalEntry: Write failed. Journal file restored.");
        errno = l_errno;
        return err;
      }
    }
    count += err;
  }

  repUserDataWritten += count;
  return count;
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

int Replicator::updateMetadata(MetadataFile& meta)
{
  return meta.writeMetadata();
}

}  // namespace storagemanager
