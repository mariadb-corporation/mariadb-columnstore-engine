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

/*
 * MetadataFile.cpp
 */
#include "MetadataFile.h"
#include "KVStorageInitializer.h"
#include <set>
#include <boost/filesystem.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/lexical_cast.hpp>
#include <filesystem>
#define BOOST_SPIRIT_THREADSAFE
#ifndef __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

#include <boost/property_tree/ptree.hpp>

#ifndef __clang__
#pragma GCC diagnostic pop
#endif
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/random_generator.hpp>
#include <unistd.h>
#include "fdbcs.hpp"
#include "KVPrefixes.hpp"

#define max(x, y) (x > y ? x : y)
#define min(x, y) (x < y ? x : y)

using namespace std;
namespace bpt = boost::property_tree;
namespace bf = boost::filesystem;

namespace
{
boost::mutex mdfLock;
storagemanager::MetadataFile::MetadataConfig* inst = NULL;
uint64_t metadataFilesAccessed = 0;
}  // namespace

namespace storagemanager
{

inline std::string getKeyName(const std::string& fileName)
{
  return KVPrefixes[static_cast<uint32_t>(KVPrefixId::SM_META)] + fileName;
}

MetadataFile::MetadataConfig* MetadataFile::MetadataConfig::get()
{
  if (inst)
    return inst;
  boost::unique_lock<boost::mutex> s(mdfLock);
  if (inst)
    return inst;
  inst = new MetadataConfig();
  return inst;
}

MetadataFile::MetadataConfig::MetadataConfig()
{
  Config* config = Config::get();
  SMLogging* logger = SMLogging::get();

  try
  {
    mObjectSize = stoul(config->getValue("ObjectStorage", "object_size"));
  }
  catch (...)
  {
    logger->log(LOG_CRIT, "ObjectStorage/object_size must be set to a numeric value");
    throw runtime_error("Please set ObjectStorage/object)size in the storagemanager.cnf file");
  }

  try
  {
    msMetadataPath = config->getValue("ObjectStorage", "metadata_path");
    if (msMetadataPath.empty())
    {
      logger->log(LOG_CRIT, "ObjectStorage/metadata_path is not set");
      throw runtime_error("Please set ObjectStorage/metadata_path in the storagemanager.cnf file");
    }
  }
  catch (...)
  {
    logger->log(LOG_CRIT, "ObjectStorage/metadata_path is not set");
    throw runtime_error("Please set ObjectStorage/metadata_path in the storagemanager.cnf file");
  }

  try
  {
    cout << "create dir for meta " << msMetadataPath.string() << endl;
    boost::filesystem::create_directories(msMetadataPath);
  }
  catch (exception& e)
  {
    logger->log(LOG_CRIT, "Failed to create %s, got: %s", msMetadataPath.c_str(), e.what());
    throw e;
  }
}

MetadataFile::MetadataFile()
{
  mpConfig = MetadataConfig::get();
  mpLogger = SMLogging::get();
  mVersion = 1;
  mRevision = 1;
  _exists = false;
}

MetadataFile::MetadataFile(const boost::filesystem::path& filename)
{
  mpConfig = MetadataConfig::get();
  mpLogger = SMLogging::get();
  _exists = true;

  mFilename = mpConfig->msMetadataPath / (filename.string() + ".meta");
  metaKVName_ = getKeyName(mFilename.string());

  boost::unique_lock<boost::mutex> s(jsonCache.getMutex());
  jsontree = jsonCache.get(mFilename);
  if (!jsontree)
  {
    auto kvStorage = KVStorageInitializer::getStorageInstance();
    auto keyGen = std::make_shared<FDBCS::BoostUIDKeyGenerator>();
    FDBCS::BlobHandler blobReader(keyGen);
    auto rPair = blobReader.readBlob(kvStorage, metaKVName_);
    if (rPair.first)
    {
      jsontree.reset(new bpt::ptree());
      stringstream stream(rPair.second);
      boost::property_tree::read_json(stream, *jsontree);
      jsonCache.put(mFilename, jsontree);
      s.unlock();
      mVersion = 1;
      mRevision = jsontree->get<int>("revision");
    }
    else
    {
      mVersion = 1;
      mRevision = 1;
      makeEmptyJsonTree();
      s.unlock();
      writeMetadata();
    }
  }
  else
  {
    s.unlock();
    mVersion = 1;
    mRevision = jsontree->get<int>("revision");
  }
  ++metadataFilesAccessed;
}

MetadataFile::MetadataFile(const boost::filesystem::path& filename, no_create_t, bool appendExt)
{
  mpConfig = MetadataConfig::get();
  mpLogger = SMLogging::get();

  mFilename = filename;
  metaKVName_ = getKeyName(mFilename.string());

  if (appendExt)
    mFilename = mpConfig->msMetadataPath / (mFilename.string() + ".meta");

  boost::unique_lock<boost::mutex> s(jsonCache.getMutex());
  jsontree = jsonCache.get(mFilename);
  if (!jsontree)
  {
    auto kvStorage = KVStorageInitializer::getStorageInstance();
    auto keyGen = std::make_shared<FDBCS::BoostUIDKeyGenerator>();
    FDBCS::BlobHandler blobReader(keyGen);
    auto rPair = blobReader.readBlob(kvStorage, metaKVName_);
    if (rPair.first)
    {
      _exists = true;
      jsontree.reset(new bpt::ptree());
      stringstream stream(rPair.second);
      boost::property_tree::read_json(stream, *jsontree);
      jsonCache.put(mFilename, jsontree);
      s.unlock();
      mVersion = 1;
      mRevision = jsontree->get<int>("revision");
    }
    else
    {
      mVersion = 1;
      mRevision = 1;
      _exists = false;
      makeEmptyJsonTree();
    }
  }
  else
  {
    s.unlock();
    _exists = true;
    mVersion = 1;
    mRevision = jsontree->get<int>("revision");
  }
  ++metadataFilesAccessed;
}

MetadataFile::~MetadataFile()
{
}

void MetadataFile::makeEmptyJsonTree()
{
  jsontree.reset(new bpt::ptree());
  boost::property_tree::ptree objs;
  jsontree->put("version", mVersion);
  jsontree->put("revision", mRevision);
  jsontree->add_child("objects", objs);
}

void MetadataFile::printKPIs()
{
  cout << "Metadata files accessed = " << metadataFilesAccessed << endl;
}

int MetadataFile::generateStatStructInfo(struct stat* out)
{
  try
  {
    const std::string statFileName =
        mpConfig->msMetadataPath.string() + "/" + boost::to_string(boost::uuids::random_generator()());
    std::ofstream statStream(statFileName);
    statStream.close();

    int err = ::stat(statFileName.c_str(), out);
    if (err)
      return -1;

    statCache.resize(sizeof(struct stat));
    std::memcpy(&statCache[0], &out, sizeof(struct stat));
    statCached = true;
    std::filesystem::remove(statFileName);
    out->st_size = getLength();
  }
  catch (const std::exception& ex)
  {
    SMLogging::get()->log(LOG_CRIT, "Metadatafile::stat() failed with error: %s", ex.what());
    return -1;
  }

  return 0;
}

int MetadataFile::stat(struct stat* out)
{
  auto kvStorage = KVStorageInitializer::getStorageInstance();
  auto tnx = kvStorage->createTransaction();
  auto rPair = tnx->get(metaKVName_);
  if (rPair.first)
  {
    if (statCached)
    {
      std::memcpy(out, (uint8_t*)&statCache[0], sizeof(struct stat));
      out->st_size = getLength();
      return 0;
    }
    return generateStatStructInfo(out);
  }
  return -1;
}

size_t MetadataFile::getLength() const
{
  size_t totalSize = 0;

  auto& objects = jsontree->get_child("objects");
  if (!objects.empty())
  {
    auto& lastObject = objects.back().second;
    totalSize = lastObject.get<off_t>("offset") + lastObject.get<size_t>("length");
  }
  return totalSize;
}

bool MetadataFile::exists() const
{
  return _exists;
}

vector<metadataObject> MetadataFile::metadataRead(off_t offset, size_t length) const
{
  // this version assumes mObjects is sorted by offset, and there are no gaps between objects
  vector<metadataObject> ret;
  size_t foundLen = 0;

  /* For first cut of optimizations, going to generate mObjects as it was to use the existing alg
     rather than write a new alg.
  */
  set<metadataObject> mObjects;
  BOOST_FOREACH (const boost::property_tree::ptree::value_type& v, jsontree->get_child("objects"))
  {
    mObjects.insert(metadataObject(v.second.get<uint64_t>("offset"), v.second.get<uint64_t>("length"),
                                   v.second.get<string>("key")));
  }

  if (mObjects.size() == 0)
    return ret;

  uint64_t lastOffset = mObjects.rbegin()->offset;
  auto i = mObjects.begin();
  // find the first object in range
  // Note, the last object in mObjects may not be full, compare the last one against its maximum
  // size rather than its current size.
  while (i != mObjects.end())
  {
    if ((uint64_t)offset <= (i->offset + i->length - 1) ||
        (i->offset == lastOffset && ((uint64_t)offset <= i->offset + mpConfig->mObjectSize - 1)))
    {
      foundLen = (i->offset == lastOffset ? mpConfig->mObjectSize : i->length) - (offset - i->offset);
      ret.push_back(*i);
      ++i;
      break;
    }
    ++i;
  }

  while (i != mObjects.end() && foundLen < length)
  {
    ret.push_back(*i);
    foundLen += i->length;
    ++i;
  }

  assert(!(offset == 0 && length == getLength()) || (ret.size() == mObjects.size()));
  return ret;
}

metadataObject MetadataFile::addMetadataObject(const boost::filesystem::path& filename, size_t length)
{
  // this needs to handle if data write is beyond the end of the last object
  // but not at start of new object
  //

  metadataObject addObject;
  auto& objects = jsontree->get_child("objects");
  if (!objects.empty())
  {
    auto& lastObject = objects.back().second;
    addObject.offset = lastObject.get<off_t>("offset") + mpConfig->mObjectSize;
  }

  addObject.length = length;
  addObject.key = getNewKey(filename.string(), addObject.offset, addObject.length);
  boost::property_tree::ptree object;
  object.put("offset", addObject.offset);
  object.put("length", addObject.length);
  object.put("key", addObject.key);
  objects.push_back(make_pair("", object));

  return addObject;
}

// TODO: Error handling...s
int MetadataFile::writeMetadata()
{
  {
    auto kvStorage = KVStorageInitializer::getStorageInstance();
    auto keyGen = std::make_shared<FDBCS::BoostUIDKeyGenerator>();
    FDBCS::BlobHandler blobWriter(keyGen);
    stringstream stream;
    write_json(stream, *jsontree);

    if (!blobWriter.writeBlob(kvStorage, metaKVName_, stream.str()))
    {
      SMLogging::get()->log(LOG_CRIT, "Metadatafile: cannot commit tnx set().");
      throw runtime_error("Metadatafile: cannot commit tnx set().");
    }
  }

  _exists = true;

  boost::unique_lock<boost::mutex> s(jsonCache.getMutex());
  jsonCache.put(mFilename, jsontree);

  return 0;
}

bool MetadataFile::getEntry(off_t offset, metadataObject* out) const
{
  metadataObject addObject;
  BOOST_FOREACH (const boost::property_tree::ptree::value_type& v, jsontree->get_child("objects"))
  {
    if (v.second.get<off_t>("offset") == offset)
    {
      out->offset = offset;
      out->length = v.second.get<size_t>("length");
      out->key = v.second.get<string>("key");
      return true;
    }
  }
  return false;
}

void MetadataFile::removeEntry(off_t offset)
{
  bpt::ptree& objects = jsontree->get_child("objects");
  for (bpt::ptree::iterator it = objects.begin(); it != objects.end(); ++it)
  {
    if (it->second.get<off_t>("offset") == offset)
    {
      objects.erase(it);
      break;
    }
  }
}

void MetadataFile::removeAllEntries()
{
  jsontree->get_child("objects").clear();
}

void MetadataFile::deletedMeta(const bf::path& p)
{
  boost::unique_lock<boost::mutex> s(jsonCache.getMutex());
  jsonCache.erase(p);
}

// There are more efficient ways to do it.  Optimize if necessary.
void MetadataFile::breakout(const string& key, vector<string>& ret)
{
  int indexes[3];  // positions of each '_' delimiter
  ret.clear();
  indexes[0] = key.find_first_of('_');
  indexes[1] = key.find_first_of('_', indexes[0] + 1);
  indexes[2] = key.find_first_of('_', indexes[1] + 1);
  ret.push_back(key.substr(0, indexes[0]));
  ret.push_back(key.substr(indexes[0] + 1, indexes[1] - indexes[0] - 1));
  ret.push_back(key.substr(indexes[1] + 1, indexes[2] - indexes[1] - 1));
  ret.push_back(key.substr(indexes[2] + 1));
}

string MetadataFile::getNewKeyFromOldKey(const string& key, size_t length)
{
  mdfLock.lock();
  boost::uuids::uuid u = boost::uuids::random_generator()();
  mdfLock.unlock();

  vector<string> split;
  breakout(key, split);
  ostringstream oss;
  oss << u << "_" << split[1] << "_" << length << "_" << split[3];
  return oss.str();
}

string MetadataFile::getNewKey(string sourceName, size_t offset, size_t length)
{
  mdfLock.lock();
  boost::uuids::uuid u = boost::uuids::random_generator()();
  mdfLock.unlock();
  stringstream ss;

  for (uint i = 0; i < sourceName.length(); i++)
  {
    if (sourceName[i] == '/')
    {
      sourceName[i] = '~';
    }
  }

  ss << u << "_" << offset << "_" << length << "_" << sourceName;
  return ss.str();
}

off_t MetadataFile::getOffsetFromKey(const string& key)
{
  vector<string> split;
  breakout(key, split);
  return stoll(split[1]);
}

string MetadataFile::getSourceFromKey(const string& key)
{
  vector<string> split;
  breakout(key, split);

  // this is to convert the munged filenames back to regular filenames
  // for consistent use in IOC locks
  for (uint i = 0; i < split[3].length(); i++)
    if (split[3][i] == '~')
      split[3][i] = '/';

  return split[3];
}

size_t MetadataFile::getLengthFromKey(const string& key)
{
  vector<string> split;
  breakout(key, split);
  return stoull(split[2]);
}

// more efficient way to do these?
void MetadataFile::setOffsetInKey(string& key, off_t newOffset)
{
  vector<string> split;
  breakout(key, split);
  ostringstream oss;
  oss << split[0] << "_" << newOffset << "_" << split[2] << "_" << split[3];
  key = oss.str();
}

void MetadataFile::setLengthInKey(string& key, size_t newLength)
{
  vector<string> split;
  breakout(key, split);
  ostringstream oss;
  oss << split[0] << "_" << split[1] << "_" << newLength << "_" << split[3];
  key = oss.str();
}

void MetadataFile::printObjects() const
{
  BOOST_FOREACH (const boost::property_tree::ptree::value_type& v, jsontree->get_child("objects"))
  {
    printf("Name: %s Length: %zu Offset: %lld\n", v.second.get<string>("key").c_str(),
           v.second.get<size_t>("length"), (long long)v.second.get<off_t>("offset"));
  }
}

void MetadataFile::updateEntry(off_t offset, const string& newName, size_t newLength)
{
  for (auto& v : jsontree->get_child("objects"))
  {
    if (v.second.get<off_t>("offset") == offset)
    {
      v.second.put("key", newName);
      v.second.put("length", newLength);
      return;
    }
  }
  stringstream ss;
  ss << "MetadataFile::updateEntry(): failed to find object at offset " << offset;
  mpLogger->log(LOG_ERR, ss.str().c_str());
  throw logic_error(ss.str());
}

void MetadataFile::updateEntryLength(off_t offset, size_t newLength)
{
  for (auto& v : jsontree->get_child("objects"))
  {
    if (v.second.get<off_t>("offset") == offset)
    {
      v.second.put("length", newLength);
      return;
    }
  }
  stringstream ss;
  ss << "MetadataFile::updateEntryLength(): failed to find object at offset " << offset;
  mpLogger->log(LOG_ERR, ss.str().c_str());
  throw logic_error(ss.str());
}

off_t MetadataFile::getMetadataNewObjectOffset()
{
  auto& objects = jsontree->get_child("objects");
  if (objects.empty())
    return 0;
  auto& lastObject = jsontree->get_child("objects").back().second;
  return lastObject.get<off_t>("offset") + lastObject.get<size_t>("length");
}

metadataObject::metadataObject() : offset(0), length(0)
{
}

metadataObject::metadataObject(uint64_t _offset) : offset(_offset), length(0)
{
}

metadataObject::metadataObject(uint64_t _offset, uint64_t _length, const std::string& _key)
 : offset(_offset), length(_length), key(_key)
{
}

MetadataFile::MetadataCache::MetadataCache()
 : max_lru_size(2000)  // 2000 is an arbitrary #.  Big enough for a large working set.
{
}

inline boost::mutex& MetadataFile::MetadataCache::getMutex()
{
  return mutex;
}

MetadataFile::Jsontree_t MetadataFile::MetadataCache::get(const bf::path& p)
{
  auto it = lookup.find(p.string());
  if (it != lookup.end())
  {
    lru.splice(lru.end(), lru, it->second.second);
    return it->second.first;
  }

  return storagemanager::MetadataFile::Jsontree_t();
}

// note, does not change an existing jsontree.  This should be OK.
void MetadataFile::MetadataCache::put(const bf::path& p, const Jsontree_t& j)
{
  string sp = p.string();
  auto it = lookup.find(sp);
  if (it == lookup.end())
  {
    while (lru.size() >= max_lru_size)
    {
      lookup.erase(lru.front());
      lru.pop_front();
    }
    lru.push_back(sp);
    Lru_t::iterator last = lru.end();
    lookup.emplace(sp, make_pair(j, --last));
  }
}

void MetadataFile::MetadataCache::erase(const bf::path& p)
{
  auto it = lookup.find(p.string());
  if (it != lookup.end())
  {
    lru.erase(it->second.second);
    lookup.erase(it);
  }
}

MetadataFile::MetadataCache MetadataFile::jsonCache;

}  // namespace storagemanager
