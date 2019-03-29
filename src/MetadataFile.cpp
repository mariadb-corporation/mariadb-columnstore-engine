/*
 * MetadataFile.cpp
 */
#include "MetadataFile.h"
#include <boost/filesystem.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/algorithm/string.hpp>
#include <unistd.h>

#define max(x, y) (x > y ? x : y)
#define min(x, y) (x < y ? x : y)

using namespace std;

namespace storagemanager
{

MetadataFile::MetadataFile()
{
    mpConfig = Config::get();
    mpLogger = SMLogging::get();
    mObjectSize = 5 * (1<<20);
    try 
    {
        mObjectSize = stoul(mpConfig->getValue("ObjectStorage", "object_size"));
    }
    catch (...)
    {
        cerr << "ObjectStorage/object_size must be set to a numeric value" << endl;
        throw;
    }
    try
    {
        msMetadataPath = mpConfig->getValue("ObjectStorage", "metadata_path");
        if (msMetadataPath.empty())
        {
            mpLogger->log(LOG_CRIT, "ObjectStorage/journal_path is not set");
            throw runtime_error("Please set ObjectStorage/journal_path in the storagemanager.cnf file");
        }
    }
    catch (...)
    {
        mpLogger->log(LOG_CRIT, "ObjectStorage/metadata_path is not set");
        throw runtime_error("Please set ObjectStorage/metadata_path in the storagemanager.cnf file");
    }

    try
    {
        boost::filesystem::create_directories(msMetadataPath);
    }
    catch (exception &e)
    {
        mpLogger->log(LOG_CRIT, "Failed to create %s, got: %s", msMetadataPath.c_str(), e.what());
        throw e;
    }
    mVersion=1;
    mRevision=1;
    _exists = false;
}


MetadataFile::MetadataFile(const char* filename)
{
    mpConfig = Config::get();
    mpLogger = SMLogging::get();
    mObjectSize = 5 * (1<<20);
    _exists = true;
    
    try
    {
        mObjectSize = stoul(mpConfig->getValue("ObjectStorage", "object_size"));
    }
    catch (...)
    {
        cerr << "ObjectStorage/object_size must be set to a numeric value" << endl;
        throw;
    }
    try
    {
        msMetadataPath = mpConfig->getValue("ObjectStorage", "metadata_path");
    }
    catch (...)
    {
        mpLogger->log(LOG_CRIT, "Could not load metadata_path from storagemanger.cnf file.");
        throw runtime_error("Please set ObjectStorage/metadata_path in the storagemanager.cnf file");
    }
    boost::filesystem::create_directories(msMetadataPath);
    try
    {
        boost::filesystem::create_directories(msMetadataPath);
    }
    catch (exception &e)
    {
        syslog(LOG_CRIT, "Failed to create %s, got: %s", msMetadataPath.c_str(), e.what());
        throw e;
    }
    mFilename = msMetadataPath + "/" + string(filename) + ".meta";
    if (boost::filesystem::exists(mFilename))
    {
        boost::property_tree::ptree jsontree;
        boost::property_tree::read_json(mFilename, jsontree);
        metadataObject newObject;
        //try catch
        mVersion = jsontree.get<int>("version");
        mRevision = jsontree.get<int>("revision");

        BOOST_FOREACH(const boost::property_tree::ptree::value_type &v, jsontree.get_child("objects"))
        {
            metadataObject newObject;
            newObject.offset = v.second.get<uint64_t>("offset");
            newObject.length = v.second.get<uint64_t>("length");
            newObject.key = v.second.get<string>("key");
            mObjects.insert(newObject);
        }
    }
    else
    {
        mVersion = 1;
        mRevision = 1;
        writeMetadata(filename);
    }
}

MetadataFile::MetadataFile(const char* filename, no_create_t)
{
    mpConfig = Config::get();
    mpLogger = SMLogging::get();
    mObjectSize = 5 * (1<<20);
    try
    {
        mObjectSize = stoul(mpConfig->getValue("ObjectStorage", "object_size"));
    }
    catch (...)
    {
        cerr << "ObjectStorage/object_size must be set to a numeric value" << endl;
        throw;
    }
    try
    {
        msMetadataPath = mpConfig->getValue("ObjectStorage", "metadata_path");
    }
    catch (...)
    {
        mpLogger->log(LOG_CRIT, "Could not load metadata_path from storagemanger.cnf file.");
        throw runtime_error("Please set ObjectStorage/metadata_path in the storagemanager.cnf file");
    }
    boost::filesystem::create_directories(msMetadataPath);
    try
    {
        boost::filesystem::create_directories(msMetadataPath);
    }
    catch (exception &e)
    {
        syslog(LOG_CRIT, "Failed to create %s, got: %s", msMetadataPath.c_str(), e.what());
        throw e;
    }
    mFilename = msMetadataPath + "/" + string(filename) + ".meta";
    if (boost::filesystem::exists(mFilename))
    {
        _exists = true;
        boost::property_tree::ptree jsontree;
        boost::property_tree::read_json(mFilename, jsontree);
        metadataObject newObject;
        //try catch
        mVersion = jsontree.get<int>("version");
        mRevision = jsontree.get<int>("revision");

        BOOST_FOREACH(const boost::property_tree::ptree::value_type &v, jsontree.get_child("objects"))
        {
            metadataObject newObject;
            newObject.offset = v.second.get<uint64_t>("offset");
            newObject.length = v.second.get<uint64_t>("length");
            newObject.key = v.second.get<string>("key");
            mObjects.insert(newObject);
        }
    }
    else
    {
        mVersion = 1;
        mRevision = 1;
        _exists = false;
    }
}

MetadataFile::~MetadataFile()
{

}

int MetadataFile::stat(struct stat *out) const
{
    int err = ::stat(mFilename.c_str(), out);
    if (err)
        return err;

    size_t totalSize = 0;
    for (auto &object : mObjects)
        totalSize += object.length;
    out->st_size = totalSize;
    return 0;
}

bool MetadataFile::exists() const
{
    return _exists;
}

vector<metadataObject> MetadataFile::metadataRead(off_t offset, size_t length)
{
    // this version assumes mObjects is sorted by offset, and there are no gaps between objects
    vector<metadataObject> ret;
    size_t foundLen = 0;
    
    auto i = mObjects.begin();
    // find the first object in range
    while (i != mObjects.end())
        if (offset >= i->offset)
            break;
            
    // append objects until foundLen >= length or EOF
    while (i != mObjects.end() && foundLen < length)
    {
        ret.push_back(*i);
        foundLen += i->length;
        ++i;
    }
    return ret;

#if 0
    // this version assumed mObjects was unsorted
    vector<metadataObject> returnObjs;
    uint64_t startData = offset;
    uint64_t endData = offset + length;
    bool foundStart = false;
    for (std::set<metadataObject>::iterator i = mObjects.begin(); i != mObjects.end(); ++i)
    {
        uint64_t startObject = i->offset;
        uint64_t endObject = i->offset + i->length;
        uint64_t maxEndObject = i->offset + mObjectSize;
        // This logic assumes objects are in ascending order of offsets
        if (startData >= startObject && (startData < endObject || startData < maxEndObject))
        {
            returnObjs.push_back(*i);
            foundStart = true;
        }
        else if (endData >= startObject && (endData < endObject || endData < maxEndObject))
        {
            // data ends in this object
            returnObjs.push_back(*i);
        }
        else if (endData >= startObject && foundStart)
        {
            // data overlaps this object
            returnObjs.push_back(*i);
        }
    }

    return returnObjs;
#endif
}

metadataObject MetadataFile::addMetadataObject(const char *filename, size_t length)
{
    // this needs to handle if data write is beyond the end of the last object
    // but not at start of new object
    // 
    metadataObject addObject;
    if (!mObjects.empty())
    {
        std::set<metadataObject>::reverse_iterator iLastObject = mObjects.rbegin();
        addObject.offset = iLastObject->offset + iLastObject->length;
    }
    else
    {
        addObject.offset = 0;
    }
    addObject.length = length;
    string newObjectKey = getNewKey(filename, addObject.offset, addObject.length);
    addObject.key = string(newObjectKey);
    mObjects.insert(addObject);

    return addObject;
}


int MetadataFile::writeMetadata(const char *filename)
{
    int error=0;
    
    string metadataFilename = msMetadataPath + "/" + string(filename) + ".meta";
    boost::filesystem::path pMetadataFilename = metadataFilename;
    boost::property_tree::ptree jsontree;
    boost::property_tree::ptree objs;
    jsontree.put("version",mVersion);
    jsontree.put("revision",mRevision);
    for (std::set<metadataObject>::const_iterator i = mObjects.begin(); i != mObjects.end(); ++i)
    {
        boost::property_tree::ptree object;
        object.put("offset",i->offset);
        object.put("length",i->length);
        object.put("key",i->key);
        objs.push_back(std::make_pair("", object));
    }
    jsontree.add_child("objects", objs);
    
    if (!boost::filesystem::exists(pMetadataFilename.parent_path()))
        boost::filesystem::create_directories(pMetadataFilename.parent_path());
    write_json(metadataFilename, jsontree);
    _exists = true;
    mFilename = metadataFilename;

    return error;
}

string MetadataFile::getNewKeyFromOldKey(const string &key, size_t length)
{
    boost::uuids::uuid u = boost::uuids::random_generator()();
    vector<string> split;
    boost::split(split, key, boost::is_any_of("_"));
    ostringstream oss;
    oss << u << "_" << split[1] << "_" << length << "_" << split[3];
    return oss.str();
}

string MetadataFile::getNewKey(string sourceName, size_t offset, size_t length)
{
    boost::uuids::uuid u = boost::uuids::random_generator()();
    stringstream ss;

    for (int i = 0; i < sourceName.length(); i++)
    {
        if (sourceName[i] == '/')
        {
            sourceName[i] = '-';
        }
    }

    ss << u << "_" << offset << "_" << length << "_" << sourceName;
    return ss.str();
}

off_t MetadataFile::getOffsetFromKey(const string &key)
{
    vector<string> split;
    boost::split(split, key, boost::is_any_of("_"));
    return stoll(split[1]);
}

string MetadataFile::getSourceFromKey(const string &key)
{
    vector<string> split;
    boost::split(split, key, boost::is_any_of("_"));
    return split[3];
}

size_t MetadataFile::getLengthFromKey(const string &key)
{
    vector<string> split;
    boost::split(split, key, boost::is_any_of("_"));
    return stoull(split[2]);
}

// more efficient way to do these?
void MetadataFile::setOffsetInKey(string &key, off_t newOffset)
{
    vector<string> split;
    boost::split(split, key, boost::is_any_of("_"));
    ostringstream oss;
    oss << split[0] << "_" << newOffset << "_" << split[2] << "_" << split[3];
    key = oss.str();
}

void MetadataFile::setLengthInKey(string &key, size_t newLength)
{
    vector<string> split;
    boost::split(split, key, boost::is_any_of("_"));
    ostringstream oss;
    oss << split[0] << "_" << split[1] << "_" << newLength << "_" << split[3];
    key = oss.str();
}

void MetadataFile::printObjects()
{
    printf("Version: %i Revision: %i\n",mVersion,mRevision);
    for (std::set<metadataObject>::const_iterator i = mObjects.begin(); i != mObjects.end(); ++i)
    {
        printf("Name: %s Length: %lu Offset: %lu\n",i->key.c_str(),i->length,i->offset);
    }
}

void MetadataFile::updateEntry(off_t offset, const string &newName, size_t newLength)
{
    metadataObject lookup;
    lookup.offset = offset;
    set<metadataObject>::iterator updateObj = mObjects.find(lookup);
    if (updateObj == mObjects.end())
    {
        stringstream ss;
        ss << "MetadataFile::updateEntry(): failed to find object at offset " << offset;
        mpLogger->log(LOG_ERR, ss.str().c_str());
        throw logic_error(ss.str());
    }
    updateObj->key = newName;
    updateObj->length = newLength;
}

void MetadataFile::updateEntryLength(off_t offset, size_t newLength)
{
    metadataObject lookup;
    lookup.offset = offset;
    set<metadataObject>::iterator updateObj = mObjects.find(lookup);
    if (updateObj == mObjects.end())
    {
        stringstream ss;
        ss << "MetadataFile::updateEntryLength(): failed to find object at offset " << offset;
        mpLogger->log(LOG_ERR, ss.str().c_str());
        throw logic_error(ss.str());
    }
    updateObj->length = newLength;
}

}



