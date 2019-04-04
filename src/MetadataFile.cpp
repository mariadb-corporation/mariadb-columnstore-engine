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

namespace
{   
    boost::mutex mutex;
    storagemanager::MetadataFile::MetadataConfig *inst = NULL;
}

namespace storagemanager
{

MetadataFile::MetadataConfig * MetadataFile::MetadataConfig::get()
{
    if (inst)
        return inst;
    boost::unique_lock<boost::mutex> s(mutex);
    if (inst)
        return inst;
    inst = new MetadataConfig();
    return inst;
}

MetadataFile::MetadataConfig::MetadataConfig()
{
    Config *config = Config::get();
    SMLogging *logger = SMLogging::get();
    
    try
    {
        mObjectSize = stoul(config->getValue("ObjectStorage", "object_size"));
    }
    catch (...)
    {
        logger->log(LOG_CRIT, "ObjectStorage/object_size must be set to a numeric value");
        throw runtime_error("Please set ObjectStorage/journal_path in the storagemanager.cnf file");
    }
        
    try
    {
        msMetadataPath = config->getValue("ObjectStorage", "metadata_path");
        if (msMetadataPath.empty())
        {
            logger->log(LOG_CRIT, "ObjectStorage/journal_path is not set");
            throw runtime_error("Please set ObjectStorage/journal_path in the storagemanager.cnf file");
        }
    }
    catch (...)
    {
        logger->log(LOG_CRIT, "ObjectStorage/metadata_path is not set");
        throw runtime_error("Please set ObjectStorage/metadata_path in the storagemanager.cnf file");
    }

    try
    {
        boost::filesystem::create_directories(msMetadataPath);
    }
    catch (exception &e)
    {
        logger->log(LOG_CRIT, "Failed to create %s, got: %s", msMetadataPath.c_str(), e.what());
        throw e;
    }

}

MetadataFile::MetadataFile()
{
    mpConfig = MetadataConfig::get();
    mpLogger = SMLogging::get();
    mVersion=1;
    mRevision=1;
    _exists = false;
}


MetadataFile::MetadataFile(const char* filename)
{
    mpConfig = MetadataConfig::get();
    mpLogger = SMLogging::get();
    _exists = true;
    
    mFilename = mpConfig->msMetadataPath + "/" + string(filename) + ".meta";
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
    mpConfig = MetadataConfig::get();
    mpLogger = SMLogging::get();
 
    mFilename = mpConfig->msMetadataPath + "/" + string(filename) + ".meta";
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

MetadataFile::MetadataFile(const boost::filesystem::path &path)
{
    mpConfig = MetadataConfig::get();
    mpLogger = SMLogging::get();
 
    mFilename = path.string();
    if (boost::filesystem::exists(path))
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
        
    out->st_size = getLength();
    return 0;
}

size_t MetadataFile::getLength() const
{
    size_t totalSize = 0;
    for (auto &object : mObjects)
        totalSize += object.length;
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
    
    auto i = mObjects.begin();
    // find the first object in range
    while (i != mObjects.end())
    {
        if (offset <= (i->offset + i->length - 1))
            break;
        ++i;
    }
    // append objects until foundLen >= length or EOF
    // first time thrus foundLen should be adjusted based on offset
    off_t foundOffset = offset - i->offset;
    while (i != mObjects.end() && foundLen < length)
    {
        ret.push_back(*i);
        foundLen += (i->length - foundOffset);
        foundOffset = 0; //zero on every other time thru this loop
        ++i;
    }
    return ret;
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
    
    string metadataFilename = mpConfig->msMetadataPath + "/" + string(filename) + ".meta";
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

void MetadataFile::removeEntry(off_t offset)
{
    mObjects.erase(offset);
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
            sourceName[i] = '~';
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
    
    // this is to convert the munged filenames back to regular filenames
    // for consistent use in IOC locks
    for (int i = 0; i < split[3].length(); i++)
        if (split[3][i] == '~')
            split[3][i] = '/';
    
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

void MetadataFile::printObjects() const
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

metadataObject::metadataObject()
{}

metadataObject::metadataObject(uint64_t _offset) : offset(_offset) 
{}

}



