
#ifndef CACHE_H_
#define CACHE_H_

#include "Downloader.h"
#include "SMLogging.h"
#include "Synchronizer.h"
#include "Replicator.h"

#include <string>
#include <vector>
#include <list>
#include <unordered_set>
#include <boost/utility.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/thread/mutex.hpp>

namespace storagemanager
{

class Synchronizer;

class Cache : public boost::noncopyable
{
    public:
        static Cache *get();
        virtual ~Cache();
        
        void read(const std::vector<std::string> &keys);
        bool exists(const std::string &key) const;
        void exists(const std::vector<std::string> &keys, std::vector<bool> *out) const;
        void newObject(const std::string &key, size_t size);
        void newJournalEntry(size_t size);
        void deletedObject(const std::string &key, size_t size);
        void deletedJournal(size_t size);
        
        // rename is used when an old obj gets merged with its journal file
        // the size will change in that process; sizediff is by how much
        void rename(const std::string &oldKey, const std::string &newKey, ssize_t sizediff);
        void setMaxCacheSize(size_t size);
        void makeSpace(size_t size);
        size_t getCurrentCacheSize() const;
        size_t getMaxCacheSize() const;
        
        // test helpers
        const boost::filesystem::path &getCachePath();
        const boost::filesystem::path &getJournalPath();
        // this will delete everything in the cache and journal paths, and empty all Cache structures.
        void reset();
    private:
        Cache();
        
        boost::filesystem::path prefix;
        boost::filesystem::path journalPrefix;
        size_t maxCacheSize;
        size_t objectSize;
        size_t currentCacheSize;
        Downloader downloader;
        Replicator *replicator;
        SMLogging *logger;
        
        void populate();
        void _makeSpace(size_t size);

        /* The main cache structures */
        // lru owns the string memory for the filenames it manages.  m_lru and DNE point to those strings.
        typedef std::list<std::string> LRU_t;
        LRU_t lru;
        
        struct M_LRU_element_t
        {
            M_LRU_element_t(const std::string &);
            M_LRU_element_t(const std::string *);
            M_LRU_element_t(const LRU_t::iterator &);
            const std::string *key;
            LRU_t::iterator lit;
        };
        struct KeyHasher
        {
            size_t operator()(const M_LRU_element_t &l) const;
        };

        struct KeyEquals 
        {
            bool operator()(const M_LRU_element_t &l1, const M_LRU_element_t &l2) const;
        };
        
        typedef std::unordered_set<M_LRU_element_t, KeyHasher, KeyEquals> M_LRU_t;
        M_LRU_t m_lru;   // the LRU entries as a hash table
        
        /* The do-not-evict list stuff */
        struct DNEElement
        {
            DNEElement(const LRU_t::iterator &);
            LRU_t::iterator key;
            uint refCount;
        };
        
        struct DNEHasher
        {
            size_t operator()(const DNEElement &d) const;
        };
        
        struct DNEEquals
        {
            bool operator()(const DNEElement &d1, const DNEElement &d2) const;
        };
        
        typedef std::unordered_set<DNEElement, DNEHasher, DNEEquals> DNE_t;
        DNE_t doNotEvict;
        void addToDNE(const LRU_t::iterator &key);
        void removeFromDNE(const LRU_t::iterator &key);
        mutable boost::recursive_mutex lru_mutex;   // protects the main cache structures & the do-not-evict set
};



}

#endif
