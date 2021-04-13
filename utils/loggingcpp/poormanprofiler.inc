#ifndef POORMANPROFILER_H
#define POORMANPROFILER_H

#include "stopwatch.h"
#include <unordered_map>
using StopWatchMapType = std::unordered_map<pthread_t, logging::StopWatch*>;

void pause_(int seconds )
{
    struct timespec req;
    struct timespec rem;
    req.tv_sec  = seconds;
    req.tv_nsec = 0;
    rem.tv_sec  = 0;
    rem.tv_nsec = 0;

    while (1)
    {
        if (nanosleep(&req, &rem) != 0)
        {
            if (rem.tv_sec > 0 || rem.tv_nsec > 0)
            {
                req = rem;
                continue;
            }
        }

        break;
    }
}

class PoorManProfiler
{
  public:
    PoorManProfiler() {};
    StopWatchMapType stopwatchMap;
    pthread_mutex_t stopwatchMapMutex;
    pthread_t timerThread;
    bool stopwatchThreadCreated = false;
    inline logging::StopWatch* start();
    inline logging::StopWatch* getTimer();
    inline void spawnAutoFinishThread();
};

void* autoFinishStopwatchThread(void* arg)
{
    struct timeval tvCurrent;

    PoorManProfiler* profiler = reinterpret_cast<PoorManProfiler*>(arg);
    StopWatchMapType& stopwatchMap = profiler->stopwatchMap;
    
    for (;;)
    {
        // Pause n seconds.
        pause_(10);
        // Iterate through the stopwatch map and see how long it's been since last activity.
        pthread_mutex_lock(&profiler->stopwatchMapMutex);
        auto stopwatchMapIter = stopwatchMap.begin();
        logging::StopWatch* stopwatch;
        gettimeofday(&tvCurrent, 0);
        bool procIsIdle = true;

        while (stopwatchMapIter != stopwatchMap.end())
        {
            stopwatch = stopwatchMapIter->second;

            // If any threads have been active in the last 5 seconds, kick out.
            if (((tvCurrent.tv_sec - stopwatch->fTvLast.tv_sec) < 5) || stopwatch->isActive())
            {
                procIsIdle = false;
                break;
            }

            stopwatchMapIter++;
        }

        // No activity in last five seconds, display timing results.
        if (procIsIdle)
        {
            stopwatchMapIter = stopwatchMap.begin();

            while (stopwatchMapIter != stopwatchMap.end())
            {
                stopwatch = stopwatchMapIter->second;
                stopwatch->finish();
                stopwatchMapIter++;
                delete stopwatch;
            }

            stopwatchMap.clear();
            profiler->stopwatchThreadCreated = false;
        }
        pthread_mutex_unlock(&profiler->stopwatchMapMutex);
    }

    return 0;
};

inline void PoorManProfiler::spawnAutoFinishThread()
{
    // Create the thread that will show timing results after five seconds of idle time.
    if (!stopwatchThreadCreated)
    {
        pthread_t timerThread;
        int err = pthread_create(&timerThread, nullptr, autoFinishStopwatchThread, this);

        if (err)
            std::cerr << "Error creating thread to complete Stopwatches." << endl;

        stopwatchThreadCreated = true;
    }
}

inline logging::StopWatch* PoorManProfiler::start()
{
    pthread_mutex_lock(&stopwatchMapMutex);
    auto stopwatchMapIter = stopwatchMap.find(pthread_self());

    if (stopwatchMapIter != stopwatchMap.end())
    {
        pthread_mutex_unlock(&stopwatchMapMutex);
        return stopwatchMapIter->second;
    }
    logging::StopWatch* stopwatch = new logging::StopWatch(stopwatchMap.size());
    stopwatchMap.insert({pthread_self(), stopwatch});

    spawnAutoFinishThread();

    pthread_mutex_unlock(&stopwatchMapMutex);
    return stopwatch;
}

inline logging::StopWatch* PoorManProfiler::getTimer()
{
    return start();
}

static PoorManProfiler profiler;

#endif
