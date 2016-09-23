#include "./thread_pool.h"


namespace
{

    void thread_function
    (
        maniscalco::thread_pool::thread_info_ptr const & threadInfo
    )
    {
        while (!threadInfo->terminateFlag_)
        {
            while ((!threadInfo->terminateFlag_) && (!threadInfo->hasTask_))
                ;
            if (threadInfo->hasTask_)
            {
                threadInfo->task_();
                threadInfo->hasTask_ = false;
            }
        }
    }
}


//==============================================================================
maniscalco::thread_pool::thread_pool
(
    int32_t numThreads
):
    threads_(),
    threadInfo_(new thread_info_ptr[numThreads])
{
    for (auto i = 0; i < numThreads; ++i)
    {
        threadInfo_[i].reset(new thread_info);
        std::thread t(thread_function, std::ref(threadInfo_[i]));
        threads_.push_back(std::move(t));
    }
}


//==============================================================================
maniscalco::thread_pool::~thread_pool
(
)
{
    for (auto i = 0; i < (int32_t)threads_.size(); ++i)
    {
        threadInfo_[i]->terminateFlag_ = true;
        threads_[i].join();
    }
}


//==============================================================================
void maniscalco::thread_pool::post_task
(
    int32_t threadId,
    std::function<void()> && task
)
{
    threadInfo_[threadId]->task_ = std::move(task);
    threadInfo_[threadId]->hasTask_ = true;
}


//==============================================================================
void maniscalco::thread_pool::wait_for_all_tasks_completed
(
)
{
    for (auto i = 0; i < (int32_t)threads_.size(); ++i)
        while (threadInfo_[i]->hasTask_)
            ;
}

