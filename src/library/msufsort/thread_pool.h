#pragma once

#include <stdint.h>
#include <vector>
#include <thread>
#include <memory>


namespace maniscalco
{
   
    class thread_pool
    {
    public:

        struct thread_info
        {
            thread_info():terminateFlag_(false), hasTask_(false), task_(){}
            bool volatile terminateFlag_;
            bool volatile hasTask_;
            std::function<void()> task_;
        };
        using thread_info_ptr = std::unique_ptr<thread_info>;

        thread_pool
        (
            int32_t
        );

        ~thread_pool();

        void post_task
        (
            int32_t,
            std::function<void()> &&
        );

        void wait_for_all_tasks_completed();

    protected:

    private:

        std::vector<std::thread>        threads_;

        std::unique_ptr<thread_info_ptr []> threadInfo_;
    };
}

