#pragma once

#include <vector>
#include <stdint.h>
#include <atomic>
#include <thread>
#include <memory>
#include <functional>


namespace maniscalco
{

    class msufsort
    {
    public:

        using suffix_index = int32_t;
        using suffix_array = std::vector<suffix_index>;

        msufsort
        (
            int32_t = 1
        );

        ~msufsort();

        suffix_array make_suffix_array
        (
	        uint8_t const *,
            uint8_t const *
        );

        int32_t forward_burrows_wheeler_transform
        (
	        uint8_t *,
            uint8_t *
        );

        void reverse_burrows_wheeler_transform
        (
	        uint8_t *,
            uint8_t *,
            int32_t
        );
 
    protected:

    private:

        // flags used in ISA
        static int32_t constexpr is_tandem_repeat_flag  = 0x80000000;
        static int32_t constexpr is_bstar_suffix_flag   = 0x40000000;
        static int32_t constexpr isa_flag_mask = (is_tandem_repeat_flag | is_bstar_suffix_flag);        
        static int32_t constexpr isa_index_mask = ~isa_flag_mask;

        // flags used in SA
        static int32_t constexpr preceding_suffix_is_type_a_flag = 0x80000000;
        static int32_t constexpr sa_index_mask = ~preceding_suffix_is_type_a_flag;
        static int32_t constexpr min_length_before_tandem_repeat_check = (int32_t)(2 + sizeof(uint64_t) + sizeof(uint64_t));

        static constexpr int32_t insertion_sort_threshold = 16;

        enum suffix_type 
        {
            a,
            b,
            bStar
        };

        struct partition_info
        {
            int32_t size_;
            int32_t matchLength_;
            uint64_t startingPattern_;
            bool     potentialTandemRepeats_;
        };

        uint64_t get_value
        (
            uint8_t const *,
            suffix_index
        ) const;

        suffix_type get_suffix_type
        (
            uint8_t const *
        );

        bool compare_suffixes
        (
            uint8_t const *,
            suffix_index,
            suffix_index
        ) const;

        void insertion_sort
        (
            suffix_index *,
            suffix_index *,
            int32_t,
            uint64_t
        );

        bool tandem_repeat_sort
        (
            suffix_index *,
            suffix_index *,
            int32_t,
            uint64_t
        );

        void second_stage_its();

        int32_t second_stage_its_as_burrows_wheeler_transform();

        void first_stage_its();

        void multikey_quicksort
        (
            suffix_index *,
            suffix_index *,
            int32_t,
            uint64_t
        );

        uint8_t const * inputBegin_;

        uint8_t const * inputEnd_;

        int32_t         inputSize_;

        uint8_t const * getValueEnd_;

        suffix_index    getValueMaxIndex_;

        uint8_t         copyEnd_[sizeof(uint64_t) << 1];

        suffix_index *  suffixArrayBegin_;

        suffix_index *  suffixArrayEnd_;

        suffix_index *  inverseSuffixArrayBegin_;

        suffix_index *  inverseSuffixArrayEnd_;

        int32_t         frontBucketOffset_[0x100];

        int32_t         backBucketOffset_[0x10000];

        bool const      tandemRepeatSortEnabled_ = true;

        class worker_thread
        {
        public:

            worker_thread
            (
            ):
                thread_(), 
                task_(), 
                terminate_(false),
                taskCompleted_(true)
            {
                auto workFunction = []
                (
                    std::function<void()> & task,
                    bool volatile & taskComplete,
                    bool volatile & terminate
                )
                {
                    while (taskComplete)
                        ;
                    while (!terminate)
                    {
                        task();
                        taskComplete = true;
                        while (taskComplete)
                            ;
                    } 
                };
                thread_ = std::thread(workFunction, std::ref(task_), std::ref(taskCompleted_), std::ref(terminate_));
            }

            ~worker_thread
            (
            )
            {
                terminate();
                thread_.join();
            }

            void terminate
            (
            )
            {
                terminate_ = true;
                taskCompleted_ = false;
            }

            template <typename ... argument_types>
            inline void post_task
            (
                argument_types && ... arguments
            )
            {
                task_ = std::bind(std::forward<argument_types>(arguments) ...);
                taskCompleted_ = false;
            }

            inline void wait
            (
            ) const
            {
                while (!taskCompleted_)
                    ;
            }

        private:

            std::thread thread_;
            std::function<void()> task_;
            bool volatile terminate_;
            bool volatile taskCompleted_;
        };

        std::unique_ptr<worker_thread []> workerThreads_;

        int32_t numWorkerThreads_;

    }; // class msufsort


    template <typename input_iter>
    msufsort::suffix_array make_suffix_array
    (
        input_iter,
        input_iter,
        int32_t = 1
    );

    template <typename input_iter>
    int32_t forward_burrows_wheeler_transform
    (
        input_iter,
        input_iter,
        int32_t = 1
    );

    template <typename input_iter>
    void reverse_burrows_wheeler_transform
    (
        input_iter,
        input_iter,
        int32_t
    );

} // namespace maniscalco


//==============================================================================
template <typename input_iter>
maniscalco::msufsort::suffix_array maniscalco::make_suffix_array
(
    input_iter begin,
    input_iter end,
    int32_t numThreads
)
{
    return msufsort(numThreads).make_suffix_array((uint8_t const *)&*begin, (uint8_t const *)&*end);
}


//==============================================================================
template <typename input_iter>
int32_t maniscalco::forward_burrows_wheeler_transform
(
    input_iter begin,
    input_iter end,
    int32_t numThreads
)
{
    return msufsort(numThreads).forward_burrows_wheeler_transform((uint8_t *)&*begin, (uint8_t *)&*end);
}


//==============================================================================
template <typename input_iter>
void maniscalco::reverse_burrows_wheeler_transform
(
    input_iter begin,
    input_iter end,
    int32_t sentinelIndex
)
{
    msufsort().reverse_burrows_wheeler_transform((uint8_t *)&*begin, (uint8_t *)&*end, sentinelIndex);
}
