/*
The MIT License (MIT)

Copyright (c) 2016 Michael A Maniscalco

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/




#pragma once

#include <vector>
#include <stdint.h>
#include <atomic>
#include <thread>
#include <memory>
#include <array>
#include <functional>


namespace maniscalco
{

    class msufsort
    {
    public:

        static auto constexpr max_radix_size = (1 << 16);
        using suffix_index = std::int32_t;
        using suffix_array = std::vector<suffix_index>;

        msufsort
        (
            std::int32_t = 1
        );

        ~msufsort();

        suffix_array make_suffix_array
        (
	        std::uint8_t const *,
            std::uint8_t const *
        );

        int32_t forward_burrows_wheeler_transform
        (
	        std::uint8_t *,
            std::uint8_t *
        );

        static void reverse_burrows_wheeler_transform
        (
	        std::uint8_t *,
            std::uint8_t *,
            std::int32_t,
            std::int32_t
        );
 
    protected:

    private:

        using suffix_value = std::uint32_t;

        // flags used in ISA
        static std::int32_t constexpr is_induced_sort = 0x40000000;
        static std::int32_t constexpr is_tandem_repeat_length  = 0x80000000;
        static std::int32_t constexpr isa_flag_mask = is_induced_sort | is_tandem_repeat_length;  
        static std::int32_t constexpr isa_index_mask = ~isa_flag_mask;

        // flags used in SA
        static std::int32_t constexpr preceding_suffix_is_type_a_flag = 0x80000000;
        static std::int32_t constexpr mark_isa_when_sorted = 0x40000000;
        static std::int32_t constexpr sa_index_mask = ~(preceding_suffix_is_type_a_flag | mark_isa_when_sorted);
        static std::int32_t constexpr suffix_is_unsorted_b_type = sa_index_mask;

        static constexpr std::int32_t insertion_sort_threshold = 16;
        static std::int32_t constexpr min_match_length_for_tandem_repeats = (2 + sizeof(suffix_value) + sizeof(suffix_value));

        enum suffix_type 
        {
            a,
            b,
            bStar
        };

        struct tandem_repeat_info
        {
            tandem_repeat_info
            (
                suffix_index * partitionBegin,
                suffix_index * partitionEnd,
                std::int32_t numTerminators,
                std::int32_t tandemRepeatLength
            ):
                partitionBegin_(partitionBegin),
                partitionEnd_(partitionEnd),
                numTerminators_(numTerminators),
                tandemRepeatLength_(tandemRepeatLength)
            {
            }

            suffix_index *  partitionBegin_;
            suffix_index *  partitionEnd_;
            std::int32_t    numTerminators_;
            std::int32_t    tandemRepeatLength_;
        };

        suffix_value get_value
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
            std::uint8_t const *,
            suffix_index,
            suffix_index
        ) const;

        int compare_suffixes
        (
            std::uint8_t const *,
            suffix_index,
            suffix_index,
            std::size_t
        ) const;

        void insertion_sort
        (
            suffix_index *,
            suffix_index *,
            int32_t,
            uint64_t
        );

        void multikey_insertion_sort
        (
            suffix_index *,
            suffix_index *,
            std::int32_t,
            suffix_value,
            std::array<suffix_value, 2>,
            std::vector<tandem_repeat_info> &
        );

        std::size_t partition_tandem_repeats
        (
            suffix_index *,
            suffix_index *,
            std::int32_t,
            std::vector<tandem_repeat_info> &
        );

        void count_suffixes
        (
            uint8_t const *,
            uint8_t const *,
            std::array<int32_t *, 4>
        );

        template <typename F, typename ... argument_types>
        void post_task_to_thread
        (
            int32_t threadId,
            F &&,
            argument_types && ...
        );

        void wait_for_all_tasks_completed() const;

        void second_stage_its();

        int32_t second_stage_its_as_burrows_wheeler_transform();

        void second_stage_its_right_to_left_pass_single_threaded();

        void second_stage_its_right_to_left_pass_multi_threaded();

        void second_stage_its_left_to_right_pass_single_threaded();

        void second_stage_its_left_to_right_pass_multi_threaded();

        void second_stage_its_as_burrows_wheeler_transform_right_to_left_pass_single_threaded();

        int32_t second_stage_its_as_burrows_wheeler_transform_left_to_right_pass_single_threaded();

        void second_stage_its_as_burrows_wheeler_transform_right_to_left_pass_multi_threaded();

        int32_t second_stage_its_as_burrows_wheeler_transform_left_to_right_pass_multi_threaded();

        void first_stage_its();

        suffix_index * multikey_quicksort
        (
            suffix_index *,
            suffix_index *,
            std::size_t,
            suffix_value,
            std::array<suffix_value, 2>,
            std::vector<tandem_repeat_info> &
        );

        void initial_two_byte_radix_sort
        (
            uint8_t const *,
            uint8_t const *,
            int32_t *
        );

        bool has_potential_tandem_repeats
        (
            suffix_value,
            std::array<suffix_value, 2>
        ) const;

        void complete_tandem_repeats
        (
            std::vector<tandem_repeat_info> &
        );

        void complete_tandem_repeat
        (
            suffix_index *,
            suffix_index *,
            std::int32_t,
            std::int32_t
        );

        struct ibwt_partition_info
        {
            ibwt_partition_info(){}

            ibwt_partition_info
            (
                suffix_index startIndex,
                suffix_index currentIndex,
                std::uint8_t * beginOutput,
                std::uint8_t * currentOutput,
                std::uint8_t * endOutput
            ):
                startIndex_(startIndex),
                currentIndex_(currentIndex),
                beginOutput_(beginOutput),
                currentOutput_(currentOutput),
                endOutput_(endOutput)
            {
            }

            suffix_index startIndex_;
            suffix_index currentIndex_;
            std::uint8_t * beginOutput_;
            std::uint8_t * currentOutput_;
            std::uint8_t * endOutput_;
        };

        uint8_t const * inputBegin_;

        uint8_t const * inputEnd_;

        int32_t         inputSize_;

        uint8_t const * getValueEnd_;

        suffix_index    getValueMaxIndex_;

        uint8_t         copyEnd_[sizeof(suffix_value) << 1];

        suffix_index *  suffixArrayBegin_;

        suffix_index *  suffixArrayEnd_;

        suffix_index *  inverseSuffixArrayBegin_;

        suffix_index *  inverseSuffixArrayEnd_;

        suffix_index *  frontBucketOffset_[0x100];

        std::unique_ptr<suffix_index * []>  backBucketOffset_;

        int32_t         aCount_[0x100];

        int32_t         bCount_[0x100];

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

        struct stack_frame
        {
            suffix_index * suffixArrayBegin;
            suffix_index * suffixArrayEnd;
            std::size_t currentMatchLength;
            suffix_value startingPattern;
            std::array<suffix_value, 2> endingPattern;
            std::vector<tandem_repeat_info> & tandemRepeatStack;
        };

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
    static void reverse_burrows_wheeler_transform
    (
        input_iter,
        input_iter,
        int32_t,
        int32_t = 1
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
    if (numThreads <= 0)
        numThreads = 1;
    if (numThreads > (int32_t)std::thread::hardware_concurrency())
        numThreads = (int32_t)std::thread::hardware_concurrency();
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
    if (numThreads <= 0)
        numThreads = 1;
    if (numThreads > (int32_t)std::thread::hardware_concurrency())
        numThreads = (int32_t)std::thread::hardware_concurrency();
    return msufsort(numThreads).forward_burrows_wheeler_transform((uint8_t *)&*begin, (uint8_t *)&*end);
}


//==============================================================================
template <typename input_iter>
void maniscalco::reverse_burrows_wheeler_transform
(
    input_iter begin,
    input_iter end,
    int32_t sentinelIndex,
    int32_t numThreads
)
{
    msufsort::reverse_burrows_wheeler_transform((uint8_t *)&*begin, (uint8_t *)&*end, sentinelIndex, numThreads);
}
