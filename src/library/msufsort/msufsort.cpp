#include "./msufsort.h"
#include <include/endian.h>
#include <atomic>
#include <iostream>
#include <thread>
#include <pthread.h>
#include <algorithm>
#include <limits>


namespace
{

    class two_byte_symbol_count
    {
    public:

        two_byte_symbol_count
        (
        ):value_(new int32_t[0x10000])
        {
        }

        ~two_byte_symbol_count
        (
        )
        {
        }

        inline int32_t & operator []
        (
            int32_t index
        )
        {
            return value_[index];
        }

        inline int32_t const & operator []
        (
            int32_t index
        ) const
        {
            return value_[index];
        }

    protected:

    private:

        std::unique_ptr<int32_t []> value_;
    };

} // namespace


//==============================================================================
maniscalco::msufsort::msufsort
(
    int32_t numThreads
):
    inputBegin_(nullptr),
    inputEnd_(nullptr),
    getValueEnd_(nullptr),
    copyEnd_(),
    suffixArrayBegin_(nullptr),
    suffixArrayEnd_(nullptr),
    inverseSuffixArrayBegin_(nullptr),
    inverseSuffixArrayEnd_(nullptr),
    threadPool_(new thread_pool(numThreads - 1))
{
}


//==============================================================================
maniscalco::msufsort::~msufsort
(
)
{
}


//==============================================================================
inline auto maniscalco::msufsort::get_suffix_type
(
    uint8_t const * suffix
) -> suffix_type 
{
    if ((suffix + 1) >= inputEnd_)
        return a;
    if (suffix[0] >= suffix[1])
    {
        auto p = suffix + 1;
        while ((p < inputEnd_) && (*p == suffix[0]))
            ++p;
        if ((p == inputEnd_) || (suffix[0] > p[0]))
            return a;
        return b;
    }
    auto p = suffix + 2;
    while ((p < inputEnd_) && (*p == suffix[1]))
        ++p;
    if ((p == inputEnd_) || (suffix[1] > p[0]))
        return bStar;
    return b;
}


//==============================================================================
inline uint64_t maniscalco::msufsort::get_value
(
    uint8_t const * inputCurrent,
    suffix_index index
) const
{
    inputCurrent += (index & sa_index_mask);
    if (inputCurrent >= getValueEnd_)
    {
        if (inputCurrent >= inputEnd_)
            return 0;
        inputCurrent = (copyEnd_ + (sizeof(uint64_t) - std::distance(inputCurrent, inputEnd_)));
    }
    return endian_swap<host_order_type, big_endian_type>(*(uint64_t const *)(inputCurrent));
}


//==============================================================================
inline bool maniscalco::msufsort::compare_suffixes
(
    uint8_t const * inputBegin,
    suffix_index indexA,
    suffix_index indexB
) const
{
    indexA &= sa_index_mask;
    indexB &= sa_index_mask;

    auto valueA = get_value(inputBegin, indexA);
    auto valueB = get_value(inputBegin, indexB);
    while (valueA == valueB)
    {
        inputBegin += sizeof(valueA);
        valueA = get_value(inputBegin, indexA);
        valueB = get_value(inputBegin, indexB);
    }
    return (valueA > valueB);
}


//==============================================================================
inline void maniscalco::msufsort::insertion_sort
(
    // private:
    // sorts the suffixes by insertion sort
    suffix_index * partitionBegin,
    suffix_index * partitionEnd,
    int32_t currentMatchLength,
    uint64_t startingPattern
)
{
    struct partition_info
    {
        int32_t matchLength_;
        int32_t size_;
    };

    partition_info partitionStack[insertion_sort_threshold];
    partitionStack[0] = {currentMatchLength, (int32_t)std::distance(partitionBegin, partitionEnd)};
    int32_t stackSize = 1;

    while (stackSize > 0)
    {
        auto & partitionInfo = partitionStack[--stackSize];
        auto matchLength = partitionInfo.matchLength_;
        auto size = partitionInfo.size_;

        if (size <= 2)
        {
            if (size == 1)
            {
                ++partitionBegin;
            }
            else
            {
                if (compare_suffixes(inputBegin_, partitionBegin[0], partitionBegin[1]) > 0)
                    std::swap(partitionBegin[0], partitionBegin[1]);
                partitionBegin += 2;
            }
            continue;
        }

        if (tandemRepeatSortEnabled_)
        {
            // check for tandem repeats within partition
            bool potentialTandemRepeats = false;
            if (matchLength >= min_length_before_tandem_repeat_check)
            {
                for (int32_t i = 0; ((!potentialTandemRepeats) && (i < (int32_t)sizeof(uint64_t))); ++i)
	                potentialTandemRepeats = (get_value(inputBegin_, partitionBegin[0] - i - 1 + matchLength) == startingPattern);
            }
            if (potentialTandemRepeats)
            {
                // potential for tandem repeats found within this partition
                if (tandem_repeat_sort(partitionBegin, partitionBegin + size, matchLength, startingPattern))
                {
                    partitionBegin += size;
                    continue;
                }
            }
        }

        uint64_t value[insertion_sort_threshold];
        value[0] = get_value(inputBegin_ + matchLength, partitionBegin[0]);
        for (int32_t i = 1; i < size; ++i)
        {
            auto currentIndex = partitionBegin[i];
            uint64_t currentValue = get_value(inputBegin_ + matchLength, partitionBegin[i]);
            auto j = i;
            while ((j > 0) && (value[j - 1] > currentValue))
            {
                value[j] = value[j - 1];
                partitionBegin[j] = partitionBegin[j - 1];
                --j;
            }
            value[j] = currentValue;
            partitionBegin[j] = currentIndex;
        }

        int32_t i = size - 1;
        while (i >= 0)
        {
            int32_t start = i--;
            while ((i >= 0) && (value[i] == value[start]))
                --i;
            partitionStack[stackSize++] = partition_info{matchLength + (int32_t)sizeof(uint64_t), start - i};
        }
    }
}


//==============================================================================
bool maniscalco::msufsort::tandem_repeat_sort
(
    // private:
    // the tandem repeat sort.  determines if the suffixes provided are tandem repeats
    // of other suffixes from within the same group.  If so, sorts the non tandem
    // repeat suffixes and then induces the sorted order of the suffixes which are
    // tandem repeats.
    suffix_index * partitionBegin,
    suffix_index * partitionEnd,
    int32_t currentMatchLength,
    uint64_t startingPattern
)
{
    std::sort(partitionBegin, partitionEnd, [](suffix_index a, suffix_index b)->bool{return ((a & sa_index_mask) < (b & sa_index_mask));});

    int32_t tandemRepeatLength = 0;
    int32_t previousSuffixIndex = std::numeric_limits<int32_t>::max();
    suffix_index * terminatorsBegin = partitionEnd;

    suffix_index currentTerminatorIndex = 0;
    for (auto cur = partitionEnd - 1; cur >= partitionBegin; --cur)
    {
	    auto currentSuffixIndex = (*cur & sa_index_mask);
	    auto distanceBetweenSuffixes = (previousSuffixIndex - currentSuffixIndex);
	    if (distanceBetweenSuffixes < (currentMatchLength >> 1))
	    {
		    // suffix is a tandem repeat
		    tandemRepeatLength = distanceBetweenSuffixes;
            auto flags = (inverseSuffixArrayBegin_[currentSuffixIndex >> 1] & isa_flag_mask);
            inverseSuffixArrayBegin_[currentSuffixIndex >> 1] = (currentTerminatorIndex | flags | is_tandem_repeat_flag);
	    }
	    else
	    {
		    // Suffix is a terminator
		    *(--terminatorsBegin) = *cur;
            currentTerminatorIndex = currentSuffixIndex;
	    }
	    previousSuffixIndex = currentSuffixIndex;
    }
    if (tandemRepeatLength == 0)
        return false;

    // tandem repeats found
    // first we sort all of the tandem repeat terminator suffixes
    int32_t numTerminators = (int64_t)std::distance(terminatorsBegin, partitionEnd);
    multikey_quicksort(terminatorsBegin, partitionEnd, currentMatchLength, startingPattern);

    // now use sorted order of terminators to determine sorted order of repeats.
    // figure out how many terminators sort before the repeat and how
    // many sort after the repeat.  put them on left and right extremes of the array.
    int32_t m = 0;
    int32_t a = 0;
    int32_t b = numTerminators - 1;
    int32_t numTypeA = 0;
    while (a <= b)
    {
	    m = (a + b) >> 1;
        if (!compare_suffixes(inputBegin_, terminatorsBegin[m], terminatorsBegin[m] + tandemRepeatLength))
	    {
		    numTypeA = m;
		    b = m - 1;
	    }
	    else
	    {
		    numTypeA = m + 1;
		    a = m + 1;
	    }
    }
    if (numTypeA > numTerminators)
	    numTypeA = numTerminators;
    int32_t numTypeB = (numTerminators - numTypeA);

    for (int32_t i = 0; i < numTypeA; ++i)
	    partitionBegin[i] = terminatorsBegin[i];

    // type A repeats
    bool done = false;
    int32_t distanceToRepeat = 0;
    auto nextRepeatSuffix = partitionBegin + numTypeA - 1;
    while (!done)
    {
        distanceToRepeat += tandemRepeatLength;
        done = true;
        for (int32_t i = 0; i < numTypeA; i++)
        {
	        auto terminatorIndex = partitionBegin[i] & sa_index_mask;
            if (terminatorIndex >= distanceToRepeat)
            {
                auto potentialTandemRepeatIndex = (terminatorIndex - distanceToRepeat);
                auto isaValue = inverseSuffixArrayBegin_[potentialTandemRepeatIndex >> 1];
                if ((isaValue & is_tandem_repeat_flag) && ((isaValue & isa_index_mask) == terminatorIndex))
                {
                    done = false;
                    auto flag = ((potentialTandemRepeatIndex > 0) && (inputBegin_[potentialTandemRepeatIndex - 1] <= inputBegin_[potentialTandemRepeatIndex])) ? 0 : preceding_suffix_is_type_a_flag;
                    *(++nextRepeatSuffix) = (potentialTandemRepeatIndex | flag);
                    inverseSuffixArrayBegin_[potentialTandemRepeatIndex >> 1] &= ~is_tandem_repeat_flag;
                }
            }
        }
    }

    // type B repeats
    done = false;
    distanceToRepeat = 0;
    nextRepeatSuffix = partitionEnd - numTypeB;
    while (!done)
    {
        distanceToRepeat += tandemRepeatLength;
        done = true;
        for (int32_t i = 1; i <= numTypeB; i++)
        {
	        int32_t terminatorIndex = partitionEnd[-i] & sa_index_mask;
            if (terminatorIndex >= distanceToRepeat)
            {
                auto potentialTandemRepeatIndex = (terminatorIndex - distanceToRepeat);
                auto isaValue = inverseSuffixArrayBegin_[potentialTandemRepeatIndex >> 1];
                if ((isaValue & is_tandem_repeat_flag) && ((isaValue & isa_index_mask) == terminatorIndex))
                {
                    done = false;
                    auto flag = ((potentialTandemRepeatIndex > 0) && (inputBegin_[potentialTandemRepeatIndex - 1] <= inputBegin_[potentialTandemRepeatIndex])) ? 0 : preceding_suffix_is_type_a_flag;
                    *(--nextRepeatSuffix) = (potentialTandemRepeatIndex | flag);
                    inverseSuffixArrayBegin_[potentialTandemRepeatIndex >> 1] &= ~is_tandem_repeat_flag;
                }
            }
        }
    }
    return true;
}


//==============================================================================
void maniscalco::msufsort::multikey_quicksort
(
    // private:
    // multi key quicksort on the input data provided
    suffix_index * suffixArrayBegin,
    suffix_index * suffixArrayEnd,
    int32_t currentMatchLength,
    uint64_t startingPattern
)
{
    auto partitionSize = std::distance(suffixArrayBegin, suffixArrayEnd);
    if (partitionSize < insertion_sort_threshold)
    {
        if (partitionSize > 1)
		    insertion_sort(suffixArrayBegin, suffixArrayEnd, currentMatchLength, startingPattern);
        return;
    }

    std::vector<partition_info> unsortedPartitions;
    unsortedPartitions.reserve(8192);
    auto partitionBegin = suffixArrayBegin;
    auto partitionEnd = suffixArrayEnd;
    auto potentialTandemRepeats = false;

    while (true)
    {
        auto partitionSize = std::distance(partitionBegin, partitionEnd);
        if (partitionSize < insertion_sort_threshold)
        {
            if (partitionSize > 1)
    		    insertion_sort(partitionBegin, partitionEnd, currentMatchLength, startingPattern);
            partitionBegin = partitionEnd;
        }
        else
        {
	        if ((tandemRepeatSortEnabled_) && (potentialTandemRepeats))
	        {
                potentialTandemRepeats = false;
		        if (!tandem_repeat_sort(partitionBegin, partitionEnd, currentMatchLength, startingPattern))
                    continue;
			    partitionBegin = partitionEnd;
	        }
            else
            {
                // median indexes
                auto offsetInputBegin = inputBegin_ + currentMatchLength;
                auto oneSixthOfPartitionSize = partitionSize / 6;
                auto pivotCandidate1 = partitionBegin + oneSixthOfPartitionSize;
                auto pivotCandidate2 = pivotCandidate1 + oneSixthOfPartitionSize;
                auto pivotCandidate3 = pivotCandidate2 + oneSixthOfPartitionSize;
                auto pivotCandidate4 = pivotCandidate3 + oneSixthOfPartitionSize;
                auto pivotCandidate5 = pivotCandidate4 + oneSixthOfPartitionSize;

                auto pivotCandidateValue1 = get_value(offsetInputBegin, *pivotCandidate1);
                auto pivotCandidateValue2 = get_value(offsetInputBegin, *pivotCandidate2);
                auto pivotCandidateValue3 = get_value(offsetInputBegin, *pivotCandidate3);
                auto pivotCandidateValue4 = get_value(offsetInputBegin, *pivotCandidate4);
                auto pivotCandidateValue5 = get_value(offsetInputBegin, *pivotCandidate5);

                if (pivotCandidateValue1 > pivotCandidateValue2)
	                std::swap(*pivotCandidate1, *pivotCandidate2), std::swap(pivotCandidateValue1, pivotCandidateValue2);
                if (pivotCandidateValue4 > pivotCandidateValue5)
	                std::swap(*pivotCandidate4, *pivotCandidate5), std::swap(pivotCandidateValue4, pivotCandidateValue5);
                if (pivotCandidateValue1 > pivotCandidateValue3)
	                std::swap(*pivotCandidate1, *pivotCandidate3), std::swap(pivotCandidateValue1, pivotCandidateValue3);
                if (pivotCandidateValue2 > pivotCandidateValue3)
	                std::swap(*pivotCandidate2, *pivotCandidate3), std::swap(pivotCandidateValue2, pivotCandidateValue3);
                if (pivotCandidateValue1 > pivotCandidateValue4)
	                std::swap(*pivotCandidate1, *pivotCandidate4), std::swap(pivotCandidateValue1, pivotCandidateValue4);
                if (pivotCandidateValue3 > pivotCandidateValue4)
	                std::swap(*pivotCandidate3, *pivotCandidate4), std::swap(pivotCandidateValue3, pivotCandidateValue4);
                if (pivotCandidateValue2 > pivotCandidateValue5)
	                std::swap(*pivotCandidate2, *pivotCandidate5), std::swap(pivotCandidateValue2, pivotCandidateValue5);
                if (pivotCandidateValue2 > pivotCandidateValue3)
	                std::swap(*pivotCandidate2, *pivotCandidate3), std::swap(pivotCandidateValue2, pivotCandidateValue3);
                if (pivotCandidateValue4 > pivotCandidateValue5)
	                std::swap(*pivotCandidate4, *pivotCandidate5), std::swap(pivotCandidateValue4, pivotCandidateValue5);

                auto pivot1 = pivotCandidateValue1;
                auto pivot2 = pivotCandidateValue3;
                auto pivot3 = pivotCandidateValue5;

                auto ptrA = partitionBegin;
                auto ptrB = partitionBegin;
                auto ptrC = partitionBegin;
                auto partitionBack = partitionEnd - 1;
                auto ptrD = partitionBack;
                auto ptrE = partitionBack;
                auto ptrF = partitionBack;
                auto ptr = partitionBegin;

                std::swap(*ptr++, *pivotCandidate1);
                ptrC += (pivot1 != pivot2);
                ptrB += (pivot1 != pivot2);
                std::swap(*ptr++, *pivotCandidate3);
                if (pivot2 != pivot3)
                {
	                std::swap(*ptrD--, *pivotCandidate5);
	                --ptrE;
                }			

                while (ptr < ptrD)
                {
	                auto temp = get_value(offsetInputBegin, *ptr);
	                if (temp <= pivot2)
	                {
		                if (temp < pivot2)
		                {
			                std::swap(*ptrC++, *ptr);
			                if (temp <= pivot1)
			                {
				                if (temp < pivot1)
					                std::swap(*ptrA++, *(ptrC - 1));
				                std::swap(*ptrB++, *(ptrC - 1));
			                }
		                }
		                ++ptr;
	                }
	                else
	                {
		                std::swap(*ptrD--, *ptr);
		                if (temp >= pivot3)
		                {
			                if (temp > pivot3)
				                std::swap(*(ptrD + 1), *ptrF--);
			                std::swap(*(ptrD + 1), *ptrE--);
		                }
	                }
                }

                if (ptr == ptrD)
                {
	                auto temp = get_value(offsetInputBegin, *ptr);
	                if (temp <= pivot2)
	                {
		                if (temp < pivot2)
		                {
			                std::swap(*ptrC++, *ptr);
			                if (temp <= pivot1)
			                {
				                if (temp < pivot1)
					                std::swap(*ptrA++, *(ptrC - 1));
				                std::swap(*ptrB++, *(ptrC - 1));
			                }
		                }
		                ++ptr;
	                }
	                else
	                {
		                if (temp >= pivot3)
		                {
			                if (temp == pivot3)
			                {
				                std::swap(*ptrD, *ptrE);
			                }
			                else
			                {
				                auto temp2 = *ptr;
				                *ptrD = *ptrE;
				                *ptrE = *ptrF;
				                *ptrF-- = temp2;
			                }
			                --ptrE;
		                }
		                --ptrD;
	                }
                }

                int32_t szNextMatchLength = (currentMatchLength + sizeof(uint64_t));

                if (partitionBack != ptrF)
	                unsortedPartitions.push_back(partition_info{(int32_t)std::distance(ptrF, partitionBack), currentMatchLength, startingPattern, false});

                if (ptrE != ptrF)
                {
                    if (currentMatchLength == 2)
                        startingPattern = get_value(inputBegin_, *ptrE);
		            if (szNextMatchLength >= min_length_before_tandem_repeat_check)
		            {
                        potentialTandemRepeats = false;
			            for (int32_t i = 0; ((!potentialTandemRepeats) && (i < (int32_t)sizeof(uint64_t))); ++i)
				            potentialTandemRepeats = (get_value(inputBegin_, (*ptrE) - i - 1 + currentMatchLength) == startingPattern);
		            }
	                unsortedPartitions.push_back(partition_info{(int32_t)std::distance(ptrE, ptrF), szNextMatchLength, startingPattern, potentialTandemRepeats});
                }

                if (ptrD != ptrE)
	                unsortedPartitions.push_back(partition_info{(int32_t)std::distance(ptrD, ptrE), currentMatchLength, startingPattern, false});

                if (ptrC <= ptrD)
                {
                    if (currentMatchLength == 2)
                        startingPattern = get_value(inputBegin_, *ptrC);
		            if (szNextMatchLength >= min_length_before_tandem_repeat_check)
		            {
                        potentialTandemRepeats = false;
			            for (int32_t i = 0; ((!potentialTandemRepeats) && (i < (int32_t)sizeof(uint64_t))); ++i)
				            potentialTandemRepeats = (get_value(inputBegin_, (*ptrC) - i - 1 + currentMatchLength) == startingPattern);
		            }
	                unsortedPartitions.push_back(partition_info{(int32_t)std::distance(ptrC, ptrD) + 1, szNextMatchLength, startingPattern, potentialTandemRepeats});
                }

                if (ptrC != ptrB)
	                unsortedPartitions.push_back(partition_info{(int32_t)std::distance(ptrB, ptrC), currentMatchLength, startingPattern, false});

                if (ptrA != ptrB)
                {
                    if (currentMatchLength == 2)
                        startingPattern = get_value(inputBegin_, *ptrA);
		            if (szNextMatchLength >= min_length_before_tandem_repeat_check)
		            {
                        potentialTandemRepeats = false;
			            for (int32_t i = 0; ((!potentialTandemRepeats) && (i < (int32_t)sizeof(uint64_t))); ++i)
				            potentialTandemRepeats = (get_value(inputBegin_, (*ptrA) - i - 1 + currentMatchLength) == startingPattern);
		            }
	                unsortedPartitions.push_back(partition_info{(int32_t)std::distance(ptrA, ptrB), szNextMatchLength, startingPattern, potentialTandemRepeats});
                }

                if (partitionBegin != ptrA)
	                unsortedPartitions.push_back(partition_info{(int32_t)std::distance(partitionBegin, ptrA), currentMatchLength, startingPattern, false});
            }
        }

        if (unsortedPartitions.empty())
	        break;	// sorted completed
        auto partitionInfo = unsortedPartitions.back();
        unsortedPartitions.pop_back();
        partitionEnd = partitionBegin + partitionInfo.size_;
        currentMatchLength = partitionInfo.matchLength_;
        startingPattern = partitionInfo.startingPattern_;
        potentialTandemRepeats = partitionInfo.potentialTandemRepeats_;
    }			
}


//==========================================================================
void maniscalco::msufsort::second_stage_its
(
    // private:
    // performs the the second stage of the improved two stage sort.
    int32_t numThreads
)
{
    auto start = std::chrono::system_clock::now();
    // single threaded version 
    // B suffixes from right to left pass
    auto currentSuffix = (suffixArrayBegin_ + std::distance(inputBegin_, inputEnd_));
    int32_t const * curbackBucketOffset_ = (backBucketOffset_ + 0xffff);
    suffix_index * nextB[0x100];
    suffix_index ** previousNextB = nextB;
    uint8_t previousPrecedingSymbol = 0;
    for (int32_t symbol = 0xff; symbol >= 0; --symbol, curbackBucketOffset_ -= 0x100)
    {
        int32_t symbolCount = (*curbackBucketOffset_ - ((symbol > 0) ? curbackBucketOffset_[-0x100] : 0));
        if (symbolCount > 0)
        {
            for (int32_t precedingSymbol = 0; precedingSymbol < 0x100; ++precedingSymbol)
                nextB[precedingSymbol] = (suffixArrayBegin_ + backBucketOffset_[(precedingSymbol << 8) | symbol]);
            auto lastSuffix = currentSuffix - symbolCount;
            while (currentSuffix != lastSuffix)
            {
                if ((*(currentSuffix) & preceding_suffix_is_type_a_flag) == 0)
                {
                    int32_t precedingSymbolIndex = ((*currentSuffix & sa_index_mask) - 1);
                    auto precedingSymbol = (inputBegin_ + precedingSymbolIndex);
                    int32_t flag = ((precedingSymbol > inputBegin_) && (precedingSymbol[-1] <= precedingSymbol[0])) ? 0 : preceding_suffix_is_type_a_flag;
                    if (*precedingSymbol != previousPrecedingSymbol)
                    {
                        previousPrecedingSymbol = *precedingSymbol;
                        previousNextB = nextB + previousPrecedingSymbol;
                    }
                    *(--(*previousNextB)) = (precedingSymbolIndex | flag);
                }
                --currentSuffix;
            }
        }
    }

    auto finish = std::chrono::system_clock::now();
    std::cout << "second stage right to left pass time: " << std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count() << " ms " << std::endl;
    start = std::chrono::system_clock::now();

    // A suffixes from left to right pass
    if (numThreads > 1)
    {
        // multi threaded left to right pass
        auto endSuffixArray = (suffixArrayBegin_ + std::distance(inputBegin_, inputEnd_) + 1);
        auto currentSuffix = suffixArrayBegin_;
        suffix_index * nextB[0x100];
        for (int32_t precedingSymbol = 0; precedingSymbol < 0x100; ++precedingSymbol)
            nextB[precedingSymbol] = (suffixArrayBegin_ + frontBucketOffset_[precedingSymbol] - 1);    

        auto max_cache_size = (1 << 12);
        struct entry_type
        {
            uint8_t precedingSymbol_;
            int32_t precedingSymbolIndex_;
        };
        std::unique_ptr<entry_type []> cache[numThreads];
        for (auto i = 0; i < numThreads; ++i)
            cache[i].reset(new entry_type[max_cache_size]);
        int32_t numSuffixes[numThreads] = {};
        int32_t sCount[numThreads][0x100] = {};
        suffix_index * dest[numThreads][0x100] = {};
        while (currentSuffix != endSuffixArray)
        {
            // calculate current 'safe' suffixes to process
            auto begin = currentSuffix;
            auto maxEnd = begin + (max_cache_size * numThreads);
            if (maxEnd > endSuffixArray)
                maxEnd = endSuffixArray;
            while ((currentSuffix != maxEnd) && ((*currentSuffix != (int32_t)0x80000000) || (currentSuffix == begin)))
                ++currentSuffix;
            auto end = currentSuffix;
            auto totalSuffixes = std::distance(begin, end);
            auto totalSuffixesPerThread = ((totalSuffixes + numThreads - 1) / numThreads);

            // process suffixes
            for (auto threadId = 0; threadId < numThreads; ++threadId)
            {
                auto task = []
                (
                    uint8_t const * inputBegin,
                    suffix_index * begin,
                    suffix_index * end,
                    entry_type * cache,
                    int32_t & numSuffixes,
                    int32_t * suffixCount
                )
                {
                    auto current = begin;
                    while (current != end)
                    {
                        auto currentSuffixIndex = *current;
                        if (currentSuffixIndex & preceding_suffix_is_type_a_flag)
                        {
                            if ((currentSuffixIndex & sa_index_mask) != 0)
                            {
                                int32_t precedingSymbolIndex = ((currentSuffixIndex & sa_index_mask) - 1);
                                auto precedingSymbol = (inputBegin + precedingSymbolIndex);
                                int32_t flag = ((precedingSymbol > inputBegin) && (precedingSymbol[-1] >= precedingSymbol[0])) ? preceding_suffix_is_type_a_flag : 0;
                                cache[numSuffixes++] = {*precedingSymbol, precedingSymbolIndex | flag};
                                ++suffixCount[*precedingSymbol];
                            }
                            *(current) &= sa_index_mask;
                        }
                        ++current;
                    }
                };
                numSuffixes[threadId] = 0;
                auto endForThisThread = begin + totalSuffixesPerThread;
                if (endForThisThread > end)
                    endForThisThread = end;
                if (threadId == (numThreads - 1))
                    task(inputBegin_, begin, endForThisThread, cache[threadId].get(), std::ref(numSuffixes[threadId]), sCount[threadId]);
                else
                    threadPool_->post_task(threadId, std::bind(task, inputBegin_, begin, endForThisThread, cache[threadId].get(), std::ref(numSuffixes[threadId]), sCount[threadId]));
                begin = endForThisThread;
            }
            threadPool_->wait_for_all_tasks_completed();

            // not worth multi threading this bit ...
            for (auto threadId = 0; threadId < numThreads; ++threadId)
            {
                for (auto symbol = 0; symbol < 0x100; ++symbol)
                {
                    dest[threadId][symbol] = nextB[symbol];
                    nextB[symbol] += sCount[threadId][symbol];
                    sCount[threadId][symbol] = 0;
                }
            }

            // back to multi threading land ...
            for (auto threadId = 0; threadId < numThreads; ++threadId)
            {
                auto task = []
                (
                    suffix_index * dest[0x100],
                    entry_type const * begin,
                    entry_type const * end
                )
                {
                    while (begin != end)
                    {
                        *(++dest[begin->precedingSymbol_]) = begin->precedingSymbolIndex_;
                        ++begin;
                    }
                };
                if (threadId == (numThreads - 1))
                    task(dest[threadId], cache[threadId].get(), cache[threadId].get() + numSuffixes[threadId]);
                else
                    threadPool_->post_task(threadId, std::bind(task, dest[threadId], cache[threadId].get(), cache[threadId].get() + numSuffixes[threadId]));
            }
            threadPool_->wait_for_all_tasks_completed();
        }
    }
    else
    {
        // single threaded left to right pass ...
        // more efficient this way if single threaded
        auto endSuffixArray = (suffixArrayBegin_ + std::distance(inputBegin_, inputEnd_) + 1);
        auto currentSuffix = suffixArrayBegin_ - 1;
        suffix_index * nextB[0x100];
        suffix_index ** previousNextB = nextB;
        uint8_t previousPrecedingSymbol = 0;
        for (int32_t precedingSymbol = 0; precedingSymbol < 0x100; ++precedingSymbol)
            nextB[precedingSymbol] = (suffixArrayBegin_ + frontBucketOffset_[precedingSymbol] - 1);

        while (++currentSuffix != endSuffixArray)
        {
            auto currentSuffixIndex = *currentSuffix;
            if (currentSuffixIndex & preceding_suffix_is_type_a_flag)
            {
                if ((currentSuffixIndex & sa_index_mask) != 0)
                {
                    int32_t precedingSymbolIndex = ((currentSuffixIndex & sa_index_mask) - 1);
                    auto precedingSymbol = (inputBegin_ + precedingSymbolIndex);
                    int32_t flag = ((precedingSymbol > inputBegin_) && (precedingSymbol[-1] >= precedingSymbol[0])) ? preceding_suffix_is_type_a_flag : 0;
                    if (*precedingSymbol != previousPrecedingSymbol)
                    {
                        previousPrecedingSymbol = *precedingSymbol;
                        previousNextB = nextB + previousPrecedingSymbol;
                    }
                    *(++(*previousNextB)) = (precedingSymbolIndex | flag);
                }
                *(currentSuffix) &= sa_index_mask;
            }
        }
    }

    finish = std::chrono::system_clock::now();
    std::cout << "second stage left to right pass time: " << std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count() << " ms " << std::endl;
}


//==========================================================================
int32_t maniscalco::msufsort::second_stage_its_as_burrows_wheeler_transform
(
    // private:
    // creates the burrows wheeler transform while completing the second stage
    // of the improved two stage sort.
    int32_t numThreads
)
{
    auto start = std::chrono::system_clock::now();
    // B suffixes from right to left pass
    auto currentSuffix = (suffixArrayBegin_ + std::distance(inputBegin_, inputEnd_) + 1);
    int32_t const * curbackBucketOffset_ = (backBucketOffset_ + 0xffff);
    suffix_index * nextB[0x100];
    suffix_index ** previousNextB = nextB;
    uint8_t previousPrecedingSymbol = 0;
    for (int32_t symbol = 0xff; symbol >= 0; --symbol, curbackBucketOffset_ -= 0x100)
    {
        int32_t symbolCount = (*curbackBucketOffset_ - ((symbol > 0) ? curbackBucketOffset_[-0x100] : 0));
        if (symbolCount > 0)
        {
            for (int32_t precedingSymbol = 0; precedingSymbol < 0x100; ++precedingSymbol)
                nextB[precedingSymbol] = (suffixArrayBegin_ + backBucketOffset_[(precedingSymbol << 8) | symbol]);
            auto lastSuffix = currentSuffix - symbolCount;
            while (currentSuffix != lastSuffix)
            {
                int32_t precedingSymbolIndex = ((*(--currentSuffix) & sa_index_mask) - 1);
                auto precedingSymbol = (inputBegin_ + precedingSymbolIndex);
                if ((*currentSuffix & preceding_suffix_is_type_a_flag) == 0)
                {
                    int32_t flag = ((precedingSymbol > inputBegin_) && (precedingSymbol[-1] <= precedingSymbol[0])) ? 0 : preceding_suffix_is_type_a_flag;
                    if (*precedingSymbol != previousPrecedingSymbol)
                    {
                        previousPrecedingSymbol = *precedingSymbol;
                        previousNextB = nextB + previousPrecedingSymbol;
                    }
                    *(--(*previousNextB)) = (precedingSymbolIndex | flag);
                    *currentSuffix = *precedingSymbol;
                }
            }
        }
    }

    auto finish = std::chrono::system_clock::now();
    std::cout << "second stage right to left pass time: " << std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count() << " ms " << std::endl;
    start = std::chrono::system_clock::now();

    // A suffixes from left to right pass
    auto * sentinel = suffixArrayBegin_;
    if (numThreads > 1)
    {
        // multi threaded left to right pass
        auto endSuffixArray = (suffixArrayBegin_ + std::distance(inputBegin_, inputEnd_) + 1);
        auto currentSuffix = suffixArrayBegin_;
        suffix_index * nextB[0x100];
        for (int32_t precedingSymbol = 0; precedingSymbol < 0x100; ++precedingSymbol)
            nextB[precedingSymbol] = (suffixArrayBegin_ + frontBucketOffset_[precedingSymbol] - 1);    

        auto max_cache_size = (1 << 12);
        struct entry_type
        {
            uint8_t precedingSymbol_;
            int32_t precedingSymbolIndex_;
        };
        std::unique_ptr<entry_type []> cache[numThreads];
        for (auto i = 0; i < numThreads; ++i)
            cache[i].reset(new entry_type[max_cache_size]);
        int32_t numSuffixes[numThreads] = {};
        int32_t sCount[numThreads][0x100] = {};
        suffix_index * dest[numThreads][0x100] = {};
        while (currentSuffix != endSuffixArray)
        {
            // calculate current 'safe' suffixes to process
            auto begin = currentSuffix;
            auto maxEnd = begin + (max_cache_size * numThreads);
            if (maxEnd > endSuffixArray)
                maxEnd = endSuffixArray;
            while ((currentSuffix != maxEnd) && ((*currentSuffix != (int32_t)0x80000000) || (currentSuffix == begin)))
                ++currentSuffix;
            auto end = currentSuffix;
            auto totalSuffixes = std::distance(begin, end);
            auto totalSuffixesPerThread = ((totalSuffixes + numThreads - 1) / numThreads);

            // process suffixes
            for (auto threadId = 0; threadId < numThreads; ++threadId)
            {
                auto task = [&sentinel]
                (
                    uint8_t const * inputBegin,
                    suffix_index * begin,
                    suffix_index * end,
                    entry_type * cache,
                    int32_t & numSuffixes,
                    int32_t * suffixCount
                )
                {
                    auto current = begin;
                    while (current != end)
                    {
                        auto currentSuffixIndex = *current;
                        if (currentSuffixIndex & preceding_suffix_is_type_a_flag)
                        {
                            int32_t precedingSymbolIndex = ((currentSuffixIndex & sa_index_mask) - 1);
                            auto precedingSymbol = (inputBegin + precedingSymbolIndex);
                            if ((currentSuffixIndex & sa_index_mask) != 0)
                            {
                                int32_t flag = ((precedingSymbol > inputBegin) && (precedingSymbol[-1] >= precedingSymbol[0])) ? preceding_suffix_is_type_a_flag : 0;
                                if (flag)
                                    cache[numSuffixes++] = {precedingSymbol[0], precedingSymbolIndex | flag};
                                else
                                    cache[numSuffixes++] = {precedingSymbol[0], precedingSymbol[-1]};
                                ++suffixCount[*precedingSymbol];
                            }
                            if (precedingSymbol >= inputBegin)
                                *current = precedingSymbol[0];
                            else
                                sentinel = current;
                        }
                        ++current;
                    }
                };
                numSuffixes[threadId] = 0;
                auto endForThisThread = begin + totalSuffixesPerThread;
                if (endForThisThread > end)
                    endForThisThread = end;
                if (threadId == (numThreads - 1))
                    task(inputBegin_, begin, endForThisThread, cache[threadId].get(), std::ref(numSuffixes[threadId]), sCount[threadId]);
                else
                    threadPool_->post_task(threadId, std::bind(task, inputBegin_, begin, endForThisThread, cache[threadId].get(), std::ref(numSuffixes[threadId]), sCount[threadId]));
                begin = endForThisThread;
            }
            threadPool_->wait_for_all_tasks_completed();

            // not worth multi threading this bit ...
            for (auto threadId = 0; threadId < numThreads; ++threadId)
            {
                for (auto symbol = 0; symbol < 0x100; ++symbol)
                {
                    dest[threadId][symbol] = nextB[symbol];
                    nextB[symbol] += sCount[threadId][symbol];
                    sCount[threadId][symbol] = 0;
                }
            }

            // back to multi threading land ...
            for (auto threadId = 0; threadId < numThreads; ++threadId)
            {
                auto task = []
                (
                    suffix_index * dest[0x100],
                    entry_type const * begin,
                    entry_type const * end
                )
                {
                    while (begin != end)
                    {
                        *(++dest[begin->precedingSymbol_]) = begin->precedingSymbolIndex_;
                        ++begin;
                    }
                };
                if (threadId == (numThreads - 1))
                    task(dest[threadId], cache[threadId].get(), cache[threadId].get() + numSuffixes[threadId]);
                else
                    threadPool_->post_task(threadId, std::bind(task, dest[threadId], cache[threadId].get(), cache[threadId].get() + numSuffixes[threadId]));
            }
            threadPool_->wait_for_all_tasks_completed();
        }

        for (auto i = 0; i < inputSize_; ++i)
            if (suffixArrayBegin_[i] & 0x80000000)
                int y = 9;
    }
    else
    {
        // single threaded version 
        // A suffixes from left to right pass
        auto endSuffixArray = (suffixArrayBegin_ + std::distance(inputBegin_, inputEnd_) + 1);
        currentSuffix = suffixArrayBegin_ - 1;
        for (int32_t precedingSymbol = 0; precedingSymbol < 0x100; ++precedingSymbol)
            nextB[precedingSymbol] = (suffixArrayBegin_ + frontBucketOffset_[precedingSymbol] - 1);    

        while (++currentSuffix != endSuffixArray)
        {
            auto currentSuffixIndex = *currentSuffix;
            if (currentSuffixIndex & preceding_suffix_is_type_a_flag)
            {
                int32_t precedingSymbolIndex = ((currentSuffixIndex & sa_index_mask) - 1);
                auto precedingSymbol = (inputBegin_ + precedingSymbolIndex);
                if ((currentSuffixIndex & sa_index_mask) != 0)
                {
                    int32_t flag = ((precedingSymbol > inputBegin_) && (precedingSymbol[-1] >= precedingSymbol[0])) ? preceding_suffix_is_type_a_flag : 0;
                    if (*precedingSymbol != previousPrecedingSymbol)
                    {
                        previousPrecedingSymbol = *precedingSymbol;
                        previousNextB = nextB + previousPrecedingSymbol;
                    }
                    if (flag)
                        *(++(*previousNextB)) = (precedingSymbolIndex | flag);
                    else
                        if (precedingSymbol > inputBegin_)
                            *(++(*previousNextB)) = precedingSymbol[-1];
                }
                if (precedingSymbol >= inputBegin_)
                    *currentSuffix = *precedingSymbol;
                else
                    sentinel = currentSuffix;
            }
        }
    }

    int32_t sentinelIndex = (int32_t)std::distance(suffixArrayBegin_, sentinel);
    finish = std::chrono::system_clock::now();
    std::cout << "second stage left to right pass time: " << std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count() << " ms " << std::endl;
    return sentinelIndex;
}


//==========================================================================
void maniscalco::msufsort::first_stage_its
(
    // private:
    // does the first stage of the improved two stage sort
    int32_t numThreads
)
{
    auto start = std::chrono::system_clock::now();
    two_byte_symbol_count count;
    two_byte_symbol_count aCount;
    two_byte_symbol_count bStarCount[numThreads];
    two_byte_symbol_count threadCount[numThreads];
    two_byte_symbol_count threadACount[numThreads];
    auto numSuffixesPerThread = ((inputSize_ + numThreads - 1) / numThreads);

    // multi threaded count of suffix types
    auto inputCurrent = inputBegin_;
    for (auto threadId = 0; threadId < numThreads; ++threadId)
    {
        auto threadFunction = [&]
        (
            uint8_t const * begin,
            uint8_t const * end,
            two_byte_symbol_count & count,
            two_byte_symbol_count & aCount,
            two_byte_symbol_count & bStarCount
        )
        {
            if (begin < end)
                return;
            auto current = begin;
            auto suffixType = get_suffix_type(current);
            if (suffixType == suffix_type::bStar)
            {
                int32_t i = (std::distance(inputBegin_, current) >> 1);
                inverseSuffixArrayBegin_[i >> 1] |= is_bstar_suffix_flag;
                ++bStarCount[endian_swap<host_order_type, big_endian_type>(*(uint16_t const *)current--)];
            }
            if ((suffixType == suffix_type::bStar) || (suffixType == suffix_type::b))
                while ((current >= end) && (current[0] <= current[1]))
                    ++count[endian_swap<host_order_type, big_endian_type>(*(uint16_t const *)current--)];
            while (current >= end)
            {
                while ((current >= end) && (current[0] >= current[1]))
                    ++aCount[endian_swap<host_order_type, big_endian_type>(*(uint16_t const *)current--)];
                if (current >= end)
                {
                    int32_t i = (std::distance(inputBegin_, current) >> 1);
                    inverseSuffixArrayBegin_[i >> 1] |= is_bstar_suffix_flag;
                    ++bStarCount[endian_swap<host_order_type, big_endian_type>(*(uint16_t const *)current--)];
                }
                while ((current >= end) && (current[0] <= current[1]))
                    ++count[endian_swap<host_order_type, big_endian_type>(*(uint16_t const *)current--)];
            }
        };
        auto inputEnd = inputCurrent + numSuffixesPerThread;
        if (inputEnd >= (inputEnd_ - 1))
            inputEnd = (inputEnd_ - 1);
        if (threadId == (numThreads - 1))
            threadFunction(inputEnd - 1, inputCurrent, std::ref(threadCount[threadId]), std::ref(threadACount[threadId]), std::ref(bStarCount[threadId]));
        else
            threadPool_->post_task(threadId, std::bind(threadFunction, inputEnd - 1, inputCurrent, std::ref(threadCount[threadId]), 
                    std::ref(threadACount[threadId]), std::ref(bStarCount[threadId])));
        inputCurrent = inputEnd;
    }
    threadPool_->wait_for_all_tasks_completed();

    // not worth multi threading this next bit
    ++aCount[((uint16_t)inputEnd_[-1]) << 8];
    for (auto threadId = 0; threadId < numThreads; ++threadId)
    {
        for (auto j = 0; j < 0x10000; ++j)
        {
            count[j] += threadCount[threadId][j];
            aCount[j] += threadACount[threadId][j];
        }
    }
    // compute bucket offsets into suffix array
    int32_t total = 1;  // 1 for sentinel
    int32_t bStarTotal = 0;
    two_byte_symbol_count totalBStarCount;
    two_byte_symbol_count bStarOffset[numThreads];
    std::pair<int32_t, int32_t> partitions[0x10000];
    auto numPartitions = 0;
    for (int32_t i = 0; i < 0x100; ++i)
    {
        int32_t s = (i << 8);
        frontBucketOffset_[i] = total;
        for (int32_t j = 0; j < 0x100; ++j, ++s)
        {
            auto partitionStartIndex = bStarTotal;
            count[s] += aCount[s];
            for (int32_t threadId = 0; threadId < numThreads; ++threadId)
            {
                bStarOffset[threadId][s] = bStarTotal;
                totalBStarCount[s] += bStarCount[threadId][s];
                bStarTotal += bStarCount[threadId][s];
                count[s] += bStarCount[threadId][s];
            }
            total += count[s];
            backBucketOffset_[s] = total;
            if (totalBStarCount[s] > 0)
                partitions[numPartitions++] = std::make_pair(partitionStartIndex, totalBStarCount[s]);
        }
    }

    // back to multi-threaded land ...
    // multi threaded first pass 2 byte sort
    inputCurrent = inputBegin_;
    for (auto threadId = 0; threadId < numThreads; ++threadId)
    {
        auto threadFunction = [&]
        (
            uint8_t const * begin,
            uint8_t const * end,
            two_byte_symbol_count & bStarOffset
        )
        {
            if (inputCurrent < end)
                return;
            auto current = begin;
            auto suffixType = get_suffix_type(current);
            if (suffixType == suffix_type::bStar)
            {
                int32_t flag = ((current > inputBegin_) && (current[-1] <= current[0])) ? 0 : preceding_suffix_is_type_a_flag;
                suffixArrayBegin_[bStarOffset[endian_swap<host_order_type, big_endian_type>(*(uint16_t const *)current)]++] = (std::distance(inputBegin_, current) | flag);
            }
            if ((suffixType == suffix_type::bStar) || (suffixType == suffix_type::b))
                while ((current >= end) && (current[0] <= current[1]))
                    --current;
            while (current >= end)
            {
                while ((current >= end) && (current[0] >= current[1]))
                    --current;
                if (current >= end)
                {
                    int32_t flag = ((current > inputBegin_) && (current[-1] <= current[0])) ? 0 : preceding_suffix_is_type_a_flag;
                    suffixArrayBegin_[bStarOffset[endian_swap<host_order_type, big_endian_type>(*(uint16_t const *)current)]++] = 
                                (std::distance(inputBegin_, current) | flag);
                }
                while ((current >= end) && (current[0] <= current[1]))
                    --current;
            }
        };
        auto inputEnd = inputCurrent + numSuffixesPerThread;
        if (inputEnd >= (inputEnd_ - 1))
            inputEnd = (inputEnd_ - 1);
        if (threadId == (numThreads - 1))
            threadFunction(inputEnd - 1, inputCurrent, std::ref(bStarOffset[threadId]));
        else
            threadPool_->post_task(threadId, std::bind(threadFunction, inputEnd - 1, inputCurrent, std::ref(bStarOffset[threadId])));
        inputCurrent = inputEnd;
    }
    threadPool_->wait_for_all_tasks_completed();

    auto finish = std::chrono::system_clock::now();
    std::cout << "direct sort initial 16 bit sort time: " << std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count() << " ms " << std::endl;
    start = std::chrono::system_clock::now();
    
    // now for the multikey quicksort on the B* 
    // this bit is easy to multi thread
    std::atomic<int32_t> partitionCount(numPartitions);
    for (auto threadId = 0; threadId < numThreads; ++threadId)
    {
        auto task = [&]()
        {
            while (true)
            {
                auto partitionIndex = partitionCount--;
                if (partitionIndex < 0)
                    break;
                auto partition = partitions[partitionIndex];
                if (partition.second > 1)
                    multikey_quicksort(suffixArrayBegin_ + partition.first, suffixArrayBegin_ + partition.first + partition.second, 2, 0);
            }
        };
        if (threadId == (numThreads - 1))
            task();
        else
            threadPool_->post_task(threadId, task);
    }
    threadPool_->wait_for_all_tasks_completed();

    // spread b* to their final locations in suffix array
    auto destination = suffixArrayBegin_ + total;
    auto source = suffixArrayBegin_ + bStarTotal;
    for (int32_t i = 0xffff; i >= 0; --i)
    {
        if (count[i])
        {
            auto bCount = (count[i] - aCount[i]);
            destination -= bCount;
            source -= totalBStarCount[i];
            for (auto j = totalBStarCount[i] - 1; j >= 0; --j)
                destination[j] = source[j];
            for (auto j = totalBStarCount[i]; j < bCount; ++j)
                destination[j] = preceding_suffix_is_type_a_flag;
            destination -= aCount[i];
            for (auto j = 0; j < aCount[i]; ++j)
                destination[j] = preceding_suffix_is_type_a_flag;
        }
    }
    suffixArrayBegin_[0] = (inputSize_ | preceding_suffix_is_type_a_flag); // sa[0] = sentinel

    finish = std::chrono::system_clock::now();
    std::cout << "direct sort time: " << std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count() << " ms " << std::endl;
}


//==============================================================================
auto maniscalco::msufsort::make_suffix_array
(
    // public:
    // computes the suffix array for the input data
    uint8_t const * inputBegin,
    uint8_t const * inputEnd,
    int32_t numThreads
) -> suffix_array
{
    inputBegin_ = inputBegin;
    inputEnd_ = inputEnd;
    inputSize_ = std::distance(inputBegin_, inputEnd_);

    if (numThreads < 1)
        numThreads = 1;
    if (numThreads > (int32_t)std::thread::hardware_concurrency())
        numThreads = (int32_t)std::thread::hardware_concurrency();
    if (numThreads > inputSize_)
        numThreads = inputSize_;

    getValueEnd_ = (inputEnd_ - sizeof(uint64_t));
    getValueMaxIndex_ = (inputSize_ - sizeof(uint64_t));
    for (auto & e : copyEnd_)
        e = 0x00;
    auto source = inputEnd_ - sizeof(uint64_t);
    if (source < inputBegin_)
        source = inputBegin_;
    std::copy(source, inputEnd_, copyEnd_);
    suffix_array suffixArray;
    auto suffixArraySize = (inputSize_ + 1);
    suffixArray.resize(suffixArraySize);
    for (auto & e : suffixArray)
        e = 0;
    suffixArrayBegin_ = suffixArray.data();
    suffixArrayEnd_ = suffixArrayBegin_ + suffixArraySize;
    inverseSuffixArrayBegin_ = (suffixArrayBegin_ + ((inputSize_ + 1) >> 1));
    inverseSuffixArrayEnd_ = suffixArrayEnd_;

    first_stage_its(numThreads);
    second_stage_its(numThreads);
    return suffixArray;
}


//==============================================================================
int32_t maniscalco::msufsort::forward_burrows_wheeler_transform
(
    // public:
    // computes the burrows wheeler transform for the input data and
    // replaces the input data with that transformed result.
    // returns the index of the sentinel character (which is removed from the
    // transformed data).
    uint8_t * inputBegin,
    uint8_t * inputEnd,
    int32_t numThreads
)
{
    inputBegin_ = inputBegin;
    inputEnd_ = inputEnd;
    inputSize_ = std::distance(inputBegin_, inputEnd_);

    if (numThreads < 1)
        numThreads = 1;
    if (numThreads > (int32_t)std::thread::hardware_concurrency())
        numThreads = (int32_t)std::thread::hardware_concurrency();
    if (numThreads > inputSize_)
        numThreads = inputSize_;

    getValueEnd_ = (inputEnd_ - sizeof(uint64_t));
    getValueMaxIndex_ = (inputSize_ - sizeof(uint64_t));
    for (auto & e : copyEnd_)
        e = 0x00;
    auto source = inputEnd_ - sizeof(uint64_t);
    if (source < inputBegin_)
        source = inputBegin_;
    std::copy(source, inputEnd_, copyEnd_);
    suffix_array suffixArray;
    auto suffixArraySize = (inputSize_ + 1);
    suffixArray.resize(suffixArraySize);
    for (auto & e : suffixArray)
        e = 0;
    suffixArrayBegin_ = suffixArray.data();
    suffixArrayEnd_ = suffixArrayBegin_ + suffixArraySize;
    inverseSuffixArrayBegin_ = (suffixArrayBegin_ + ((inputSize_ + 1) >> 1));
    inverseSuffixArrayEnd_ = suffixArrayEnd_;

    first_stage_its(numThreads);
    int32_t sentinelIndex = second_stage_its_as_burrows_wheeler_transform(numThreads);
    for (int32_t i = 0; i < (inputSize_ + 1); ++i)
    {
        if (i != sentinelIndex)
            *inputBegin++ = (uint8_t)suffixArray[i];
    }
    return sentinelIndex;
}


//==============================================================================
void maniscalco::msufsort::reverse_burrows_wheeler_transform
(
    // public:
    // reverse the input (which is a BWT with sentinel symbol removed at index provided).
    // overwrites the input data with the reverse transformed data.
    uint8_t * inputBegin,
    uint8_t * inputEnd,
    int32_t sentinelIndex
)
{
    auto inputSize = std::distance(inputBegin, inputEnd);
    std::vector<suffix_index> index;
    index.resize(inputSize + 1);
	int32_t symbolRange[0x102];
	for (auto & e : symbolRange)
		e = 0;

	symbolRange[0] = 1;
	for (auto inputCurent = inputBegin; inputCurent < inputEnd; ++inputCurent)
		symbolRange[((uint32_t)*inputCurent) + 1]++;

	int32_t n = 0;
	for (auto & e : symbolRange)
	{
		auto temp = e;
		e = n;
		n += temp;
	}

	n = 0;
	index[0] = sentinelIndex;
	for (int32_t i = 0; i < inputSize; i++, n++)
	{
		n += (i == sentinelIndex);
		index[symbolRange[(uint32_t)inputBegin[i] + 1]++] = n;
	}
	n = sentinelIndex;
	std::vector<uint8_t> reversedBuffer;
    reversedBuffer.resize(inputSize);
	for (auto & e : reversedBuffer)
	{
		n = index[n];
		e = inputBegin[n - (n >= sentinelIndex)];
	}
    for (auto e : reversedBuffer)
        *(inputBegin++) = e;
}
