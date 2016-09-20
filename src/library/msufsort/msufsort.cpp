#include "./msufsort.h"
#include <include/endian.h>
#include <atomic>
#include <iostream>
#include <thread>
#include <pthread.h>
#include <algorithm>
#include <limits>


//==============================================================================
maniscalco::msufsort::msufsort
(
):
    inputBegin_(nullptr),
    inputEnd_(nullptr),
    getValueEnd_(nullptr),
    copyEnd_(),
    suffixArrayBegin_(nullptr),
    suffixArrayEnd_(nullptr),
    inverseSuffixArrayBegin_(nullptr),
    inverseSuffixArrayEnd_(nullptr)
{
}


//==============================================================================
maniscalco::msufsort::~msufsort
(
)
{
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
    suffix_index * partitionBegin,
    suffix_index * partitionEnd,
    int32_t currentMatchLength,
    uint64_t //startingPattern  // future use ... when tandem repeat is added to insertion sort
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

        if (size == 1)
        {
            ++partitionBegin;
            continue;
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
    suffix_index * partitionBegin,
    suffix_index * partitionEnd,
    int32_t currentMatchLength,
    uint64_t startingPattern
)
{
    struct compare_type
    {
        inline bool operator()(suffix_index a, suffix_index b) const
        {
            return ((a & sa_index_mask) < (b & sa_index_mask));
        }
    };
    std::sort(partitionBegin, partitionEnd, compare_type());  // std sort?? i know, i know ... it's on the todo list ;^)

    int32_t tandemRepeatLength = 0;
    bool tandemRepeatFound = false;
    int32_t previousSuffixIndex = std::numeric_limits<int32_t>::max();
    suffix_index * terminatorsBegin = partitionEnd;

    suffix_index currentTerminatorIndex = 0;
    for (auto cur = partitionEnd - 1; cur >= partitionBegin; --cur)
    {
	    auto currentSuffixIndex = (*cur & sa_index_mask);
	    auto distanceBetweenSuffixes = (previousSuffixIndex - currentSuffixIndex);
	    if (distanceBetweenSuffixes < (currentMatchLength >> 1))
	    {
		    // Suffix is a tandem repeat
		    if ((tandemRepeatLength != 0) && (tandemRepeatLength != distanceBetweenSuffixes))
		        throw std::exception(); // should not be possible ... during development throw logic error - two differnet tandem repeat lengths ...
		    tandemRepeatFound = true;
		    tandemRepeatLength = distanceBetweenSuffixes;
            inverseSuffixArrayBegin_[currentSuffixIndex >> 1] = (currentTerminatorIndex | is_tandem_repeat_flag);
	    }
	    else
	    {
		    // Suffix is a terminator
		    *(--terminatorsBegin) = *cur;
            currentTerminatorIndex = currentSuffixIndex;
	    }
	    previousSuffixIndex = currentSuffixIndex;
    }
    if (!tandemRepeatFound)
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
                if (inverseSuffixArrayBegin_[potentialTandemRepeatIndex >> 1] == (terminatorIndex | is_tandem_repeat_flag))
                {
                    done = false;
                    auto flag = ((potentialTandemRepeatIndex > 0) && (inputBegin_[potentialTandemRepeatIndex - 1] <= inputBegin_[potentialTandemRepeatIndex])) ? 0 : preceding_suffix_is_type_a_flag;
                    *(++nextRepeatSuffix) = (potentialTandemRepeatIndex | flag);
                    inverseSuffixArrayBegin_[potentialTandemRepeatIndex >> 1] = 0;
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
                if (inverseSuffixArrayBegin_[potentialTandemRepeatIndex >> 1] == (terminatorIndex | is_tandem_repeat_flag))
                {
                    done = false;
                    auto flag = ((potentialTandemRepeatIndex > 0) && (inputBegin_[potentialTandemRepeatIndex - 1] <= inputBegin_[potentialTandemRepeatIndex])) ? 0 : preceding_suffix_is_type_a_flag;
                    *(--nextRepeatSuffix) = (potentialTandemRepeatIndex | flag);
                    inverseSuffixArrayBegin_[potentialTandemRepeatIndex >> 1] = 0;
                }
            }
        }
    }
    return true;
}


//==============================================================================
void maniscalco::msufsort::multikey_quicksort
(
    suffix_index * suffixArrayBegin,
    suffix_index * suffixArrayEnd,
    int32_t currentMatchLength,
    uint64_t startingPattern
)
{
    if (std::distance(suffixArrayBegin, suffixArrayEnd) < 2)
	    return;

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
	        if ((potentialTandemRepeats) && (true))
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
                if ((*(--currentSuffix) & preceding_suffix_is_type_a_flag) == 0)
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
            }
        }
    }

    auto finish = std::chrono::system_clock::now();
    std::cout << "second stage right to left pass time: " << std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count() << " ms " << std::endl;
    start = std::chrono::system_clock::now();

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

    finish = std::chrono::system_clock::now();
    std::cout << "second stage left to right pass time: " << std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count() << " ms " << std::endl;
}


//==========================================================================
void maniscalco::msufsort::first_stage_its
(
    int32_t numThreads
)
{
    auto start = std::chrono::system_clock::now();
    std::vector<std::pair<int32_t, int32_t>> partitions;
    partitions.reserve(0x10000);

    // 16 bit bucket sort
    int32_t count[0x10000];
    for (auto & e : count)
        e = 0;
    int32_t aCount[0x10000];
    for (auto & e : aCount)
        e = 0;
    int32_t bStarCount[0x10000];
    for (auto & e : bStarCount)
        e = 0;

    // count b* bucket sizes (and all bucket sizes)
    ++aCount[((uint16_t)inputEnd_[-1]) << 8];
    auto inputCurrent = inputEnd_ - 2;

    while (inputCurrent >= inputBegin_)
    {
        while ((inputCurrent >= inputBegin_) && (inputCurrent[0] >= inputCurrent[1]))
            ++aCount[endian_swap<host_order_type, big_endian_type>(*(uint16_t const *)inputCurrent--)];
        if (inputCurrent >= inputBegin_)
            ++bStarCount[endian_swap<host_order_type, big_endian_type>(*(uint16_t const *)inputCurrent)];
        --inputCurrent;
        while ((inputCurrent >= inputBegin_) && (inputCurrent[0] <= inputCurrent[1]))
            ++count[endian_swap<host_order_type, big_endian_type>(*(uint16_t const *)inputCurrent--)];
    }

    // compute bucket offsets into suffix array
    int32_t total = 1;  // 1 for sentinel
    int32_t bStarTotal = 0;
    int32_t bStarOffset[0x10000];
    for (int32_t i = 0; i < 0x100; ++i)
    {
        int32_t s = (i << 8);
        frontBucketOffset_[i] = total;
        for (int32_t j = 0; j < 0x100; ++j, ++s)
        {
            if (bStarCount[s] > 0)
                partitions.push_back(std::make_pair(bStarTotal, bStarCount[s]));
            bStarOffset[s] = bStarTotal;
            bStarTotal += bStarCount[s];

            count[s] += (aCount[s] + bStarCount[s]);
            total += count[s];
            backBucketOffset_[s] = total;
        }
    }

    // place b* in suffix array (at front)
    inputCurrent = inputEnd_ - 2;
    while (inputCurrent >= inputBegin_)
    {
        while ((inputCurrent >= inputBegin_) && (inputCurrent[0] >= inputCurrent[1]))
            --inputCurrent;
        if (inputCurrent >= inputBegin_)
        {
            int32_t flag = ((inputCurrent > inputBegin_) && (inputCurrent[-1] <= inputCurrent[0])) ? 0 : preceding_suffix_is_type_a_flag;
            suffixArrayBegin_[bStarOffset[endian_swap<host_order_type, big_endian_type>(*(uint16_t const *)inputCurrent)]++] = 
                        (std::distance(inputBegin_, inputCurrent) | flag);
        }
        while ((inputCurrent >= inputBegin_) && (inputCurrent[0] <= inputCurrent[1]))
            --inputCurrent;
    }

    auto finish = std::chrono::system_clock::now();
    std::cout << "direct sort initialization time: " << std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count() << " ms " << std::endl;
    start = std::chrono::system_clock::now();

    std::vector<std::thread> threads;
    std::atomic<int32_t> partitionCount(partitions.size());
    for (int32_t i = 0; i < numThreads; ++i)
    {
        auto task = [&]()
        {
            while (true)
            {
                auto j = partitionCount--;
                if (j < 0)
                    break;
                auto partition = partitions[j];
                if (partition.second > 1)
                {
                    multikey_quicksort(suffixArrayBegin_ + partition.first, 
                            suffixArrayBegin_ + partition.first + partition.second, 2, 0);
                }
            }
        };
        threads.push_back(std::move(std::thread(task)));
    }
    for (auto & e : threads)
        e.join();

    // sort dependent b*
    auto cur = suffixArrayBegin_;
    auto endCur = cur + bStarTotal;
    while (cur != endCur)
    {
        while ((cur != endCur) && ((*cur & 0x40000000) == 0))
            ++cur;
        if (cur != endCur)
        {
            auto begin = cur;
            while ((cur != endCur) && (*cur & 0x40000000))
            {
                *cur &= ~0x40000000;
                ++cur;
            }
            ++cur;
            std::sort(begin, cur);
        }
    }

    // spread b* to their final locations in suffix array
    auto destination = suffixArrayBegin_ + total;
    auto source = suffixArrayBegin_ + bStarTotal;
    for (int32_t i = 0xffff; i >= 0; --i)
    {
        if (count[i])
        {
            auto bCount = (count[i] - aCount[i]);
            destination -= bCount;
            source -= bStarCount[i];
            for (auto j = bStarCount[i] - 1; j >= 0; --j)
                destination[j] = source[j];
            for (auto j = bStarCount[i]; j < bCount; ++j)
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
maniscalco::msufsort::suffix_array maniscalco::msufsort::make_suffix_array
(
    uint8_t const * inputBegin,
    uint8_t const * inputEnd,
    int32_t numThreads
)
{
    inputBegin_ = inputBegin;
    inputEnd_ = inputEnd;
    inputSize_ = std::distance(inputBegin_, inputEnd_);
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
    second_stage_its();

    return suffixArray;
}

