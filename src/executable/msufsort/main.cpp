#include <stdint.h>
#include <vector>
#include <fstream>
#include <iostream>
#include <signal.h>
#include <chrono>
#include <string>
#include <library/msufsort.h>
#include <iomanip>


namespace
{

    //==============================================================================
    inline int32_t match_length
    (
        int8_t const * beginInput,
        int8_t const * endInput,
        int32_t indexA,
        int32_t indexB,
        int32_t matchLength
    )
    {
        if (indexA > indexB)
            std::swap(indexA, indexB);

        int8_t const * inputA = beginInput + indexA + matchLength;
        int8_t const * inputB = beginInput + indexB + matchLength;
        endInput -= sizeof(int64_t);
        while ((inputB < endInput) && (*(int64_t const *)inputA == *(int64_t const *)inputB))
            inputA += sizeof(int64_t), inputB += sizeof(int64_t); 
        endInput += sizeof(int64_t);
        while ((inputB < endInput) && (*inputA == *inputB))
            ++inputA, ++inputB;
        return (inputA - (beginInput + indexA));
    }


    //==============================================================================
    void lcp
    (
        int8_t const * beginInput,
        int8_t const * endInput,
        int32_t * begin,
        int32_t size,
        std::size_t currentMatchLength
    )
    {
        if (size <= 4)
        {
            for (auto i = 0; i < size; ++i)
                begin[i] = match_length(beginInput, endInput, begin[i], begin[i + 1], currentMatchLength);
        }
        else
        {
            auto mid = (size / 2);
            auto nextMatchLength = match_length(beginInput, endInput, begin[0], begin[mid], currentMatchLength);
            lcp(beginInput, endInput, begin, mid, nextMatchLength);
            lcp(beginInput, endInput, begin + mid, size - mid, currentMatchLength);
        }
    }


    //==============================================================================
    void lcp_multithreaded
    (
        int8_t const * beginInput,
        int8_t const * endInput,
        int32_t * begin,
        int32_t size,
        int32_t numThreads
    )
    {
        auto perThread = ((size + numThreads - 1) / numThreads);
        std::vector<std::thread> threads(numThreads);
        int32_t temp[numThreads];

        auto n = 0;
        for (auto i = 0; i < numThreads; ++i)
        {
            auto s = perThread;
            if ((n + s) > size)
                s = (size - n);
            temp[i] = match_length(beginInput, endInput, begin[n + s - 1], begin[n + s], 0);
            threads[i] = std::thread(lcp, beginInput, endInput, begin + n, s - 1, 0);
            n += s;
        }

        for (auto & e : threads)
            e.join();
        n = 0;
        for (auto i = 0; i < numThreads; ++i)
        {
            auto s = perThread;
            if ((n + s) > size)
                s = (size - n);
            begin[n + s - 1] = temp[i];
            n += s;
        }
    }



    //==============================================================================
    void validate_lcp
    (
        int8_t const * beginInput,
        int8_t const * endInput,
        int32_t const * sa,
        int32_t size,
        int32_t const * lcp
    )
    {
        auto numSuffixes = size;
        auto errorCount = 0;
        auto updateInterval = ((numSuffixes + 99) / 100);
        auto nextUpdate = 0;
        auto counter = 0;

        for (auto i = 0; i < size; ++i)
        {
            if (counter++ >= nextUpdate)
            {
                nextUpdate += updateInterval;
                if (errorCount)
                    std::cout << "**** ERRORS DETECTED (" << errorCount << ") **** ";
                std::cout << (counter / updateInterval) << "% verified" << (char)13 << std::flush;
            }

            auto m = match_length(beginInput, endInput, sa[i], sa[i + 1], 0);
            if (m != lcp[i])
                errorCount++;
        }
        if (errorCount > 0)
            std::cout << "lcp array error count = " << errorCount << std::endl;
        else
            std::cout << "lcp array validated" << std::endl;
    }


    //==========================================================================
    void make_lcp_array
    (
            std::vector<int32_t> const & suffixArray,
            std::vector<int8_t> const & input,
        int32_t numThreads
    )
    {
        // lcp can be computed using the existing suffix array space but we make a copy instead
        // so that we can validate the lcp using the suffix array.
        std::vector<int32_t> output(suffixArray.begin() + 1, suffixArray.end());
        auto start = std::chrono::system_clock::now();
        lcp_multithreaded(input.data(), input.data() + input.size(), output.data(), output.size(), numThreads);
        auto finish = std::chrono::system_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start);
        std::cout << "lcp array completed - total elapsed time: " << elapsed.count() << " ms" << std::endl;
        validate_lcp(input.data(), input.data() + input.size(), suffixArray.data() + 1, suffixArray.size() - 1, output.data());
    }


    //==============================================================================
    std::vector<int8_t> load_file
    (
        std::string const & inputPath
    )
    {
        std::vector<int8_t> input;
        std::ifstream inputStream(inputPath, std::ios_base::in | std::ios_base::binary);
        if (inputStream)
        {
            inputStream.seekg(0, std::ios_base::end);
            int64_t size = inputStream.tellg();
            input.reserve(size);
            input.resize(size);
            inputStream.seekg(0, std::ios_base::beg);
            inputStream.read((char *)input.data(), input.size());
            inputStream.close();
        }
        else
        {
            std::cout << "failed to load file: " << inputPath << std::endl;
            throw std::exception();
        }
        return input;
    }


    //==============================================================================
    template <typename input_iter>
    bool write_file
    (
        std::string const & outputPath,
        input_iter begin,
        input_iter end
    )
    {
        std::ofstream outputStream(outputPath, std::ios_base::out | std::ios_base::binary);
        if (outputStream)
        {
            outputStream.write((char const *)&*begin, std::distance(begin, end));
            outputStream.close();
            return true;
        }
        return false;
    }


    //==============================================================================
    template <typename InputIter>
    int compare
    (
        InputIter a,
        InputIter b,
        InputIter end
    )
    {
        uint8_t const * pA = (uint8_t const *)&*a;
        uint8_t const * pB = (uint8_t const *)&*b;
        uint8_t const * pEnd = (uint8_t const *)&*end;

        while ((pA < pEnd) && (pB < pEnd) && (*pA == *pB))
        {
            ++pA;
            ++pB;
        }
        if (pA == pEnd)
            return -1;
        if (pB == pEnd)
            return 1;
        return (*pA < *pB) ? -1 : 1;
    }


    //==============================================================================
    int32_t validate_suffix_array
    (
        std::vector<int8_t> const & input,
        std::vector<int32_t> const & suffixArray
    )
    {
        if (suffixArray[0] != (int32_t)input.size())
            return 1; // first entry in SA should be sentinel

        auto numSuffixes = input.size();
        auto errorCount = 0;
        auto updateInterval = ((numSuffixes + 99) / 100);
        auto nextUpdate = 0;
        auto counter = 0;

        for (auto i = 2; i < (int32_t)suffixArray.size(); ++i)
        {
            if (counter++ >= nextUpdate)
            {
                nextUpdate += updateInterval;
                if (errorCount)
                    std::cout << "**** ERRORS DETECTED (" << errorCount << ") **** ";
                std::cout << (counter / updateInterval) << "% verified" << (char)13 << std::flush;
            }

            auto suffixA = input.data() + suffixArray[i - 1];
            auto suffixB = input.data() + suffixArray[i];
            int32_t c = compare(suffixA, suffixB, input.data() + input.size());
            if (c != -1)
            {
                ++errorCount;
            }
        }
        return errorCount;
    }


    //==============================================================================
    std::vector<int8_t> make_input
    (
        int32_t numUniqueSymbols,
        int32_t size
    )
    {
        std::vector<int8_t> input;
        input.reserve(size);
        input.resize(size);
        for (auto & e : input)
            e = rand() % numUniqueSymbols;
        return input;
    }


    //==============================================================================
    void print_usage
    (
    )
    {
        std::cout << "================================================================" << std::endl;
        std::cout << "msufsort - version 4a-demo" << std::endl;
        std::cout << "author: Michael A Maniscalco" << std::endl;
        std::cout << "**** this is a pre-release demo ****" << std::endl;
        std::cout << "**** this version is incomplete and lacks induction sorting ****" << std::endl;
        std::cout << "================================================================" << std::endl << std::endl;

        std::cout << "usage: msufsort [b|s|l] input [num threads]" << std::endl;
        std::cout << "\tb = bwt" << std::endl;
        std::cout << "\ts = suffix array" << std::endl;
        std::cout << "\tl = lcp array" << std::endl;
    }

}


//==============================================================================
int32_t main
(
    int32_t argumentCount,
    char const ** inputArguments
)
{
    try
    {
        if (argumentCount < 3)
        {
            print_usage();
            return 0;
        }

        enum task_type
        {
            burrows_wheeler_transform,
            suffix_array,
            lcp_array,
            test_mode,
            invalid
        };

        task_type taskType = invalid;
        
        std::string task(inputArguments[1]);
        if ((task == "b") || (task == "B"))
            taskType = burrows_wheeler_transform;
        if ((task == "s") || (task == "S"))
            taskType = suffix_array;
        if ((task == "l") || (task == "L"))
            taskType = lcp_array;
        if ((task == "t") || (task == "T"))
            taskType = test_mode;
        if (taskType == invalid)
        {
            print_usage();
            return 0;
        }

        std::string inputPath = inputArguments[2];
        std::vector<int8_t> input;
        if (taskType != test_mode)
        {
            input = load_file(inputPath);

            int32_t inputSize = input.size();
            std::cout << "================================================================" << std::endl;
            std::cout << "msufsort - version 4a-demo" << std::endl;
            std::cout << "author: Michael A Maniscalco" << std::endl;
            std::cout << "**** this is a pre-release demo ****" << std::endl;
            std::cout << "**** this version is incomplete and lacks induction sorting ****" << std::endl;
            std::cout << "================================================================" << std::endl << std::endl;

            std::cout << "loaded " << inputSize << " bytes" << std::endl;
        }
        else
        {
            std::cout << "test mode ... " << std::endl;
        }

        auto numWorkerThreads = 1;
        if (argumentCount >= 4)
        {
            try
            {
                numWorkerThreads = std::stoi(inputArguments[3]);
            }
            catch (...)
            {
                std::cout << "INVALID THREAD COUNT: " << inputArguments[3] << std::endl;
                throw std::exception();
            }
        }

        auto start = std::chrono::system_clock::now();
        switch (taskType)
        {
            case test_mode:
            {
                auto errorCount = 0;

                for (auto numUniqueSymbols = 1; ((!errorCount) && (numUniqueSymbols < 0x100)); ++numUniqueSymbols)
                {
                    for (auto inputSize = 1; ((!errorCount) && (inputSize < (1 << 10))); ++inputSize)
                    {
                        for (int32_t numWorkerThreads = 1; numWorkerThreads < (int32_t)std::thread::hardware_concurrency(); ++numWorkerThreads)
                        {
                            srand(numUniqueSymbols * inputSize * numWorkerThreads);
                            std::cout << "sa test: num unique symbols = " << numUniqueSymbols << ", input size = " << inputSize << ", num threads = " << numWorkerThreads << std::endl;

                            auto input = make_input(numUniqueSymbols, inputSize);
                            auto suffixArray = ::maniscalco::make_suffix_array(input.begin(), input.end(), numWorkerThreads);
                             // validate
                            errorCount = validate_suffix_array(input, suffixArray);
                            if (errorCount)
                                std::cout << "**** ERRORS DETECTED (" << errorCount << ") **** " << std::endl;
                        }
                    }
                }

                for (auto numUniqueSymbols = 1; ((!errorCount) && (numUniqueSymbols < 0x100)); ++numUniqueSymbols)
                {
                    for (auto inputSize = 1; ((!errorCount) && (inputSize < (1 << 10))); ++inputSize)
                    {
                        for (int32_t numWorkerThreads = 1; ((!errorCount) && (numWorkerThreads <= (int32_t)std::thread::hardware_concurrency())); ++numWorkerThreads)
                        {
                            srand(numUniqueSymbols * inputSize * numWorkerThreads);
                            std::cout << "bwt test: num unique symbols = " << numUniqueSymbols << ", input size = " << inputSize << ", num threads = " << numWorkerThreads << std::endl;

                            auto input = make_input(numUniqueSymbols, inputSize);
                            auto copyOfInput = input;
                            auto sentinelIndex = ::maniscalco::forward_burrows_wheeler_transform(input.begin(), input.end(), numWorkerThreads);
                            // validate
                            ::maniscalco::reverse_burrows_wheeler_transform(input.begin(), input.end(), sentinelIndex, numWorkerThreads);
                            if (input != copyOfInput)
                            {
                                std::cout << "**** BWT ERROR DETECTED" << std::endl;
                                errorCount++;
                            }
                        }
                    }
                }
                break;
            }

            case suffix_array:
            {
                std::cout << "computing suffix array" << std::endl;
                auto suffixArray = ::maniscalco::make_suffix_array(input.begin(), input.end(), numWorkerThreads);
                auto finish = std::chrono::system_clock::now();
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start);
                std::cout << "suffix array completed - total elapsed time: " << elapsed.count() << " ms" << std::endl;

                // validate 
                std::cout << "validating suffix array" << std::endl;
                auto errorCount = validate_suffix_array(input, suffixArray);
                if (errorCount)
                    std::cout << "**** ERRORS DETECTED (" << errorCount << ") **** " << std::endl;
                else
                    std::cout << "test completed and results validated successfully" << std::endl;
                break;
            }

            case lcp_array:
            {
                std::cout << "computing lcp array" << std::endl;
                auto suffixArray = ::maniscalco::make_suffix_array(input.begin(), input.end(), numWorkerThreads);
                auto finish = std::chrono::system_clock::now();
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start);
                std::cout << "suffix array completed - total elapsed time: " << elapsed.count() << " ms" << std::endl;
                make_lcp_array(suffixArray, input, numWorkerThreads);
                break;
            }

            case burrows_wheeler_transform:
            {
                auto copyOfInput = input;
                std::cout << "computing burrows wheeler transform" << std::endl;
                auto sentinelIndex = ::maniscalco::forward_burrows_wheeler_transform(input.begin(), input.end(), numWorkerThreads);
                auto finish = std::chrono::system_clock::now();
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start);
                std::cout << "burrows wheeler transform completed - total elapsed time: " << elapsed.count() << " ms" << std::endl;

                // validate
                start = std::chrono::system_clock::now();
                ::maniscalco::reverse_burrows_wheeler_transform(input.begin(), input.end(), sentinelIndex, numWorkerThreads);
                finish = std::chrono::system_clock::now();
                elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start);
                std::cout << "inverse burrows wheeler transform completed - total elapsed time: " << elapsed.count() << " ms" << std::endl;

                if (input != copyOfInput)
                    std::cout << "**** BWT ERROR DETECTED" << std::endl;
                else
                    std::cout << "test completed and results validated successfully" << std::endl;
                break;
            }

            default:
            {
                print_usage();
                break;
            }
        }
    }
    catch (...)
    {
        std::cout << "caught exception" << std::endl;
    }

    return 0;
}

