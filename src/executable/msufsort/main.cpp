#include <stdint.h>
#include <vector>
#include <fstream>
#include <iostream>
#include <signal.h>
#include <chrono>
#include <string>
#include <library/msufsort.h>
#include <iomanip>


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
        input.reserve(size + sizeof(int32_t));
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
template <typename InputIter, typename SaIter>
int32_t validate
(
    InputIter beginInput,
    SaIter saBegin,
    SaIter saEnd
)
{
    auto numSuffixes = std::distance(saBegin, saEnd);
    auto endInput = beginInput + numSuffixes;

    auto saCurrent = saBegin + 1;
    auto errorCount = 0;
    auto updateInterval = ((numSuffixes + 99) / 100);
    auto nextUpdate = 0;
    auto counter = 0;

    while (saCurrent != saEnd)
    {
        if (counter++ >= nextUpdate)
        {
            nextUpdate += updateInterval;
            if (errorCount)
                std::cout << "**** ERRORS DETECTED (" << errorCount << ") **** ";
            std::cout << (counter / updateInterval) << "% verified" << (char)13 << std::flush;
        }

        auto indexA = saCurrent[-1];
        auto indexB = saCurrent[0];
        int32_t c = compare(beginInput + indexA, beginInput + indexB, endInput);
        if (c != -1)
        {
            ++errorCount;
        }
        ++saCurrent;
    }
    return errorCount;
}


//==============================================================================
void print_usage
(
)
{
    std::cout << "invalid arguments" << std::endl;
    std::cout << "usage: msufsort [b|s] input [num threads]" << std::endl;
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
            invalid
        };

        task_type taskType = invalid;
        
        std::string task(inputArguments[1]);
        if ((task == "b") || (task == "B"))
            taskType = burrows_wheeler_transform;
        if ((task == "s") || (task == "s"))
            taskType = suffix_array;
        if (taskType == invalid)
        {
            print_usage();
            return 0;
        }

        std::string inputPath = inputArguments[2];
        std::vector<int8_t> input = load_file(inputPath);

        int32_t inputSize = input.size();
        std::cout << "================================================================" << std::endl;
        std::cout << "msufsort - version 4a-demo" << std::endl;
        std::cout << "author: Michael A Maniscalco" << std::endl;
        std::cout << "**** this is a pre-release demo ****" << std::endl;
        std::cout << "**** this version is incomplete and lacks induction sorting ****" << std::endl;
        std::cout << "================================================================" << std::endl << std::endl;

        std::cout << "loaded " << inputSize << " bytes" << std::endl;

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
            case suffix_array:
            {
                std::cout << "computing suffix array" << std::endl;
                auto suffixArray = ::maniscalco::make_suffix_array(input.begin(), input.end(), numWorkerThreads);
                auto finish = std::chrono::system_clock::now();
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start);
                std::cout << "suffix array completed - total elapsed time: " << elapsed.count() << " ms" << std::endl;

                // validate 
                std::cout << "validating suffix array" << std::endl;
                auto errorCount = validate(input.data(), suffixArray.data(), suffixArray.data() + suffixArray.size());
                if (errorCount)
                    std::cout << "**** ERRORS DETECTED (" << errorCount << ") **** " << std::endl;
                else
                    std::cout << "test completed and results validated successfully" << std::endl;
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
                ::maniscalco::reverse_burrows_wheeler_transform(input.begin(), input.end(), sentinelIndex);
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

