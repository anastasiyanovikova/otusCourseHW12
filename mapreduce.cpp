#include "mapreduce.h"
#include "fstream"
#include "iostream"
#include "algorithm"
#include "numeric"
#include "cctype"
#include "future"
#include "thread"
#include "list"
#include "iterator"
#include "sstream"

MapReduce::MapReduce(int mapN, int redN) :
m_mappers_count(mapN), m_reducers_count(redN)
{
}

void MapReduce::run(const std::string& input)
{
    std::cout <<"run start"<<std::endl;
    auto mapList = map(input);
    std::cout <<"map done "<<std::endl;
    auto shuffleList = shuffle(mapList);
    std::cout <<"shuffle done"<<std::endl;
    reduce(shuffleList);
    std::cout <<"reduce done"<<std::endl;

}

bool MapReduce::setMapper(const std::shared_ptr <MapReduceBaseFunctions> & mapper)
{
    m_mapper = mapper;

    return true;
}
bool MapReduce::setReducer(const std::shared_ptr <MapReduceBaseFunctions> & reducer)
{
    m_reducer = reducer;
    return true;
}

std::vector<Block> MapReduce::split_file(const std::string& file, int blocks_count)
{
    std::ifstream fileStream(file, std::ifstream::ate | std::fstream::binary);
    if(!fileStream)
        throw std::invalid_argument("File not found");
    if(blocks_count < 1)
        throw std::invalid_argument("blocks count < 1");
    auto size = static_cast<size_t>(fileStream.tellg());
    std::vector<Block> result;
    if(size == 0)
        return result;
    auto sectionSize = size / blocks_count;
    if(0 == sectionSize)
        sectionSize = 1;

    int startSection = 0;
    int endSection = 0;
    for(size_t i = 1; i < (size_t)blocks_count; i++)
    {
        auto currentPosition = i * sectionSize;
        int prevEnd = 0;
        if(result.size() > 0)
            prevEnd = (int) result.at(result.size() - 1).to;
        
        if(fileStream.peek() != '\n')
        {
            auto nextNewLinePos = findNewLine(fileStream, size, true);
            fileStream.seekg(currentPosition);
            auto prevNewLinePos = findNewLine(fileStream, size, false);
            if(-1 == nextNewLinePos)
            {
                if(-1 != prevNewLinePos)
                    endSection = prevNewLinePos;
            }
            else
            {
                if(-1 == prevNewLinePos)
                    endSection = nextNewLinePos;
                else
                {
                    endSection = (currentPosition - prevNewLinePos) > (nextNewLinePos - currentPosition) ?
                    nextNewLinePos : prevNewLinePos;
                }
            }
        }
        else
            endSection = currentPosition;

        if(-1 == endSection)
            break;
        if(endSection != prevEnd)
        {
            result.push_back(Block(static_cast<size_t>(startSection), static_cast<size_t>(endSection) ));
            if(endSection < static_cast<int> (size - 2))
                startSection = endSection + 1;
        }
    }
    result.push_back(Block(static_cast<size_t>(startSection), static_cast<size_t>(size - 1) ));
    return result;
}
std::list<std::shared_ptr<std::list<std::string> >> MapReduce::map(const std::string& file)
{
    auto blocks = split_file(file, m_mappers_count);
    std::vector<std::future<std::shared_ptr<std::list<std::string> > > > mapResult;
    std::mutex fileLock{};
    for(const auto & block: blocks)
    {
        mapResult.push_back(std::async(
            std::launch::async, 
            &MapReduce::mapF, 
            std::ref(file), 
            block,
            m_mapper,
            std::ref(fileLock)));
    }

    std::list<std::shared_ptr<std::list<std::string> > > sortedMapLists;
    for(auto& res: mapResult)
    {
        if(res.valid())
        {
            auto curList = res.get();
            curList->sort();
            sortedMapLists.push_back(curList);
        }
    }
    return sortedMapLists;
}

std::list<std::shared_ptr<std::list<std::string> >> MapReduce::shuffle(std::list<std::shared_ptr<std::list<std::string> >> mapLists)
{
    std::list<std::shared_ptr<std::list<std::string> >> shuffleRes;

    std::vector<std::string> sortedLines;
    auto listsAmount = mapLists.size();
    if(listsAmount == 0)
        return shuffleRes;
    auto it = mapLists.begin();
    auto endIt = mapLists.end();

    for(auto itList = it->get()->begin(); itList != it->get()->end(); itList++)
    {
        sortedLines.push_back(*itList);
    }

    it++;
    auto vectorIt = sortedLines.begin();

    while (it != endIt)
    {
        for(auto itList = it->get()->begin(); itList != it->get()->end(); itList++)
        {
            bool insertVsal = false;
            while(vectorIt != sortedLines.end())
            {
                if(*itList < *vectorIt)
                {
                    sortedLines.insert(vectorIt, *itList);
                    insertVsal = true;
                    ++vectorIt;
                    break;
                }
                ++vectorIt;
            }
            if(!insertVsal)
            {
                sortedLines.push_back(*itList);
                ++vectorIt;
            }
        }
        it++;
    }
    int reduceSize = static_cast<int> ( sortedLines.size() /  m_reducers_count );
    if(reduceSize < 1)
        reduceSize = 1;
    
    size_t curLineI = 0;
    for(size_t i = 0; i < (size_t) m_reducers_count && curLineI < sortedLines.size(); ++i)
    {
        std::shared_ptr<std::list<std::string> > curList = std::make_shared<std::list<std::string> >();
        int addedLines = 0;
        while(addedLines < reduceSize && curLineI < sortedLines.size())
        {
            curList->push_back(sortedLines.at(curLineI));
            ++curLineI;
            ++addedLines;
        }
        auto lastLine = curList->back();
        while(lastLine == sortedLines.at(curLineI))
        {
            curList->push_back(sortedLines.at(curLineI));
            ++curLineI;
        }
        if(curList->size() > 0)
            shuffleRes.push_back(curList);
    }
    return shuffleRes;
}

void MapReduce::reduce(std::list<std::shared_ptr<std::list<std::string> >> shuffleLists)
{
    std::vector<std::future<std::shared_ptr<std::list<std::string> > > > reduceResult;
    std::mutex fileLock{};
    for(auto lst: shuffleLists)
    {
        reduceResult.push_back(std::async(
            std::launch::async, 
            &MapReduce::reduceF, 
            lst,
            m_reducer));
    }
    int i = 1;
    std::list<std::shared_ptr<std::list<std::string>>> values{};
    for(auto& res: reduceResult)
    {
        if(res.valid())
        {
            values.push_back(res.get());
            std::string file{"reduce_data_" + std::to_string(i) + ".txt"};
            saveListToFile(file, values.back());
        }
        ++i;
    }

}

int MapReduce::findNewLine(std::ifstream& file, const size_t& file_size, bool forward)
{
    auto curPos = static_cast<size_t>(file.tellg());
    if(forward)
    {
        while(curPos < file_size - 1)
        {
            ++curPos;
            file.seekg(curPos);
            if(file.peek() == '\n')
                return curPos;
        }
        return -1;
    }

    if(curPos > file_size - 1)
        curPos = file_size - 1;
    while(curPos > 0)
    {
        --curPos;
        file.seekg(curPos);
        if(file.peek() == '\n')
            return curPos;
    }
    return -1;
}

std::shared_ptr<std::list<std::string>> MapReduce::mapF(const std::string& fileName,
                                                        const Block block, 
                                                        std::shared_ptr <MapReduceBaseFunctions> mapper,
                                                        std::mutex& fileLock)
{
    auto startPos = block.from;
    auto endPos = block.to;

    std::list<std::string> lst;
    {
        std::lock_guard<std::mutex> lockFile(fileLock);
        std::ifstream inputFile(fileName);
        if(!inputFile)
            return nullptr;
        inputFile.seekg(startPos);
        std::string str;
        while(inputFile && inputFile.tellg() < static_cast<std::ofstream::pos_type>(endPos))
        {
            if(std::getline(inputFile, str))
            {
                std::transform(str.begin(), str.end(), str.begin(), ::tolower);
                lst.push_back(str);
            }

        }
    }

    auto dataRes {std::make_shared<std::list<std::string>>()};
    for(auto line: lst)
    {
        std::string res;
        mapper->operator()(line, res);
        (*dataRes).push_back(res);
    }
    return dataRes;
}

std::shared_ptr<std::list<std::string>> MapReduce::reduceF(std::shared_ptr<std::list<std::string>> shuffleList, 
                                                        std::shared_ptr <MapReduceBaseFunctions>  reducer)
{
    auto dataRes {std::make_shared<std::list<std::string>>()};
    for(auto line: *shuffleList)
    {
        std::string res;
        reducer->operator()(line, res);
        dataRes->push_back(res);
    }
    return dataRes;
}

void MapReduce::saveListToFile(const std::string& file, const std::shared_ptr<std::list<std::string>> & data)
{
    std::ofstream outFile{file};
    if(!outFile)
    {
        throw std::ios_base::failure{"can not create file"};
    }

    for(const auto& line: *data)
    {
        outFile << line << "\n";
    }
    outFile.close();
    std::ifstream checkFile{file};
    if(!checkFile)
    {
        throw std::ios_base::failure{"can not write file"};
    }
}
