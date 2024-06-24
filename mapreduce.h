#include <iostream>
#include <filesystem>
#include <vector>
#include <functional>
#include <list>
#include <mutex>


class MapReduceBaseFunctions
{
    public:
    virtual void operator()(const std::string& input, std::string& resultStr) = 0;
};

class Block {
public:
    Block(size_t start, size_t end) : from(start), to(end){}
    size_t from;
    size_t to;
};

class MapReduce {
public:
    MapReduce(int mapN, int redN);
    void run(const std::string& input);
    bool setMapper(const std::shared_ptr <MapReduceBaseFunctions> & mapper);
    bool setReducer(const std::shared_ptr <MapReduceBaseFunctions> &reducer);
private:
    std::vector<Block> split_file(const std::string& file, int blocks_count);

    int m_mappers_count;
    int m_reducers_count;
    std::shared_ptr <MapReduceBaseFunctions> m_mapper;
    std::shared_ptr <MapReduceBaseFunctions> m_reducer;

    std::list<std::shared_ptr<std::list<std::string> >> map(const std::string& file); 
    std::list<std::shared_ptr<std::list<std::string> >> shuffle(std::list<std::shared_ptr<std::list<std::string> >> mapLists); 
    void reduce(std::list<std::shared_ptr<std::list<std::string> >> shuffleLists); 

    int findNewLine(std::ifstream& file, const size_t& file_size, bool forward);

    static std::shared_ptr<std::list<std::string>> mapF(const std::string& fileName,
                                                        const Block block, 
                                                        std::shared_ptr <MapReduceBaseFunctions>  mapper,
                                                        std::mutex& fileLock);

    static std::shared_ptr<std::list<std::string>> reduceF(std::shared_ptr<std::list<std::string>> shuffleList, 
                                                          std::shared_ptr <MapReduceBaseFunctions>  reducer);

    static void saveListToFile(const std::string& file, const std::shared_ptr<std::list<std::string>> & data);
};