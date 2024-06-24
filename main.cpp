#include "mapreduce.h"
#include <iostream>


class mapper: public MapReduceBaseFunctions
{
  public:
  void operator()(const std::string& input, std::string& resultStr) override{
    resultStr = input;
  }
};

class reducer: public MapReduceBaseFunctions
{
  public:
  void operator()(const std::string& input, std::string& resultStr) override{
    resultStr = input;
  }
};

int main(int argc, char const *argv[])
{
  if(argc <= 3)
  {
    std::cout << "map reduce" <<std::endl;
    return 0;
  } 

  std::shared_ptr<mapper> m {new mapper()};
  std::shared_ptr<reducer> r {new reducer()};
  std::string fileName = argv[1];
  int mappers_count = atoi (argv [2]);
  int reducers_count = atoi(argv [3]);
    
  MapReduce mr(mappers_count, reducers_count);
  mr.setMapper(m);
  mr.setReducer(r);
  mr.run(fileName);
}