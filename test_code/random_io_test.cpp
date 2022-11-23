#include <fstream>
#include <iostream>
#include <random>
#include <string>

using namespace std;


class RandomBenchmark {
public:
  std::random_device seed_gen;
  std::mt19937 *engine;
  std::uniform_int_distribution<> *distr;

  std::ifstream *inf;

  char *filename = nullptr;

  std::ifstream::pos_type filesize() {
    std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
    return in.tellg();
  }

  RandomBenchmark(char *file):filename(file) {
     engine = new mt19937(seed_gen());
     distr = new uniform_int_distribution<>(0,filesize());
     inf = new ifstream(file);
     cout << "file name: " << filename << '\n';
     if(!(*inf)){
       cout << "error: cannot open file\n";
       exit(1);
     }
  }

  void start(int times){
    cout << "start benchmarking for " << times << " times\n";
    int pos = 0;
    string str;
    while(times--){
      pos = choose_random_pos();
      access_pos(pos, str);
      cout << str << '\n';
    }
    cout << "benchmarking finished\n";
  }


private:

  void access_pos(int pos, string &res){
    inf->seekg(pos);
    getline(*inf, res);
  }
  int choose_random_pos() {
    return (*distr)(*engine);
  }
};

int main(int argc, char* argv[]) {
  char* filename = argv[1];
  auto benchmark = new RandomBenchmark(filename);
  int times = atoi(argv[2]);
  benchmark->start(times);
}