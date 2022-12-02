#ifndef OCEANBASE_STORAGE_OB_DEMO_SPLIT_H_
#define OCEANBASE_STORAGE_OB_DEMO_SPLIT_H_

#include <chrono>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <algorithm>
#include <cassert>
#include <functional>
#include <future>
#include <iterator>
#include <vector>
#include <atomic>

template <class ForwardIterator,
  typename Compare=std::less<
    typename std::iterator_traits<ForwardIterator>::value_type
  >
>
void quicksort(ForwardIterator first, ForwardIterator last, Compare comp = Compare())
{
  using value_type = typename std::iterator_traits<ForwardIterator>::value_type;
  using difference_type = typename std::iterator_traits<ForwardIterator>::difference_type;
  difference_type dist = std::distance(first,last);
  assert(dist >= 0);
  if (dist < 2) {
    return;
  }
  else if (dist < 100000) {
    std::sort(first,last,comp);
  }
  else {
    auto pivot = *std::next(first, dist/2);
    auto     ucomp = [pivot,&comp](const value_type& em){ return  comp(em,pivot); };
    auto not_ucomp = [pivot,&comp](const value_type& em){ return !comp(pivot,em); };

    auto middle1 = std::partition(first, last, ucomp);
    auto middle2 = std::partition(middle1, last, not_ucomp);

    // auto policy = multithreaded ? std::launch::async : std::launch::deferred;
    auto policy = std::launch::async;
    auto f1 = std::async(policy, [first  ,middle1,&comp]{quicksort(first  ,middle1,comp);});
    auto f2 = std::async(policy, [middle2,last   ,&comp]{quicksort(middle2,last   ,comp);});
    f1.wait();
    f2.wait();
  }
}

template <class T>
class SafeQueue
{
public:
  SafeQueue(void)
    : q(), m(), c()
  {}

  ~SafeQueue(void)
  {}

  // Add an element to the queue.
  void enqueue(T t)
  {
    std::lock_guard<std::mutex> lock(m);
    q.push(t);
    c.notify_one();
  }

  // Get the "front"-element.
  // If the queue is empty, wait till a element is avaiable.
  T dequeue(void)
  {
    std::unique_lock<std::mutex> lock(m);
    while(q.empty())
    {
      // release lock as long as the wait and reaquire it afterwards.
      c.wait(lock);
    }
    T val = q.front();
    q.pop();
    return val;
  }

  void close(void) 
  {
    std::unique_lock<std::mutex> lock(m);
    done_ = true;
  }

private:
  std::queue<T> q;
  mutable std::mutex m;
  std::condition_variable c;

  bool done_ = false;
};


const static std::vector<std::pair<int,int>> c6=
  {{1, 1}, {50012903, 1}, {100013121, 6}, {150006562, 1}, {200003526, 2}, {249994720, 1}};
const static std::vector<std::pair<int,int>> c60 = 
  {{1,1},{4998912,2},{10000804,3}, {15003686,2}, {20005733,4}, 
  {25002433,1}, {30000743,5}, {35001284,1}, {40004258,4}, {45009124,4}, 
  {50012900,1}, {55016289,4}, {60015136,1}, {65014596,2}, {70016353,3}, 
  {75015141,3}, {80011461,2}, {85014020,3}, {90014439,3}, {95011044,1}, 
  {100013091,1}, {105015778,3}, {110013730,3}, {115008672,4}, {120007687,4}, 
  {125003621,5}, {130004768,1}, {135004451,2}, {140003552,6}, {145003813,4}, 
  {150006530,5}, {155002919,4}, {160004934,4}, {165004321,2}, {170006054,3}, 
  {175007716,1}, {180005026,3}, {185002852,2}, {190000709,6}, {195005250,1}, 
  {200003492,3}, {205001252,2}, {209996387,2}, {214993377,6}, {219994054,4}, 
  {224993793,1}, {229992711,3}, {234991078,3}, {239992198,2}, {244991908,1}, 
  {249994658,1}, {254995971,4}, {259993542,1}, {264992451,1}, {269993059,2}, 
  {274995365,1}, {279992960,3}, {284992576,1}, {289993762,4}, {294998561,2}};
const static std::vector<std::pair<int,int>> c64 = 
  {{1,1},{4686561,4},{9375143,1},{14065923,2},
  {18754119,2}, {23442053,1}, {28126496,1}, {32813314,2},
  {37502914,4}, {42192834,5}, {46886850,3}, {51575141,2},
  {56267719,4}, {60953571,1}, {65638919,1}, {70327106,1},
  {75015141,3}, {79699685,3}, {84388865,3}, {89079428,2},
  {93760740,3}, {98451111,5}, {103139298,2}, {107825383,1},
  {112509861,5}, {117193797,2}, {121880261,2}, {126564997,2},
  {131253958,2}, {135940993,7}, {140628545,4}, {145316610,6},
  {150006530,5}, {154690662,6}, {159380261,2}, {164067363,2},
  {168755426,1}, {173445733,1}, {178132355,3}, {182815072,3},
  {187501444,4}, {192190338,2}, {196880324,1}, {201566437,4},
  {206248773,5}, {210933091,2}, {215618468,4}, {220306531,1},
  {224993793,1}, {229681799,3}, {234366693,2}, {239054592,1},
  {243743687,4}, {248431873,1}, {253121216,3}, {257808641,4},
  {262492736,6}, {267180132,2}, {271869666,4}, {276557892,3},
  {281242467,2}, {285930087,1}, {290618789,5}, {295310946,5}};
const static std::vector<std::pair<int,int>> c32 = {{1,1},{9375143,2},{18754119,4}, {28126498,1}, 
  {37502915,3}, {46886851,4}, {56267745,5}, {65638944,5}, 
  {75015142,7}, {84388867,3}, {93760743,2}, {103139300,2}, 
  {112509889,2}, {121880288,3}, {131253986,3}, {140628548,2}, 
  {150006534,2}, {159380291,2}, {168755430,1}, {178132385,2}, 
  {187501473,1}, {196880354,1}, {206248800,7}, {215618497,2}, 
  {224993799,2}, {234366722,6}, {243743716,3}, {253121223,4}, 
  {262492743,4}, {271869696,2}, {281242500,2}, {290618820,5}};
const static std::vector<std::pair<int,int>> c120 = 
  {{1, 1}, {2499910, 2}, {4998912, 2}, {7501153, 1}, {10000804, 3}, {12503655, 1},
  {15003686, 2}, {17503813, 6}, {20005733, 4}, {22504801, 1}, {25002433, 1}, {27502113, 2},
  {30000743, 5}, {32500485, 1}, {35001284, 1}, {37502914, 4}, {40004258, 4}, {42505766, 6},
  {45009124, 4}, {47512546, 2}, {50012900, 1}, {52513187, 1}, {55016289, 4}, {57518053, 2},
  {60015136, 1}, {62514562, 4}, {65014596, 2}, {67514630, 2}, {70016353, 3}, {72515523, 6},
  {75015141, 3}, {77512903, 2}, {80011461, 2}, {82513572, 7}, {85014020, 3}, {87515271, 1},
  {90014439, 3}, {92512195, 2}, {95011044, 1}, {97513060, 2}, {100013091, 1}, {102513924, 3},
  {105015778, 3}, {107513445, 6}, {110013730, 3}, {112509861, 5}, {115008672, 4}, {117506437, 5},
  {120007687, 4}, {122505443, 2}, {125003621, 5}, {127502661, 2}, {130004768, 1}, {132504388, 3},
  {135004451, 2}, {137503426, 3}, {140003552, 6}, {142503524, 5}, {145003813, 4}, {147505154, 1},
  {150006530, 5}, {152505376, 5}, {155002919, 4}, {157505121, 4}, {160004934, 4}, {162506723, 1},
  {165004321, 2}, {167505126, 4}, {170006054, 3}, {172507173, 6}, {175007716, 1}, {177506373, 2},
  {180005026, 3}, {182503555, 1}, {185002852, 2}, {187501444, 4}, {190000709, 6}, {192503264, 5},
  {195005250, 1}, {197505287, 3}, {200003492, 3}, {202503335, 6}, {205001252, 2}, {207496673, 1},
  {209996387, 2}, {212494789, 1}, {214993377, 6}, {217492998, 7}, {219994054, 4}, {222492898, 1},
  {224993793, 1}, {227493511, 1}, {229992711, 3}, {232492130, 5}, {234991078, 3}, {237491717, 2},
  {239992198, 2}, {242492066, 7}, {244991908, 1}, {247493316, 3}, {249994658, 1}, {252495015, 6},
  {254995971, 4}, {257496485, 2}, {259993542, 1}, {262492736, 6}, {264992451, 1}, {267492611, 7},
  {269993059, 2}, {272495138, 1}, {274995365, 1}, {277494599, 7}, {279992960, 3}, {282491847, 4},
  {284992576, 1}, {287492033, 6}, {289993762, 4}, {292495623, 3}, {294998561, 2}, {297498245, 4}};
const static std::vector<std::pair<int,int>> c240 = 
  {{1,1},{1250213,3},{2499910,2},{3749285,3},{4998912,2},{6249382,2},
  {7501153,1},{8749828,2},{10000804,3},{11251426,2},{12503655,1},{13754279,1},
  {15003686,2},{16253220,6},{17503813,6},{18754119,2},{20005733,4},{21255072,1},
  {22504801,1},{23754466,1},{25002433,1},{26253571,3},{27502113,2},{28751969,6},
  {30000743,5},{31251971,3},{32500485,1},{33752100,4},{35001284,1},{36251393,4},
  {37502914,4},{38753891,4},{40004258,4},{41255654,6},{42505766,6},{43758306,4},
  {45009124,4},{46261696,7},{47512546,2},{48763108,2},{50012900,1},{51262273,6},
  {52513187,1},{53764929,4},{55016289,4},{56267719,4},{57518053,2},{58767362,2},
  {60015136,1},{61265957,3},{62514562,4},{63764256,6},{65014596,2},{66264224,2},
  {67514630,2},{68766116,6},{70016353,3},{71265893,6},{72515523,6},{73765536,6},
  {75015141,3},{76264711,7},{77512903,2},{78762821,2},{80011461,2},{81263461,2},
  {82513572,7},{83763621,4},{85014020,3},{86267174,3},{87515271,1},{88765666,4},
  {90014439,3},{91263654,4},{92512195,2},{93760740,3},{95011044,1},{96263393,4},
  {97513060,2},{98763270,3},{100013091,1},{101263812,2},{102513924,3},{103764803,4},
  {105015778,3},{106265250,4},{107513445,6},{108763303,3},{110013730,3},{111261281,3},
  {112509861,5},{113758692,4},{115008672,4},{116256931,2},{117506437,5},{118757825,2},
  {120007687,4},{121257122,3},{122505443,2},{123754308,4},{125003621,5},{126252483,2},
  {127502661,2},{128752642,6},{130004768,1},{131253958,2},{132504388,3},{133753312,2},
  {135004451,2},{136253159,5},{137503426,3},{138754275,1},{140003552,6},{141253666,1},
  {142503524,5},{143753188,4},{145003813,4},{146254016,4},{147505154,1},{148755011,3},
  {150006530,5},{151256163,5},{152505376,5},{153752678,2},{155002919,4},{156254469,3},
  {157505121,4},{158752545,4},{160004934,4},{161254176,3},{162506723,1},{163755237,1},
  {165004321,2},{166254151,4},{167505126,4},{168755426,1},{170006054,3},{171255968,4},
  {172507173,6},{173757447,1},{175007716,1},{176257254,5},{177506373,2},{178755108,1},
  {180005026,3},{181255937,3},{182503555,1},{183752996,1},{185002852,2},{186251840,3},
  {187501444,4},{188750336,6},{190000709,6},{191252422,4},{192503264,5},{193754496,3},
  {195005250,1},{196255042,1},{197505287,3},{198754247,3},{200003492,3},{201254183,1},
  {202503335,6},{203752547,3},{205001252,2},{206248773,5},{207496673,1},{208746566,4},
  {209996387,2},{211246370,1},{212494789,1},{213743751,5},{214993377,6},{216244802,1},
  {217492998,7},{218743078,4},{219994054,4},{221243687,2},{222492898,1},{223742980,3},
  {224993793,1},{226244807,1},{227493511,1},{228742949,3},{229992711,3},{231241286,4},
  {232492130,5},{233742241,2},{234991078,3},{236241861,1},{237491717,2},{238742178,1},
  {239992198,2},{241241924,1},{242492066,7},{243743687,4},{244991908,1},{246241766,7},
  {247493316,3},{248744866,4},{249994658,1},{251243843,4},{252495015,6},{253744832,4},
  {254995971,4},{256246178,2},{257496485,2},{258744837,2},{259993542,1},{261241632,7},
  {262492736,6},{263743328,2},{264992451,1},{266242849,3},{267492611,7},{268743173,2},
  {269993059,2},{271243750,1},{272495138,1},{273745766,5},{274995365,1},{276245664,3},
  {277494599,7},{278743491,1},{279992960,3},{281242467,2},{282491847,4},{283741634,3},
  {284992576,1},{286241216,2},{287492033,6},{288741761,1},{289993762,4},{291245092,4},
  {292495623,3},{293746435,2},{294998561,2},{296249090,3},{297498245,4},{298747648,2}};
const std::vector<std::pair<int,int>> c210 =
  {{1, 1}, {1428294, 3}, {2857188, 6}, {4285092, 4}, {5714150, 3}, {7144001, 1},
  {8571527, 5}, {10000804, 4}, {11430336, 1}, {12861062, 1}, {14289120, 3}, {15717991, 1},
  {17146021, 1}, {18574849, 3}, {20005733, 6}, {21434081, 2}, {22861827, 4}, {24288964, 3},
  {25718660, 3}, {27145218, 3}, {28572837, 6}, {30000768, 2}, {31430371, 3}, {32857894, 1},
  {34287813, 5}, {35715462, 6}, {37145088, 3}, {38575106, 5}, {40004259, 2}, {41433703, 6},
  {42863975, 3}, {44293606, 2}, {45725541, 6}, {47156005, 4}, {48584866, 1}, {50012902, 2},
  {51441696, 2}, {52871872, 2}, {54300452, 2}, {55731716, 3}, {57161856, 2}, {58589090, 1},
  {60015137, 3}, {61443876, 6}, {62871840, 3}, {64300709, 2}, {65728516, 3}, {67157313, 4},
  {68587427, 5}, {70016355, 3}, {71444576, 4}, {72872544, 3}, {74300641, 6}, {75729031, 4},
  {77156259, 4}, {78584002, 1}, {80011463, 2}, {81441766, 3}, {82869958, 2}, {84299398, 2},
  {85730279, 1}, {87158049, 1}, {88586913, 6}, {90014466, 3}, {91442439, 3}, {92868517, 1},
  {94297248, 6}, {95726884, 6}, {97156359, 4}, {98585093, 1}, {100013120, 2}, {101442020, 7},
  {102871008, 1}, {104301057, 2}, {105731109, 4}, {107156710, 1}, {108584869, 2}, {110013733, 1},
  {111439399, 5}, {112866502, 3}, {114294181, 1}, {115721735, 1}, {117148992, 6}, {118578628, 3},
  {120007715, 2}, {121435015, 4}, {122862306, 1}, {124289572, 2}, {125717731, 2}, {127145317, 2},
  {128574374, 3}, {130004770, 6}, {131433222, 4}, {132861094, 1}, {134290753, 1}, {135717639, 1},
  {137146464, 2}, {138575235, 3}, {140003555, 7}, {141432802, 4}, {142859616, 1}, {144288359, 2},
  {145718180, 2}, {147148322, 5}, {148576581, 2}, {150006534, 1}, {151434787, 1}, {152860998, 5},
  {154288548, 2}, {155718624, 1}, {157147491, 5}, {158574245, 1}, {160004961, 3}, {161433669, 3},
  {162864547, 3}, {164290656, 1}, {165718112, 2}, {167147045, 3}, {168576550, 2}, {170006082, 3},
  {171434087, 7}, {172864804, 4}, {174292864, 1}, {175721188, 6}, {177149312, 3}, {178577508, 5},
  {180005030, 1}, {181434368, 5}, {182859939, 4}, {184287940, 1}, {185716545, 3}, {187144517, 5},
  {188571463, 1}, {190000739, 1}, {191430855, 1}, {192860642, 4}, {194291105, 2}, {195719265, 1},
  {197148485, 1}, {198575078, 5}, {200003523, 2}, {201432419, 5}, {202860513, 1}, {204288006, 5},
  {205714369, 2}, {207140739, 1}, {208567715, 3}, {209996417, 5}, {211424485, 7}, {212851367, 2},
  {214278723, 6}, {215707555, 2}, {217136194, 1}, {218564231, 7}, {219994084, 2}, {221422497, 1},
  {222849859, 4}, {224280452, 1}, {225708739, 3}, {227137986, 5}, {228564354, 3}, {229992741, 5},
  {231420933, 1}, {232849376, 1}, {234277252, 2}, {235706400, 2}, {237134500, 1}, {238563526, 2},
  {239992229, 1}, {241420994, 2}, {242850336, 1}, {244279234, 1}, {245707456, 5}, {247135588, 1},
  {248565762, 1}, {249994691, 1}, {251422913, 2}, {252852615, 2}, {254281191, 4}, {255710470, 3},
  {257139206, 1}, {258567332, 4}, {259993572, 2}, {261420228, 1}, {262849890, 4}, {264278209, 2},
  {265706950, 7}, {267135525, 1}, {268564260, 2}, {269993090, 2}, {271422789, 1}, {272852673, 2},
  {274280996, 4}, {275710118, 1}, {277137984, 2}, {278564960, 6}, {279992967, 1}, {281420612, 1},
  {282849029, 4}, {284277958, 2}, {285707361, 2}, {287133732, 6}, {288563270, 4}, {289993767, 5},
  {291423136, 1}, {292852452, 3}, {294282564, 3}, {295713826, 4}, {297140611, 1}, {298569248, 7}};
const std::vector<std::pair<int,int>> c270 = 
  {{1, 1}, {1111267, 2}, {2222145, 1}, {3333473, 1}, {4443815, 1}, {5555330, 4},
  {6666758, 6}, {7778438, 1}, {8889186, 3}, {10000803, 5}, {11112612, 4}, {12225091, 2},
  {13336866, 5}, {14447331, 2}, {15559458, 3}, {16670279, 5}, {17781926, 2}, {18892993, 1},
  {20005731, 5}, {21116580, 4}, {22227622, 6}, {23337668, 1}, {24448096, 2}, {25558629, 5},
  {26669504, 1}, {27779012, 3}, {28890695, 2}, {30000740, 6}, {31112996, 1}, {32222213, 6},
  {33334597, 3}, {34446311, 4}, {35556899, 6}, {36668647, 2}, {37780321, 4}, {38892481, 1},
  {40004230, 1}, {41116262, 2}, {42227426, 5}, {43340420, 1}, {44452322, 4}, {45567239, 1},
  {46678692, 1}, {47789889, 6}, {48901637, 5}, {50012896, 2}, {51123552, 5}, {52235488, 6},
  {53348230, 2}, {54459684, 1}, {55572965, 6}, {56685217, 1}, {57795490, 7}, {58906018, 3},
  {60015105, 6}, {61126818, 6}, {62236772, 1}, {63347299, 2}, {64459362, 2}, {65569862, 5},
  {66681476, 1}, {67793540, 2}, {68904295, 3}, {70016322, 1}, {71126464, 2}, {72238178, 3},
  {73348225, 6}, {74459906, 1}, {75570049, 2}, {76680992, 1}, {77790563, 2}, {78901473, 3},
  {80011428, 2}, {81124640, 1}, {82236130, 1}, {83346725, 3}, {84458114, 1}, {85570247, 1},
  {86683138, 3}, {87793508, 5}, {88904772, 1}, {90014407, 3}, {91125254, 3}, {92234148, 4},
  {93343618, 2}, {94455874, 2}, {95568193, 5}, {96680356, 1}, {97791463, 1}, {98901824, 1},
  {100013057, 3}, {101124965, 2}, {102235972, 3}, {103348098, 1}, {104459812, 4}, {105572774, 1},
  {106681286, 2}, {107790978, 1}, {108902310, 6}, {110013699, 1}, {111122692, 7}, {112232423, 6},
  {113343271, 7}, {114452960, 2}, {115563969, 1}, {116673569, 1}, {117783910, 2}, {118896833, 4},
  {120007651, 5}, {121118144, 4}, {122227296, 1}, {123337573, 1}, {124447968, 1}, {125559296, 4},
  {126668898, 6}, {127780803, 3}, {128891268, 3}, {130004708, 4}, {131114656, 4}, {132226720, 4},
  {133337154, 4}, {134449799, 5}, {135559652, 5}, {136669793, 6}, {137780710, 1}, {138892929, 1},
  {140003488, 6}, {141114950, 6}, {142225955, 2}, {143336197, 1}, {144447042, 1}, {145559621, 3},
  {146670469, 2}, {147781862, 3}, {148893735, 3}, {150006467, 4}, {151116387, 5}, {152228162, 1},
  {153336608, 5}, {154447296, 3}, {155559495, 1}, {156671460, 1}, {157782564, 7}, {158892357, 4},
  {160004871, 1}, {161115461, 5}, {162228736, 2}, {163339072, 4}, {164448644, 3}, {165559362, 4},
  {166671139, 1}, {167782214, 6}, {168894402, 2}, {170005989, 3}, {171117539, 2}, {172229223, 1},
  {173341414, 1}, {174451333, 5}, {175562820, 5}, {176673606, 2}, {177784581, 6}, {178893825, 1},
  {180004962, 2}, {181117287, 5}, {182225700, 6}, {183335813, 1}, {184446880, 1}, {185558212, 6},
  {186667968, 3}, {187779076, 2}, {188889376, 4}, {190000642, 1}, {191112581, 4}, {192225058, 4},
  {193337921, 5}, {194449568, 2}, {195560545, 2}, {196671751, 3}, {197782403, 2}, {198893377, 3},
  {200003398, 5}, {201115587, 1}, {202225056, 5}, {203336066, 2}, {204446915, 7}, {205556067, 2},
  {206664066, 1}, {207774400, 3}, {208886212, 7}, {209996320, 1}, {211106886, 3}, {212216384, 4},
  {213326628, 5}, {214437444, 1}, {215549024, 3}, {216661030, 2}, {217771106, 4}, {218881635, 3},
  {219993959, 1}, {221105219, 2}, {222215623, 2}, {223326148, 6}, {224439075, 4}, {225549378, 1},
  {226661095, 4}, {227770823, 2}, {228881956, 2}, {229992640, 1}, {231102529, 2}, {232214627, 3},
  {233325029, 2}, {234436295, 2}, {235547969, 1}, {236658246, 2}, {237770305, 2}, {238880963, 7},
  {239992101, 2}, {241103139, 1}, {242214468, 1}, {243326720, 6}, {244437475, 3}, {245547680, 6},
  {246658660, 1}, {247771009, 3}, {248883584, 1}, {249994562, 1}, {251104640, 1}, {252216519, 3},
  {253328707, 6}, {254439622, 4}, {255551238, 2}, {256662752, 4}, {257774146, 1}, {258883619, 2},
  {259993440, 4}, {261102436, 1}, {262214951, 3}, {263326596, 6}, {264436901, 4}, {265547845, 6},
  {266659170, 6}, {267769858, 2}, {268882724, 6}, {269992965, 2}, {271104996, 1}, {272216993, 3},
  {273329472, 2}, {274439719, 2}, {275551236, 3}, {276662407, 1}, {277771366, 4}, {278882208, 3},
  {279992866, 3}, {281103367, 1}, {282213185, 7}, {283325477, 5}, {284437058, 4}, {285548613, 1},
  {286658087, 5}, {287769441, 1}, {288880641, 6}, {289993633, 1}, {291105377, 3}, {292217063, 3},
  {293329061, 2}, {294441379, 1}, {295555239, 1}, {296665447, 4}, {297775111, 1}, {298886372, 6}};


#endif