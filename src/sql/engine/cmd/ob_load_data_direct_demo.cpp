#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/cmd/ob_load_data_direct_demo.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace sql
{
using namespace blocksstable;
using namespace common;
using namespace lib;
using namespace observer;
using namespace share;
using namespace share::schema;

/**
 * ObLoadDataBuffer
 */

ObLoadDataBuffer::ObLoadDataBuffer()
  : allocator_(ObModIds::OB_SQL_LOAD_DATA), data_(nullptr), begin_pos_(0), end_pos_(0), capacity_(0)
{
}

ObLoadDataBuffer::~ObLoadDataBuffer()
{
  reset();
}

void ObLoadDataBuffer::reuse()
{
  begin_pos_ = 0;
  end_pos_ = 0;
}

void ObLoadDataBuffer::reset()
{
  // allocator_.reset();
  free(data_);
  data_ = nullptr;
  begin_pos_ = 0;
  end_pos_ = 0;
  capacity_ = 0;
}

int ObLoadDataBuffer::create(int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr != data_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadDataBuffer init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(capacity));
  } else {
    if (OB_ISNULL(data_ = static_cast<char *>(malloc(capacity)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), K(capacity));
    } else {
      capacity_ = capacity;
    }
    /*
    allocator_.set_tenant_id(MTL_ID());
    if (OB_ISNULL(data_ = static_cast<char *>(allocator_.alloc(capacity)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), K(capacity));
    } else {
      capacity_ = capacity;
    }
    */
  }
  return ret;
}

int ObLoadDataBuffer::squash()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == data_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadDataBuffer not init", KR(ret), KP(this));
  } else {
    const int64_t data_size = get_data_size();
    if (data_size > 0) {
      MEMMOVE(data_, data_ + begin_pos_, data_size);
    }
    begin_pos_ = 0;
    end_pos_ = data_size;
  }
  return ret;
}

ObLoadSequentialFileAppender::ObLoadSequentialFileAppender()
  : offset_(0), is_read_end_(false)
{
}

ObLoadSequentialFileAppender::~ObLoadSequentialFileAppender()
{
}



int ObLoadSequentialFileAppender::open(const ObString &filepath, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(buffer_.create(capacity))) {
    LOG_INFO("MMMMM buffer create fail", KR(ret));
  } else if (OB_FAIL(file_writer_.open(filepath, true, true))) {
    LOG_INFO("MMMMM fail to open file", K(filepath), KR(ret));
  }
  return ret;
}

int ObLoadSequentialFileAppender::write(const char *data, int64_t len) 
{
  // LOG_INFO("MMMMM write", K(data));
  std::lock_guard<std::mutex> guard(mutex_);
  int ret = OB_SUCCESS;
  if (len > buffer_.get_remain_size()) {
    if (OB_FAIL(file_writer_.append(buffer_.begin(), buffer_.get_data_size(), true))) {
      LOG_INFO("MMMMM fail to write", KR(ret));
    }
    buffer_.reuse();
  }
  MEMCPY(buffer_.end(), data, len);
  buffer_.produce(len);
  return ret;
}

void ObLoadSequentialFileAppender::close()
{
  file_writer_.append(buffer_.begin(), buffer_.get_data_size(), true);
  buffer_.reset();
  file_writer_.close();
}

/**
 * ObLoadSequentialFileReader
 */

ObLoadSequentialFileReader::ObLoadSequentialFileReader()
  : offset_(0), is_read_end_(false)
{
}

ObLoadSequentialFileReader::~ObLoadSequentialFileReader()
{
}

void ObLoadSequentialFileReader::close()
{
  file_reader_.close();
  offset_ = 0;
  is_read_end_ = false;
}

int ObLoadSequentialFileReader::open(const ObString &filepath)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(file_reader_.open(filepath, false))) {
    LOG_WARN("fail to open file", KR(ret));
  }
  return ret;
}

int ObLoadSequentialFileReader::read_next_buffer_from(char *buf, int64_t size, int64_t offset, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!file_reader_.is_opened())) {
    ret = OB_FILE_NOT_OPENED;
    LOG_WARN("MMMMM file not opened", KR(ret));
  } else if (OB_FAIL(file_reader_.pread(buf, size, offset, read_size))) {
    LOG_WARN("MMMMM fail to do pread", KR(ret));
  }
  return ret;
}

int ObLoadSequentialFileReader::read_next_buffer(ObLoadDataBuffer &buffer)
{
  // std::lock_guard<std::mutex> guard(mutex_);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!file_reader_.is_opened())) {
    ret = OB_FILE_NOT_OPENED;
    LOG_WARN("file not opened", KR(ret));
  } else if (is_read_end_) {
    ret = OB_ITER_END;
  } else if (OB_LIKELY(buffer.get_remain_size() > 0)) {
    const int64_t buffer_remain_size = buffer.get_remain_size();
    int64_t read_size = 0;
    if (OB_FAIL(file_reader_.pread(buffer.end(), buffer_remain_size, offset_, read_size))) {
      LOG_WARN("fail to do pread", KR(ret));
    } else if (read_size == 0) {
      is_read_end_ = true;
      ret = OB_ITER_END;
    } else {
      offset_ += read_size;
      buffer.produce(read_size);
      // LOG_WARN("MMMMM buffer size", K(read_size), K(offset_), K(buffer.data()));
    }
  }
  return ret;
}

/**
 * ObLoadCSVPaser
 */

ObLoadCSVPaser::ObLoadCSVPaser()
  : allocator_(ObModIds::OB_SQL_LOAD_DATA), collation_type_(CS_TYPE_INVALID), is_inited_(false)
{
}

ObLoadCSVPaser::~ObLoadCSVPaser()
{
  reset();
}

void ObLoadCSVPaser::reset()
{
  allocator_.reset();
  collation_type_ = CS_TYPE_INVALID;
  row_.reset();
  err_records_.reset();
  is_inited_ = false;
}

int ObLoadCSVPaser::init(const ObDataInFileStruct &format, int64_t column_count,
                         ObCollationType collation_type, int field_num)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadCSVPaser init twice", KR(ret), KP(this));
  } else if (OB_FAIL(csv_parser_.init(format, column_count, collation_type))) {
    LOG_WARN("fail to init csv parser", KR(ret));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    ObObj *objs = nullptr;
    if (OB_ISNULL(objs = static_cast<ObObj *>(allocator_.alloc(sizeof(ObObj) * column_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    } else {
      new (objs) ObObj[column_count];
      row_.cells_ = objs;
      row_.count_ = column_count;
      collation_type_ = collation_type;
      is_inited_ = true;
      field_num_ = field_num;
    }
  }
  return ret;
}

/*
MMMMM 14686916|7112590|237605|5|24|38453.76|0.01|0.06|R|F|1994-01-01|1994-03-02|1994-01-08|TAKE BACK RETURN|MAIL| regular pinto beans boost. idly pending|
MMMMM 7112590|237605|5|24|38453.76|0.01|0.06|R|F|1994-01-01|1994-03-02|1994-01-08|TAKE BACK RETURN|MAIL| regular pinto beans boost. idly pending|
MMMMM 237605|5|24|38453.76|0.01|0.06|R|F|1994-01-01|1994-03-02|1994-01-08|TAKE BACK RETURN|MAIL| regular pinto beans boost. idly pending|
MMMMM 5|24|38453.76|0.01|0.06|R|F|1994-01-01|1994-03-02|1994-01-08|TAKE BACK RETURN|MAIL| regular pinto beans boost. idly pending|
MMMMM 24|38453.76|0.01|0.06|R|F|1994-01-01|1994-03-02|1994-01-08|TAKE BACK RETURN|MAIL| regular pinto beans boost. idly pending|
MMMMM 38453.76|0.01|0.06|R|F|1994-01-01|1994-03-02|1994-01-08|TAKE BACK RETURN|MAIL| regular pinto beans boost. idly pending|
MMMMM 0.01|0.06|R|F|1994-01-01|1994-03-02|1994-01-08|TAKE BACK RETURN|MAIL| regular pinto beans boost. idly pending|
MMMMM 0.06|R|F|1994-01-01|1994-03-02|1994-01-08|TAKE BACK RETURN|MAIL| regular pinto beans boost. idly pending|
MMMMM R|F|1994-01-01|1994-03-02|1994-01-08|TAKE BACK RETURN|MAIL| regular pinto beans boost. idly pending|
MMMMM F|1994-01-01|1994-03-02|1994-01-08|TAKE BACK RETURN|MAIL| regular pinto beans boost. idly pending|
MMMMM 1994-01-01|1994-03-02|1994-01-08|TAKE BACK RETURN|MAIL| regular pinto beans boost. idly pending|
MMMMM 1994-03-02|1994-01-08|TAKE BACK RETURN|MAIL| regular pinto beans boost. idly pending|
MMMMM 1994-01-08|TAKE BACK RETURN|MAIL| regular pinto beans boost. idly pending|
MMMMM TAKE BACK RETURN|MAIL| regular pinto beans boost. idly pending|
MMMMM MAIL| regular pinto beans boost. idly pending|
MMMMM  regular pinto beans boost. idly pending|
*/

inline int dic_compare(int64_t key1, int key2, int64_t b1, int b2)
{
  return key1 < b1 || (key1 == b1 && key2 < b2);
}

int get_group_id(int key1, int key2, int num)
{
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
  const std::vector<std::pair<int,int>> *c;
  switch(num) {
    case 6:
      c = &c6;break;
    case 32: 
      c = &c32;break;
    case 60:
      c = &c60;break;
    case 64: 
      c = &c64;break;    
    case 240:
      c = &c240;break;
    default:
      c = nullptr;
  }
  if (c == nullptr) {
    return -1;
  }
  auto iter = std::lower_bound(c->begin(), c->end(), std::make_pair(key1, key2));
  int dis = std::distance(c->begin(), iter);
  return dis ? dis - 1 : 0;
}

int get_group_id_8(int64_t key1, int key2)
{
  if (dic_compare(key1, key2, 37502915, 5)) {
    return 0;
  } else if (dic_compare(key1, key2, 75015168, 1)) {
    return 1;
  } else if (dic_compare(key1, key2, 112509890, 2)) {
    return 2;
  } else if (dic_compare(key1, key2, 150006562, 1)) {
    return 3;
  } else if (dic_compare(key1, key2, 187501475, 2)) {
    return 4;
  } else if (dic_compare(key1, key2, 224993826, 1)) {
    return 5;
  } else if (dic_compare(key1, key2, 262492779, 3)) {
    return 6;
  } else {
    return 7;
  }
}

// magic!
int get_group_id_4(int64_t key1, int key2)
{
  // 4
  if (dic_compare(key1, key2, 75015168, 1)) {
    return 0;
  } else if (dic_compare(key1, key2, 150006562, 1)) {
    return 1;
  }  else if (dic_compare(key1, key2, 224993826, 1)) {
    return 2;
  }  else {
    return 3;
  }
}

int ObLoadCSVPaser::fast_get_next_row(const char *begin, const char *end, const common::ObNewRow *&row)
{

  // LOG_INFO("MMMMM get row", K(begin));
  const char *iter = begin;
  // printf("MMMMM %-*s\n", (int)(end-begin), begin);
  int field_cnt = 0;
  bool first = true;

  const char *ptr = nullptr;
  while (iter < end && *iter != '\n') {
    if (first) {
      ptr = iter;
      first = false;
    }
    if (*iter == '|') {
      ObObj &obj = row_.cells_[field_cnt];
      obj.set_string(ObVarcharType, ObString(std::distance(ptr, iter), ptr));
      int len = std::distance(ptr, iter);
      // printf("MMMMM len %d, %-*s\n", len, len, ptr);
      obj.set_collation_type(collation_type_);
      field_cnt++;
      first = true;
    }
    iter++;
  }
  if (field_cnt != field_num_ || iter == end) {
    LOG_INFO("MMMMM fast get bad row", K(field_cnt), K(iter == end), K(begin));
    return OB_ITER_END;
  }
  if (end != iter) {
    iter++;
  }
  row = &row_;
  assert(*(iter-1) == '\n');

  return OB_SUCCESS;
}

int ObLoadCSVPaser::fast_get_next_row(ObLoadDataBuffer &buffer, const common::ObNewRow *&row) 
{
  if (buffer.empty()) {
    return OB_ITER_END;
  }

  // LOG_INFO("MMMMM get row");
  const char *begin = buffer.begin();
  const char *end = buffer.end();
  const char *iter = begin;
  // const char *iters[field_num_];

  int field_cnt = 0;
  bool first = true;

  const char *ptr = nullptr;

  int64_t key1 = 0;
  int key2 = 0;
  while (iter < end && *iter != '\n') {
    if (first) {
      ptr = iter;
      first = false;
    }
    if (*iter == '|') {
      ObObj &obj = row_.cells_[field_cnt];
      obj.set_string(ObVarcharType, ObString(std::distance(ptr, iter), ptr));
      int len = std::distance(ptr, iter);
      // printf("MMMMM len %d, %-*s\n", len, len, ptr);
      obj.set_collation_type(collation_type_);
      field_cnt++;
      first = true;
    }
    if (field_cnt == 0) {
      key1 = key1 * 10 + (*iter - '0');
    }
    if (field_cnt == 3 && isdigit(*iter)) {
      key2 = key2 * 10 + (*iter - '0');
    }
    iter++;
  }
  if (field_cnt != field_num_ || iter == end) {
    return OB_ITER_END;
  }
  if (end != iter) {
    iter++;
  }
  buffer.consume(iter - begin);
  row = &row_;
  assert(*(iter-1) == '\n');

  return OB_SUCCESS;
}

int ObLoadCSVPaser::fast_get_next_row(ObLoadDataBuffer &buffer, const common::ObNewRow *&row, int &group_id)
{
  // LOG_INFO("MMMMM get row");
  if (buffer.empty()) {
    return OB_ITER_END;
  }

  // LOG_INFO("MMMMM get row");
  const char *begin = buffer.begin();
  const char *end = buffer.end();
  const char *iter = begin;
  // const char *iters[field_num_];

  int field_cnt = 0;
  bool first = true;

  const char *ptr = nullptr;

  int64_t key1 = 0;
  int key2 = 0;
  while (iter < end && *iter != '\n') {
    if (first) {
      ptr = iter;
      first = false;
    }
    if (*iter == '|') {
      ObObj &obj = row_.cells_[field_cnt];
      obj.set_string(ObVarcharType, ObString(std::distance(ptr, iter), ptr));
      int len = std::distance(ptr, iter);
      // printf("MMMMM len %d, %-*s\n", len, len, ptr);
      obj.set_collation_type(collation_type_);
      field_cnt++;
      first = true;
    }
    if (field_cnt == 0) {
      key1 = key1 * 10 + (*iter - '0');
    }
    if (field_cnt == 3 && isdigit(*iter)) {
      key2 = key2 * 10 + (*iter - '0');
    }
    iter++;
  }
  if (field_cnt != field_num_ || iter == end) {
    return OB_ITER_END;
  }
  if (end != iter) {
    iter++;
  }
  group_id = get_group_id(key1, key2, 32);
  buffer.consume(iter - begin);
  row = &row_;
  assert(*(iter-1) == '\n');

  return OB_SUCCESS;
}

int ObLoadCSVPaser::get_next_row(ObLoadDataBuffer &buffer, const ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  // LOG_INFO("MMMMM csv");
  buffer.lock();
  if (buffer.empty()) {
    ret = OB_ITER_END;
    buffer.unlock();
  } else {
    const char *str = buffer.begin();
    const char *end = buffer.end();
    int64_t nrows = 1;
    if (OB_FAIL(csv_parser_.scan(str, end, nrows, nullptr, nullptr, unused_row_handler_,
                                 err_records_, false))) {
      LOG_WARN("MMMMM fail to scan buffer", KR(ret));
      buffer.unlock();
    } else if (OB_UNLIKELY(!err_records_.empty())) {
      ret = err_records_.at(0).err_code;
      LOG_WARN("MMMMM fail to parse line", KR(ret));
      buffer.unlock();
    } else if (0 == nrows) {
      ret = OB_ITER_END;
      buffer.unlock();
    } else {
      buffer.consume(str - buffer.begin());
      buffer.unlock();
      const ObIArray<ObCSVGeneralParser::FieldValue> &field_values_in_file =
        csv_parser_.get_fields_per_line();
      for (int64_t i = 0; i < row_.count_; ++i) {
        const ObCSVGeneralParser::FieldValue &str_v = field_values_in_file.at(i);
        ObObj &obj = row_.cells_[i];
        if (str_v.is_null_) {
          obj.set_null();
        } else {
          obj.set_string(ObVarcharType, ObString(str_v.len_, str_v.ptr_));
          printf("MMMMM len: %d, %-*s\n", str_v.len_, str_v.len_, str_v.ptr_);
          obj.set_collation_type(collation_type_);
        }
      }
      row = &row_;
    }
  }
  return ret;
}

/**
 * ObLoadDatumRow
 */

ObLoadDatumRow::ObLoadDatumRow()
  : allocator_(ObModIds::OB_SQL_LOAD_DATA), capacity_(0), count_(0), datums_(nullptr)
{
}

ObLoadDatumRow::~ObLoadDatumRow()
{
}

void ObLoadDatumRow::reset()
{
  allocator_.reset();
  capacity_ = 0;
  count_ = 0;
  datums_ = nullptr;
}

int ObLoadDatumRow::init(int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(capacity));
  } else {
    reset();
    allocator_.set_tenant_id(MTL_ID());
    if (OB_ISNULL(datums_ = static_cast<ObStorageDatum *>(
                    allocator_.alloc(sizeof(ObStorageDatum) * capacity)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    } else {
      new (datums_) ObStorageDatum[capacity];
      capacity_ = capacity;
      count_ = capacity;
    }
  }
  return ret;
}

int64_t ObLoadDatumRow::get_deep_copy_size() const
{
  int64_t size = 0;
  size += sizeof(ObStorageDatum) * count_;
  for (int64_t i = 0; i < count_; ++i) {
    size += datums_[i].get_deep_copy_size();
  }
  return size;
}

int ObLoadDatumRow::deep_copy(const ObLoadDatumRow &src, char *buf, int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!src.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(src));
  } else {
    reset();
    ObStorageDatum *datums = nullptr;
    const int64_t datum_cnt = src.count_;
    datums = new (buf + pos) ObStorageDatum[datum_cnt];
    pos += sizeof(ObStorageDatum) * datum_cnt;
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_cnt; ++i) {
      if (OB_FAIL(datums[i].deep_copy(src.datums_[i], buf, len, pos))) {
        LOG_WARN("fail to deep copy storage datum", KR(ret), K(src.datums_[i]));
      }
    }
    if (OB_SUCC(ret)) {
      capacity_ = datum_cnt;
      count_ = datum_cnt;
      datums_ = datums;
    }
  }
  return ret;
}

DEF_TO_STRING(ObLoadDatumRow)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(capacity), K_(count));
  if (nullptr != datums_) {
    J_ARRAY_START();
    for (int64_t i = 0; i < count_; ++i) {
      databuff_printf(buf, buf_len, pos, "col_id=%ld:", i);
      pos += datums_[i].storage_to_string(buf + pos, buf_len - pos);
      databuff_printf(buf, buf_len, pos, ",");
    }
    J_ARRAY_END();
  }
  J_OBJ_END();
  return pos;
}

OB_DEF_SERIALIZE(ObLoadDatumRow)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE_ARRAY(datums_, count_);
  return ret;
}

OB_DEF_DESERIALIZE(ObLoadDatumRow)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  OB_UNIS_DECODE(count);
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(count <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected count", K(count));
    } else if (count > capacity_ && OB_FAIL(init(count))) {
      LOG_WARN("fail to init", KR(ret));
    } else {
      OB_UNIS_DECODE_ARRAY(datums_, count);
      count_ = count;
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLoadDatumRow)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN_ARRAY(datums_, count_);
  return len;
}

/**
 * ObLoadDatumRowCompare
 */

ObLoadDatumRowCompare::ObLoadDatumRowCompare()
  : result_code_(OB_SUCCESS), rowkey_column_num_(0), datum_utils_(nullptr), is_inited_(false)
{
}

ObLoadDatumRowCompare::~ObLoadDatumRowCompare()
{
}

int ObLoadDatumRowCompare::init(int64_t rowkey_column_num, const ObStorageDatumUtils *datum_utils)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadDatumRowCompare init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(rowkey_column_num <= 0 || nullptr == datum_utils)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(rowkey_column_num), KP(datum_utils));
  } else {
    rowkey_column_num_ = rowkey_column_num;
    datum_utils_ = datum_utils;
    is_inited_ = true;
  }
  return ret;
}

void ObLoadDatumRowCompare::clean_up() 
{
  is_inited_ = false;
  datum_utils_ = nullptr;
  rowkey_column_num_ = 0;
}

bool ObLoadDatumRowCompare::operator()(const ObLoadDatumRow *lhs, const ObLoadDatumRow *rhs)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDatumRowCompare not init", KR(ret), KP(this));
  } else if (OB_ISNULL(lhs) || OB_ISNULL(rhs) ||
             OB_UNLIKELY(!lhs->is_valid() || !rhs->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(lhs), KPC(rhs));
  } else if (OB_UNLIKELY(lhs->count_ < rowkey_column_num_ || rhs->count_ < rowkey_column_num_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected column count", KR(ret), KPC(lhs), KPC(rhs), K_(rowkey_column_num));
  } else {
    // skip many checks in ObDatumRowkey
    const ObStoreCmpFuncs &cmp_funcs = datum_utils_->get_cmp_funcs();
    int64_t cmp_cnt = rowkey_column_num_;
    for (int64_t i = 0; OB_SUCC(ret) && i < cmp_cnt && 0 == cmp_ret; ++i) {
      if (OB_FAIL(cmp_funcs.at(i).compare(lhs->datums_[i], rhs->datums_[i], cmp_ret))) {
        STORAGE_LOG(WARN, "Failed to compare datum rowkey", K(ret), K(i), K(*lhs), K(*rhs));
      }
    }
    /*
    if (OB_FAIL(lhs_rowkey_.assign(lhs->datums_, rowkey_column_num_))) {
      LOG_WARN("fail to assign datum rowkey", KR(ret), K(lhs), K_(rowkey_column_num));
    } else if (OB_FAIL(rhs_rowkey_.assign(rhs->datums_, rowkey_column_num_))) {
      LOG_WARN("fail to assign datum rowkey", KR(ret), K(rhs), K_(rowkey_column_num));
    } else if (OB_FAIL(lhs_rowkey_.compare(rhs_rowkey_, *datum_utils_, cmp_ret))) {
      LOG_WARN("fail to compare rowkey", KR(ret), K(rhs_rowkey_), K(rhs_rowkey_), KP(datum_utils_));
    }
    */
  }
  if (OB_FAIL(ret)) {
    result_code_ = ret;
  }
  return cmp_ret < 0;
}

/**
 * ObLoadRowCaster
 */

ObLoadRowCaster::ObLoadRowCaster()
  : column_count_(0),
    collation_type_(CS_TYPE_INVALID),
    cast_allocator_(ObModIds::OB_SQL_LOAD_DATA),
    is_inited_(false)
{
}

ObLoadRowCaster::~ObLoadRowCaster()
{
}

int ObLoadRowCaster::init(const ObTableSchema *table_schema,
                          const ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadRowCaster init twice", KR(ret));
  } else if (OB_UNLIKELY(nullptr == table_schema || field_or_var_list.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_schema), K(field_or_var_list));
  } else if (OB_FAIL(OTTZ_MGR.get_tenant_tz(MTL_ID(), tz_info_.get_tz_map_wrap()))) {
    LOG_WARN("fail to get tenant time zone", KR(ret));
  } else if (OB_FAIL(init_column_schemas_and_idxs(table_schema, field_or_var_list))) {
    LOG_WARN("fail to init column schemas and idxs", KR(ret));
  } else if (OB_FAIL(datum_row_.init(table_schema->get_column_count()))) {
    LOG_WARN("fail to init datum row", KR(ret));
  } else {
    column_count_ = table_schema->get_column_count();
    collation_type_ = table_schema->get_collation_type();
    cast_allocator_.set_tenant_id(MTL_ID());
    is_inited_ = true;
  }
  return ret;
}

int ObLoadRowCaster::init_column_schemas_and_idxs(
  const ObTableSchema *table_schema,
  const ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObColDesc, 64> column_descs;
  if (OB_FAIL(table_schema->get_column_ids(column_descs))) {
    LOG_WARN("fail to get column descs", KR(ret), KPC(table_schema));
  } else {
    bool found_column = true;
    for (int64_t i = 0; OB_SUCC(ret) && OB_LIKELY(found_column) && i < column_descs.count(); ++i) {
      const ObColDesc &col_desc = column_descs.at(i);
      const ObColumnSchemaV2 *col_schema = table_schema->get_column_schema(col_desc.col_id_);
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema", KR(ret), K(col_desc));
      } else if (OB_UNLIKELY(col_schema->is_hidden())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected hidden column", KR(ret), K(i), KPC(col_schema));
      } else if (OB_FAIL(column_schemas_.push_back(col_schema))) {
        LOG_WARN("fail to push back column schema", KR(ret));
      } else {
        found_column = false;
      }
      // find column in source data columns
      for (int64_t j = 0; OB_SUCC(ret) && OB_LIKELY(!found_column) && j < field_or_var_list.count();
           ++j) {
        const ObLoadDataStmt::FieldOrVarStruct &field_or_var_struct = field_or_var_list.at(j);
        if (col_desc.col_id_ == field_or_var_struct.column_id_) {
          found_column = true;
          if (OB_FAIL(column_idxs_.push_back(j))) {
            LOG_WARN("fail to push back column idx", KR(ret), K(column_idxs_), K(i), K(col_desc),
                     K(j), K(field_or_var_struct));
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(!found_column)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported incomplete column data", KR(ret), K(column_idxs_), K(column_descs),
               K(field_or_var_list));
    }
  }
  return ret;
}

int ObLoadRowCaster::get_casted_row(const ObNewRow &new_row, const ObLoadDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  // LOG_INFO("MMMMM cast row");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadRowCaster not init", KR(ret));
  } else {
    const int64_t extra_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    cast_allocator_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < column_idxs_.count(); ++i) {
      int64_t column_idx = column_idxs_.at(i);
      if (OB_UNLIKELY(column_idx < 0 || column_idx >= new_row.count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column idx", KR(ret), K(column_idx), K(new_row.count_));
      } else {
        const ObColumnSchemaV2 *column_schema = column_schemas_.at(i);
        const ObObj &src_obj = new_row.cells_[column_idx];
        ObStorageDatum &dest_datum = datum_row_.datums_[i];
        if (OB_FAIL(cast_obj_to_datum(column_schema, src_obj, dest_datum))) {
          LOG_WARN("fail to cast obj to datum", KR(ret), K(src_obj));
        }
      }
    }
    if (OB_SUCC(ret)) {
      datum_row = &datum_row_;
    }
  }
  return ret;
}

int ObLoadRowCaster::cast_obj_to_datum(const ObColumnSchemaV2 *column_schema, const ObObj &obj,
                                       ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  ObDataTypeCastParams cast_params(&tz_info_);
  ObCastCtx cast_ctx(&cast_allocator_, &cast_params, CM_NONE, collation_type_);
  const ObObjType expect_type = column_schema->get_meta_type().get_type();
  ObObj casted_obj;
  if (obj.is_null()) {
    casted_obj.set_null();
  } else if (is_oracle_mode() && (obj.is_null_oracle() || 0 == obj.get_val_len())) {
    casted_obj.set_null();
  } else if (is_mysql_mode() && 0 == obj.get_val_len() && !ob_is_string_tc(expect_type)) {
    ObObj zero_obj;
    zero_obj.set_int(0);
    if (OB_FAIL(ObObjCaster::to_type(expect_type, cast_ctx, zero_obj, casted_obj))) {
      LOG_WARN("fail to do to type", KR(ret), K(zero_obj), K(expect_type));
    }
  } else {
    if (OB_FAIL(ObObjCaster::to_type(expect_type, cast_ctx, obj, casted_obj))) {
      LOG_WARN("fail to do to type", KR(ret), K(obj), K(expect_type));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(datum.from_obj_enhance(casted_obj))) {
      LOG_WARN("fail to from obj enhance", KR(ret), K(casted_obj));
    }
  }
  return ret;
}

/**
 * ObLoadExternalSort
 */

ObLoadExternalSort::ObLoadExternalSort()
  : allocator_(ObModIds::OB_SQL_LOAD_DATA), is_closed_(false), is_inited_(false)
{
}

ObLoadExternalSort::~ObLoadExternalSort()
{
  external_sort_.clean_up();
}

int ObLoadExternalSort::init(const ObTableSchema *table_schema, int64_t mem_size,
                             int64_t file_buf_size)
{
  int ret = OB_SUCCESS;
  // LOG_INFO("MMMMM sort init");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("MMMMM ObLoadExternalSort init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_schema));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    const int64_t rowkey_column_num = table_schema->get_rowkey_column_num();
    ObArray<ObColDesc> multi_version_column_descs;
    if (OB_FAIL(table_schema->get_multi_version_column_descs(multi_version_column_descs))) {
      LOG_WARN("MMMMM fail to get multi version column descs", KR(ret));
    } else if (OB_FAIL(datum_utils_.init(multi_version_column_descs, rowkey_column_num,
                                         is_oracle_mode(), allocator_))) {
      LOG_WARN("MMMMM fail to init datum utils", KR(ret));
    } else if (OB_FAIL(compare_.init(rowkey_column_num, &datum_utils_))) {
      LOG_WARN("MMMMM fail to init compare", KR(ret));
    } else if (OB_FAIL(external_sort_.init(mem_size, file_buf_size, 0, MTL_ID(), &compare_))) {
      LOG_WARN("MMMMM fail to init external sort", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

// make sure there is no 
// void ObLoadExternalSort::partition(int n) 
// {
//   external_sort_.partition(n);
// }

int ObLoadExternalSort::final_merge(int64_t total, int split_num)
{
  return external_sort_.final_merge(total, split_num);
}

int ObLoadExternalSort::append_row(const ObLoadDatumRow &datum_row)
{ 
  // LOG_INFO("MMMMM append row");
  std::lock_guard<std::mutex> guard(mutex_);
  count_++;
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadExternalSort not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected closed external sort", KR(ret));
  } else if (OB_FAIL(external_sort_.add_item(datum_row))) {
    LOG_WARN("fail to add item", KR(ret));
  }
  return ret;
}

int ObLoadExternalSort::trivial_close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadExternalSort not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected closed external sort", KR(ret));
  } else if (OB_FAIL(external_sort_.trivial_do_sort())) {
    LOG_INFO("MMMMM fail to do sort", KR(ret));
  } else {
    is_closed_ = true;
  }
  return ret;
}

int ObLoadExternalSort::trivial_append_row(const ObLoadDatumRow &datum_row)
{ 
  // LOG_INFO("MMMMM append row");
  std::lock_guard<std::mutex> guard(mutex_);
  count_++;
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadExternalSort not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected closed external sort", KR(ret));
  } else if (OB_FAIL(external_sort_.trivial_add_item(datum_row))) {
    LOG_WARN("MMMMM fail to add item", KR(ret));
  }
  return ret;
}

int ObLoadExternalSort::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("MMMMM ObLoadExternalSort not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("MMMMM unexpected closed external sort", KR(ret));
  } else if (OB_FAIL(external_sort_.do_sort(true))) {
    LOG_INFO("MMMMM fail to do sort", KR(ret));
  } else {
    is_closed_ = true;
  }
  return ret;
}

void ObLoadExternalSort::clean_up()
{
  is_inited_ = false;
  is_closed_ = false;
  external_sort_.clean_up();
  allocator_.reset();
  datum_utils_.reset();
  compare_.clean_up();
}

int ObLoadExternalSort::get_next_row(const ObLoadDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadExternalSort not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not closed external sort", KR(ret));
  } else if (OB_FAIL(external_sort_.get_next_item(datum_row))) {
    LOG_WARN("fail to get next item", KR(ret));
  }
  return ret;
}

int ObLoadExternalSort::get_next_partition_row(int id, const ObLoadDatumRow *&datum_row)
{
  return external_sort_.get_next_partition_item(id, datum_row);
}

/**
 * ObLoadSSTableWriter
 */

ObLoadSSTableWriter::ObLoadSSTableWriter()
  : rowkey_column_num_(0),
    extra_rowkey_column_num_(0),
    column_count_(0),
    is_closed_(false),
    is_inited_(false)
{
}

ObLoadSSTableWriter::~ObLoadSSTableWriter()
{
}

int ObLoadSSTableWriter::init(const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadSSTableWriter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_schema));
  } else {
    tablet_id_ = table_schema->get_tablet_id();
    rowkey_column_num_ = table_schema->get_rowkey_column_num();
    extra_rowkey_column_num_ = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    column_count_ = table_schema->get_column_count();
    ObLocationService *location_service = nullptr;
    bool is_cache_hit = false;
    ObLSService *ls_service = nullptr;
    ObLS *ls = nullptr;
    if (OB_ISNULL(location_service = GCTX.location_service_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("location service is null", KR(ret), KP(location_service));
    } else if (OB_FAIL(
                 location_service->get(MTL_ID(), tablet_id_, INT64_MAX, is_cache_hit, ls_id_))) {
      LOG_WARN("fail to get ls id", KR(ret), K(tablet_id_));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_SYS;
      LOG_ERROR("ls service is null", KR(ret));
    } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle_, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get ls", KR(ret), K(ls_id_));
    } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ls should not be null", KR(ret));
    } else if (OB_FAIL(ls->get_tablet(tablet_id_, tablet_handle_))) {
      LOG_WARN("fail to get tablet handle", KR(ret), K(tablet_id_));
    } else if (OB_FAIL(init_sstable_index_builder(table_schema))) {
      LOG_WARN("fail to init sstable index builder", KR(ret));
    } else if (OB_FAIL(init_macro_block_writer(table_schema))) {
      LOG_WARN("fail to init macro block writer", KR(ret));
    } else if (OB_FAIL(datum_row_.init(column_count_ + extra_rowkey_column_num_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else {
      table_key_.table_type_ = ObITable::MAJOR_SSTABLE;
      table_key_.tablet_id_ = tablet_id_;
      table_key_.log_ts_range_.start_log_ts_ = 0;
      table_key_.log_ts_range_.end_log_ts_ = ObTimeUtil::current_time_ns();
      datum_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      datum_row_.mvcc_row_flag_.set_last_multi_version_row(true);
      datum_row_.storage_datums_[rowkey_column_num_].set_int(-1); // fill trans_version
      datum_row_.storage_datums_[rowkey_column_num_ + 1].set_int(0); // fill sql_no
      for (int i = 0; i < 16 && OB_SUCC(ret); i++) {
        if (OB_FAIL(datum_rows_[i].init(column_count_ + extra_rowkey_column_num_))) {
          LOG_WARN("MMMMM fail to init datum row", KR(ret));    
        }
        datum_rows_[i].row_flag_.set_flag(ObDmlFlag::DF_INSERT);
        datum_rows_[i].mvcc_row_flag_.set_last_multi_version_row(true);
        datum_rows_[i].storage_datums_[rowkey_column_num_].set_int(-1); // fill trans_version
        datum_rows_[i].storage_datums_[rowkey_column_num_ + 1].set_int(0); // fill sql_no
      }
      is_inited_ = true;
    }
  }
  return ret;
}

int ObLoadSSTableWriter::init_sstable_index_builder(const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  ObDataStoreDesc data_desc;
  if (OB_FAIL(data_desc.init(*table_schema, ls_id_, tablet_id_, MAJOR_MERGE, 1L))) {
    LOG_WARN("fail to init data desc", KR(ret));
  } else {
    data_desc.row_column_count_ = data_desc.rowkey_column_count_ + 1;
    data_desc.need_prebuild_bloomfilter_ = false;
    data_desc.col_desc_array_.reset();
    if (OB_FAIL(data_desc.col_desc_array_.init(data_desc.row_column_count_))) {
      LOG_WARN("fail to reserve column desc array", KR(ret));
    } else if (OB_FAIL(table_schema->get_rowkey_column_ids(data_desc.col_desc_array_))) {
      LOG_WARN("fail to get rowkey column ids", KR(ret));
    } else if (OB_FAIL(
                 ObMultiVersionRowkeyHelpper::add_extra_rowkey_cols(data_desc.col_desc_array_))) {
      LOG_WARN("fail to add extra rowkey cols", KR(ret));
    } else {
      ObObjMeta meta;
      meta.set_varchar();
      meta.set_collation_type(CS_TYPE_BINARY);
      ObColDesc col;
      col.col_id_ = static_cast<uint64_t>(data_desc.row_column_count_ + OB_APP_MIN_COLUMN_ID);
      col.col_type_ = meta;
      col.col_order_ = DESC;
      if (OB_FAIL(data_desc.col_desc_array_.push_back(col))) {
        LOG_WARN("fail to push back last col for index", KR(ret), K(col));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sstable_index_builder_.init(data_desc))) {
      LOG_WARN("fail to init index builder", KR(ret), K(data_desc));
    }
  }
  return ret;
}

int ObLoadSSTableWriter::init_macro_block_writer(const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(data_store_desc_.init(*table_schema, ls_id_, tablet_id_, MAJOR_MERGE, 1))) {
    LOG_WARN("fail to init data_store_desc", KR(ret), K(tablet_id_));
  } else {
    data_store_desc_.sstable_index_builder_ = &sstable_index_builder_;
  }
  if (OB_SUCC(ret)) {
    ObMacroDataSeq data_seq;
    if (OB_FAIL(macro_block_writer_.open(data_store_desc_, data_seq))) {
      LOG_WARN("fail to init macro block writer", KR(ret), K(data_store_desc_), K(data_seq));
    }
  }
  return ret;
}

int ObLoadSSTableWriter::init_macro_block_writer(const ObTableSchema *table_schema, int idx)
{
  int ret = OB_SUCCESS;
  ObMacroDataSeq data_seq;
  data_seq.set_parallel_degree(idx);
  if (OB_FAIL(macro_block_writers_[idx].open(data_store_desc_, data_seq))) {
    LOG_WARN("MMMMM fail to init macro block writer", KR(ret), K(data_store_desc_), K(data_seq));
  }
  return ret;
}

int ObLoadSSTableWriter::append_row(int idx, const ObLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadSSTableWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected closed external sort", KR(ret));
  } else if (OB_UNLIKELY(!datum_row.is_valid() || datum_row.count_ != column_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(datum_row), K(column_count_));
  } else {
    for (int64_t i = 0; i < column_count_; ++i) {
      if (i < rowkey_column_num_) {
        datum_rows_[idx].storage_datums_[i] = datum_row.datums_[i];
      } else {
        datum_rows_[idx].storage_datums_[i + extra_rowkey_column_num_] = datum_row.datums_[i];
      }
    }
    if (OB_FAIL(macro_block_writers_[idx].append_row(datum_rows_[idx]))) {
      LOG_WARN("MMMMM fail to append row", KR(ret));
    }
  }
  return ret;
}

int ObLoadSSTableWriter::append_row(const ObLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadSSTableWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected closed external sort", KR(ret));
  } else if (OB_UNLIKELY(!datum_row.is_valid() || datum_row.count_ != column_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(datum_row), K(column_count_));
  } else {
    for (int64_t i = 0; i < column_count_; ++i) {
      if (i < rowkey_column_num_) {
        datum_row_.storage_datums_[i] = datum_row.datums_[i];
      } else {
        datum_row_.storage_datums_[i + extra_rowkey_column_num_] = datum_row.datums_[i];
      }
    }
    if (OB_FAIL(macro_block_writer_.append_row(datum_row_))) {
      LOG_WARN("fail to append row", KR(ret));
    }
  }
  return ret;
}

int ObLoadSSTableWriter::create_sstable()
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 table_handle;
  SMART_VAR(ObSSTableMergeRes, merge_res)
  {
    const ObStorageSchema &storage_schema = tablet_handle_.get_obj()->get_storage_schema();
    int64_t column_count = 0;
    if (OB_FAIL(storage_schema.get_stored_column_count_in_sstable(column_count))) {
      LOG_WARN("fail to get stored column count in sstable", KR(ret));
    } else if (OB_FAIL(sstable_index_builder_.close(column_count, merge_res))) {
      LOG_WARN("fail to close sstable index builder", KR(ret));
    } else {
      ObTabletCreateSSTableParam create_param;
      create_param.table_key_ = table_key_;
      create_param.table_mode_ = storage_schema.get_table_mode_struct();
      create_param.index_type_ = storage_schema.get_index_type();
      create_param.rowkey_column_cnt_ = storage_schema.get_rowkey_column_num() +
                                        ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
      create_param.schema_version_ = storage_schema.get_schema_version();
      create_param.create_snapshot_version_ = 0;
      ObSSTableMergeRes::fill_addr_and_data(merge_res.root_desc_, create_param.root_block_addr_,
                                            create_param.root_block_data_);
      ObSSTableMergeRes::fill_addr_and_data(merge_res.data_root_desc_,
                                            create_param.data_block_macro_meta_addr_,
                                            create_param.data_block_macro_meta_);
      create_param.root_row_store_type_ = merge_res.root_desc_.row_type_;
      create_param.data_index_tree_height_ = merge_res.root_desc_.height_;
      create_param.index_blocks_cnt_ = merge_res.index_blocks_cnt_;
      create_param.data_blocks_cnt_ = merge_res.data_blocks_cnt_;
      create_param.micro_block_cnt_ = merge_res.micro_block_cnt_;
      create_param.use_old_macro_block_count_ = merge_res.use_old_macro_block_count_;
      create_param.row_count_ = merge_res.row_count_;
      create_param.column_cnt_ = merge_res.data_column_cnt_;
      create_param.data_checksum_ = merge_res.data_checksum_;
      create_param.occupy_size_ = merge_res.occupy_size_;
      create_param.original_size_ = merge_res.original_size_;
      create_param.max_merged_trans_version_ = merge_res.max_merged_trans_version_;
      create_param.contain_uncommitted_row_ = merge_res.contain_uncommitted_row_;
      create_param.compressor_type_ = merge_res.compressor_type_;
      create_param.encrypt_id_ = merge_res.encrypt_id_;
      create_param.master_key_id_ = merge_res.master_key_id_;
      create_param.data_block_ids_ = merge_res.data_block_ids_;
      create_param.other_block_ids_ = merge_res.other_block_ids_;
      MEMCPY(create_param.encrypt_key_, merge_res.encrypt_key_,
             OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
      if (OB_FAIL(
            merge_res.fill_column_checksum(&storage_schema, create_param.column_checksums_))) {
        LOG_WARN("fail to fill column checksum for empty major", KR(ret), K(create_param));
      } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(create_param, table_handle))) {
        LOG_WARN("fail to create sstable", KR(ret), K(create_param));
      } else {
        const int64_t rebuild_seq = ls_handle_.get_ls()->get_rebuild_seq();
        ObTabletHandle new_tablet_handle;
        ObUpdateTableStoreParam table_store_param(table_handle,
                                                  tablet_handle_.get_obj()->get_snapshot_version(),
                                                  false, &storage_schema, rebuild_seq, true, true);
        if (OB_FAIL(ls_handle_.get_ls()->update_tablet_table_store(tablet_id_, table_store_param,
                                                                   new_tablet_handle))) {
          LOG_WARN("fail to update tablet table store", KR(ret), K(tablet_id_),
                   K(table_store_param));
        }
      }
    }
  }
  return ret;
}

int ObLoadSSTableWriter::close_macro_blocks()
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < 16; i++) {
    if (OB_FAIL(macro_block_writers_[i].close())) {
      LOG_WARN("fail to close macro block writer", KR(ret));  
    }
  }
  return ret;
}

int ObLoadSSTableWriter::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadSSTableWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected closed sstable writer", KR(ret));
  } else {
    ObSSTable *sstable = nullptr;
    for (int i = 0; i < 16; i++) {
      if (OB_FAIL(macro_block_writers_[i].close())) {
        LOG_WARN("fail to close macro block writer", KR(ret));  
      }
    }
    if (OB_FAIL(macro_block_writer_.close())) {
      LOG_WARN("fail to close macro block writer", KR(ret));
    } else if (OB_FAIL(create_sstable())) {
      LOG_WARN("fail to create sstable", KR(ret));
    } else {
      is_closed_ = true;
    }
  }
  return ret;
}

/**
 * ObLoadDataDirectDemo
 */

ObLoadDataDirectDemo::ObLoadDataDirectDemo()
{
}

ObLoadDataDirectDemo::~ObLoadDataDirectDemo()
{
}

int ObLoadDataDirectDemo::execute(ObExecContext &ctx, ObLoadDataStmt &load_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_init(load_stmt))) {
    LOG_WARN("fail to inner init", KR(ret));
  } else if (OB_FAIL(do_load())) {
    LOG_WARN("fail to do load", KR(ret));
  }
  return ret;
}

int ObLoadDataDirectDemo::inner_init(ObLoadDataStmt &load_stmt)
{
  int ret = OB_SUCCESS;
  const ObLoadArgument &load_args = load_stmt.get_load_arguments();
  const ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list =
    load_stmt.get_field_or_var_list();
  const uint64_t tenant_id = load_args.tenant_id_;
  const uint64_t table_id = load_args.table_id_;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id,
                                                                                  schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_UNLIKELY(table_schema->is_heap_table())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support heap table", KR(ret));
  }
  // init csv_parser_
  for (int i = 0; i < WRITER_THREAD_NUM; i++) {
    if (OB_FAIL(csv_parsers_[i].init(load_stmt.get_data_struct_in_file(), field_or_var_list.count(),
                                    load_args.file_cs_type_, 16))) {
      LOG_WARN("fail to init csv parser", KR(ret));
    }
  }

  // init file_reader_
  filepath_ = load_args.full_file_path_;
  LOG_INFO("MMMMM file", K(filepath_));
  if (OB_FAIL(file_reader_.open(load_args.full_file_path_))) {
    LOG_WARN("fail to open file", KR(ret), K(load_args.full_file_path_));
  }
  
  for (int i = 0; i < SPLIT_THREAD_NUM; i++) {
    if (OB_FAIL(file_split_readers_[i].open(load_args.full_file_path_))) {
      LOG_WARN("fail to open file", KR(ret), K(load_args.full_file_path_));
    }  
  }
  
  
  // init buffer_
  // for (int i = 0; i < DEMO_BUF_NUM; i++) {
  //   if (OB_FAIL(buffers_[i].create(FILE_BUFFER_SIZE))) {
  //     LOG_WARN("fail to create buffer", KR(ret));
  //   }
  // }
  if (OB_FAIL(buffer_.create(BUF_SIZE))) {
    LOG_WARN("fail to create buffer", KR(ret));
  }
  for (int i = 0; i < WRITER_THREAD_NUM; i++) {
    if (OB_FAIL(buffers_[i].create(BUF_SIZE))) {
      LOG_WARN("fail to create buffer", KR(ret));
    }  
  }

  for (int i = 0; i < SPLIT_THREAD_NUM; i++) {
    if (OB_FAIL(split_buffers_[i].create(BUF_SIZE))) {
      LOG_WARN("fail to create buffer", KR(ret));
    }  
  }
  // init row_caster_
  for (int i = 0; i < WRITER_THREAD_NUM; i++) {
    if (OB_FAIL(row_casters_[i].init(table_schema, field_or_var_list))) {
      LOG_WARN("fail to init row caster", KR(ret)); 
    }
  }

  
  for (int i = 0; i < WRITER_THREAD_NUM; i++) {
    if (OB_FAIL(external_sorts_[i].init(table_schema, MEM_BUFFER_SIZE, FILE_BUFFER_SIZE))) {
      LOG_WARN("MMMMM fail to init external sort", KR(ret), K(i));
    }
  }
  // init external_sort_
  // if (OB_FAIL(external_sort_.init(table_schema, MEM_BUFFER_SIZE, FILE_BUFFER_SIZE))) {
  //   LOG_WARN("fail to init row caster", KR(ret));
  // }
  
  // for (int i = 0; i < SPLIT_NUM; i++) {
  //   if (OB_FAIL(external_sorts_[i].init(table_schema, MEM_BUFFER_SIZE, FILE_BUFFER_SIZE))) {
  //     LOG_WARN("fail to init row caster", KR(ret));
  //   } 
  // }
  // init sstable_writer_
  if (OB_FAIL(sstable_writer_.init(table_schema))) {
    LOG_WARN("fail to init sstable writer", KR(ret));
  }
  // init datum_row_buffers_
  // datum_row_buffers_.resize(DEMO_BUF_NUM);
  table_schema_ = table_schema;
  return ret;
}

int get_row_data(ObLoadDataBuffer &buffer, const char *&buf, int64_t &len, int &group_id, int split)
{
  if (buffer.empty()) {
    return OB_ITER_END;
  }
  const char *begin = buffer.begin();
  const char *end = buffer.end();
  const char *iter = begin;
  int field_cnt = 0;
  bool first = true;

  const char *ptr = nullptr;

  int64_t key1 = 0;
  int key2 = 0;
  while (iter < end && *iter != '\n') {
    if (first) {
      ptr = iter;
      first = false;
    }
    if (*iter == '|') {
      field_cnt++;
      first = true;
    }
    if (field_cnt == 0) {
      key1 = key1 * 10 + (*iter - '0');
    }
    if (field_cnt == 3 && isdigit(*iter)) {
      key2 = key2 * 10 + (*iter - '0');
    }
    iter++;
  }
  if (field_cnt != 16 || iter == end) {
    return OB_ITER_END;
  }
  if (end != iter) {
    iter++;
  }
  switch (split) {
    case 4: 
      group_id = get_group_id_4(key1, key2);break;
    case 8:
      group_id = get_group_id_8(key1, key2);break;
    default:
      group_id = get_group_id(key1, key2, split);break;
  }
  // LOG_INFO("MMMMM", K(group_id));
  // LOG_INFO("MMMMM get key", K(key.key1), K(key.key2), K(key.offset));
  len = iter - begin;
  buf = begin;
  buffer.consume(iter - begin);

  return OB_SUCCESS;
}

int parse_row_and_key(ObLoadDataBuffer &buffer, Key *keys, int &key_cnt, size_t &offset)
{
  if (buffer.empty()) {
    return OB_ITER_END;
  }
  const char *begin = buffer.begin();
  const char *end = buffer.end();
  const char *iter = begin;
  int field_cnt = 0;
  bool first = true;

  const char *ptr = nullptr;

  int64_t key1 = 0;
  int key2 = 0;
  while (iter < end && *iter != '\n') {
    if (first) {
      ptr = iter;
      first = false;
    }
    if (*iter == '|') {
      field_cnt++;
      first = true;
    }
    if (field_cnt == 0) {
      key1 = key1 * 10 + (*iter - '0');
    }
    if (field_cnt == 3 && isdigit(*iter)) {
      key2 = key2 * 10 + (*iter - '0');
    }
    iter++;
  }
  if (field_cnt != 16 || iter == end) {
    return OB_ITER_END;
  }
  if (end != iter) {
    iter++;
  }
  if (key2 > 250) {
    LOG_INFO("MMMMM ERROR");
  }
  // int group_id = get_group_id_4(key1, key2);
  // Key &key = keylists[group_id][key_cnts[group_id]];
  Key &key = keys[key_cnt];
  key.key1 = key1;
  key.key2 = key2;
  key.offset = offset;
  key_cnt++;
  // key_cnts[group_id]++;
  // if (key_cnts[group_id] % 100000 == 0) {
  //   LOG_INFO("MMMMM parse", K(group_id), K(key_cnts[group_id]));
  // }
  // LOG_INFO("MMMMM get key", K(key.key1), K(key.key2), K(key.offset));
  buffer.consume(iter - begin);
  offset += iter - begin;

  return OB_SUCCESS;
}

int do_load_buffer(ObLoadDataBuffer &buffer, ObLoadSequentialFileReader &file_reader) 
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(buffer.squash())) {
    LOG_INFO("MMMMM fail to squash buffer", KR(ret));
  } else if (OB_FAIL(file_reader.read_next_buffer(buffer))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_INFO("MMMMM fail to read next buffer", KR(ret));
    } else {
      LOG_INFO("MMMMM ERROR", KR(ret));
      if (OB_UNLIKELY(!buffer.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_INFO("MMMMM unexpected incomplete data", KR(ret));
      }
    }
  } else if (OB_UNLIKELY(buffer.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_INFO("MMMMM unexpected empty buffer", KR(ret));
  } 
  // LOG_INFO("MMMMM load buffer");
  return ret;
}

void ObTrivialSortThread::run(int64_t idx) 
{
  common::ObFileReader file_reader;
  const ObNewRow *new_row = nullptr;
  const ObLoadDatumRow *datum_row = nullptr;
  ObLoadCSVPaser &csv_parser = csv_parsers_[idx];
  ObLoadRowCaster &row_caster = row_casters_[idx];
  ObLoadExternalSort &external_sort = external_sorts_[idx + start_idx_];

  int ret = OB_SUCCESS;
  const size_t BUF_SIZE = 350;
  char buf[BUF_SIZE];

  if (OB_FAIL(external_sort.init(table_schema_, MEM_BUFFER_SIZE, FILE_BUFFER_SIZE))) {
    LOG_WARN("MMMMM fail to init external sort", KR(ret), K(idx));
  }
  
  if (OB_FAIL(file_reader.open(filepath_, false))) {
    LOG_WARN("MMMMM fail to open file", KR(ret));
  }

  // Key *&keys = keylists_[idx];
  // int &key_cnt = key_cnts_[idx];
  int64_t size = 0;
  int n = key_cnt_ / thread_num_;
  int start_idx = n * idx;
  int end_idx = idx == thread_num_ - 1 ? key_cnt_ : n * (idx + 1);
  LOG_INFO("MMMMM trivial", K(idx), K(start_idx), K(end_idx), K(key_cnt_));
  for (int i = start_idx; i < end_idx && OB_SUCC(ret); i++) {
    if (i % 100000 == 0) {
      LOG_INFO("MMMMM trivial sort", K(i), K(idx));
    }
    Key &key = keys_[i];
    if (OB_FAIL(file_reader.pread(buf, 350, key.offset, size))) {
      LOG_WARN("fail to do pread", K(ret), K(key.offset), K(i));
    } else if (OB_FAIL(csv_parser.fast_get_next_row(buf, buf+size, new_row))) {
      LOG_INFO("MMMMM fail to get row", KR(ret), K(key.offset), K(i), K(end_idx), K(idx));
    } else if (OB_FAIL(row_caster.get_casted_row(*new_row, datum_row))) {
      LOG_INFO("MMMMM fail to cast row", KR(ret), K(idx), K(i));
    } else if (OB_FAIL(external_sort.trivial_append_row(*datum_row))) {
      LOG_INFO("MMMMM fail to append row", KR(ret), K(idx), K(i));
    }
  }
  if (OB_FAIL(external_sort.trivial_close())) {
    LOG_INFO("MMMMM fail to close external sort", KR(ret));
  }
  LOG_INFO("MMMMM trivial", K(idx), K(key_cnt_), KR(ret));
}

void ObWriterThread::run(int64_t idx) 
{
  const ObLoadDatumRow *datum_row;
  int ret = OB_SUCCESS;
  int cnt = 0;
  idx = start_idx_ + idx;
  sstable_writer_.init_macro_block_writer(table_schema_, idx);
  
  LOG_INFO("MMMMM writer", K(idx));
  
  while (OB_SUCC(ret)) {
    cnt++;
    
    if (cnt % 100000 == 0) {
      LOG_INFO("MMMMM sstable append", K(cnt), K(idx));
    }
    if (OB_FAIL(external_sorts_[idx].get_next_row(datum_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_INFO("MMMMM fail to get next row", KR(ret), K(idx));
      } else {
        LOG_INFO("MMMMM writer", K(idx), K(cnt), KR(ret));
        ret = OB_SUCCESS;
        break;
      }
    } else {
      if (OB_FAIL(sstable_writer_.append_row(idx, *datum_row))) {
        LOG_INFO("MMMMM fail to append row", KR(ret), K(idx), K(cnt));
      }
    }
  }
  rets_[idx] = ret;
}

void ObParseDataThread::run(int64_t idx)
{
  int ret = OB_SUCCESS;
  // LOG_INFO("MMMMM process", KR(ret), K(idx));
  const ObNewRow *new_row = nullptr;
  const ObLoadDatumRow *datum_row = nullptr;
  // ObLoadDataBuffer &buffer = buffers_[idx];
  ObLoadCSVPaser &csv_parser = csv_parsers_[idx];
  ObLoadRowCaster &row_caster = row_casters_[idx];

  // parse whole file
  int cnt = 0;
  while (OB_SUCC(ret)) {
    int group_id = 0;
    {
      std::lock_guard<std::mutex> guard(mutex_);
      ret = csv_parser.fast_get_next_row(buffer_, new_row, group_id);
      // ret = csv_parser.get_next_row(buffer_, new_row);
    }
    // LOG_INFO("MMMMM writer to group", K(group_id));
    if (OB_FAIL(ret)) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_INFO("MMMMM fail to get next row", KR(ret), K(idx));
      } else {
        ret = OB_SUCCESS;
        break;
      } 
    } else if (OB_FAIL(row_caster.get_casted_row(*new_row, datum_row))) {
      LOG_INFO("MMMMM fail to cast row", KR(ret), K(idx));
    } else if (OB_FAIL(external_sorts_[group_id].trivial_append_row(*datum_row))) {
      LOG_INFO("MMMMM fail to append row", KR(ret), K(idx));
    } else {
      cnt++;
    }
  }
  cnts_[idx] = cnt;
  LOG_INFO("MMMMM external sort append lines", K(cnt), KR(ret), K(idx));

  rets_[idx] = ret;
}

// we don't use allocator:D
// thread_num_ | split_num_
void ObReadSortWriteThread::run(int64_t idx) 
{
  LOG_INFO("MMMMM readsortwrite start", K(idx));
  
  int n = split_num_ / thread_num_;

  const ObNewRow *new_row = nullptr;
  const ObLoadDatumRow *datum_row = nullptr;
  ObLoadCSVPaser &csv_parser = csv_parsers_[idx];
  ObLoadRowCaster &row_caster = row_casters_[idx];
  ObLoadDataBuffer &buffer = buffers_[idx];
  ObLoadDatumRowCompare &compare = external_sorts_[idx].compare();
  char *&buf = bufs_[idx];

  int ret = OB_SUCCESS;

  sstable_writer_.init_macro_block_writer(table_schema_, idx);
  int64_t max_size = 0;

  for (int i = idx * n; i < (idx + 1) * n && OB_SUCC(ret); i++) {
    int64_t pos = 0;
    common::ObVector<ObLoadDatumRow *> item_list;
    ObLoadSequentialFileReader file_reader;
    ObString file_path(file_paths_[i].size(), file_paths_[i].c_str());
    LOG_INFO("MMMMM read", K(file_paths_[i].c_str()));
    if (OB_FAIL(file_reader.open(file_path))) {
      LOG_INFO("MMMMM can't open", K(file_paths_[i].c_str()));
      break;
    } 
    // read the data, and put them in buffer
    while (OB_SUCC(ret)) {
      if (OB_FAIL(do_load_buffer(buffer, file_reader))) {
        LOG_INFO("MMMMM fail to load buffer", K(ret), K(idx));
      }
      while (OB_SUCC(ret)) {
        if (OB_FAIL(csv_parser.fast_get_next_row(buffer, new_row))) {
          if (OB_FAIL(ret)) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_INFO("MMMMM fail to get next row", KR(ret), K(idx));
            } else {
              ret = OB_SUCCESS;
              break;
            } 
          }
        } else if (OB_FAIL(row_caster.get_casted_row(*new_row, datum_row))) {
          LOG_INFO("MMMMM fail to cast row", KR(ret), K(idx), K(i));
        } else {
          ObLoadDatumRow *new_item = nullptr;
          const int64_t item_size = sizeof(ObLoadDatumRow) + datum_row->get_deep_copy_size();
          max_size = max(item_size, max_size);
          if (item_size + pos > thread_buf_size_) {
            LOG_INFO("MMMMMM DAMN!!!!!!!!", K(item_list.size()), K(item_size), K(pos));
          } else if (OB_ISNULL(new_item = new (buf + pos) ObLoadDatumRow())) {
            ret = common::OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "MMMMM fail to placement new item", K(ret));
          } else {
            int64_t buf_pos = sizeof(ObLoadDatumRow);
            if (OB_FAIL(new_item->deep_copy(*datum_row, buf + pos, item_size, buf_pos))) {
              STORAGE_LOG(WARN, "fail to deep copy item", K(ret));
            } else if (OB_FAIL(item_list.push_back(new_item))) {
              STORAGE_LOG(WARN, "fail to push back new item", K(ret));
            } else {
              pos += item_size;
            }
          }
        }
      }
    }
    LOG_INFO("MMMMM read done", KR(ret), K(idx), K(item_list.size()), K(pos));
    // sort
    quicksort(item_list.begin(), item_list.end(), compare);
    LOG_INFO("MMMMM sort done", KR(ret), K(idx));
    // append
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
    for (int i = 0; i < item_list.size() && OB_SUCC(ret); i++) {
      if (OB_FAIL(sstable_writer_.append_row(idx, *item_list[i]))) {
        LOG_INFO("MMMMM fail to append row", KR(ret), K(idx), K(i));
      }
    }
    LOG_INFO("MMMMM write done", KR(ret), K(idx));
  }
  LOG_INFO("MMMMM", K(max_size));
  rets_[idx] = ret;
}


/*
int ObLoadDataDirectDemo::do_load()
{
  int ret = OB_SUCCESS;
  int cnt = 0;
  int total = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(do_load_buffer())) {
      break;
    }
    while (OB_SUCC(ret)) {
      cnt++;
      int rets[PARSE_THREAD_NUM];
      if (OB_FAIL(do_load_buffer())) {
        break;
      }
      ObParseDataThread threads(buffer_, csv_parsers_, row_casters_, external_sorts_, file_reader_, rets);
      threads.set_thread_count(PARSE_THREAD_NUM);
      threads.set_run_wrapper(MTL_CTX());
      threads.start();
      threads.wait();
      total += threads.cnts();
      LOG_INFO("MMMMM threads succeed", K(cnt));
      for (int i = 0; i < PARSE_THREAD_NUM; i++) {
        ret = rets[i] == OB_SUCCESS ? ret : rets[i];
      }
    }
  }
  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }
  ObWriterThread threads(external_sorts_, SPLIT_NUM, sstable_writer_, table_schema_, rets, WRITER_THREAD_NUM);
  threads.set_thread_count(WRITER_THREAD_NUM);
  threads.set_run_wrapper(MTL_CTX());
  threads.start();
  threads.wait();

  int rets[WRITER_THREAD_NUM];
  for (int i = 0; i < WRITER_THREAD_NUM; i++) {
    ret = rets[i] != OB_SUCCESS ? rets[i] : ret;
  }
  LOG_INFO("MMMMM write done", KR(ret));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sstable_writer_.close())) {
      LOG_INFO("MMMMM fail to close sstable writer", KR(ret));
    }
    LOG_INFO("MMMMM close done", KR(ret));
  }
  return ret;
}
*/

void ObSplitFileThread::run(int64_t idx)
{
  const char *buf;
  int64_t len;
  int group_id;
  int buf_cnt = 0;

  int ret = OB_SUCCESS;
  int64_t offset = idx == 0 ? 0 : end_[idx-1];
  int64_t pos = offset;
  int64_t end = end_[idx];
  ObLoadSequentialFileReader &file_reader = file_readers_[idx];
  ObLoadDataBuffer &buffer = buffers_[idx];
  file_reader.set_offset(offset);

  LOG_INFO("MMMMM split thread", K(idx));

  while (OB_SUCC(ret) && pos < end) {
    if (OB_FAIL(do_load_buffer(buffer, file_reader))) {
      break;
    }
    if (buf_cnt % 10 == 0) {
      LOG_INFO("MMMMM read buffer", K(buf_cnt), K(idx));
    }
    buf_cnt++;
    while (OB_SUCC(ret) && pos < end) {
      if (OB_FAIL(get_row_data(buffer, buf, len, group_id, split_num_))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("MMMMM fail to get next row", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        if (OB_FAIL(file_writers_[group_id].write(buf, len))) {
          LOG_INFO("MMMMM can't write!");
        } else {
          pos += len;
        }
      }
      // LOG_INFO("MMMMM split", K(idx), K(pos), K(end), K(group_id));
    }
  }
  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }
  rets_[idx] = ret;
}

int ObLoadDataDirectDemo::pre_process_with_thread()
{
  int ret = OB_SUCCESS;
  std::string tmp(filepath_.ptr(), filepath_.length());
  for (int i = 0; i < SPLIT_NUM; i++) {
    std::string tmpp = tmp + "." + std::to_string(i);
    filepaths_.push_back(tmpp);
    ObString file_path(tmpp.size(), tmpp.c_str());
    file_writers_[i].open(file_path, SPLIT_BUF_SIZE);
  }
  // try to get 4 separate pos
  std::ifstream in(tmp, std::ifstream::ate | std::ifstream::binary);
  int64_t file_size = in.tellg();
  split_pos_[SPLIT_THREAD_NUM - 1] = file_size;
  {
    char buf[350];
    int64_t read_size = 0;
    for (int i = 0; i < SPLIT_THREAD_NUM - 1; i++) {
      int64_t &pos = split_pos_[i];
      
      pos = file_size / SPLIT_THREAD_NUM * (i+1);
      if (OB_FAIL(file_reader_.read_next_buffer_from(buf, 350, pos, read_size))) {
        LOG_INFO("MMMMM read fail");
      }
      char *buf_ptr = buf;
      while (*buf_ptr != '\n') {
        buf_ptr++;
        pos++;
      }
      pos++;
      buf_ptr++;
      
      LOG_INFO("MMMMM split file", K(i), K(pos));
      // LOG_INFO("MMMMM split", K(buf_ptr));
    }
    LOG_INFO("MMMMM split file", K(filepaths_[SPLIT_THREAD_NUM-1].c_str()), K(file_size));
  }
  file_reader_.close();

  int rets[SPLIT_THREAD_NUM];
  ObSplitFileThread threads(file_writers_, file_split_readers_, split_buffers_, split_pos_, SPLIT_NUM, rets);
  threads.set_thread_count(SPLIT_THREAD_NUM);
  threads.set_run_wrapper(MTL_CTX());
  threads.start();
  threads.wait();
  for (int i = 0; i < SPLIT_THREAD_NUM; i++) {
    ret = rets[i] == OB_SUCCESS ? ret : rets[i];
  }
  for (int i = 0; i < SPLIT_NUM; i++) {
    file_writers_[i].close();
  }
  return ret;
}

int ObLoadDataDirectDemo::pre_process()
{
  int ret = OB_SUCCESS;
  std::string tmp(filepath_.ptr(), filepath_.length());
  for (int i = 0; i < SPLIT_NUM; i++) {
    std::string tmpp = tmp + "." + std::to_string(i);
    LOG_INFO("MMMMM", K(tmpp.c_str()));
    filepaths_.push_back(tmpp);
    ObString file_path(tmpp.size(), tmpp.c_str());
    file_writers_[i].open(file_path, SPLIT_BUF_SIZE);
  }

  const char *buf;
  int64_t len;
  int group_id;
  int buf_cnt = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(do_load_buffer(buffer_, file_reader_))) {
      break;
    }
    if (buf_cnt % 100 == 0) {
      LOG_INFO("MMMMM read buffer", K(buf_cnt));
    }
    buf_cnt++;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(get_row_data(buffer_, buf, len, group_id, SPLIT_NUM))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("MMMMM fail to get next row", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(file_writers_[group_id].write(buf, len))) {
        LOG_INFO("MMMMM can't write!");
      }
      // LOG_INFO("MMMMM write to", K(group_id));
    }
  }
  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }
  for (int i = 0; i < SPLIT_NUM; i++) {
    file_writers_[i].close();
  }
  return ret;
}

// split file -> sort in memory -> directly write
// 2 read 2 write, save a lot of io
// four thread each take care of each part
int ObLoadDataDirectDemo::do_load()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pre_process_with_thread())) {
    LOG_INFO("MMMMM pre process fail", KR(ret));
    return ret;
  }
  LOG_INFO("MMMMM pre done", KR(ret));
  char *bufs[WRITER_THREAD_NUM];
  for (int i = 0; i < WRITER_THREAD_NUM; i++) {
    // LOG_INFO("MMMMM", K(i));
    bufs[i] = (char *)malloc(THREAD_BUF_SIZE);
  }
  LOG_INFO("MMMMM allocate memory done");
  int rets[WRITER_THREAD_NUM];
  ObReadSortWriteThread threads(SPLIT_NUM, WRITER_THREAD_NUM, filepaths_,
    csv_parsers_, row_casters_, buffers_, sstable_writer_, table_schema_, external_sorts_, bufs, THREAD_BUF_SIZE, rets);
  threads.set_thread_count(WRITER_THREAD_NUM);
  threads.set_run_wrapper(MTL_CTX());
  threads.start();
  threads.wait();    
  for (int i = 0; i < WRITER_THREAD_NUM; i++) {
    ret = rets[i] == OB_SUCCESS ? ret : rets[i];
  }
  LOG_INFO("MMMMM thread done", KR(ret));
  for (int i = 0; i < WRITER_THREAD_NUM; i++) {
    free(bufs[i]);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sstable_writer_.close())) {
      LOG_INFO("MMMMM fail to close sstable writer", KR(ret));
    }
    for (auto &s : filepaths_) {
      if (remove(s.c_str()) != 0) {
        LOG_INFO("MMMMM remove fail", K(s.c_str()));
      }
    }
    LOG_INFO("MMMMM close done", KR(ret));
  }

  
  return ret;
}

/*
int ObLoadDataDirectDemo::do_load()
{
  int ret = OB_SUCCESS;
  int cnt = 0;
  int64_t total = 0;
  LOG_INFO("MMMMM load start");
  if (OB_FAIL(pre_process())) {
    LOG_INFO("MMMMM pre process fail", KR(ret));
    return ret;
  }
  LOG_INFO("MMMMM pre process done", KR(ret));
  for (int i = 0; i < SPLIT_NUM && OB_SUCC(ret); i++) {
    LOG_INFO("MMMMM partition", K(i), K(filepaths_[i].c_str()), KR(ret));  

    ObLoadSequentialFileReader &file_reader = file_readers_[i];
    ObString file_path(filepaths_[i].size(), filepaths_[i].c_str());
    if (OB_FAIL(file_reader.open(file_path))) {
      LOG_INFO("MMMMM can't open", K(filepaths_[i].c_str()));
      break;
    }
    int64_t COUNT = 10000000;

    // LOG_INFO("MMMMM size", K(sizeof(Key)), K(sizeof(Key) * COUNT));
    Key *keys = (Key *)malloc(COUNT * sizeof(Key));
    // Key *keylists[WRITER_THREAD_NUM];
    // int key_cnts[WRITER_THREAD_NUM];
    // for (int i = 0; i < WRITER_THREAD_NUM; i++) {
    //   key_cnts[i] = 0;
    //   keylists[i] = (Key *)malloc(COUNT * sizeof(Key));
    //   if (OB_ISNULL(keylists[i])) {
    //     ret = OB_ALLOCATE_MEMORY_FAILED;
    //     LOG_INFO("MMMMM fail to alloc memory", KR(ret), K(i));
    //     return ret;
    //   }
    // }
    LOG_INFO("MMMMM allocate enough space");
    int key_cnt = 0;
    size_t offset = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(do_load_buffer(file_reader))) {
        break;
      }
      while (OB_SUCC(ret)) {
        // if (OB_FAIL(parse_row_and_key(buffer_, keylists, key_cnts, offset))) {
        if (OB_FAIL(parse_row_and_key(buffer_, keys, key_cnt, offset))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("MMMMM fail to get next row", KR(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        }
        // LOG_INFO("MMMMM", K(key_cnt), K(offset), K(ret));
      }
    }
    LOG_INFO("MMMMM trivial", K(key_cnt));
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
    quicksort(keys, keys + key_cnt, [](const Key &k1, const Key &k2) {
        return k1.key1 < k2.key1 || (k1.key1 == k2.key1 && k1.key2 < k2.key2);
      });
    // for (int i = 0; i < WRITER_THREAD_NUM; i++) {
    //   quicksort(keylists[i], keylists[i] + key_cnts[i], [](const Key &k1, const Key &k2) {
    //     return k1.key1 < k2.key1 || (k1.key1 == k2.key1 && k1.key2 < k2.key2);
    //   });
    // }
    ObTrivialSortThread trivial_threads(external_sorts_, i * WRITER_THREAD_NUM, csv_parsers_, row_casters_, key_cnt, WRITER_THREAD_NUM, file_path, keys, table_schema_);
    trivial_threads.set_thread_count(WRITER_THREAD_NUM);
    trivial_threads.set_run_wrapper(MTL_CTX());
    trivial_threads.start();
    trivial_threads.wait();
    
    // alloc.free(keys);
    // ob_free(keys);
    LOG_INFO("MMMMM trivial done");
    free(keys);
    
    LOG_INFO("MMMMM sort done", KR(ret));
    // if (OB_FAIL(external_sort_.final_merge(total, WRITER_THREAD_NUM))) {
    //   LOG_INFO("MMMMM final merge fail", KR(ret));
    // } else {
    int rets[WRITER_THREAD_NUM];
    ObWriterThread threads(external_sorts_, i * WRITER_THREAD_NUM, sstable_writer_, table_schema_, rets, WRITER_THREAD_NUM);
    threads.set_thread_count(WRITER_THREAD_NUM);
    threads.set_run_wrapper(MTL_CTX());
    threads.start();
    threads.wait();

    for (int i = 0; i < WRITER_THREAD_NUM; i++) {
      ret = rets[i] != OB_SUCCESS ? rets[i] : ret;
    }
    LOG_INFO("MMMMM write done", KR(ret));
    
    if (OB_SUCC(ret)) {
      for (int j = i * WRITER_THREAD_NUM; j < (i+1) * WRITER_THREAD_NUM; j++) {
        // free(keylists[i]);
        external_sorts_[j].clean_up();
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sstable_writer_.close())) {
      LOG_INFO("MMMMM fail to close sstable writer", KR(ret));
    }
    LOG_INFO("MMMMM close done", KR(ret));
  }

  for (auto &s : filepaths_) {
    if (remove(s.c_str()) != 0) {
      LOG_INFO("MMMMM remove fail", K(s.c_str()));
    }
  }
  
  return ret;
  
  // while (OB_SUCC(ret)) {
  //   cnt++;
  //   int rets[DEMO_BUF_NUM];
  //   memset(rets, 0, sizeof(rets));
  //   if (OB_FAIL(do_load_buffer())) {
  //     break;
  //   }
    
  //   ObParseDataThread threads(buffer_, csv_parsers_, row_casters_, external_sorts_, file_reader_, rets);
  //   threads.set_thread_count(DEMO_BUF_NUM);
  //   threads.set_run_wrapper(MTL_CTX());
  //   threads.start();
  //   // threads.stop();
  //   threads.wait();
  //   total += threads.cnts();
  //   LOG_INFO("MMMMM threads succeed", K(cnt));

  //   for (int i = 0; i < DEMO_BUF_NUM; i++) {
  //     ret = rets[i] == OB_SUCCESS ? ret : rets[i];
  //   }
  // }
  // LOG_INFO("MMMMM finish reading", KR(ret));

  // if (OB_FAIL(external_sort_.close())) {
  //     LOG_INFO("MMMMM fail to close external sort", KR(ret));
  // }
  // for (int i = 0 ; i < WRITER_THREAD_NUM; i++) {
  //   if (OB_FAIL(external_sorts_[i].close())) {
  //     LOG_INFO("MMMMM fail to close external sort", KR(ret));
  //   }
  // }
  

  // }

  // if (OB_SUCC(ret)) {
  //   if (OB_FAIL(sstable_writer_.close())) {
  //     LOG_INFO("MMMMM fail to close sstable writer", KR(ret));
  //   }
  // }
  
  // const ObLoadDatumRow *datum_row = nullptr;
  // cnt = 0;
  // while (OB_SUCC(ret)) {
  //   cnt++;
  //   if (cnt % 100000 == 0) {
  //     LOG_INFO("MMMMM sstable append", K(cnt));
  //   }
  //   if (OB_FAIL(external_sort_.get_next_row(datum_row))) {
  //     if (OB_UNLIKELY(OB_ITER_END != ret)) {
  //       LOG_INFO("MMMMM fail to get next row", KR(ret));
  //     } else {
  //       ret = OB_SUCCESS;
  //       break;
  //     }
  //   } else if (OB_FAIL(sstable_writer_.append_row(*datum_row))) {
  //     LOG_INFO("MMMMM fail to append row", KR(ret));
  //   }
  // }
}
*/
} // namespace sql
} // namespace oceanbase


