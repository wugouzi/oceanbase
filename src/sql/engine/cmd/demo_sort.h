/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_OB_DEMO_SORT_H_
#define OCEANBASE_STORAGE_OB_DEMO_SORT_H_

#include "storage/blocksstable/ob_block_manager.h"
#include "share/ob_define.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_heap.h"
#include "lib/container/ob_vector.h"
#include "lib/thread/threads.h"
#include "share/io/ob_io_manager.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_tmp_file.h"
#include "share/config/ob_server_config.h"
#include "share/ob_thread_pool.h"
#include <chrono>
#include <thread>
#include <mutex>
#include <algorithm>
#include <cassert>
#include <functional>
#include <future>
#include <iterator>


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
    if (dist < 2)
    {
        return;
    }
    else if (dist < 100000)
    {
      std::sort(first,last,comp);
    }
    else
    {
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


namespace oceanbase
{
namespace storage
{


struct ObDemoExternalSortConstant
{
  static const int64_t BUF_HEADER_LENGTH = sizeof(int64_t); //serialization::encoded_length_i64(0);
  static const int64_t MIN_MEMORY_LIMIT = 8 * 1024LL * 1024LL;// min memory limit is 8m
  static const int64_t DEFAULT_FILE_READ_WRITE_BUFFER = 2 * 1024 * 1024LL; // 2m
  static const int64_t MIN_MULTIPLE_MERGE_COUNT = 2;
  static inline int get_io_timeout_ms(const int64_t expire_timestamp, int64_t &wait_time_ms);
  static inline bool is_timeout(const int64_t expire_timestamp);
};

int ObDemoExternalSortConstant::get_io_timeout_ms(
    const int64_t expire_timestamp, int64_t &wait_time_ms)
{
  int ret = common::OB_SUCCESS;

  wait_time_ms = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
  if (expire_timestamp < 0) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(expire_timestamp));
  } else if (0 != expire_timestamp) {
    const int64_t left_time_ms = (expire_timestamp - common::ObTimeUtility::current_time()) / 1000;
    if (0 == left_time_ms) {
      wait_time_ms = -1; // 0 means use default io timeout limit, so set -1 instead
    } else {
      wait_time_ms = std::min(wait_time_ms, left_time_ms);
    }
  }
  return ret;
}

bool ObDemoExternalSortConstant::is_timeout(const int64_t expire_timestamp)
{
  bool is_timeout = false;
  if (0 != expire_timestamp) {
    const int64_t cur_time = common::ObTimeUtility::current_time();
    if (cur_time > expire_timestamp) {
      is_timeout = true;
    }
  }
  return is_timeout;
}

template <typename T>
class ObDemoFragmentIterator
{
public:
  ObDemoFragmentIterator() {}
  virtual ~ObDemoFragmentIterator() {}
  virtual int get_next_item(const T *&item) = 0;
  virtual int clean_up() { return common::OB_SUCCESS; }
  virtual int prefetch() {return common::OB_SUCCESS; }
  virtual TO_STRING_KV(K(""));
};

template<typename T>
class ObDemoMacroBufferWriter
{
public:
  ObDemoMacroBufferWriter();
  virtual ~ObDemoMacroBufferWriter();
  int write_item(const T &item);
  int write_items(const ObVector<T *> &items, int idx);
  int write_item_compress(const T &item);
  void assign(const int64_t buf_pos, const int64_t buf_cap, char *buf);
  int serialize_header();
  bool has_item();
  TO_STRING_KV(KP(buf_), K(buf_pos_), K(buf_cap_));

  int just_write() { return just_write_; }
private:
  char *buf_;
  int64_t buf_pos_;
  int64_t buf_cap_;
  DISALLOW_COPY_AND_ASSIGN(ObDemoMacroBufferWriter);
  // threads
  static const int WRITER_THREAD_NUM = 10;
  // for failure and flush
  // the written index of array
  int just_write_ = 0;
  ObCompressor *compressor_;
  static const int TMP_BUF_SIZE = 350;
  char tmp_buf_[TMP_BUF_SIZE];
};

template<typename T>
ObDemoMacroBufferWriter<T>::ObDemoMacroBufferWriter()
  : buf_(NULL), buf_pos_(0), buf_cap_(0)
{
  ObCompressorPool &cp = ObCompressorPool::get_instance();
  // 
  cp.get_compressor("zstd_1.3.8", compressor_);
}

template<typename T>
ObDemoMacroBufferWriter<T>::~ObDemoMacroBufferWriter()
{
}

// naive implementation: serialize 10 and then write 10
// better strategy: serialization thread continue to serialize
template<typename T>
class ObDemoMacroBufferWriterSerializeThreads : public lib::Threads 
{
public: 
  ObDemoMacroBufferWriterSerializeThreads(const ObVector<T *> &items, int start_idx,
                                      int *rets, char *buf, int64_t cap,
                                      std::unordered_map<int, int64_t> pos_map)
    : items_(items), start_idx_(start_idx), rets_(rets), buf_(buf), cap_(cap),
      pos_map_(pos_map)  {}
  void run(int64_t idx) final {
    rets_[idx] = items_[idx + start_idx_]->serialize(buf_, cap_, pos_map_[idx]);
  }
private: 
  const ObVector<T *> &items_;
  int start_idx_;
  int *rets_;
  char *buf_;
  int64_t cap_;
  std::unordered_map<int, int64_t> pos_map_;
};

// threads -> generate serialized bufs
template<typename T>
int ObDemoMacroBufferWriter<T>::write_items(const ObVector<T *> &items, int idx)
{
  int ret = common::OB_SUCCESS;
  int rets[WRITER_THREAD_NUM];
  memset(rets, 0, sizeof(rets));
  LOG_INFO("MMMMM macro buffer write", K(items.size()), K(idx));

  if (idx == 0) {
    just_write_ = 0;
  }

  // TODO: int pos_map[NUM]
  int n = items.size();
  int cnt = idx;
  int i = 0;
  while (cnt < n && OB_SUCC(ret)) {
    int N = min(WRITER_THREAD_NUM, n - cnt);
    std::unordered_map<int, int64_t> pos_map;
    int64_t base = buf_pos_;
    for (int i = 0; i < N; i++) {
      pos_map[i] = base;
      base += items[cnt + i]->get_serialize_size();
      if (base > buf_cap_) {
        LOG_INFO("MMMMM macro buffer full", K(cnt), K(items.size()));
        return common::OB_EAGAIN;
      }
    }
    ObDemoMacroBufferWriterSerializeThreads<T> threads(items, cnt, rets, buf_, buf_cap_, pos_map);
    threads.set_thread_count(WRITER_THREAD_NUM);
    threads.set_run_wrapper(MTL_CTX());
    threads.start();
    threads.wait();
    cnt += N;
    just_write_ = cnt;
    buf_pos_ = base;
    i++;
    if (i % 10000 == 0) {
      LOG_INFO("MMMMM macro buffer write", K(N), K(cnt), K(n));
    } 
  }
  return ret;
}

template<typename T>
int ObDemoMacroBufferWriter<T>::write_item_compress(const T &item)
{
  int ret = common::OB_SUCCESS;
  int64_t size = item.get_serialize_size();
  int64_t size2;
  // LOG_INFO("MMMMM serialize_size", K(size));
  // int64_t size = 350;

  if (size + buf_pos_ > buf_cap_) {
    LOG_INFO("MMMMM macro writer full");
    ret = common::OB_EAGAIN;
  } else if (OB_FAIL(item.serialize(tmp_buf_, TMP_BUF_SIZE, 0))) {
    STORAGE_LOG(WARN, "fail to serialize item", K(ret));
  } else if (compressor_->compress(tmp_buf_, size, tmp_buf_ + buf_pos_, buf_cap_ - buf_pos_, size2)) {
    STORAGE_LOG(WARN, "MMMMM compress fail");
  } else {
    STORAGE_LOG(DEBUG, "write_item", K(buf_pos_), K(item));
  }
  return ret;
}

template<typename T>
int ObDemoMacroBufferWriter<T>::write_item(const T &item)
{
  int ret = common::OB_SUCCESS;
  // int64_t size = item.get_serialize_size();
  // LOG_INFO("MMMMM serialize_size", K(size));
  int64_t size = 350;
  if (size + buf_pos_ > buf_cap_) {
    LOG_INFO("MMMMM macro writer full");
    ret = common::OB_EAGAIN;
  } else if (OB_FAIL(item.serialize(buf_, buf_cap_, buf_pos_))) {
    STORAGE_LOG(WARN, "fail to serialize item", K(ret));
  } else {
    STORAGE_LOG(DEBUG, "write_item", K(buf_pos_), K(item));
  }
  return ret;
}

template<typename T>
int ObDemoMacroBufferWriter<T>::serialize_header()
{
  int ret = common::OB_SUCCESS;
  const int64_t header_size = ObDemoExternalSortConstant::BUF_HEADER_LENGTH;
  int64_t tmp_pos_ = 0;
  if (OB_FAIL(common::serialization::encode_i64(buf_, header_size, tmp_pos_, buf_pos_))) {
    STORAGE_LOG(WARN, "fail to encode macro block buffer header", K(ret), K(tmp_pos_),
        K(header_size), K(buf_pos_));
  } else {
    STORAGE_LOG(DEBUG, "serialize header success", K(tmp_pos_), K(buf_pos_));
  }
  return ret;
}

template<typename T>
void ObDemoMacroBufferWriter<T>::assign(const int64_t pos, const int64_t buf_cap, char *buf)
{
  buf_pos_ = pos;
  buf_cap_ = buf_cap;
  buf_ = buf;
}

template<typename T>
bool ObDemoMacroBufferWriter<T>::has_item()
{
  return buf_pos_ > ObDemoExternalSortConstant::BUF_HEADER_LENGTH;
}

template<typename T>
class ObDemoFragmentWriterV2
{
public:
  ObDemoFragmentWriterV2();
  virtual ~ObDemoFragmentWriterV2();
  int open(const int64_t buf_size, const int64_t expire_timestamp,
      const uint64_t tenant_id, const int64_t dir_id);
  int write_item(const T &item);
  int write_items(const ObVector<T *> &items);
  int sync();
  void reset();
  int64_t get_fd() const { return fd_; }
  int64_t get_dir_id() const { return dir_id_; }
  const T &get_sample_item() const { return sample_item_; }
private:
  int flush_buffer();
  int check_need_flush(bool &need_flush);
private:
  bool is_inited_;
  char *buf_;
  int64_t buf_size_;
  int64_t expire_timestamp_;
  common::ObArenaAllocator allocator_;
  ObDemoMacroBufferWriter<T> macro_buffer_writer_;
  bool has_sample_item_;
  T sample_item_;
  blocksstable::ObTmpFileIOHandle file_io_handle_;
  int64_t fd_;
  int64_t dir_id_;
  uint64_t tenant_id_;
  DISALLOW_COPY_AND_ASSIGN(ObDemoFragmentWriterV2);

  // for debug
  int write_cnt_ = 0;
};

template<typename T>
ObDemoFragmentWriterV2<T>::ObDemoFragmentWriterV2()
  : is_inited_(false), buf_(NULL), buf_size_(0), expire_timestamp_(0),
    allocator_(common::ObNewModIds::OB_ASYNC_EXTERNAL_SORTER, common::OB_MALLOC_BIG_BLOCK_SIZE),
    macro_buffer_writer_(), has_sample_item_(false), sample_item_(),
    file_io_handle_(), fd_(-1), dir_id_(-1), tenant_id_(common::OB_INVALID_ID)
{
}

template<typename T>
ObDemoFragmentWriterV2<T>::~ObDemoFragmentWriterV2()
{
  reset();
}

template<typename T>
int ObDemoFragmentWriterV2<T>::open(const int64_t buf_size, const int64_t expire_timestamp,
    const uint64_t tenant_id, const int64_t dir_id)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObFragmentWriter has already been inited", K(ret));
  } else if (buf_size < OB_SERVER_BLOCK_MGR.get_macro_block_size()
      || buf_size % DIO_ALIGN_SIZE != 0
      || expire_timestamp < 0
      || common::OB_INVALID_ID == tenant_id) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(buf_size), K(expire_timestamp));
  } else {
    dir_id_ = dir_id;
    const int64_t align_buf_size = common::lower_align(buf_size, OB_SERVER_BLOCK_MGR.get_macro_block_size());
    if (NULL == (buf_ = static_cast<char *>(allocator_.alloc(align_buf_size)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate buffer", K(ret), K(align_buf_size));
    } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.open(fd_, dir_id_))) {
      STORAGE_LOG(WARN, "fail to open file", K(ret));
    } else {
      buf_size_ = align_buf_size;
      expire_timestamp_ = expire_timestamp;
      macro_buffer_writer_.assign(ObDemoExternalSortConstant::BUF_HEADER_LENGTH, buf_size_, buf_);
      has_sample_item_ = false;
      tenant_id_ = tenant_id;
      is_inited_ = true;
    }
  }
  return ret;
}

template<typename T>
int ObDemoFragmentWriterV2<T>::write_items(const ObVector<T *> &items)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObFragmentWriter has not been inited", K(ret));
  } else if (OB_FAIL(macro_buffer_writer_.write_items(items, 0))) {
    for (int i = 0; i < 40; i++) {
      if (common::OB_EAGAIN == ret) {
        if (OB_FAIL(flush_buffer())) {
          STORAGE_LOG(WARN, "MMMMM switch next macro buffer failed", K(ret));
        } else if (OB_FAIL(macro_buffer_writer_.write_items(items, 
                                                            macro_buffer_writer_.just_write() + 1))) {
          STORAGE_LOG(WARN, "MMMMM fail to write item", K(ret));
        }
      } else {
        STORAGE_LOG(WARN, "MMMMM fail to write item", K(ret));
        break;
      }
    }
  }

  if (OB_SUCC(ret) && !has_sample_item_) {
    const T& item = *(items[0]);
    const int64_t buf_len = item.get_deep_copy_size(); // deep copy size may be 0
    char *buf = NULL;
    int64_t pos = 0;
    if (buf_len > 0 && OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(buf_len)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc buf", K(ret), K(buf_len));
    } else if (OB_FAIL(sample_item_.deep_copy(item, buf, buf_len, pos))) {
      STORAGE_LOG(WARN, "failed to deep copy item", K(ret));
    } else {
      has_sample_item_ = true;
    }
  }
  return ret;
}

template<typename T>
int ObDemoFragmentWriterV2<T>::write_item(const T &item)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObFragmentWriter has not been inited", K(ret));
  } else if (OB_FAIL(macro_buffer_writer_.write_item(item))) {
    if (common::OB_EAGAIN == ret) {
      if (OB_FAIL(flush_buffer())) {
        STORAGE_LOG(WARN, "switch next macro buffer failed", K(ret));
      } else if (OB_FAIL(macro_buffer_writer_.write_item(item))) {
        STORAGE_LOG(WARN, "fail to write item", K(ret));
      } else {
        write_cnt_++;
      }
    } else {
      STORAGE_LOG(WARN, "fail to write item", K(ret));
    }
  } else {
    write_cnt_++;
  }

  if (OB_SUCC(ret) && !has_sample_item_) {
    const int64_t buf_len = item.get_deep_copy_size(); // deep copy size may be 0
    char *buf = NULL;
    int64_t pos = 0;
    if (buf_len > 0 && OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(buf_len)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc buf", K(ret), K(buf_len));
    } else if (OB_FAIL(sample_item_.deep_copy(item, buf, buf_len, pos))) {
      STORAGE_LOG(WARN, "failed to deep copy item", K(ret));
    } else {
      has_sample_item_ = true;
    }
  }
  return ret;
}

template<typename T>
int ObDemoFragmentWriterV2<T>::check_need_flush(bool &need_flush)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoFragmentWriterV2 has not been inited", K(ret));
  } else {
    need_flush = macro_buffer_writer_.has_item();
  }
  return ret;
}

template<typename T>
int ObDemoFragmentWriterV2<T>::flush_buffer()
{
  int ret = common::OB_SUCCESS;
  int64_t timeout_ms = 0;
  LOG_INFO("MMMMM flush buffer", K(write_cnt_));
  // write_cnt_ = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoFragmentWriterV2 has not been inited", K(ret));
  } else if (OB_FAIL(ObDemoExternalSortConstant::get_io_timeout_ms(expire_timestamp_, timeout_ms))) {
    STORAGE_LOG(WARN, "fail to get io timeout ms", K(ret), K(expire_timestamp_));
  } else if (OB_FAIL(file_io_handle_.wait(timeout_ms))) {
    STORAGE_LOG(WARN, "fail to wait io finish", K(ret));
  } else if (OB_FAIL(macro_buffer_writer_.serialize_header())) {
    STORAGE_LOG(WARN, "fail to serialize header", K(ret));
  } else {
    blocksstable::ObTmpFileIOInfo io_info;
    io_info.fd_ = fd_;
    io_info.dir_id_ = dir_id_;
    io_info.size_ = buf_size_;
    io_info.tenant_id_ = tenant_id_;
    io_info.buf_ = buf_;
    io_info.io_desc_.set_category(common::ObIOCategory::SYS_IO);
    io_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_INDEX_BUILD_WRITE);
    int64_t start = common::ObTimeUtility::current_time();
    if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.aio_write(io_info, file_io_handle_))) {
      STORAGE_LOG(WARN, "MMMMM fail to do aio write macro file", K(ret), K(io_info));
    } else {
      int64_t dif = common::ObTimeUtility::current_time() - start;
      LOG_INFO("AIO time", K(dif));
      macro_buffer_writer_.assign(ObDemoExternalSortConstant::BUF_HEADER_LENGTH, buf_size_, buf_);
      dif = common::ObTimeUtility::current_time() - start - dif;
      LOG_INFO("buf time", K(dif));
    }
  }
  return ret;
}

template<typename T>
int ObDemoFragmentWriterV2<T>::sync()
{
  int ret = common::OB_SUCCESS;
  bool need_flush = false;
  if (is_inited_) {
    if (OB_FAIL(check_need_flush(need_flush))) {
      STORAGE_LOG(WARN, "fail to check need flush", K(ret));
    } else if (need_flush) {
      if (OB_FAIL(flush_buffer())) {
        STORAGE_LOG(WARN, "fail to flush buffer", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t timeout_ms = 0;
      if (OB_FAIL(ObDemoExternalSortConstant::get_io_timeout_ms(expire_timestamp_, timeout_ms))) {
        STORAGE_LOG(WARN, "fail to get io timeout ms", K(ret), K(expire_timestamp_));
      } else if (OB_FAIL(file_io_handle_.wait(timeout_ms))) {
        STORAGE_LOG(WARN, "fail to wait io finish", K(ret));
      } else if (OB_FAIL(ObDemoExternalSortConstant::get_io_timeout_ms(expire_timestamp_, timeout_ms))) {
        STORAGE_LOG(WARN, "fail to get io timeout ms", K(ret), K(expire_timestamp_));
      } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.sync(fd_, timeout_ms))) {
        STORAGE_LOG(WARN, "fail to sync macro file", K(ret));
      }
    }
  }
  return ret;
}

template<typename T>
void ObDemoFragmentWriterV2<T>::reset()
{
  is_inited_ = false;
  buf_ = NULL;
  buf_size_ = 0;
  expire_timestamp_ = 0;
  allocator_.reuse();
  macro_buffer_writer_.assign(0, 0, NULL);
  has_sample_item_ = false;
  file_io_handle_.reset();
  fd_ = -1;
  dir_id_ = -1;
  tenant_id_ = common::OB_INVALID_ID;
}

template<typename T>
class ObDemoMacroBufferReader
{
public:
  ObDemoMacroBufferReader();
  virtual ~ObDemoMacroBufferReader();
  int read_item(T &item);
  int read_item_decompress(T &item);
  int deserialize_header();
  void assign(const int64_t buf_pos, const int64_t buf_cap, const char *buf);
  TO_STRING_KV(KP(buf_), K(buf_pos_), K(buf_len_), K(buf_cap_));
private:
  const char *buf_;
  int64_t buf_pos_;
  int64_t buf_len_;
  int64_t buf_cap_;

  ObCompressor *compressor_;
};

template<typename T>
ObDemoMacroBufferReader<T>::ObDemoMacroBufferReader()
  : buf_(NULL), buf_pos_(0), buf_len_(0), buf_cap_(0)
{
  ObCompressorPool &cp = ObCompressorPool::get_instance();
  cp.get_compressor("zstd_1.3.8", compressor_);
}

template<typename T>
ObDemoMacroBufferReader<T>::~ObDemoMacroBufferReader()
{
}

template<typename T>
int ObDemoMacroBufferReader<T>::read_item(T &item)
{
  int ret = common::OB_SUCCESS;
  if (0 == buf_len_) {
    if (OB_FAIL(deserialize_header())) {
      STORAGE_LOG(WARN, "fail to deserialize header");
    }
  }
  if (OB_SUCC(ret)) {
    if (buf_pos_ == buf_len_) {
      ret = common::OB_EAGAIN;
    } else if (OB_FAIL(item.deserialize(buf_, buf_len_, buf_pos_))) {
      STORAGE_LOG(WARN, "fail to deserialize buffer", K(ret), K(buf_len_), K(buf_pos_));
    } else {
      STORAGE_LOG(DEBUG, "macro buffer reader", K(buf_len_), K(buf_pos_));
    }
  }
  return ret;
}

template<typename T>
int ObDemoMacroBufferReader<T>::deserialize_header()
{
  int ret = common::OB_SUCCESS;
  const int64_t header_size = ObDemoExternalSortConstant::BUF_HEADER_LENGTH;
  if (OB_FAIL(common::serialization::decode_i64(buf_, header_size, buf_pos_, &buf_len_))) {
    STORAGE_LOG(WARN, "fail to encode macro block buffer header", K(ret), K(buf_pos_),
        K(header_size), K(buf_len_));
  } else {
    STORAGE_LOG(DEBUG, "deserialize header success", K(buf_len_), K(buf_pos_));
  }
  return ret;
}

template<typename T>
void ObDemoMacroBufferReader<T>::assign(const int64_t buf_pos, const int64_t buf_cap, const char *buf)
{
  buf_pos_ = buf_pos;
  buf_cap_ = buf_cap;
  buf_len_ = 0;
  buf_ = buf;
}

template <typename T>
class ObDemoFragmentReaderV2 : public ObDemoFragmentIterator<T>
{
public:
  ObDemoFragmentReaderV2();
  virtual ~ObDemoFragmentReaderV2();
  int init(const int64_t fd, const int64_t dir_id, const int64_t expire_timestamp,
      const uint64_t tenant_id, const T &sample_item, const int64_t buf_size);
  int open();
  virtual int get_next_item(const T *&item);
  virtual int clean_up();
private:
  int prefetch();
  int wait();
  int pipeline();
  void reset();
private:
  static const int64_t MAX_HANDLE_COUNT = 2;
  bool is_inited_;
  int64_t expire_timestamp_;
  common::ObArenaAllocator allocator_;
  common::ObArenaAllocator sample_allocator_;
  ObDemoMacroBufferReader<T> macro_buffer_reader_;
  int64_t fd_;
  int64_t dir_id_;
  T curr_item_;
  blocksstable::ObTmpFileIOHandle file_io_handles_[MAX_HANDLE_COUNT];
  int64_t handle_cursor_;
  char *buf_;
  uint64_t tenant_id_;
  bool is_prefetch_end_;
  int64_t buf_size_;
  bool is_first_prefetch_;
};

template<typename T>
ObDemoFragmentReaderV2<T>::ObDemoFragmentReaderV2()
  : is_inited_(false), expire_timestamp_(0),
    allocator_(common::ObNewModIds::OB_ASYNC_EXTERNAL_SORTER, common::OB_MALLOC_BIG_BLOCK_SIZE),
    sample_allocator_(common::ObNewModIds::OB_ASYNC_EXTERNAL_SORTER, OB_MALLOC_NORMAL_BLOCK_SIZE),
    macro_buffer_reader_(), fd_(-1), dir_id_(-1), curr_item_(),
    file_io_handles_(), handle_cursor_(-1), buf_(NULL), tenant_id_(common::OB_INVALID_ID),
    is_prefetch_end_(false), buf_size_(0), is_first_prefetch_(true)
{
}

template <typename T>
ObDemoFragmentReaderV2<T>::~ObDemoFragmentReaderV2()
{
  reset();
}

template<typename T>
int ObDemoFragmentReaderV2<T>::init(const int64_t fd, const int64_t dir_id, const int64_t expire_timestamp,
    const uint64_t tenant_id, const T &sample_item, const int64_t buf_size)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObFragmentReader has already been inited", K(ret));
  } else if (common::OB_INVALID_ID == tenant_id
      || buf_size % DIO_ALIGN_SIZE != 0
      || expire_timestamp < 0
      || buf_size < 0) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id),
        K(expire_timestamp), K(buf_size));
  } else {
    const int64_t buf_len = sample_item.get_deep_copy_size(); // deep copy size may be 0
    int64_t pos = 0;
    char *buf = NULL;
    if (buf_len > 0 && OB_ISNULL(buf = static_cast<char *>(sample_allocator_.alloc(buf_len)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc buf", K(ret), K(buf_len));
    } else if (OB_FAIL(curr_item_.deep_copy(
            sample_item, buf, buf_len, pos))) {
      STORAGE_LOG(WARN, "failed to deep copy item", K(ret));
    } else {
      expire_timestamp_ = expire_timestamp;
      handle_cursor_ = 0;
      fd_ = fd;
      dir_id_ = dir_id;
      tenant_id_ = tenant_id;
      is_first_prefetch_ = true;
      buf_size_ = common::lower_align(buf_size, OB_SERVER_BLOCK_MGR.get_macro_block_size());
      is_inited_ = true;
    }
  }
  return ret;
}

template<typename T>
int ObDemoFragmentReaderV2<T>::open()
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoFragmentReaderV2 has not been inited", K(ret));
  } else if (OB_FAIL(prefetch())) {
    STORAGE_LOG(WARN, "fail to prefetch data", K(ret));
  }
  return ret;
}

template<typename T>
int ObDemoFragmentReaderV2<T>::prefetch()
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoFragmentReaderV2 has not been inited", K(ret));
  } else {
    if (nullptr == buf_) {
      if (OB_ISNULL(buf_ = static_cast<char *>(allocator_.alloc(buf_size_)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to allocate memory", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      blocksstable::ObTmpFileIOInfo io_info;
      io_info.fd_ = fd_;
      io_info.dir_id_ = dir_id_;
      io_info.size_ = buf_size_;
      io_info.tenant_id_ = tenant_id_;
      io_info.buf_ = buf_;
      io_info.io_desc_.set_category(common::ObIOCategory::SYS_IO);
      io_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_INDEX_BUILD_READ);
      if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.aio_read(io_info, file_io_handles_[handle_cursor_ % MAX_HANDLE_COUNT]))) {
        if (common::OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "fail to do aio read from macro file", K(ret), K(fd_));
        } else {
          is_prefetch_end_ = true;
          ret = OB_SUCCESS;
        }
      } else {
        ++handle_cursor_;
      }
    }
  }
  return ret;
}

template <typename T>
int ObDemoFragmentReaderV2<T>::wait()
{
  int ret = common::OB_SUCCESS;
  const int64_t wait_cursor = (handle_cursor_ +  1) % MAX_HANDLE_COUNT;
  int64_t timeout_ms = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoFragmentReaderV2 has not been inited", K(ret));
  } else if (is_prefetch_end_) {
    ret = common::OB_ITER_END;
  } else if (OB_FAIL(ObDemoExternalSortConstant::get_io_timeout_ms(expire_timestamp_, timeout_ms))) {
    STORAGE_LOG(WARN, "fail to get io timeout ms", K(ret), K(expire_timestamp_), K(timeout_ms));
  } else if (OB_FAIL(file_io_handles_[wait_cursor].wait(timeout_ms))) {
    STORAGE_LOG(WARN, "fail to wait io finish", K(ret), K(timeout_ms));
  } else {
    macro_buffer_reader_.assign(0, buf_size_, file_io_handles_[wait_cursor].get_buffer());
  }
  return ret;
}

template <typename T>
int ObDemoFragmentReaderV2<T>::pipeline()
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoFragmentReaderV2 has not been inited", K(ret));
  } else if (OB_FAIL(wait())) {
    if (common::OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to wait io finish", K(ret));
    }
  } else if (OB_FAIL(prefetch())) {
    STORAGE_LOG(WARN, "fail to prefetch data", K(ret));
  }
  return ret;
}

template<typename T>
int ObDemoFragmentReaderV2<T>::get_next_item(const T *&item)
{
  int ret = common::OB_SUCCESS;
  item = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoFragmentReaderV2 has not been inited", K(ret));
  } else if (is_first_prefetch_) {
    if (OB_FAIL(pipeline())) {
      if (common::OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to pipeline data", K(ret));
      }
    } else {
      is_first_prefetch_ = false;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(macro_buffer_reader_.read_item(curr_item_))) {
      if (common::OB_EAGAIN == ret) {
        if (OB_FAIL(pipeline())) {
          if (common::OB_ITER_END != ret) {
            STORAGE_LOG(WARN, "fail to switch next buffer", K(ret));
          }
        } else if (OB_FAIL(macro_buffer_reader_.read_item(curr_item_))) {
          STORAGE_LOG(WARN, "fail to read item", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    item = &curr_item_;
  }
  return ret;
}

template<typename T>
void ObDemoFragmentReaderV2<T>::reset()
{
  is_inited_ = false;
  expire_timestamp_ = 0;
  allocator_.reset();
  sample_allocator_.reset();
  macro_buffer_reader_.assign(0, 0, NULL);
  fd_ = -1;
  dir_id_ = -1;
  for (int64_t i = 0; i < MAX_HANDLE_COUNT; ++i) {
    file_io_handles_[i].reset();
  }
  handle_cursor_ = 0;
  buf_ = NULL;
  tenant_id_ = common::OB_INVALID_ID;
  is_prefetch_end_ = false;
  buf_size_ = 0;
  is_first_prefetch_ = true;
}

template<typename T>
int ObDemoFragmentReaderV2<T>::clean_up()
{
  int ret = common::OB_SUCCESS;
  if (is_inited_) {
    if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.remove(fd_))) {
      STORAGE_LOG(WARN, "fail to remove macro file", K(ret));
    }
    reset();
  }
  return ret;
}

template<typename T, typename Compare>
class ObDemoFragmentMerge
{
public:
  typedef ObDemoFragmentIterator<T> FragmentIterator;
  static const int64_t DEFAULT_ITERATOR_NUM = 64;
  ObDemoFragmentMerge();
  virtual ~ObDemoFragmentMerge();
  int init(const common::ObIArray<FragmentIterator *> &readers, Compare *compare);
  int open();
  int get_next_item(const T *&item);
  void reset();
  bool is_opened() const { return is_opened_; }
private:
  int direct_get_next_item(const T *&item);
  int heap_get_next_item(const T *&item);
  int build_heap();
private:
  struct HeapItem
  {
    const T *item_;
    int64_t idx_;
    HeapItem() : item_(NULL), idx_(0)
    {}
    void reset()
    {
      item_ = NULL;
      idx_ = 0;
    }
    TO_STRING_KV(K_(item), K_(idx));
  };
  class HeapCompare
  {
  public:
    explicit HeapCompare(int &ret);
    virtual ~HeapCompare();
    bool operator() (const HeapItem &left_item, const HeapItem &right_item) const;
    void set_compare(Compare *compare) { compare_ = compare; }
    int get_error_code() { return ret_; }
  private:
    Compare *compare_;
    int &ret_;
  };
private:
  bool is_inited_;
  bool is_opened_;
  HeapCompare compare_;
  common::ObSEArray<FragmentIterator *, DEFAULT_ITERATOR_NUM> iters_;
  int64_t last_iter_idx_;
  common::ObBinaryHeap<HeapItem, HeapCompare, DEFAULT_ITERATOR_NUM> heap_;
  int sort_ret_;
};

template<typename T, typename Compare>
ObDemoFragmentMerge<T, Compare>::HeapCompare::HeapCompare(int &ret)
  : compare_(NULL), ret_(ret)
{
}

template<typename T, typename Compare>
ObDemoFragmentMerge<T, Compare>::HeapCompare::~HeapCompare()
{
}

template<typename T, typename Compare>
void ObDemoFragmentMerge<T, Compare>::reset()
{
  is_inited_ = false;
  is_opened_ = false;
  iters_.reset();
  last_iter_idx_ = -1;
  heap_.reset();
  sort_ret_ = common::OB_SUCCESS;
}

template<typename T, typename Compare>
bool ObDemoFragmentMerge<T, Compare>::HeapCompare::operator()(
    const HeapItem &left_item, const HeapItem &right_item) const
{
  int ret = common::OB_SUCCESS;
  bool bret = false;
  if (NULL == compare_) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(compare_));
  } else if (NULL == left_item.item_ || NULL == right_item.item_) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid compare items", K(ret),
        KP(left_item.item_), KP(right_item.item_));
  } else {
    bret = !compare_->operator()(left_item.item_, right_item.item_);
  }
  if (OB_FAIL(ret)) {
    ret_ = ret;
  } else if (OB_FAIL(compare_->result_code_)) {
    ret_ = compare_->result_code_;
  } else {
    ret_ = common::OB_SUCCESS;
  }
  return bret;
}

template<typename T, typename Compare>
ObDemoFragmentMerge<T, Compare>::ObDemoFragmentMerge()
  : is_inited_(false), is_opened_(false), compare_(sort_ret_), iters_(),
    last_iter_idx_(-1), heap_(compare_), sort_ret_(common::OB_SUCCESS)
{
}

template<typename T, typename Compare>
ObDemoFragmentMerge<T, Compare>::~ObDemoFragmentMerge()
{
}

template<typename T, typename Compare>
int ObDemoFragmentMerge<T, Compare>::init(
    const common::ObIArray<FragmentIterator *> &iters, Compare *compare)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObDemoFragmentMerge has been inited", K(ret));
  } else if (0 == iters.count() || NULL == compare) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(iters.count()), KP(compare));
  } else if (OB_FAIL(iters_.assign(iters))) {
    STORAGE_LOG(WARN, "fail to assign iterators", K(ret));
  } else {
    compare_.set_compare(compare);
    is_inited_ = true;
    is_opened_ = false;
  }
  return ret;
}

template<typename T, typename Compare>
int ObDemoFragmentMerge<T, Compare>::open()
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoFragmentMerge has not been inited", K(ret));
  } else if (is_opened_) {
    ret = common::OB_ERR_SYS;
    STORAGE_LOG(WARN, "ObDemoFragmentMerge has been opened before", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < iters_.count(); ++i) {
      if (OB_FAIL(iters_.at(i)->prefetch())) {
        STORAGE_LOG(WARN, "fail to prefetch", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (iters_.count() > 1 && OB_FAIL(build_heap())) {
        STORAGE_LOG(WARN, "fail to build heap", K(ret));
      } else {
        is_opened_ = true;
      }
    }
  }
  return ret;
}

template<typename T, typename Compare>
int ObDemoFragmentMerge<T, Compare>::build_heap()
{
  int ret = common::OB_SUCCESS;
  const T *item = NULL;
  HeapItem heap_item;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoFragmentMerge has not been inited", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < iters_.count(); ++i) {
    if (OB_FAIL(iters_.at(i)->get_next_item(item))) {
      if (common::OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to get next item", K(ret), K(i));
      } else {
        ret = common::OB_SUCCESS;
      }
    } else if (NULL == item) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid item", K(ret), KP(item));
    } else {
      heap_item.item_ = item;
      heap_item.idx_ = i;
      if (OB_FAIL(heap_.push(heap_item))) {
        STORAGE_LOG(WARN, "fail to push heap", K(ret));
      } else if (OB_FAIL(compare_.get_error_code())) {
        STORAGE_LOG(WARN, "fail to compare items", K(ret));
      } else {
        STORAGE_LOG(DEBUG, "push a heap item", K(*item));
      }
    }
  }

  return ret;
}

template<typename T, typename Compare>
int ObDemoFragmentMerge<T, Compare>::direct_get_next_item(const T *&item)
{
  int ret = common::OB_SUCCESS;
  item = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoFragmentMerge has not been inited", K(ret));
  } else if (1 != iters_.count()) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(iters_.count()));
  } else if (OB_FAIL(iters_.at(0)->get_next_item(item))) {
    if (common::OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to get next item", K(ret));
    }
  }
  return ret;
}

template<typename T, typename Compare>
int ObDemoFragmentMerge<T, Compare>::heap_get_next_item(const T *&item)
{
  int ret = common::OB_SUCCESS;
  HeapItem heap_item;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoFragmentMerge has not been inited", K(ret));
  } else if (last_iter_idx_ >= 0 && last_iter_idx_ < iters_.count()) {
    FragmentIterator *iter = iters_.at(last_iter_idx_);
    if (OB_FAIL(iter->get_next_item(heap_item.item_))) {
      if (common::OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to get next item", K(ret));
      } else if (OB_FAIL(heap_.pop())) { // overwrite OB_ITER_END
        STORAGE_LOG(WARN, "fail to pop heap item", K(ret));
      } else if (OB_FAIL(compare_.get_error_code())) {
        STORAGE_LOG(WARN, "fail to compare items", K(ret));
      } else {
        STORAGE_LOG(DEBUG, "pop a heap item");
      }
    } else if (NULL == heap_item.item_) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid item", K(ret), KP(heap_item.item_));
    } else {
      heap_item.idx_ = last_iter_idx_;
      if (OB_FAIL(heap_.replace_top(heap_item))) {
        STORAGE_LOG(WARN, "fail to replace heap top", K(ret));
      } else if (OB_FAIL(compare_.get_error_code())) {
        STORAGE_LOG(WARN, "fail to compare items", K(ret));
      } else {
        STORAGE_LOG(DEBUG, "replace heap item", K(*heap_item.item_), K(last_iter_idx_));
      }
    }
    last_iter_idx_ = -1;
  }

  if (OB_SUCC(ret) && heap_.empty()) {
    ret = common::OB_ITER_END;
  }

  if (OB_SUCC(ret)) {
    const HeapItem *item_ptr = NULL;
    if (OB_FAIL(heap_.top(item_ptr))) {
      STORAGE_LOG(WARN, "fail to get heap top item", K(ret));
    } else if (OB_FAIL(compare_.get_error_code())) {
      STORAGE_LOG(WARN, "fail to compare items", K(ret));
    } else if (NULL == item_ptr || NULL == item_ptr->item_) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid heap item", K(ret), KP(item_ptr));
    } else {
      last_iter_idx_ = item_ptr->idx_;
      item = item_ptr->item_;
      STORAGE_LOG(DEBUG, "top heap item", K(*item), K(last_iter_idx_));
    }
  }

  return ret;
}

template<typename T, typename Compare>
int ObDemoFragmentMerge<T, Compare>::get_next_item(const T *&item)
{
  int ret = common::OB_SUCCESS;
  item = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoFragmentMerge has not been inited", K(ret));
  } else if (1 == iters_.count()) {
    if (OB_FAIL(direct_get_next_item(item))) {
      if (common::OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to directly get next item from reader", K(ret));
      }
    }
  } else if (OB_FAIL(heap_get_next_item(item))) {
    if (common::OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to get next item from heap", K(ret));
    }
  }
  return ret;
}

template<typename T, typename Compare>
class ObDemoExternalSortRound
{
public:
  ObDemoExternalSortRound();
  virtual ~ObDemoExternalSortRound();
  int init(const int64_t merge_count, const int64_t file_buf_size, const int64_t expire_timestamp,
      const uint64_t tenant_id, Compare *compare);
  bool is_inited() const { return is_inited_; }
  int add_item(const T &item);
  int build_fragment();
  int do_merge(ObDemoExternalSortRound &next_round, int thread_num);
  int do_one_run(const int64_t start_reader_idx, ObDemoExternalSortRound &next_round);
  int do_one_run_with_threads(const int64_t start_reader_idx, ObDemoExternalSortRound &next_round, int thread_num);
  int finish_write();
  int clean_up();
  int build_merger();
  int get_next_item(const T *&item);
  int64_t get_fragment_count();
  int add_fragment_iter(ObDemoFragmentIterator<T> *iter);
  int transfer_final_sorted_fragment_iter(ObDemoExternalSortRound &dest_round);
  void combine_round(ObDemoExternalSortRound &round);
  int add_items(const ObVector<T *> &item);
private:
  typedef ObDemoFragmentReaderV2<T> FragmentReader;
  typedef ObDemoFragmentIterator<T> FragmentIterator;
  typedef common::ObArray<FragmentIterator *> FragmentIteratorList;
  typedef ObDemoFragmentWriterV2<T> FragmentWriter;
  typedef ObDemoFragmentMerge<T, Compare> FragmentMerger;
  bool is_inited_;
  int64_t merge_count_;
  int64_t file_buf_size_;
  FragmentIteratorList iters_;
  FragmentWriter writer_;
  int64_t expire_timestamp_;
  Compare *compare_;
  FragmentMerger mergers_[16];
  FragmentMerger merger_;
  common::ObArenaAllocator allocator_;
  uint64_t tenant_id_;
  int64_t dir_id_;
  bool is_writer_opened_;
};

template<typename T, typename Compare>
void ObDemoExternalSortRound<T, Compare>::combine_round(ObDemoExternalSortRound &round)
{
  FragmentIteratorList &iters = round.iters_;
  for (int i = 0; i < iters.size(); i++) {
    iters_.push_back(iters[i]);
  }
  iters.reset();
}

template<typename T, typename Compare>
ObDemoExternalSortRound<T, Compare>::ObDemoExternalSortRound()
  : is_inited_(false), merge_count_(0), file_buf_size_(0), iters_(), writer_(),
    expire_timestamp_(0), compare_(NULL), merger_(),
    allocator_(common::ObNewModIds::OB_ASYNC_EXTERNAL_SORTER, common::OB_MALLOC_BIG_BLOCK_SIZE),
    tenant_id_(common::OB_INVALID_ID), dir_id_(-1), is_writer_opened_(false)
{
}

template<typename T, typename Compare>
ObDemoExternalSortRound<T, Compare>::~ObDemoExternalSortRound()
{
}

template<typename T, typename Compare>
int ObDemoExternalSortRound<T, Compare>::init(
    const int64_t merge_count, const int64_t file_buf_size, const int64_t expire_timestamp,
    const uint64_t tenant_id, Compare *compare)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObDemoExternalSortRound has been inited", K(ret));
  } else if (merge_count < ObDemoExternalSortConstant::MIN_MULTIPLE_MERGE_COUNT
      || file_buf_size % DIO_ALIGN_SIZE != 0
      || common::OB_INVALID_ID == tenant_id
      || NULL == compare) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(merge_count), K(file_buf_size),
        KP(compare));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.alloc_dir(dir_id_))) {
    STORAGE_LOG(WARN, "fail to alloc dir", K(ret));
  } else {
    is_inited_ = true;
    merge_count_ = merge_count;
    file_buf_size_ = file_buf_size;
    iters_.reset();
    expire_timestamp_ = expire_timestamp;
    compare_ = compare;
    tenant_id_ = tenant_id;
    is_writer_opened_ = false;
    merger_.reset();
    for (int i = 0; i < 16; i++) {
        mergers_[i].reset();
    }
  }
  return ret;
}

template<typename T, typename Compare>
int ObDemoExternalSortRound<T, Compare>::add_items(const ObVector<T *> &items)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoExternalSortRound has not been inited", K(ret));
  } else if (ObDemoExternalSortConstant::is_timeout(expire_timestamp_)) {
    ret = common::OB_TIMEOUT;
    STORAGE_LOG(WARN, "ObDemoExternalSortRound timeout", K(ret), K(expire_timestamp_));
  } else if (!is_writer_opened_ && OB_FAIL(writer_.open(file_buf_size_,
      expire_timestamp_, tenant_id_, dir_id_))) {
    STORAGE_LOG(WARN, "fail to open writer", K(ret), K_(tenant_id), K_(dir_id));
  } else {
    is_writer_opened_ = true;
    if (OB_FAIL(writer_.write_items(items))) {
      STORAGE_LOG(WARN, "fail to write item", K(ret));
    }
    LOG_INFO("MMMMM external sort round add items", KR(ret));
  }
  return ret;
}

template<typename T, typename Compare>
int ObDemoExternalSortRound<T, Compare>::add_item(const T &item)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoExternalSortRound has not been inited", K(ret));
  } else if (ObDemoExternalSortConstant::is_timeout(expire_timestamp_)) {
    ret = common::OB_TIMEOUT;
    STORAGE_LOG(WARN, "ObDemoExternalSortRound timeout", K(ret), K(expire_timestamp_));
  } else if (!is_writer_opened_ && OB_FAIL(writer_.open(file_buf_size_,
      expire_timestamp_, tenant_id_, dir_id_))) {
    STORAGE_LOG(WARN, "fail to open writer", K(ret), K_(tenant_id), K_(dir_id));
  } else {
    is_writer_opened_ = true;
    if (OB_FAIL(writer_.write_item(item))) {
      STORAGE_LOG(WARN, "fail to write items", K(ret));
    }
  }
  return ret;
}

template<typename T, typename Compare>
int ObDemoExternalSortRound<T, Compare>::build_fragment()
{
  int ret = common::OB_SUCCESS;
  void *buf = NULL;
  FragmentReader *reader = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoExternalSortRound has not been inited", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(FragmentReader)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory", K(ret));
  } else if (OB_ISNULL(reader = new (buf) FragmentReader())) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to placement new FragmentReader", K(ret));
  } else if (OB_FAIL(writer_.sync())) {
    STORAGE_LOG(WARN, "fail to sync macro file", K(ret));
  } else {
    STORAGE_LOG(INFO, "build fragment", K(writer_.get_fd()), K(writer_.get_sample_item()));
    if (OB_FAIL(reader->init(writer_.get_fd(), writer_.get_dir_id(), expire_timestamp_, tenant_id_,
        writer_.get_sample_item(), file_buf_size_))) {
      STORAGE_LOG(WARN, "fail to open reader", K(ret), K(file_buf_size_),
          K(expire_timestamp_));
    } else if (OB_FAIL(iters_.push_back(reader))) {
      STORAGE_LOG(WARN, "fail to push back reader", K(ret));
    } else {
      writer_.reset();
      is_writer_opened_ = false;
    }
  }
  return ret;
}

template<typename T, typename Compare>
int ObDemoExternalSortRound<T, Compare>::add_fragment_iter(ObDemoFragmentIterator<T> *iter)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoExternalSortRound has not been inited", K(ret));
  } else if (OB_FAIL(iters_.push_back(iter))) {
    STORAGE_LOG(WARN, "fail to add iterator", K(ret));
  }
  return ret;
}

template<typename T, typename Compare>
int ObDemoExternalSortRound<T, Compare>::transfer_final_sorted_fragment_iter(
    ObDemoExternalSortRound<T, Compare> &dest_round)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoExternalSortRound has not been inited", K(ret));
  } else if (1 != iters_.count()) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid reader count", K(ret), K(iters_.count()));
  } else {
    if (OB_FAIL(dest_round.add_fragment_iter(iters_.at(0)))) {
      STORAGE_LOG(WARN, "fail to add fragment iterator", K(ret));
    } else {
      // iter will be freed in dest_round
      iters_.reset();
    }
  }
  return ret;
}

template<typename T, typename Compare>
int ObDemoExternalSortRound<T, Compare>::build_merger()
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoExternalSortRound has not been inited", K(ret));
  } else if (OB_FAIL(merger_.init(iters_, compare_))) {
    STORAGE_LOG(WARN, "fail to init FragmentMerger", K(ret));
  }
  return ret;
}

template<typename T, typename Compare>
int ObDemoExternalSortRound<T, Compare>::finish_write()
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoExternalSortRound has not been inited", K(ret));
  } else if (OB_FAIL(writer_.sync())) {
    STORAGE_LOG(WARN, "fail to finish writer", K(ret));
  }
  return ret;
}

template<typename T, typename Compare>
int ObDemoExternalSortRound<T, Compare>::do_merge(
    ObDemoExternalSortRound &next_round, int thread_num)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoExternalSortRound has not been inited", K(ret));
  } else {
    int64_t reader_idx = 0;
    STORAGE_LOG(INFO, "external sort do merge start");
    while (OB_SUCC(ret) && reader_idx < iters_.count()) {
      /*if (OB_FAIL(do_one_run(reader_idx, next_round))) {
        STORAGE_LOG(WARN, "fail to do one run merge", K(ret));
      }*/
      if (OB_FAIL(do_one_run_with_threads(reader_idx, next_round, thread_num))) {
        STORAGE_LOG(WARN, "MMMMM fail to do one run merge", K(ret));
      } else {
        reader_idx += merge_count_ * thread_num;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(next_round.finish_write())) {
        STORAGE_LOG(WARN, "MMMMM fail to finsh next round", K(ret));
      }
    }
    STORAGE_LOG(INFO, "external sort do merge end");
  }
  return ret;
}

template<typename T, typename Compare>
class SortDemoThreadPool : public lib::Threads 
{
  public:
  typedef ObDemoFragmentMerge<T, Compare> FragmentMerger;
  typedef ObDemoFragmentIterator<T> FragmentIterator;
  typedef common::ObArray<FragmentIterator *> FragmentIteratorList;
  typedef ObDemoExternalSortRound<T, Compare> ExternalSortRound;
  SortDemoThreadPool(FragmentMerger *mergers, Compare *compare, FragmentIteratorList &iters, 
                     ExternalSortRound &next_round, int64_t start_idx, int64_t end_idx,
                     int64_t merge_cnt, const int64_t file_buf_size, const int64_t expire_timestamp,
                     const uint64_t tenant_id, int *rets)
                    : mergers_(mergers), compare_(compare), iters_(iters),
                      next_round_(next_round), start_idx_(start_idx), end_idx_(end_idx),
                      merge_cnt_(merge_cnt), file_buf_size_(file_buf_size),
                      expire_timestamp_(expire_timestamp), tenant_id_(tenant_id), rets_(rets) {}
  void run(int64_t idx) final
  {
    int ret = OB_SUCCESS;
    const T *item = NULL;
    int64_t start_idx = start_idx_ + idx * merge_cnt_;
    int64_t end_idx = min(end_idx_, start_idx + merge_cnt_);
    FragmentIteratorList iters;
    for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; ++i) {
      if (OB_FAIL(iters.push_back(iters_.at(i)))) {
        STORAGE_LOG(WARN, "fail to push back iterator list", K(ret));
      }
    }
    FragmentMerger &merger = mergers_[idx];
    if (OB_SUCC(ret)) {
      merger.reset();
      if (OB_FAIL(merger.init(iters, compare_))) {
        STORAGE_LOG(WARN, "fail to init ObFragmentMerger", K(ret));
      } else if (OB_FAIL(merger.open())) {
        STORAGE_LOG(WARN, "fail to open merger", K(ret));
      }
    }

    ExternalSortRound tmp_round;
    tmp_round.init(merge_cnt_, file_buf_size_, expire_timestamp_, tenant_id_, compare_);

    while (OB_SUCC(ret)) {
      // share::dag_yield();
      if (OB_FAIL(merger.get_next_item(item))) {
        if (common::OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "MMMMM fail to get next item", K(ret));
        } else {
          ret = common::OB_SUCCESS;
          break;
        }
      } else {
        if (OB_FAIL(tmp_round.add_item(*item))) {
          STORAGE_LOG(WARN, "MMMMM fail to add item", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(tmp_round.build_fragment())) {
        STORAGE_LOG(WARN, "MMMMM fail to build fragment", K(ret));
      }
    }

    {
      std::lock_guard<std::mutex> guard(mutex_);
      next_round_.combine_round(tmp_round);
    }

    tmp_round.clean_up();

    for (int64_t i = start_idx; i < end_idx; ++i) {
      if (nullptr != iters_[i]) {
        // will do clean up ignore return
        int tmp_ret;
        if (common::OB_SUCCESS != (tmp_ret = iters_[i]->clean_up())) {
          STORAGE_LOG(WARN, "fail to do reader clean up", K(tmp_ret), K(i));
        }
        iters_[i]->~ObDemoFragmentIterator();
        iters_[i] = nullptr;
      }
    }

    rets_[idx] = ret;
  }
  private: 
  FragmentMerger *mergers_;
  Compare *compare_;
  FragmentIteratorList &iters_;
  ExternalSortRound &next_round_;
  int64_t start_idx_;
  int64_t end_idx_;
  int64_t merge_cnt_;
  int64_t file_buf_size_;
  int64_t expire_timestamp_;
  uint64_t tenant_id_;
  std::mutex mutex_;
  int *rets_;
  // int threads_;
};

template<typename T, typename Compare>
int ObDemoExternalSortRound<T, Compare>::do_one_run_with_threads(
    const int64_t start_reader_idx, ObDemoExternalSortRound &next_round, int thread_num)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoExternalSortRound has not been inited", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    const int64_t end_reader_idx = std::min(start_reader_idx + merge_count_ * thread_num, iters_.count());
    int64_t diff = end_reader_idx - start_reader_idx;
    int actual_thread = diff / merge_count_ + (diff % merge_count_ > 0);
    int rets[actual_thread];
    SortDemoThreadPool<T, Compare> threads(mergers_, compare_, iters_, next_round, 
      start_reader_idx, end_reader_idx, merge_count_, file_buf_size_, expire_timestamp_, tenant_id_, rets);
    threads.set_thread_count(actual_thread);
    threads.set_run_wrapper(MTL_CTX());
    threads.start();
    threads.wait();
    
    for (int i = 0; i < actual_thread; i++) {
      ret = rets[i] == OB_SUCCESS ? ret : rets[i];
    }
  }
  return ret;
}

template<typename T, typename Compare>
int ObDemoExternalSortRound<T, Compare>::do_one_run(
    const int64_t start_reader_idx, ObDemoExternalSortRound &next_round)
{
  int ret = common::OB_SUCCESS;
  const T *item = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoExternalSortRound has not been inited", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    const int64_t end_reader_idx = std::min(start_reader_idx + merge_count_, iters_.count());
    FragmentIteratorList iters;
    for (int64_t i = start_reader_idx; OB_SUCC(ret) && i < end_reader_idx; ++i) {
      if (OB_FAIL(iters.push_back(iters_.at(i)))) {
        STORAGE_LOG(WARN, "fail to push back iterator list", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      merger_.reset();
      if (OB_FAIL(merger_.init(iters, compare_))) {
        STORAGE_LOG(WARN, "fail to init ObFragmentMerger", K(ret));
      } else if (OB_FAIL(merger_.open())) {
        STORAGE_LOG(WARN, "fail to open merger", K(ret));
      }
    }

    while (OB_SUCC(ret)) {
      share::dag_yield();
      if (OB_FAIL(merger_.get_next_item(item))) {
        if (common::OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "fail to get next item", K(ret));
        } else {
          ret = common::OB_SUCCESS;
          break;
        }
      } else {
        if (OB_FAIL(next_round.add_item(*item))) {
          STORAGE_LOG(WARN, "fail to add item", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(next_round.build_fragment())) {
        STORAGE_LOG(WARN, "fail to build fragment", K(ret));
      }
    }

    for (int64_t i = start_reader_idx; i < end_reader_idx; ++i) {
      if (nullptr != iters_[i]) {
        // will do clean up ignore return
        if (common::OB_SUCCESS != (tmp_ret = iters_[i]->clean_up())) {
          STORAGE_LOG(WARN, "fail to do reader clean up", K(tmp_ret), K(i));
        }
        iters_[i]->~ObDemoFragmentIterator();
        iters_[i] = nullptr;
      }
    }
  }
  return ret;
}

template<typename T, typename Compare>
int ObDemoExternalSortRound<T, Compare>::get_next_item(const T *&item)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoExternalSortRound has not been inited", K(ret));
  } else if (!merger_.is_opened() && OB_FAIL(merger_.open())) {
    STORAGE_LOG(WARN, "fail to open merger", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(merger_.get_next_item(item))) {
      if (common::OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to get next item", K(ret));
      }
    }
  }
  return ret;
}

template<typename T, typename Compare>
int64_t ObDemoExternalSortRound<T, Compare>::get_fragment_count()
{
  return iters_.count();
}

template<typename T, typename Compare>
int ObDemoExternalSortRound<T, Compare>::clean_up()
{
  int ret = common::OB_SUCCESS;
  int tmp_ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoExternalSortRound has not been inited", K(ret));
  }

  for (int64_t i = 0; i < iters_.count(); ++i) {
    if (NULL != iters_[i]) {
      if (common::OB_SUCCESS != (tmp_ret = iters_[i]->clean_up())) {
        STORAGE_LOG(WARN, "fail to do reader clean up", K(tmp_ret), K(i));
        ret = (common::OB_SUCCESS == ret) ? tmp_ret : ret;
      }
      iters_[i]->~ObDemoFragmentIterator();
    }
  }

  if (common::OB_SUCCESS != (tmp_ret = writer_.sync())) {
    STORAGE_LOG(WARN, "fail to do writer finish", K(tmp_ret));
    ret = (common::OB_SUCCESS == ret) ? tmp_ret : ret;
  }
  is_inited_ = false;
  merge_count_ = 0;
  file_buf_size_ = 0;
  iters_.reset();
  expire_timestamp_ = 0;
  compare_ = NULL;
  merger_.reset();
  allocator_.reset();
  return ret;
}

template <typename T>
class ObDemoMemoryFragmentIterator : public ObDemoFragmentIterator<T>
{
public:
  ObDemoMemoryFragmentIterator();
  virtual ~ObDemoMemoryFragmentIterator();
  int init(common::ObVector<T *> &item_list);
  virtual int get_next_item(const T *&item);
  virtual int clean_up() { return common::OB_SUCCESS; }
private:
  bool is_inited_;
  int64_t curr_item_index_;
  common::ObVector<T *> *item_list_;
};

template<typename T>
ObDemoMemoryFragmentIterator<T>::ObDemoMemoryFragmentIterator()
  : is_inited_(false), curr_item_index_(0), item_list_(NULL)
{
}

template<typename T>
ObDemoMemoryFragmentIterator<T>::~ObDemoMemoryFragmentIterator()
{
}

template<typename T>
int ObDemoMemoryFragmentIterator<T>::init(common::ObVector<T *> &item_list)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObDemoMemoryFragmentIterator has been inited twice", K(ret));
  } else {
    item_list_ = &item_list;
    is_inited_ = true;
  }
  return ret;
}

template<typename T>
int ObDemoMemoryFragmentIterator<T>::get_next_item(const T *&item)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoMemoryFragmentIterator has not been inited", K(ret));
  } else if (curr_item_index_ >= item_list_->size()) {
    ret = common::OB_ITER_END;
  } else {
    item = (*item_list_)[curr_item_index_];
    ++curr_item_index_;
  }
  return ret;
}

// template<typename T, typename Compare>
// class ObMemorySortRoundAddItems : public lib::Threads 
// {
//   ObMemorySortRoundAddItems(): next_round_(NULL) {}
//   void init(ExternalSortRound *next_round) {
//     next_round_ = next_round;
//   }

//   void push(ObVector<T *> item_list, common::ObArenaAllocator &allocator) {
//     std::lock_guard<std::mutex> guard(mutex_);
//     item_list_.push(std::move(item_list));
//     allocators.push(std::move(allocator));
//   }

//   void run(int64_t idx) {
//     while (!ATOMIC_LOAD(&has_set_stop()) || size() > 0) {
//       while (size() == 0) {
//         std::this_thread::sleep_for(std::chrono::milliseconds(100));
//       }
//       mutex_.lock();
//       ObVector<T *> &item_list = item_lists_.front();
//       common::ObArenaAllocator &allocator = allocators_.front();
//       mutex_.unlcok();
//       LOG_INFO("MMMMM build fragment", K(item_list_.size()));
//       for (int64_t i = 0; OB_SUCC(ret) && i < item_list_.size(); ++i) {
//         if (OB_FAIL(next_round_->add_item(*item_list_.at(i)))) {
//           STORAGE_LOG(WARN, "fail to add item", K(ret));
//         }
//       }
//       next_round_->build_fragment();
//       allocator.free();
//       mutex_.lock();
//       item_lists_.pop();
//       mutex_.unlock();
//     }
//   }

//   int size() {
//     std::lock_guard<std::mutex> guard(mutex_);
//     return item_lists_.size();
//   }
// private:
//   ExternalSortRound *next_round_;
//   std::queue<ObVector<T *>> item_lists_;
//   std::queue<common::ObArenaAllocator> allocators_;
//   std::mutex mutex_;
// }

template<typename T, typename Compare>
class ObDemoMemorySortRound
{
public:
  typedef ObDemoExternalSortRound<T, Compare> ExternalSortRound;
  ObDemoMemorySortRound();
  virtual ~ObDemoMemorySortRound();
  int init(const int64_t mem_limit, const int64_t expire_timestamp,
      Compare *compare, ExternalSortRound *next_round);
  int add_item(const T &item);
  int build_fragment();
  virtual int get_next_item(const T *&item);
  int finish();
  bool is_in_memory() const { return is_in_memory_; }
  bool has_data() const { return has_data_; }
  void reset();
  int transfer_final_sorted_fragment_iter(ExternalSortRound &dest_round);
  TO_STRING_KV(K(is_inited_), K(is_in_memory_), K(has_data_), K(buf_mem_limit_),
      K(expire_timestamp_), KP(next_round_), KP(compare_), KP(iter_));
private:
  int build_iterator();
private:
  bool is_inited_;
  bool is_in_memory_;
  bool has_data_;
  int64_t buf_mem_limit_;
  int64_t expire_timestamp_;
  ExternalSortRound *next_round_;
  common::ObArenaAllocator allocator_;
  common::ObVector<T *> item_list_;
  Compare *compare_;
  ObDemoMemoryFragmentIterator<T> *iter_;

  // ObMemorySortRoundAddItems add_item_thread_;
};

template<typename T, typename Compare>
ObDemoMemorySortRound<T, Compare>::ObDemoMemorySortRound()
  : is_inited_(false), is_in_memory_(false), has_data_(false), buf_mem_limit_(0), expire_timestamp_(0),
    next_round_(NULL), allocator_(common::ObNewModIds::OB_ASYNC_EXTERNAL_SORTER, common::OB_MALLOC_BIG_BLOCK_SIZE), item_list_(NULL, common::ObNewModIds::OB_ASYNC_EXTERNAL_SORTER),
    compare_(NULL), iter_(NULL)
{
}

template<typename T, typename Compare>
ObDemoMemorySortRound<T, Compare>::~ObDemoMemorySortRound()
{
  // add_item_thread_.stop();
  // add_item_thread_.wait();
}

template<typename T, typename Compare>
int ObDemoMemorySortRound<T, Compare>::init(
    const int64_t mem_limit, const int64_t expire_timestamp,
    Compare *compare, ExternalSortRound *next_round)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObDemoMemorySortRound has been inited", K(ret));
  } else if (mem_limit < ObDemoExternalSortConstant::MIN_MEMORY_LIMIT
      || NULL == compare
      || NULL == next_round
      || !next_round->is_inited()) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(mem_limit), KP(compare),
        KP(next_round), "next round inited", next_round->is_inited());
  } else {
    is_inited_ = true;
    is_in_memory_ = false;
    has_data_ = false;
    buf_mem_limit_ = mem_limit;
    expire_timestamp_ = expire_timestamp;
    compare_ = compare;
    next_round_ = next_round;
    // add_item_thread_.init(next_round_);
    // add_item_thread_.set_run_wrapper(MTL_CTX());
    // add_item_thread_.set_thread_count(1);
    // add_item_thread_.start();
  
    iter_ = NULL;
    
  }
  return ret;
}

template<typename T, typename Compare>
int ObDemoMemorySortRound<T, Compare>::add_item(const T &item)
{
  int ret = common::OB_SUCCESS;
  const int64_t item_size = sizeof(T) + item.get_deep_copy_size();
  char *buf = NULL;
  T *new_item = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoMemorySortRound has not been inited", K(ret));
  } else if (ObDemoExternalSortConstant::is_timeout(expire_timestamp_)) {
    ret = common::OB_TIMEOUT;
    STORAGE_LOG(WARN, "ObDemoMemorySortRound timeout", K(ret), K(expire_timestamp_));
  } else if (item_size > buf_mem_limit_) {
    ret = common::OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(WARN, "invalid item size, must not larger than buf memory limit",
        K(ret), K(item_size), K(buf_mem_limit_));
  } else if (allocator_.used() + item_size > buf_mem_limit_ && OB_FAIL(build_fragment())) {
    STORAGE_LOG(WARN, "fail to build fragment", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(item_size)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory", K(ret), K(item_size));
  } else if (OB_ISNULL(new_item = new (buf) T())) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to placement new item", K(ret));
  } else {
    int64_t buf_pos = sizeof(T);
    if (OB_FAIL(new_item->deep_copy(item, buf, item_size, buf_pos))) {
      STORAGE_LOG(WARN, "fail to deep copy item", K(ret));
    } else if (OB_FAIL(item_list_.push_back(new_item))) {
      STORAGE_LOG(WARN, "fail to push back new item", K(ret));
    }
  }
  return ret;
}

template<typename T, typename Compare>
int ObDemoMemorySortRound<T, Compare>::build_fragment()
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoMemorySortRound has not been inited", K(ret));
  } else if (item_list_.size() > 0) {
    int64_t start = common::ObTimeUtility::current_time();
    quicksort(item_list_.begin(), item_list_.end(), *compare_);
    // std::sort(item_list_.begin(), item_list_.end(), *compare_);
    if (OB_FAIL(compare_->result_code_)) {
      ret = compare_->result_code_;
    } else {
      const int64_t sort_fragment_time = common::ObTimeUtility::current_time() - start;
      // STORAGE_LOG(INFO, "MMMMM ObDemoMemorySortRound", K(sort_fragment_time));
    }

    start = common::ObTimeUtility::current_time();
    
    /*
    if (OB_FAIL(next_round_->add_items(item_list_))) {
      STORAGE_LOG(WARN, "fail to add item", K(ret));
    }
    LOG_INFO("MMMMM Memory sort build fragment", KR(ret));
    */
    

    // here, we can create a thread, move the item_list_ into thread, and append, since it's irrelevant
    LOG_INFO("MMMMM build fragment", K(item_list_.size()));
    for (int64_t i = 0; OB_SUCC(ret) && i < item_list_.size(); ++i) {
      if (OB_FAIL(next_round_->add_item(*item_list_.at(i)))) {
        STORAGE_LOG(WARN, "fail to add item", K(ret));
      }
    }
    

    if (OB_SUCC(ret)) {
      if (OB_FAIL(next_round_->build_fragment())) {
        STORAGE_LOG(WARN, "fail to build fragment", K(ret));
      } else {
        const int64_t write_fragment_time = common::ObTimeUtility::current_time() - start;
        STORAGE_LOG(INFO, "MMMMM ObDemoMemorySortRound", K(write_fragment_time));
        item_list_.reset();
        allocator_.reuse();
      }
    }
  }
  return ret;
}

template<typename T, typename Compare>
int ObDemoMemorySortRound<T, Compare>::finish()
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoMemorySortRound has not been inited", K(ret));
  } else if (0 == item_list_.size()) {
    has_data_ = false;
  } else if (0 == next_round_->get_fragment_count()) {
    is_in_memory_ = true;
    has_data_ = true;
    quicksort(item_list_.begin(), item_list_.end(), *compare_);
    // std::sort(item_list_.begin(), item_list_.end(), *compare_);
    if (OB_FAIL(compare_->result_code_)) {
      STORAGE_LOG(WARN, "fail to sort item list", K(ret));
    }
  } else {
    is_in_memory_ = false;
    has_data_ = true;
    if (OB_FAIL(build_fragment())) {
      STORAGE_LOG(WARN, "fail to build fragment", K(ret));
    } else if (OB_FAIL(next_round_->finish_write())) {
      STORAGE_LOG(WARN, "fail to do next round finish write", K(ret));
    } else {
      item_list_.reset();
      allocator_.reset();
    }
  }
  return ret;
}

template<typename T, typename Compare>
int ObDemoMemorySortRound<T, Compare>::build_iterator()
{
  int ret = common::OB_SUCCESS;
  void *buf = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoMemorySortRound has not been inited", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObDemoMemoryFragmentIterator<T>)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory for ObDemoMemoryFragmentIterator", K(ret));
  } else if (OB_ISNULL(iter_ = new (buf) ObDemoMemoryFragmentIterator<T>())) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to placement new ObDemoMemoryFragmentIterator", K(ret));
  } else if (OB_FAIL(iter_->init(item_list_))) {
    STORAGE_LOG(WARN, "fail to init iterator", K(ret));
  }
  return ret;
}

template<typename T, typename Compare>
int ObDemoMemorySortRound<T, Compare>::get_next_item(const T *&item)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoMemorySortRound has not been inited", K(ret));
  } else if (NULL == iter_) {
    if (OB_FAIL(build_iterator())) {
      STORAGE_LOG(WARN, "fail to build iterator", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (nullptr == iter_) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "error unexpected, iter must not be null", K(ret), KP(iter_));
    } else if (OB_FAIL(iter_->get_next_item(item))) {
      STORAGE_LOG(WARN, "fail to get next item", K(ret));
    }
  }
  return ret;
}

template<typename T, typename Compare>
void ObDemoMemorySortRound<T, Compare>::reset()
{
  is_inited_ = false;
  is_in_memory_ = false;
  buf_mem_limit_ = 0;
  expire_timestamp_ = 0;
  next_round_ = NULL;
  allocator_.reset();
  item_list_.reset();
  compare_ = NULL;
  iter_ = NULL;
}

template<typename T, typename Compare>
int ObDemoMemorySortRound<T, Compare>::transfer_final_sorted_fragment_iter(
    ExternalSortRound &dest_round)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoMemorySortRound has not been inited", K(ret));
  } else if (!is_in_memory()) {
    ret = common::OB_ERR_SYS;
    STORAGE_LOG(WARN, "ObDemoMemorySortRound has not data", K(ret));
  } else if (NULL == iter_ && OB_FAIL(build_iterator())) {
    STORAGE_LOG(WARN, "fail to build iterator", K(ret));
  } else if (OB_FAIL(dest_round.add_fragment_iter(iter_))) {
    STORAGE_LOG(WARN, "fail to add fragment iterator", K(ret));
  } else {
    iter_ = NULL;
  }
  return ret;
}

template<typename T, typename Compare>
class ObDemoExternalSort
{
public:
  typedef ObDemoMemorySortRound<T, Compare> MemorySortRound;
  typedef ObDemoExternalSortRound<T, Compare> ExternalSortRound;
  ObDemoExternalSort();
  virtual ~ObDemoExternalSort();
  int init(const int64_t mem_limit, const int64_t file_buf_size, const int64_t expire_timestamp,
      const uint64_t tenant_id, Compare *compare);
  int add_item(const T &item);
  int do_sort(const bool final_merge);
  int get_next_item(const T *&item);
  void clean_up();
  int add_fragment_iter(ObDemoFragmentIterator<T> *iter);
  int transfer_final_sorted_fragment_iter(ObDemoExternalSort<T, Compare> &merge_sorter);
  int get_current_round(ExternalSortRound *&round);
  TO_STRING_KV(K(is_inited_), K(file_buf_size_), K(buf_mem_limit_), K(expire_timestamp_),
      K(merge_count_per_round_), KP(tenant_id_), KP(compare_));
private:
  static const int64_t SORT_THREAD_NUM = 8;
  static const int64_t EXTERNAL_SORT_ROUND_CNT = 2;
  bool is_inited_;
  int64_t file_buf_size_;
  int64_t buf_mem_limit_;
  int64_t expire_timestamp_;
  int64_t merge_count_per_round_;
  Compare *compare_;
  MemorySortRound memory_sort_round_;
  ExternalSortRound sort_rounds_[EXTERNAL_SORT_ROUND_CNT];
  ExternalSortRound *curr_round_;
  ExternalSortRound *next_round_;
  bool is_empty_;
  uint64_t tenant_id_;
};

template<typename T, typename Compare>
ObDemoExternalSort<T, Compare>::ObDemoExternalSort()
  : is_inited_(false), file_buf_size_(0), buf_mem_limit_(0), expire_timestamp_(0), merge_count_per_round_(0),
    compare_(NULL), memory_sort_round_(), curr_round_(NULL), next_round_(NULL),
    is_empty_(true), tenant_id_(common::OB_INVALID_ID)
{
}

template<typename T, typename Compare>
ObDemoExternalSort<T, Compare>::~ObDemoExternalSort()
{
}

template<typename T, typename Compare>
int ObDemoExternalSort<T, Compare>::init(
    const int64_t mem_limit, const int64_t file_buf_size, const int64_t expire_timestamp,
    const uint64_t tenant_id, Compare *compare)
{
  int ret = common::OB_SUCCESS;
  int64_t macro_block_size = OB_SERVER_BLOCK_MGR.get_macro_block_size();
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObDemoExternalSort has already been inited", K(ret));
  } else if (mem_limit < ObDemoExternalSortConstant::MIN_MEMORY_LIMIT
      || file_buf_size % DIO_ALIGN_SIZE != 0
      || file_buf_size < macro_block_size
      || common::OB_INVALID_ID == tenant_id
      || NULL == compare) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(mem_limit),
        K(file_buf_size), KP(compare));
  } else {
    file_buf_size_ = common::lower_align(file_buf_size, macro_block_size);
    buf_mem_limit_ = mem_limit;
    expire_timestamp_ = expire_timestamp;
    merge_count_per_round_ = buf_mem_limit_ / file_buf_size_ / 2;
    compare_ = compare;
    tenant_id_ = tenant_id;
    curr_round_ = &sort_rounds_[0];
    next_round_ = &sort_rounds_[1];
    is_empty_ = true;
    if (merge_count_per_round_ < ObDemoExternalSortConstant::MIN_MULTIPLE_MERGE_COUNT) {
      ret = common::OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument, invalid memory limit", K(ret),
          K(buf_mem_limit_), K(file_buf_size_), K(merge_count_per_round_));
    } else if (OB_FAIL(curr_round_->init(merge_count_per_round_, file_buf_size_,
        expire_timestamp, tenant_id_, compare_))) {
      STORAGE_LOG(WARN, "fail to init current sort round", K(ret));
    } else if (OB_FAIL(memory_sort_round_.init(buf_mem_limit_,
        expire_timestamp, compare_, curr_round_))) {
      STORAGE_LOG(WARN, "fail to init memory sort round", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

template<typename T, typename Compare>
int ObDemoExternalSort<T, Compare>::add_item(const T &item)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoExternalSort has not been inited", K(ret));
  } else if (OB_FAIL(memory_sort_round_.add_item(item))) {
    STORAGE_LOG(WARN, "fail to add item in memory sort round", K(ret));
  }
  return ret;
}

template<typename T, typename Compare>
int ObDemoExternalSort<T, Compare>::do_sort(const bool final_merge)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoExternalSort has not been inited", K(ret));
  } else if (OB_FAIL(memory_sort_round_.finish())) {
    STORAGE_LOG(WARN, "fail to finish memory sort round", K(ret));
  } else if (memory_sort_round_.has_data() && memory_sort_round_.is_in_memory()) {
    STORAGE_LOG(INFO, "all data sorted in memory");
    is_empty_ = false;
  } else if (0 == curr_round_->get_fragment_count()) {
    is_empty_ = true;
    ret = common::OB_SUCCESS;
  } else {
    // final_merge = true is for performance optimization, the count of fragments is reduced to lower than merge_count_per_round,
    // then the last round of merge this fragment is skipped
    const int64_t final_round_limit = final_merge ? merge_count_per_round_ : 1;
    int64_t round_id = 1;
    is_empty_ = false;
    while (OB_SUCC(ret) && curr_round_->get_fragment_count() > final_round_limit) {
      const int64_t start_time = common::ObTimeUtility::current_time();
      STORAGE_LOG(INFO, "do sort start round", K(round_id));
      if (OB_FAIL(next_round_->init(merge_count_per_round_, file_buf_size_,
          expire_timestamp_, tenant_id_, compare_))) {
        STORAGE_LOG(WARN, "fail to init next sort round", K(ret));
      } else if (OB_FAIL(curr_round_->do_merge(*next_round_, SORT_THREAD_NUM))) {
        STORAGE_LOG(WARN, "fail to do merge fragments of current round", K(ret));
      } else if (OB_FAIL(curr_round_->clean_up())) {
        STORAGE_LOG(WARN, "fail to do clean up of current round", K(ret));
      } else {
        std::swap(curr_round_, next_round_);
        const int64_t round_cost_time = common::ObTimeUtility::current_time() - start_time;
        STORAGE_LOG(INFO, "do sort end round", K(round_id), K(round_cost_time));
        ++round_id;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(curr_round_->build_merger())) {
        STORAGE_LOG(WARN, "fail to build merger", K(ret));
      }
    }
  }
  return ret;
}

template<typename T, typename Compare>
int ObDemoExternalSort<T, Compare>::get_next_item(const T *&item)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoExternalSort has not been inited", K(ret));
  } else if (is_empty_) {
    ret = common::OB_ITER_END;
  } else if (memory_sort_round_.has_data() && memory_sort_round_.is_in_memory()) {
    if (OB_FAIL(memory_sort_round_.get_next_item(item))) {
      if (common::OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to get next item", K(ret));
      }
    }
  } else if (OB_FAIL(curr_round_->get_next_item(item))) {
    if (common::OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to get next item", K(ret));
    }
  }
  return ret;
}

template<typename T, typename Compare>
void ObDemoExternalSort<T, Compare>::clean_up()
{
  int tmp_ret = common::OB_SUCCESS;
  is_inited_ = false;
  file_buf_size_ = 0;
  buf_mem_limit_ = 0;
  expire_timestamp_ = 0;
  merge_count_per_round_ = 0;
  compare_ = NULL;
  memory_sort_round_.reset();
  curr_round_ = NULL;
  next_round_ = NULL;
  is_empty_ = true;
  STORAGE_LOG(INFO, "do external sort clean up");
  for (int64_t i = 0; i < EXTERNAL_SORT_ROUND_CNT; ++i) {
    // ignore ret
    if (sort_rounds_[i].is_inited() && common::OB_SUCCESS != (tmp_ret = sort_rounds_[i].clean_up())) {
      STORAGE_LOG(WARN, "fail to clean up sort rounds", K(tmp_ret), K(i));
    }
  }
}

template<typename T, typename Compare>
int ObDemoExternalSort<T, Compare>::add_fragment_iter(ObDemoFragmentIterator<T> *iter)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoExternalSort has not been inited", K(ret));
  } else if (OB_FAIL(curr_round_->add_fragment_iter(iter))) {
    STORAGE_LOG(WARN, "fail to add fragment iter");
  } else {
    is_empty_ = false;
  }
  return ret;
}

template<typename T, typename Compare>
int ObDemoExternalSort<T, Compare>::get_current_round(ExternalSortRound *&curr_round)
{
  int ret = common::OB_SUCCESS;
  curr_round = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoExternalSort has not been inited", K(ret));
  } else if (NULL == curr_round_) {
    ret = common::OB_ERR_SYS;
    STORAGE_LOG(WARN, "invalid current round", K(ret), KP(curr_round_));
  } else {
    curr_round = curr_round_;
  }
  return ret;
}

template<typename T, typename Compare>
int ObDemoExternalSort<T, Compare>::transfer_final_sorted_fragment_iter(
    ObDemoExternalSort<T, Compare> &merge_sorter)
{
  int ret = common::OB_SUCCESS;
  ExternalSortRound *curr_round = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDemoExternalSort has not been inited", K(ret));
  } else if (is_empty_) {
    ret = common::OB_SUCCESS;
  } else if (OB_FAIL(merge_sorter.get_current_round(curr_round))) {
    STORAGE_LOG(WARN, "fail to get current round", K(ret));
  } else if (NULL == curr_round) {
    ret = common::OB_ERR_SYS;
    STORAGE_LOG(WARN, "invalid inner state", K(ret), KP(curr_round));
  } else if (memory_sort_round_.is_in_memory()) {
    if (OB_FAIL(memory_sort_round_.transfer_final_sorted_fragment_iter(*curr_round))) {
      STORAGE_LOG(WARN, "fail to transfer final sorted fragment iterator", K(ret));
    } else {
      merge_sorter.is_empty_ = false;
    }
  } else if (OB_FAIL(curr_round_->transfer_final_sorted_fragment_iter(*curr_round))) {
    STORAGE_LOG(WARN, "fail to get transfer sorted fragment iterator", K(ret));
  } else {
    merge_sorter.is_empty_ = false;
  }
  return ret;
}

}  // end namespace storage
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_PARALLEL_EXTERNAL_SORT_H_
