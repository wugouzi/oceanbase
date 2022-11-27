#ifndef OCEANBASE_STORAGE_OB_DEMO_SPLIT_H_
#define OCEANBASE_STORAGE_OB_DEMO_SPLIT_H_

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
namespace oceanbase
{
namespace storage
{
class SplitItem {
  char *data;
  int64_t len;
};
class ObSplitMacroBufferWriter
{
public:
  ObSplitMacroBufferWriter();
  virtual ~ObSplitMacroBufferWriter();
  int write_item(SplitItem &item);
  void assign(const int64_t buf_pos, const int64_t buf_cap, char *buf);
  bool has_item() { return buf_pos_ > 0; };
  TO_STRING_KV(KP(buf_), K(buf_pos_), K(buf_cap_));
private:
  char *buf_;
  int64_t buf_pos_;
  int64_t buf_cap_;
  DISALLOW_COPY_AND_ASSIGN(ObSplitMacroBufferWriter);
};

template<typename T>
ObSplitMacroBufferWriter<T>::ObSplitMacroBufferWriter()
  : buf_(NULL), buf_pos_(0), buf_cap_(0)
{
}

template<typename T>
ObSplitMacroBufferWriter<T>::~ObSplitMacroBufferWriter()
{
}

template<typename T>
int ObSplitMacroBufferWriter<T>::write_item(SplitItem &item)
{
  int ret = common::OB_SUCCESS;
  if (item.len + buf_pos_ > buf_cap_) {
    // LOG_INFO("MMMMM macro writer full");
    ret = common::OB_EAGAIN;
  } else {
    memcpy(buf_ + buf_pos_, item.data, item.len);
    buf_pos_ += item.len;
  }
  return ret;
}

template<typename T>
void ObSplitMacroBufferWriter<T>::assign(const int64_t pos, const int64_t buf_cap, char *buf)
{
  buf_pos_ = pos;
  buf_cap_ = buf_cap;
  buf_ = buf;
}

class ObSplitFragmentWriterV2
{
public:
  ObSplitFragmentWriterV2();
  virtual ~ObSplitFragmentWriterV2();
  int open(const int64_t buf_size, const int64_t expire_timestamp,
      const uint64_t tenant_id, const int64_t dir_id);
  int write_item(SplitItem &item);
  int sync();
  void reset();
  int64_t get_fd() const { return fd_; }
  int64_t get_dir_id() const { return dir_id_; }
private:
  int flush_buffer();
  int check_need_flush(bool &need_flush);
private:
  bool is_inited_;
  char *buf_;
  int64_t buf_size_;
  int64_t expire_timestamp_;
  common::ObArenaAllocator allocator_;
  ObSplitMacroBufferWriter<T> macro_buffer_writer_;
  blocksstable::ObTmpFileIOHandle file_io_handle_;
  int64_t fd_;
  int64_t dir_id_;
  uint64_t tenant_id_;
  DISALLOW_COPY_AND_ASSIGN(ObSplitFragmentWriterV2);

  // for debug
  int write_cnt_ = 0;
};


ObSplitFragmentWriterV2::ObSplitFragmentWriterV2()
: is_inited_(false), buf_(NULL), buf_size_(0), expire_timestamp_(0),
    allocator_(common::ObNewModIds::OB_ASYNC_EXTERNAL_SORTER, common::OB_MALLOC_BIG_BLOCK_SIZE),
    macro_buffer_writer_(), has_sample_item_(false), sample_item_(nullptr),
    file_io_handle_(), fd_(-1), dir_id_(-1), tenant_id_(common::OB_INVALID_ID)
{
}

ObSplitFragmentWriterV2::~ObSplitFragmentWriterV2()
{
    reset();
}

int ObSplitFragmentWriterV2::open(const int64_t buf_size, const int64_t expire_timestamp,
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

int ObSplitFragmentWriterV2::write_item(SplitItem &item)
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
  return ret;
}

int ObSplitFragmentWriterV2::check_need_flush(bool &need_flush)
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

int ObSplitFragmentWriterV2::flush_buffer()
{
  int ret = common::OB_SUCCESS;
  int64_t timeout_ms = 0;
  // LOG_INFO("MMMMM flush buffer", K(write_cnt_));
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

int ObSplitFragmentWriterV2::sync()
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

void ObSplitFragmentWriterV2::reset()
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

class ObSplitMacroBufferReader
{
public:
  ObSplitMacroBufferReader();
  virtual ~ObSplitMacroBufferReader();
  int read_item(char *&data, int64_t &len);
  void assign(const int64_t buf_pos, const int64_t buf_cap, const char *buf);
  TO_STRING_KV(KP(buf_), K(buf_pos_), K(buf_len_), K(buf_cap_));
private:
  const char *buf_;
  int64_t buf_pos_;
  int64_t buf_len_;
  int64_t buf_cap_;
};

ObDemoMacroBufferReader::ObDemoMacroBufferReader()
  : buf_(NULL), buf_pos_(0), buf_len_(0), buf_cap_(0)
{
}

ObDemoMacroBufferReader::~ObDemoMacroBufferReader()
{
}

int ObDemoMacroBufferReader::read_item(T &item)
{
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

class ObSplitFragmentReaderV2 : public ObDemoFragmentIterator<T>
{
public:
  ObSplitFragmentReaderV2();
  virtual ~ObSplitFragmentReaderV2();
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




}  // end namespace storage
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_PARALLEL_EXTERNAL_SORT_H_
