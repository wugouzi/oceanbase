#pragma once

#include "lib/file/ob_file.h"
#include "lib/timezone/ob_timezone_info.h"
#include "lib/thread/ob_async_task_queue.h"
#include "lib/thread/ob_work_queue.h"
#include "lib/thread/threads.h"
#include "lib/queue/ob_lighty_queue.h"
#include "lib/thread/ob_simple_thread_pool.h"
#include "demo_obj_cast.h"
#include "sql/engine/cmd/ob_load_data_impl.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "storage/blocksstable/ob_index_block_builder.h"
#include "observer/omt/ob_tenant.h"
// #include "storage/ob_parallel_external_sort.h"
#include "sql/engine/cmd/demo_sort.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "demo_macro_block_writer.h"
#include "stdio.h"
#include <future>
#include <mutex>
#include <thread>
#include <vector>
#include <fstream>

namespace oceanbase
{
  namespace sql
  {
    static const int64_t MEM_BUFFER_SIZE = (1LL << 30);  // 1G -> 2G -> 4G
    static const int64_t FILE_BUFFER_SIZE = (2LL << 20); // 2M
    static const int64_t BUF_SIZE = (2LL << 25); // 
    static const int64_t READ_BUF_SIZE = (2LL << 20); // 
    static const int64_t SPLIT_BUF_SIZE = (2LL << 20); // 
    static const int64_t THREAD_BUF_SIZE = (1L << 30) * 0.9; // (1G) 1.5G
    static const int SPLIT_NUM = 240;
    static const int WRITER_THREAD_NUM = 8;
    // static const int SPLIT_NUM = 4;
    // static const int WRITER_THREAD_NUM = 2;

    typedef struct 
    {
      unsigned int key1 = 0;
      char key2 = 0;
      // size_t offset = 0;
      size_t offset = 0;
    } Key;

    typedef struct 
    {
      int key1 = 0;
      int key2 = 0;
      ObNewRow *row = nullptr;
    } KeyRow;

  
    class ObLoadDataBuffer
    {
    public:
      ObLoadDataBuffer();
      ~ObLoadDataBuffer();
      void reuse();
      void reset();
      int create(int64_t capacity);
      int squash();
      OB_INLINE char *data() const { return data_; }
      OB_INLINE char *begin() const { return data_ + begin_pos_; }
      OB_INLINE char *end() const { return data_ + end_pos_; }
      OB_INLINE bool empty() const { return end_pos_ == begin_pos_; }
      OB_INLINE int64_t get_data_size() const { return end_pos_ - begin_pos_; }
      OB_INLINE int64_t get_remain_size() const { return capacity_ - end_pos_; }
      OB_INLINE void consume(int64_t size) { begin_pos_ += size; }
      OB_INLINE void produce(int64_t size) { end_pos_ += size; }

      void lock() { mutex_.lock(); }
      void unlock() { mutex_.unlock(); }
    private:
      common::ObArenaAllocator allocator_;
      char *data_;
      int64_t begin_pos_;
      int64_t end_pos_;
      int64_t capacity_;
      std::mutex mutex_;
    };

    class ObLoadSequentialFileReader
    {
    public:
      ObLoadSequentialFileReader();
      ~ObLoadSequentialFileReader();
      int open(const ObString &filepath);
      int read_next_buffer_from(char *buf, int64_t size, int64_t offset, int64_t &read_size);
      int read_next_buffer(ObLoadDataBuffer &buffer);
      void set_offset(int64_t offset) { offset_ = offset; }
      void close();
    private:
      common::ObFileReader file_reader_;
      int64_t offset_;
      bool is_read_end_;
      std::mutex mutex_;
    };

    class ObLoadSequentialFileAppender
    {
    public:
      ObLoadSequentialFileAppender();
      ~ObLoadSequentialFileAppender();
      int open(const ObString &filepath, int64_t capacity);
      int write(const char *data, int64_t len);
      void close();
    private:
      common::ObFileAppender file_writer_;
      ObLoadDataBuffer buffer_;
      int64_t offset_;
      bool is_read_end_;
      std::mutex mutex_;      
    };

    class ObLoadCSVPaser
    {
    public:
      ObLoadCSVPaser();
      ~ObLoadCSVPaser();
      void reset();
      int init(const ObDataInFileStruct &format, int64_t column_count,
               common::ObCollationType collation_type, int field_num);
      int get_next_row(ObLoadDataBuffer &buffer, const common::ObNewRow *&row);
      int fast_get_next_row(ObLoadDataBuffer &buffer, const common::ObNewRow *&row, int &group_id);
      int fast_get_next_row(ObLoadDataBuffer &buffer, const common::ObNewRow *&row);
      int fast_get_next_row_with_key(ObLoadDataBuffer &buffer, const common::ObNewRow *&row, KeyRow &key);
      int fast_get_next_row_with_key(char *&begin, char *end, const common::ObNewRow *&row, KeyRow &key);
      int fast_get_next_row_with_key_and_row(char *&begin, char *end, const common::ObNewRow &row, KeyRow &key);
      int fast_get_next_row(const char *begin, const char *end, const common::ObNewRow *&row);
      // int parse_next_row(const common::ObNewRow *&row);
    private:
      // int get_group_id(int64_t key1, int key2);
      struct UnusedRowHandler
      {
        int operator()(common::ObIArray<ObCSVGeneralParser::FieldValue> &fields_per_line)
        {
          UNUSED(fields_per_line);
          return OB_SUCCESS;
        }
      };
    private:
      common::ObArenaAllocator allocator_;
      common::ObCollationType collation_type_;
      ObCSVGeneralParser csv_parser_;
      common::ObNewRow row_;
      UnusedRowHandler unused_row_handler_;
      common::ObSEArray<ObCSVGeneralParser::LineErrRec, 1> err_records_;
      bool is_inited_;
      int field_num_;

    };

    class ObLoadDatumRow
    {
      OB_UNIS_VERSION(1);
    public:
      ObLoadDatumRow();
      ~ObLoadDatumRow();
      void reset();
      int init(int64_t capacity);
      int64_t get_deep_copy_size() const;
      int deep_copy(const ObLoadDatumRow &src, char *buf, int64_t len, int64_t &pos);
      OB_INLINE bool is_valid() const { return count_ > 0 && nullptr != datums_; }
      DECLARE_TO_STRING;
    public:
      common::ObArenaAllocator allocator_;
      int64_t capacity_;
      int64_t count_;
      blocksstable::ObStorageDatum *datums_;
    };

    class ObLoadDatumRowCompare
    {
    public:
      ObLoadDatumRowCompare();
      ~ObLoadDatumRowCompare();
      int init(int64_t rowkey_column_num, const blocksstable::ObStorageDatumUtils *datum_utils);
      bool operator()(const ObLoadDatumRow *lhs, const ObLoadDatumRow *rhs);
      int get_error_code() const { return result_code_; }
      void clean_up();
    public:
      int result_code_;
    private:
      int64_t rowkey_column_num_;
      const blocksstable::ObStorageDatumUtils *datum_utils_;
      blocksstable::ObDatumRowkey lhs_rowkey_;
      blocksstable::ObDatumRowkey rhs_rowkey_;
      bool is_inited_;
    };

    class ObLoadRowCaster
    {
    public:
      ObLoadRowCaster();
      ~ObLoadRowCaster();
      int init(const share::schema::ObTableSchema *table_schema,
               const common::ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list);
      int get_casted_row(const common::ObNewRow &new_row, const ObLoadDatumRow *&datum_row);
      int get_casted_datum_row(const ObNewRow &new_row, const blocksstable::ObDatumRow *&datum_row);
      int unfold_get_casted_datum_row(const ObNewRow &new_row, const blocksstable::ObDatumRow *&datum_row);
      void reuse() { 
        // 244
        // LOG_INFO("MMMMM reuse", K(ob_datum_row_num_));
        ob_datum_row_num_ = 0;
        cast_allocator_.reuse();
      }
    private:
      int init_column_schemas_and_idxs(
          const share::schema::ObTableSchema *table_schema,
          const common::ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list);
      int cast_obj_to_datum(const share::schema::ObColumnSchemaV2 *column_schema,
                            const common::ObObj &obj, blocksstable::ObStorageDatum &datum);
      OB_INLINE int cast_obj_to_type_datum(const ObObjType &expect_type,
                                            const ObObj &obj,
                                            blocksstable::ObStorageDatum &datum,
                                            int idx);
    private:
      static const int CACHE_DATUM_NUM = 300;
      common::ObArray<const share::schema::ObColumnSchemaV2 *> column_schemas_;
      common::ObArray<int64_t> column_idxs_; // Mapping of store columns to source data columns
      int64_t column_count_;
      common::ObCollationType collation_type_;
      ObLoadDatumRow datum_row_;
      blocksstable::ObDatumRow ob_datum_row_;
      blocksstable::ObDatumRow ob_datum_rows_[CACHE_DATUM_NUM];
      int ob_datum_row_num_ = 0;
      common::ObArenaAllocator cast_allocator_;
      common::ObTimeZoneInfo tz_info_;
      bool is_inited_;
      int64_t extra_rowkey_column_num_;
      int64_t rowkey_column_num_;
      ObDemoCastCtx cast_ctx_;
      ObObjType expect_types_[20];
      int column_indexes_[20];
      int32_t min_len_[20];
      int32_t max_len_[20];
      ObObj casted_obj_;
      int64_t time1;
      int64_t time2;
      int64_t time3;
      int64_t time4;
    };

    class ObLoadExternalSort
    {
    public:
      ObLoadExternalSort();
      ~ObLoadExternalSort();
      int init(const share::schema::ObTableSchema *table_schema, int64_t mem_size,
               int64_t file_buf_size);
      int append_row(const ObLoadDatumRow &datum_row);
      int trivial_append_row(const ObLoadDatumRow &datum_row);
      // void partition(int n);
      int close();
      int trivial_close();
      int get_next_row(const ObLoadDatumRow *&datum_row);
      int get_next_partition_row(int id, const ObLoadDatumRow *&datum_row);
      int final_merge(int64_t total, int split_num);
      ObLoadDatumRowCompare &compare() { return compare_; }
      int count() { return count_; }
      void clean_up();
    private:
      common::ObArenaAllocator allocator_;
      blocksstable::ObStorageDatumUtils datum_utils_;
      ObLoadDatumRowCompare compare_;
      storage::ObDemoExternalSort<ObLoadDatumRow, ObLoadDatumRowCompare> external_sort_;
      bool is_closed_;
      bool is_inited_;
      std::mutex mutex_;
      int count_ = 0;
    };

    class ObLoadSSTableWriter
    {
    public:
      ObLoadSSTableWriter();
      ~ObLoadSSTableWriter();
      // int init(const share::schema::ObTableSchema *table_schema);
      int init(const share::schema::ObTableSchema *table_schema);
      // int append_row(const ObLoadDatumRow &datum_row);
      int append_row(int idx, const ObLoadDatumRow &datum_row);
      int append_datum_row(int idx, const blocksstable::ObDatumRow &datum_row);
      int init_macro_block_writer(const ObTableSchema *table_schema, int idx);
      int close_macro_blocks();
      blocksstable::ObDataStoreDesc &desc() { return data_store_desc_; }
      int close_macro_block(int idx);
      // OB_INLINE bool has_wrote_block(int idx) { return macro_block_writers_[idx].has_wrote_block(); }
      // OB_INLINE int build_micro_block(int idx) { return macro_block_writers_[idx].build_micro_block(); }
      // OB_INLINE int flush_current_macro_block(int idx) { return macro_block_writers_[idx].flush_current_macro_block(); }
      // OB_INLINE int switch_macro_block(int idx) { return macro_block_writers_[idx].try_switch_macro_block(); }
      int close();
    private:
      int init_sstable_index_builder(const share::schema::ObTableSchema *table_schema);
      int init_macro_block_writer(const share::schema::ObTableSchema *table_schema);
      int create_sstable();
    private:
      common::ObTabletID tablet_id_;
      storage::ObTabletHandle tablet_handle_;
      share::ObLSID ls_id_;
      storage::ObLSHandle ls_handle_;
      int64_t rowkey_column_num_;
      int64_t extra_rowkey_column_num_;
      int64_t column_count_;
      storage::ObITable::TableKey table_key_;
      blocksstable::ObSSTableIndexBuilder sstable_index_builder_;
      blocksstable::ObDataStoreDesc data_store_desc_;
      blocksstable::ObDatumRow datum_row_;
      blocksstable::ObDatumRow datum_rows_[WRITER_THREAD_NUM];
      // blocksstable::ObMacroBlockWriter macro_block_writer_;
      blocksstable::ObMacroBlockWriter macro_block_writers_[WRITER_THREAD_NUM];
      bool is_closed_;
      bool is_inited_;
    };

    class ObParseDataThread : public lib::Threads 
    {
    public:
      ObParseDataThread(ObLoadDataBuffer &buffer, ObLoadCSVPaser *csv_parsers, 
                        ObLoadRowCaster *row_casters, ObLoadExternalSort *external_sorts,
                        ObLoadSequentialFileReader &file_reader, int *rets)
                        : buffer_(buffer), csv_parsers_(csv_parsers),
                          row_casters_(row_casters), external_sorts_(external_sorts),
                          file_reader_(file_reader), rets_(rets) 
                        {
                          memset(cnts_, 0, sizeof(cnts_));
                        }
      void run(int64_t idx) final;
      int64_t cnts() { 
        int64_t ans = 0;
        for (int i = 0; i < 16; i++) {
          ans += cnts_[i];
        }
        return ans;
      }
    private:
      // ObLoadDataBuffer *buffers_;
      ObLoadDataBuffer &buffer_;
      ObLoadCSVPaser *csv_parsers_;
      ObLoadRowCaster *row_casters_;
      ObLoadExternalSort *external_sorts_;
      ObLoadSequentialFileReader &file_reader_;
      int *rets_;
      int64_t cnts_[16];
      std::mutex mutex_;

    };

    class ObReadSortWriteThread : public lib::Threads
    {
    public:
      ObReadSortWriteThread(int split_num, int thread_num, 
        std::vector<std::string> &file_paths, ObLoadCSVPaser *csv_parsers,
        ObLoadRowCaster *row_casters, ObLoadDataBuffer *buffers,
        ObLoadSSTableWriter &sstable_writer, const ObTableSchema *table_schema,
        ObLoadExternalSort *external_sorts, char **bufs,
        int64_t thread_buf_size,
        int *rets,
        std::vector<std::vector<int>> &file_pos,
        blocksstable::ObDataStoreDesc &data_store_desc
        )
      : split_num_(split_num), thread_num_(thread_num),
        file_paths_(file_paths), csv_parsers_(csv_parsers),
        row_casters_(row_casters), buffers_(buffers),
        sstable_writer_(sstable_writer), table_schema_(table_schema),
        external_sorts_(external_sorts), bufs_(bufs),
        thread_buf_size_(thread_buf_size),
        rets_(rets),
        file_pos_(file_pos),
        data_store_desc_(data_store_desc)
      {
        ObCompressorPool::get_instance().get_compressor("zstd_1.3.8", compressor_);
        additional_size_ = column_count_ * sizeof(ObObj);
      }
      void run(int64_t idx) final;
      // void run1() override;
      int handle_file(int64_t idx, char *file_data, int64_t len);
      int handle_file_decrypt(int64_t idx, int group_id, char *file_data, int64_t len, blocksstable::ObMacroBlockWriter &macro_block_writer);
    private:
      common::ObCompressor *compressor_;  
      int split_num_;
      int thread_num_;
      std::vector<std::string> &file_paths_;
      ObLoadCSVPaser *csv_parsers_;
      ObLoadRowCaster *row_casters_;
      ObLoadDataBuffer *buffers_;
      ObLoadSSTableWriter &sstable_writer_;
      const ObTableSchema *table_schema_;
      ObLoadExternalSort *external_sorts_;
      char **bufs_;
      int64_t thread_buf_size_;
      int *rets_;
      int column_count_ = 16;
      int64_t additional_size_;
      std::vector<std::vector<int>> &file_pos_;
      blocksstable::ObDataStoreDesc &data_store_desc_;
      // int idx_ = 0;
      // std::mutex mutex_;
    };

    // so the reader thread have SPLIT_NUM number of buffers from each file,
    // and a buffer pool. When a buffer is full, send it the the write thread,
    // fetch a buffer from buffer pool, and continue

    // This thread accept a buffer from the reader, compress the data,
    // write it to the buffer, and save its location
    typedef struct _CompWriteTask{
      ObLoadDataBuffer* buffer;
      int group_id;
      _CompWriteTask(ObLoadDataBuffer* b, int g) : buffer(b), group_id(g) {}
    } CompWriteTask;
    class ObCompressWritePool : public common::ObSimpleThreadPool
    {
    public:
      static const int64_t COMP_BUF_SIZE = 3 * READ_BUF_SIZE;
      ObCompressWritePool(common::ObLightyQueue &buffer_queue,
        common::ObFileAppender *file_writers,
        std::vector<std::vector<int>> &file_pos
        )
      : buffer_queue_(buffer_queue), file_writers_(file_writers),
        file_pos_(file_pos)
      {
        ObCompressorPool::get_instance().get_compressor("zstd_1.3.8", compressor_);
        for (int i = 0; i < SPLIT_NUM; i++) {
          if ((comp_bufs_[i] = (char*)malloc(COMP_BUF_SIZE)) == nullptr) {
            LOG_INFO("MMMMM malloc fails");
          }
          
        }
      }
      ~ObCompressWritePool()
      {
        for (int i = 0; i < SPLIT_NUM; i++) {
          free(comp_bufs_[i]);
        }
      }
    private:
      void handle(void *task);
      common::ObCompressor *compressor_;
      common::ObLightyQueue &buffer_queue_;
      common::ObFileAppender *file_writers_;
      std::mutex mtxs_[SPLIT_NUM];
      // trivial solution, non trivial needs lighty_queue
      char *comp_bufs_[SPLIT_NUM];
      std::vector<std::vector<int>> &file_pos_;

    };

    class ObSplitFileThread : public lib::Threads 
    {
    public:
      ObSplitFileThread(ObLoadSequentialFileAppender *file_writers,
        ObLoadSequentialFileReader *file_readers,
        ObLoadDataBuffer *buffers, int64_t *end,
        int split_num, int *rets)
      : 
        file_writers_(file_writers), file_readers_(file_readers),
        buffers_(buffers), end_(end),
        split_num_(split_num), rets_(rets)
      {}
      void run(int64_t idx) final;
    private:
      ObLoadSequentialFileAppender *file_writers_;
      ObLoadSequentialFileReader *file_readers_;
      ObLoadDataBuffer *buffers_;
      int64_t *end_;
      int split_num_;
      int *rets_;
      
    };

    class ObLoadDataDirectDemo : public ObLoadDataBase
    {
      // TODO: fine tuning

    public:
      ObLoadDataDirectDemo();
      virtual ~ObLoadDataDirectDemo();
      int execute(ObExecContext &ctx, ObLoadDataStmt &load_stmt) override;
    private:
      int inner_init(ObLoadDataStmt &load_stmt);
      int do_load();
      // int do_load_buffer(ObLoadSequentialFileReader &file_reader);
      int pre_process();
      int pre_processV2();
      int pre_processV3();
      // int do_load_buffer(int i);
      // int do_parse_buffer(int i);
    private:
      static const int SPLIT_THREAD_NUM = 2;
      // static const int SPLIT_NUM = 4;
      static const int PARSE_THREAD_NUM = 4;
      static const int IN_MEMORY_FILE_NUM = 1;
      static const int64_t IN_MEMORY_FILE_SIZE = (1LL << 30) * 0.2; //200M
      std::vector<std::vector<int>> file_pos_;
      
      ObLoadSequentialFileReader file_reader_;
      ObLoadSequentialFileReader file_split_readers_[SPLIT_THREAD_NUM];
      ObLoadDataBuffer split_buffers_[SPLIT_THREAD_NUM];
      int64_t split_pos_[SPLIT_THREAD_NUM];
      // ObLoadSequentialFileReader file_readers_[SPLIT_NUM];
      // we have BUF_NUM buffers and we load data simultaneously
      // ObLoadDataBuffer buffers_[DEMO_BUF_NUM];
      ObLoadDataBuffer buffer_;
      ObLoadDataBuffer buffers_[WRITER_THREAD_NUM];
      ObLoadDataBuffer pre_buffer_;
      ObLoadCSVPaser csv_parsers_[WRITER_THREAD_NUM];
      ObLoadRowCaster row_casters_[WRITER_THREAD_NUM];
      // ObLoadDataBuffer buffer_;
      ObLoadExternalSort external_sort_;
      ObLoadExternalSort external_sorts_[WRITER_THREAD_NUM];
      ObLoadSSTableWriter sstable_writer_;
      const ObTableSchema *table_schema_;
      common::ObString filepath_;
      ObLoadSequentialFileAppender file_writers_[SPLIT_NUM];
      common::ObFileAppender single_file_writers_[SPLIT_NUM];
      std::vector<std::string> filepaths_;
      char *in_memory_files_[WRITER_THREAD_NUM * IN_MEMORY_FILE_NUM];
      int in_memory_files_len_[WRITER_THREAD_NUM * IN_MEMORY_FILE_NUM];
      
    };

  } // namespace sql
} // namespace oceanbase
