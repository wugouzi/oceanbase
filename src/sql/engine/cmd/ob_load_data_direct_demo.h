#pragma once

#include "lib/file/ob_file.h"
#include "lib/timezone/ob_timezone_info.h"
#include "lib/thread/ob_async_task_queue.h"
#include "lib/thread/ob_work_queue.h"
#include "lib/thread/threads.h"
#include "sql/engine/cmd/ob_load_data_impl.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "storage/blocksstable/ob_index_block_builder.h"
#include "storage/ob_parallel_external_sort.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include <future>
#include <mutex>
#include <thread>

namespace oceanbase
{
  namespace sql
  {

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
      int read_next_buffer(ObLoadDataBuffer &buffer);
    private:
      common::ObFileReader file_reader_;
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
               common::ObCollationType collation_type, field_num);
      int get_next_row(ObLoadDataBuffer &buffer, const common::ObNewRow *&row);
      int fast_get_next_row(ObLoadDataBuffer &buffer);
      int parse_next_row(const common::ObNewRow *&row);
    private:
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
      const char *str_;
      const char *end_;
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
    private:
      int init_column_schemas_and_idxs(
          const share::schema::ObTableSchema *table_schema,
          const common::ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list);
      int cast_obj_to_datum(const share::schema::ObColumnSchemaV2 *column_schema,
                            const common::ObObj &obj, blocksstable::ObStorageDatum &datum);
    private:
      common::ObArray<const share::schema::ObColumnSchemaV2 *> column_schemas_;
      common::ObArray<int64_t> column_idxs_; // Mapping of store columns to source data columns
      int64_t column_count_;
      common::ObCollationType collation_type_;
      ObLoadDatumRow datum_row_;
      common::ObArenaAllocator cast_allocator_;
      common::ObTimeZoneInfo tz_info_;
      bool is_inited_;
    };

    class ObLoadExternalSort
    {
    public:
      ObLoadExternalSort();
      ~ObLoadExternalSort();
      int init(const share::schema::ObTableSchema *table_schema, int64_t mem_size,
               int64_t file_buf_size);
      int append_row(const ObLoadDatumRow &datum_row);
      int close();
      int get_next_row(const ObLoadDatumRow *&datum_row);
      int combine_sort(ObLoadExternalSort &sort);
    private:
      common::ObArenaAllocator allocator_;
      blocksstable::ObStorageDatumUtils datum_utils_;
      ObLoadDatumRowCompare compare_;
      storage::ObExternalSort<ObLoadDatumRow, ObLoadDatumRowCompare> external_sort_;
      bool is_closed_;
      bool is_inited_;
      std::mutex mutex_;
    };

    class ObLoadSSTableWriter
    {
    public:
      ObLoadSSTableWriter();
      ~ObLoadSSTableWriter();
      int init(const share::schema::ObTableSchema *table_schema);
      int append_row(const ObLoadDatumRow &datum_row);
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
      blocksstable::ObMacroBlockWriter macro_block_writer_;
      blocksstable::ObDatumRow datum_row_;
      bool is_closed_;
      bool is_inited_;
    };

    class ObParseDataThread : public lib::Threads 
    {
    public:
      ObParseDataThread(ObLoadDataBuffer &buffer, ObLoadCSVPaser *csv_parsers, 
                        ObLoadRowCaster *row_casters, ObLoadExternalSort *external_sorts,
                        ObLoadSequentialFileReader &file_reader, int *rets, int sort_num)
                        : buffer_(buffer), csv_parsers_(csv_parsers),
                          row_casters_(row_casters), external_sorts_(external_sorts),
                          file_reader_(file_reader), rets_(rets), sort_num_(sort_num) 
                        {}
      void run(int64_t idx) final;
    private:
      // ObLoadDataBuffer *buffers_;
      ObLoadDataBuffer &buffer_;
      ObLoadCSVPaser *csv_parsers_;
      ObLoadRowCaster *row_casters_;
      ObLoadExternalSort *external_sorts_;
      ObLoadSequentialFileReader &file_reader_;
      int *rets_;
      std::mutex mutex_;
      int sort_num_;
    };

    class ObLoadDataDirectDemo : public ObLoadDataBase
    {
      static const int DEMO_THREAD_LOG = 3;
      static const int DEMO_BUF_NUM = 1 << DEMO_THREAD_LOG;
      static const int SORT_NUM = 2;
      static const int64_t MEM_BUFFER_SIZE = (1LL << 30);  // 1G
      static const int64_t MEM_THREAD_BUFFER_SIZE = (1LL << 27);
      static const int64_t FILE_BUFFER_SIZE = (2LL << 20); // 2M
      static const int64_t BUF_SIZE = (2LL << 24); // 
    public:
      ObLoadDataDirectDemo();
      virtual ~ObLoadDataDirectDemo();
      int execute(ObExecContext &ctx, ObLoadDataStmt &load_stmt) override;
    private:
      int inner_init(ObLoadDataStmt &load_stmt);
      int do_load();
      int do_load_buffer();
      // int do_load_buffer(int i);
      // int do_parse_buffer(int i);
    private:
      
      ObLoadSequentialFileReader file_reader_;
      // we have BUF_NUM buffers and we load data simultaneously
      // ObLoadDataBuffer buffers_[DEMO_BUF_NUM];
      ObLoadDataBuffer buffer_;
      ObLoadCSVPaser csv_parsers_[DEMO_BUF_NUM];
      ObLoadRowCaster row_casters_[DEMO_BUF_NUM];
      // ObLoadDataBuffer buffer_;
      ObLoadExternalSort external_sorts_[SORT_NUM];
      ObLoadExternalSort external_final_sort_;
      ObLoadSSTableWriter sstable_writer_;
    };

  } // namespace sql
} // namespace oceanbase
