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

#ifndef OCEANBASE_COMMON_DEMO_OBJ_CAST_
#define OCEANBASE_COMMON_DEMO_OBJ_CAST_

#include "common/object/ob_object.h"
#include "common/ob_accuracy.h"
#include "common/ob_zerofill_info.h"
#include "lib/timezone/ob_timezone_info.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/charset/ob_charset.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace common
{

#define DOUBLE_TRUE_VALUE_THRESHOLD (1e-50)

#define OB_IS_DOUBLE_ZERO(d)  (d < DOUBLE_TRUE_VALUE_THRESHOLD && d > -DOUBLE_TRUE_VALUE_THRESHOLD)

#define OB_IS_DOUBLE_NOT_ZERO(d)  (d >= DOUBLE_TRUE_VALUE_THRESHOLD || d <= -DOUBLE_TRUE_VALUE_THRESHOLD)

#define CM_NONE                          (0ULL)
#define CM_WARN_ON_FAIL                  (1ULL << 0)
#define CM_NULL_ON_WARN                  (1ULL << 1)
#define CM_NO_RANGE_CHECK                (1ULL << 2)
#define CM_NO_CAST_INT_UINT              (1ULL << 3)
#define CM_ZERO_FILL                     (1ULL << 4)
#define CM_FORMAT_NUMBER_WITH_LIMIT      (1ULL << 5)
#define CM_CHARSET_CONVERT_IGNORE_ERR    (1ULL << 6)
#define CM_FORCE_USE_STANDARD_NLS_FORMAT (1ULL << 7)
#define CM_STRICT_MODE                   (1ULL << 8)
#define CM_SET_MIN_IF_OVERFLOW           (1ULL << 9)
#define CM_ERROR_ON_SCALE_OVER           (1ULL << 10)

#define CM_ERROR_FOR_DIVISION_BY_ZERO    (1ULL << 53)
#define CM_NO_ZERO_IN_DATE               (1ULL << 54) // reserve
#define CM_NO_ZERO_DATE                  (1ULL << 55)
#define CM_ALLOW_INVALID_DATES           (1ULL << 56)
// string->integer(int/uint)时默认进行round(round to nearest)，
// 如果设置该标记，则会进行trunc(round to zero)
// ceil(round to +inf)以及floor(round to -inf)暂时没有支持
#define CM_STRING_INTEGER_TRUNC          (1ULL << 57)
#define CM_COLUMN_CONVERT                (1ULL << 58)
#define CM_ENABLE_BLOB_CAST              (1ULL << 59)
#define CM_EXPLICIT_CAST                 (1ULL << 60)
#define CM_ORACLE_MODE                   (1ULL << 61)
#define CM_INSERT_UPDATE_SCOPE           (1ULL << 62)
#define CM_INTERNAL_CALL                 (1ULL << 63)

typedef uint64_t ObDemoCastMode;

#define CM_IS_WARN_ON_FAIL(mode)              ((CM_WARN_ON_FAIL & (mode)) != 0)
#define CM_IS_ERROR_ON_FAIL(mode)             (!CM_IS_WARN_ON_FAIL(mode))
#define CM_SET_WARN_ON_FAIL(mode)             (CM_WARN_ON_FAIL | (mode))
#define CM_IS_NULL_ON_WARN(mode)              ((CM_NULL_ON_WARN & (mode)) != 0)
#define CM_IS_ZERO_ON_WARN(mode)              (!CM_IS_NULL_ON_WARN(mode))
#define CM_SKIP_RANGE_CHECK(mode)             ((CM_NO_RANGE_CHECK & (mode)) != 0)
#define CM_NEED_RANGE_CHECK(mode)             (!CM_SKIP_RANGE_CHECK(mode))
#define CM_SKIP_CAST_INT_UINT(mode)           ((CM_NO_CAST_INT_UINT & (mode)) != 0)
#define CM_NEED_CAST_INT_UINT(mode)           (!CM_SKIP_CAST_INT_UINT(mode))
#define CM_UNSET_NO_CAST_INT_UINT(mode)       ((~CM_NO_CAST_INT_UINT & (mode)) != 0)
#define CM_IS_COLUMN_CONVERT(mode)            ((CM_COLUMN_CONVERT & (mode)) != 0)
#define CM_IS_BLOB_CAST_ENABLED(mode)         ((CM_ENABLE_BLOB_CAST & (mode)) != 0)
#define CM_IS_EXPLICIT_CAST(mode)             ((CM_EXPLICIT_CAST & (mode)) != 0)
#define CM_IS_IMPLICIT_CAST(mode)             (!CM_IS_EXPLICIT_CAST(mode))
#define CM_IS_ORACLE_MODE(mode)               ((CM_ORACLE_MODE & (mode)) != 0)
#define CM_SET_ORACLE_MODE(mode)              (CM_ORACLE_MODE | (mode))
#define CM_IS_INTERNAL_CALL(mode)             ((CM_INTERNAL_CALL & (mode)) != 0)
#define CM_IS_EXTERNAL_CALL(mode)             (!CM_IS_INTERNAL_CALL(mode))
#define CM_IS_STRICT_MODE(mode)               ((CM_STRICT_MODE & (mode)) != 0)
#define CM_IS_ERROR_FOR_DIVISION_BY_ZERO(mode)    \
  ((CM_ERROR_FOR_DIVISION_BY_ZERO & (mode)) != 0)
#define CM_IS_NO_ZERO_IN_DATE(mode)           ((CM_NO_ZERO_IN_DATE & (mode)) != 0)
#define CM_IS_NO_ZERO_DATE(mode)              ((CM_NO_ZERO_DATE & (mode)) != 0)
#define CM_IS_ALLOW_INVALID_DATES(mode)       ((CM_ALLOW_INVALID_DATES & (mode)) != 0)
#define CM_IS_STRING_INTEGER_TRUNC(mode)      ((CM_STRING_INTEGER_TRUNC & (mode)) != 0)
#define CM_UNSET_STRING_INTEGER_TRUNC(mode)   ((~CM_STRING_INTEGER_TRUNC & (mode)))
#define CM_IS_IGNORE_ON_TRUNC(mode)           (!CM_IS_FAIL_ON_ROUNDING(mode))
#define CM_IS_ZERO_FILL(mode)                 ((CM_ZERO_FILL & (mode)) != 0)
#define CM_IS_FORMAT_NUMBER_WITH_LIMIT(mode)      \
  ((CM_FORMAT_NUMBER_WITH_LIMIT & (mode)) != 0)
#define CM_IS_IGNORE_CHARSET_CONVERT_ERR(mode)    \
  ((CM_CHARSET_CONVERT_IGNORE_ERR & (mode)) != 0)
#define CM_IS_FORCE_USE_STANDARD_NLS_FORMAT(mode) \
  ((CM_FORCE_USE_STANDARD_NLS_FORMAT & (mode)) != 0)
#define CM_IS_SET_MIN_IF_OVERFLOW(mode)       ((CM_SET_MIN_IF_OVERFLOW & (mode)) != 0)
#define CM_IS_ERROR_ON_SCALE_OVER(mode)       ((CM_ERROR_ON_SCALE_OVER & (mode)) != 0)
#define CM_IS_JSON_VALUE(mode)                CM_IS_ERROR_ON_SCALE_OVER(mode)

struct ObDemoObjCastParams
{
  // add params when necessary
  DEFINE_ALLOCATOR_WRAPPER
  ObDemoObjCastParams()
    : allocator_(NULL),
      allocator_v2_(NULL),
      cur_time_(0),
      cast_mode_(CM_NONE),
      warning_(OB_SUCCESS),
      zf_info_(NULL),
      dest_collation_(CS_TYPE_INVALID),
      expect_obj_collation_(CS_TYPE_INVALID),
      res_accuracy_(NULL),
      dtc_params_(),
      format_number_with_limit_(true),
      is_ignore_(false)
  {
    set_compatible_cast_mode();
  }

  ObDemoObjCastParams(ObIAllocator *allocator_v2, const ObDataTypeCastParams *dtc_params,
                  ObDemoCastMode cast_mode, ObCollationType dest_collation,
                  ObAccuracy *res_accuracy = NULL)
    : allocator_(NULL),
      allocator_v2_(allocator_v2),
      cur_time_(0),
      cast_mode_(cast_mode),
      warning_(OB_SUCCESS),
      zf_info_(NULL),
      dest_collation_(dest_collation),
      expect_obj_collation_(dest_collation),
      res_accuracy_(res_accuracy),
      dtc_params_(),
      format_number_with_limit_(true),
      is_ignore_(false)
  {
    set_compatible_cast_mode();
    if (NULL != dtc_params) {
    	dtc_params_ = *dtc_params;
    }
  }

  ObDemoObjCastParams(ObIAllocator *allocator_v2, const ObDataTypeCastParams *dtc_params,
                  int64_t cur_time, ObDemoCastMode cast_mode,
                  ObCollationType dest_collation,
                  const ObZerofillInfo *zf_info = NULL,
                  ObAccuracy *res_accuracy = NULL)
    : allocator_(NULL),
      allocator_v2_(allocator_v2),
      cur_time_(cur_time),
      cast_mode_(cast_mode),
      warning_(OB_SUCCESS),
      zf_info_(zf_info),
      dest_collation_(dest_collation),
      expect_obj_collation_(dest_collation),
      res_accuracy_(res_accuracy),
      dtc_params_(),
      format_number_with_limit_(true),
      is_ignore_(false)
  {
    set_compatible_cast_mode();
    if (NULL != dtc_params) {
    	dtc_params_ = *dtc_params;
    }
  }

  void *alloc(const int64_t size) const
  {
    void *ret = NULL;
    if (NULL != allocator_v2_) {
      ret = allocator_v2_->alloc(size);
    } else if (NULL != allocator_) {
      ret = allocator_->alloc(size);
    }
    return ret;
  }
  void *alloc(const int64_t size, const lib::ObMemAttr &attr) const
  {
    UNUSED(attr);
    return alloc(size);
  }

  void set_compatible_cast_mode()
  {
    if (lib::is_oracle_mode()) {
      cast_mode_ &= ~CM_WARN_ON_FAIL;
      cast_mode_ |= CM_ORACLE_MODE;
    } else {
      cast_mode_ &= ~CM_ORACLE_MODE;
    }
    return;
  }

  TO_STRING_KV(K(cur_time_),
               KP(cast_mode_),
               K(warning_),
               K(dest_collation_),
               K(expect_obj_collation_),
               K(res_accuracy_),
               K(format_number_with_limit_),
               K(is_ignore_));

  IAllocator *allocator_;
  ObIAllocator *allocator_v2_;
  int64_t cur_time_;
  ObDemoCastMode cast_mode_;
  int warning_;
  const ObZerofillInfo *zf_info_;
  ObCollationType dest_collation_;//seems like a global collection for one statement, not for each column
  ObCollationType expect_obj_collation_;//for each column obj
  ObAccuracy *res_accuracy_;
  ObDataTypeCastParams dtc_params_;
  bool format_number_with_limit_;
  bool is_ignore_;
};

class ObDemoExpectType
{
public:
  ObDemoExpectType()
    : type_(ObMaxType),
      cs_type_(CS_TYPE_INVALID),
      type_infos_(NULL)
      {}
  explicit ObDemoExpectType(const ObObjType type)
    : type_(type),
      cs_type_(CS_TYPE_INVALID),
      type_infos_(NULL)
      {}
  explicit ObDemoExpectType(const ObObjType type, const ObCollationType cs_type)
    : type_(type),
      cs_type_(cs_type),
      type_infos_(NULL)
      {}
  ~ObDemoExpectType()  { reset(); }
  void reset()
  {
    type_ = ObMaxType;
    cs_type_ = CS_TYPE_INVALID;
    type_infos_ = NULL;
  }
  OB_INLINE void set_type(ObObjType type) { type_ = type; }
  OB_INLINE void set_collation_type(ObCollationType cs_type) { cs_type_ = cs_type; }
  OB_INLINE void set_type_infos(const ObIArray<ObString> *type_infos) { type_infos_ = type_infos; }
  OB_INLINE ObObjType get_type() const { return type_; }
  OB_INLINE ObObjTypeClass get_type_class() const { return ob_obj_type_class(type_); }
  OB_INLINE ObCollationType get_collation_type() const { return cs_type_; }
  OB_INLINE const ObIArray<ObString> *get_type_infos() const { return type_infos_; }

  TO_STRING_KV(K(type_), K(cs_type_), KPC(type_infos_));
private:
  DISALLOW_COPY_AND_ASSIGN(ObDemoExpectType);
private:
  ObObjType type_;
  ObCollationType cs_type_;
  const ObIArray<ObString> *type_infos_;
};


typedef int (*ObDemoObjCastFunc)(ObObjType expect_type, ObDemoObjCastParams &params,
                             const ObObj &in_obj, ObObj &out_obj, const ObDemoCastMode cast_mode);

typedef int (*ObDemoCastEnumOrSetFunc)(const ObDemoExpectType &expect_type, ObDemoObjCastParams &params, const ObObj &in_obj, ObObj &out_obj);


// whether the cast is supported
bool demo_cast_supported(const ObObjType orig_type, const ObCollationType orig_cs_type,
                    const ObObjType expect_type, const ObCollationType expect_cs_type);
int ob_demo_obj_to_ob_time_with_date(const ObObj& obj, const ObTimeZoneInfo* tz_info, ObTime& ob_time,
                                const int64_t cur_ts_value, bool is_dayofmonth = false,
                                const ObDateSqlMode date_sql_mode = 0);
int ob_demo_obj_to_ob_time_without_date(const ObObj &obj, const ObTimeZoneInfo *tz_info, ObTime &ob_time);


// CM_STRING_INTEGER_TRUNC only affect string to [unsigned] integer cast.
// ignore CM_STRING_INTEGER_TRUNC if not string to integer cast (is_str_integer_cast is false)
// e.g:
//  cast number to int will invoke this functon, but it's not string to integer cast.
int demo_common_string_unsigned_integer(const ObDemoCastMode &cast_mode,
                                 const ObObjType &in_type,
                                 const ObCollationType &in_cs_type,
                                 const ObString &in_str,
                                 const bool is_str_integer_cast,
                                 uint64_t &out_val);
int demo_common_string_integer(const ObDemoCastMode &cast_mode,
                                 const ObObjType &in_type,
                                 const ObCollationType &in_cs_type,
                                 const ObString &in_str,
                                 const bool is_str_integer_cast,
                                 int64_t &out_val);

typedef ObDemoObjCastParams ObDemoCastCtx;

class ObDemoHexUtils
{
public:
  //text can be odd number, like 'aaa', treat as '0aaa'
  static int unhex(const common::ObString &text, common::ObDemoCastCtx &cast_ctx, common::ObObj &result);
  static int hex(const common::ObString &text, common::ObDemoCastCtx &cast_ctx, common::ObObj &result);
  static int hex_for_mysql(const uint64_t uint_val, common::ObDemoCastCtx &cast_ctx, common::ObObj &result);
  static int rawtohex(const common::ObObj &text, common::ObDemoCastCtx &cast_ctx, common::ObObj &result);
  static int hextoraw(const common::ObObj &text, common::ObDemoCastCtx &cast_ctx, common::ObObj &result);
  static int get_uint(const common::ObObj &obj, common::ObDemoCastCtx &cast_ctx, common::number::ObNumber &out);
  static int copy_raw(const common::ObObj &obj, common::ObDemoCastCtx &cast_ctx, common::ObObj &result);
private:
  static int uint_to_raw(const common::number::ObNumber &text, common::ObDemoCastCtx &cast_ctx, common::ObObj &result);
};

//==============================


int demo_obj_collation_check(const bool is_strict_mode, const ObCollationType cs_type, ObObj &obj);
int demo_obj_accuracy_check(ObDemoCastCtx &cast_ctx, const ObAccuracy &accuracy, const ObCollationType cs_type,
                       const ObObj &obj, ObObj &buf_obj, const ObObj *&res_obj);
int demo_get_bit_len(const ObString &str, int32_t &bit_len);
int demo_get_bit_len(uint64_t value, int32_t &bit_len);
int demo_ob_obj_accuracy_check_only(const ObAccuracy &accuracy, const ObCollationType cs_type, const ObObj &obj);
class ObDemoObjCaster
{
public:
  /*
   * Please note that,
   *
   * if &in_obj == &out_obj(buf_obj) holds, which means in_obj and out_obj(buf_obj) refer to the identical obj,
   *
   * to_type will work as expected. It will do cast in place.
   *
   *
   */
  //{{
  //不支持向enum/set转换的版本
  static int to_type(const ObObjType expect_type, ObDemoCastCtx &cast_ctx,
                     const ObObj &in_obj, ObObj &buf_obj, const ObObj *&out_obj);
  static int simp_to_type(const ObObjType &expect_type,
                          ObDemoCastCtx &cast_ctx,
                          const ObObj &in_obj,
                          ObObj &out_obj);
  static int to_datetime(const ObObjType expect_type, ObDemoCastCtx &cast_ctx,
                         const ObObj &in_obj, ObObj &buf_obj, const ObObj *&res_obj);
  static int bool_to_json(const ObObjType expect_type, ObDemoCastCtx &cast_ctx,
                          const ObObj &in_obj, ObObj &buf_obj, const ObObj *&res_obj);
  static int enumset_to_json(const ObObjType expect_type, ObDemoCastCtx &cast_ctx,
                             const ObObj &in_obj, ObObj &buf_obj, const ObObj *&res_obj);
  //支持向enum/set转换的版本
  static int to_type(const ObDemoExpectType &expect_type, ObDemoCastCtx &cast_ctx,
                     const ObObj &in_obj, ObObj &buf_obj, const ObObj *&res_obj);
  //}}
  static int to_type(const ObObjType expect_type, ObDemoCastCtx &cast_ctx,
                     const ObObj &in_obj, ObObj &out_obj);
  static int to_type(const ObObjType expect_type, ObCollationType expect_cs_type,
                     ObDemoCastCtx &cast_ctx, const ObObj &in_obj, ObObj &out_obj);
  static int get_zero_value(const ObObjType expect_type, ObCollationType expect_cs_type, ObObj &zero_obj);
  static int enumset_to_inner(const ObObjMeta &expect_meta,
                              const ObObj &in_obj, ObObj &out_obj,
                              common::ObIAllocator &allocator,
                              const common::ObIArray<common::ObString> &str_values);
  static int to_type(const ObDemoExpectType &expect_type, ObDemoCastCtx &cast_ctx, const ObObj &in_obj, ObObj &out_obj);
  static int is_cast_monotonic(ObObjType t1, ObObjType t2, bool &is_monotonic);
  static int is_order_consistent(const ObObjMeta &from,
                                 const ObObjMeta &to,
                                 bool &result);
  static int is_const_consistent(const ObObjMeta &const_mt,
                                 const ObObjMeta &column_mt,
                                 const ObObjType calc_type,
                                 const ObCollationType calc_collation,
                                 bool &result);
  static int is_injection(const ObObjMeta &from,
                          const ObObjMeta &to,
                          bool &result);
  static int oracle_number_to_char(const number::ObNumber &number_val,
                                   const bool is_from_number_type,
                                   const int16_t scale,
                                   const int64_t len,
                                   char *buf,
                                   int64_t &pos);
  static int can_cast_in_oracle_mode(const ObObjType dest_type, const ObCollationType dest_coll_type,
                                     const ObObjType src_type, const ObCollationType src_coll_type);
private:
  inline static int64_t get_idx_of_collate(ObCollationType cs_type)
  {
    int64_t idx = -1;
    switch(cs_type) {
      case CS_TYPE_UTF8MB4_GENERAL_CI:
        idx = 0;
        break;
      case CS_TYPE_UTF8MB4_BIN:
        idx = 1;
        break;
      case CS_TYPE_BINARY:
        idx = 2;
        break;
      case CS_TYPE_GBK_BIN:
        idx = 3;
        break;
      case CS_TYPE_GBK_CHINESE_CI:
        idx = 4;
        break;
      case CS_TYPE_UTF16_GENERAL_CI:
        idx = 5;
        break;
      case CS_TYPE_UTF16_BIN:
        idx = 6;
        break;
      case CS_TYPE_UTF8MB4_UNICODE_CI:
        idx = 7;
        break;
      case CS_TYPE_UTF16_UNICODE_CI:
        idx = 8;
        break;
      case CS_TYPE_GB18030_BIN:
        idx = 9;
        break;
      case CS_TYPE_GB18030_CHINESE_CI:
        idx = 10;
        break;
      default:
        idx = -1;
    }
    return idx;
  }
private:
  static const bool CAST_MONOTONIC[ObMaxTC][ObMaxTC];
  static const bool ORDER_CONSISTENT[ObMaxTC][ObMaxTC];
  static const bool ORDER_CONSISTENT_WITH_BOTH_STRING[ObCharset::VALID_COLLATION_TYPES][ObCharset::VALID_COLLATION_TYPES][ObCharset::VALID_COLLATION_TYPES];
  static const bool INJECTION[ObMaxTC][ObMaxTC];
  static const bool INJECTION_WITH_BOTH_STRING[ObCharset::VALID_COLLATION_TYPES][ObCharset::VALID_COLLATION_TYPES][ObCharset::VALID_COLLATION_TYPES];
};

class ObDemoObjEvaluator
{
public:
  inline static int is_true(const ObObj &obj, bool &result)
  {
    return ObDemoObjEvaluator::is_true(obj, CM_WARN_ON_FAIL, result);
  }
  inline static int is_false(const ObObj &obj, bool &result)
  {
    return ObDemoObjEvaluator::is_false(obj, CM_WARN_ON_FAIL, result);
  }
  static int is_true(const ObObj &obj, ObDemoCastMode cast_mode, bool &result);
  // is_false() 不是 !is_true()，因为布尔表达式的计算结果有三种：true，false，unknown
  static int is_false(const ObObj &obj, ObDemoCastMode cast_mode, bool &result);
};

int demo_number_range_check(ObDemoObjCastParams &params, const ObAccuracy &accuracy,
                       const ObObj &obj, ObObj &buf_obj, const ObObj *&res_obj,
                       const ObDemoCastMode cast_mode);
int demo_number_range_check_for_oracle(ObDemoObjCastParams &params, const ObAccuracy &accuracy,
                       const ObObj &obj, ObObj &buf_obj, const ObObj *&res_obj,
                       const ObDemoCastMode cast_mode);
int demo_number_range_check_v2(ObDemoObjCastParams &params, const ObAccuracy &accuracy,
                          const ObObj &obj, ObObj &buf_obj, const ObObj *&res_obj,
                          const ObDemoCastMode cast_mode);

class ObDemoNumberConstValue
{
public:
  ObDemoNumberConstValue() {}
  ~ObDemoNumberConstValue() {}
  static int init(ObIAllocator &allocator, const lib::ObMemAttr &attr);

public:
  static const ObScale MAX_ORACLE_SCALE_DELTA = 0 - number::ObNumber::MIN_SCALE;
  static const ObScale MAX_ORACLE_SCALE_SIZE = number::ObNumber::MAX_SCALE - number::ObNumber::MIN_SCALE;

  static number::ObNumber MYSQL_MIN[number::ObNumber::MAX_PRECISION + 1][number::ObNumber::MAX_SCALE + 1];
  static number::ObNumber MYSQL_MAX[number::ObNumber::MAX_PRECISION + 1][number::ObNumber::MAX_SCALE + 1];
  static number::ObNumber MYSQL_CHECK_MIN[number::ObNumber::MAX_PRECISION + 1][number::ObNumber::MAX_SCALE + 1];
  static number::ObNumber MYSQL_CHECK_MAX[number::ObNumber::MAX_PRECISION + 1][number::ObNumber::MAX_SCALE + 1];
  static number::ObNumber ORACLE_CHECK_MIN[OB_MAX_NUMBER_PRECISION + 1][MAX_ORACLE_SCALE_SIZE + 1];
  static number::ObNumber ORACLE_CHECK_MAX[OB_MAX_NUMBER_PRECISION + 1][MAX_ORACLE_SCALE_SIZE + 1];
};

} // end namespace common
} // end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_OBJ_CAST_
