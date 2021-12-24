/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <arrow/array.h>
#include <arrow/json/api.h>
#include <arrow/json/parser.h>
#include <arrow/util/decimal.h>
#include <math.h>

#include <cstdint>
#include <type_traits>

#include "third_party/gandiva/decimal_ops.h"
#include "third_party/gandiva/types.h"

int32_t castDATE32(int32_t in) { return castDATE_int32(in); }
int64_t castDATE64(int32_t in) { return castDATE_date32(in); }
int64_t extractYear(int64_t millis) { return extractYear_timestamp(millis); }
template <typename T>
T round2(T val, int precision = 2) {
  double dVal = (double)val;
  int charsNeeded = 1 + snprintf(NULL, 0, "%.*f", (int)precision, dVal);
  char* buffer = reinterpret_cast<char*>(malloc(charsNeeded));
  snprintf(buffer, charsNeeded, "%.*f", (int)precision, nextafter(val, val + 0.5));
  double result = atof(buffer);
  free(buffer);
  return static_cast<T>(result);
}

arrow::Decimal128 castDECIMAL(double val, int32_t precision, int32_t scale) {
  return arrow::Decimal128::FromReal(val, precision, scale).ValueOrDie();
}

std::string castStringFromDecimal(arrow::Decimal128 val, int32_t scale) {
  return val.ToString(scale);
}

double castFloatFromDecimal(arrow::Decimal128 val, int32_t scale) {
  return val.ToDouble(scale);
}

int64_t castLongFromDecimal(arrow::Decimal128 val, int32_t scale) {
  return static_cast<int64_t>(val.ToDouble(scale));
}

arrow::Decimal128 castDECIMAL(arrow::Decimal128 in, int32_t original_precision,
                              int32_t original_scale, int32_t new_precision,
                              int32_t new_scale) {
  bool overflow = false;
  gandiva::BasicDecimalScalar128 val(in, original_precision, original_scale);
  auto out = gandiva::decimalops::Convert(val, new_precision, new_scale, &overflow);
  if (overflow) {
    throw std::overflow_error("castDECIMAL overflowed!");
  }
  return arrow::Decimal128(out);
}

arrow::Decimal128 castDECIMALNullOnOverflow(arrow::Decimal128 in,
                                            int32_t original_precision,
                                            int32_t original_scale, int32_t new_precision,
                                            int32_t new_scale, bool* overflow_) {
  bool overflow = false;
  gandiva::BasicDecimalScalar128 val(in, original_precision, original_scale);
  auto out = gandiva::decimalops::Convert(val, new_precision, new_scale, &overflow);
  if (overflow) {
    *overflow_ = true;
  }
  return arrow::Decimal128(out);
}

arrow::Decimal128 add(arrow::Decimal128 left, int32_t left_precision, int32_t left_scale,
                      arrow::Decimal128 right, int32_t right_precision,
                      int32_t right_scale, int32_t out_precision, int32_t out_scale) {
  gandiva::BasicDecimalScalar128 x(left, left_precision, left_scale);
  gandiva::BasicDecimalScalar128 y(right, right_precision, right_scale);
  arrow::BasicDecimal128 out = gandiva::decimalops::Add(x, y, out_precision, out_scale);
  return arrow::Decimal128(out);
}

arrow::Decimal128 subtract(arrow::Decimal128 left, int32_t left_precision,
                           int32_t left_scale, arrow::Decimal128 right,
                           int32_t right_precision, int32_t right_scale,
                           int32_t out_precision, int32_t out_scale) {
  gandiva::BasicDecimalScalar128 x(left, left_precision, left_scale);
  gandiva::BasicDecimalScalar128 y(right, right_precision, right_scale);
  arrow::BasicDecimal128 out =
      gandiva::decimalops::Subtract(x, y, out_precision, out_scale);
  return arrow::Decimal128(out);
}

arrow::Decimal128 multiply(arrow::Decimal128 left, int32_t left_precision,
                           int32_t left_scale, arrow::Decimal128 right,
                           int32_t right_precision, int32_t right_scale,
                           int32_t out_precision, int32_t out_scale, bool* overflow_) {
  gandiva::BasicDecimalScalar128 x(left, left_precision, left_scale);
  gandiva::BasicDecimalScalar128 y(right, right_precision, right_scale);
  bool overflow = false;
  arrow::BasicDecimal128 out =
      gandiva::decimalops::Multiply(x, y, out_precision, out_scale, &overflow);
  if (overflow) {
    *overflow_ = true;
  }
  return arrow::Decimal128(out);
}

arrow::Decimal128 divide(arrow::Decimal128 left, int32_t left_precision,
                         int32_t left_scale, arrow::Decimal128 right,
                         int32_t right_precision, int32_t right_scale,
                         int32_t out_precision, int32_t out_scale, bool* overflow_) {
  gandiva::BasicDecimalScalar128 x(left, left_precision, left_scale);
  gandiva::BasicDecimalScalar128 y(right, right_precision, right_scale);
  bool overflow = false;
  arrow::BasicDecimal128 out =
      gandiva::decimalops::Divide(0, x, y, out_precision, out_scale, &overflow);
  if (overflow) {
    *overflow_ = true;
  }
  return arrow::Decimal128(out);
}

arrow::Decimal128 divide(const arrow::Decimal128& x, int32_t precision, int32_t scale,
                         int64_t y) {
  gandiva::BasicDecimalScalar128 val(x, precision, scale);
  arrow::BasicDecimal128 out = gandiva::decimalops::Divide(val, y);
  return arrow::Decimal128(out);
}

// A comparison with a NaN always returns false even when comparing with itself.
// To get the same result as spark, we can regard NaN as big as Infinity when
// doing comparison.
bool less_than_with_nan(double left, double right) {
  bool left_is_nan = std::isnan(left);
  bool right_is_nan = std::isnan(right);
  if (left_is_nan && right_is_nan) {
    return false;
  } else if (left_is_nan) {
    return false;
  } else if (right_is_nan) {
    return true;
  }
  return left < right;
}

bool greater_than_with_nan(double left, double right) {
  bool left_is_nan = std::isnan(left);
  bool right_is_nan = std::isnan(right);
  if (left_is_nan && right_is_nan) {
    return false;
  } else if (left_is_nan) {
    return true;
  } else if (right_is_nan) {
    return false;
  }
  return left > right;
}

bool less_than_or_equal_to_with_nan(double left, double right) {
  bool left_is_nan = std::isnan(left);
  bool right_is_nan = std::isnan(right);
  if (left_is_nan && right_is_nan) {
    return true;
  } else if (left_is_nan) {
    return false;
  } else if (right_is_nan) {
    return true;
  }
  return left <= right;
}

bool greater_than_or_equal_to_with_nan(double left, double right) {
  bool left_is_nan = std::isnan(left);
  bool right_is_nan = std::isnan(right);
  if (left_is_nan && right_is_nan) {
    return true;
  } else if (left_is_nan) {
    return true;
  } else if (right_is_nan) {
    return false;
  }
  return left >= right;
}

bool equal_with_nan(double left, double right) {
  bool left_is_nan = std::isnan(left);
  bool right_is_nan = std::isnan(right);
  if (left_is_nan && right_is_nan) {
    return true;
  } else if (left_is_nan) {
    return false;
  } else if (right_is_nan) {
    return false;
  }
  return left == right;
}

double normalize_nan_zero(double in) {
  if (std::isnan(in)) {
    return 0.0 / 0.0;
  } else if (std::abs(in) < 0.0000001) {
    return 0.0;
  } else {
    return in;
  }
}

arrow::Decimal128 round(arrow::Decimal128 in, int32_t original_precision,
                        int32_t original_scale, bool* overflow_, int32_t res_scale = 2) {
  bool overflow = false;
  gandiva::BasicDecimalScalar128 val(in, original_precision, original_scale);
  auto out = gandiva::decimalops::Round(val, original_precision, res_scale, res_scale,
                                        &overflow);
  if (overflow) {
    *overflow_ = true;
  }
  return arrow::Decimal128(out);
}

std::string get_json_object(const std::string& json_str, const std::string& json_path,
                            bool* validity) {
  std::unique_ptr<arrow::json::BlockParser> parser;
  (arrow::json::BlockParser::Make(arrow::json::ParseOptions::Defaults(), &parser));
  (parser->Parse(std::make_shared<arrow::Buffer>(json_str)));
  std::shared_ptr<arrow::Array> parsed;
  (parser->Finish(&parsed));
  auto struct_parsed = std::dynamic_pointer_cast<arrow::StructArray>(parsed);
  // json_path example: $.col_14, will extract col_14 here
  if (json_path.length() < 3) {
    *validity = false;
    return "";
  }
  auto col_name = json_path.substr(2);
  // illegal json string.
  if (struct_parsed == nullptr) {
    *validity = false;
    return "";
  }
  auto dict_parsed = std::dynamic_pointer_cast<arrow::DictionaryArray>(
      struct_parsed->GetFieldByName(col_name));
  // no data contained for given field.
  if (dict_parsed == nullptr) {
    *validity = false;
    return "";
  }

  auto dict_array = dict_parsed->dictionary();
  // needs to see whether there is a case that has more than one indices.
  auto res_index = dict_parsed->GetValueIndex(0);
  // TODO(): check null results
  auto utf8_array = std::dynamic_pointer_cast<arrow::BinaryArray>(dict_array);
  auto res = utf8_array->GetString(res_index);
  *validity = true;
  return res;
}