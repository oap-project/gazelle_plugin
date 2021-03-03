#pragma once

#include <arrow/util/decimal.h>
#include <math.h>

#include <cstdint>
#include <type_traits>

#include "third_party/gandiva/types.h"
#include "third_party/gandiva/decimal_ops.h"

int32_t castDATE32(int32_t in) { return castDATE_int32(in); }
int64_t castDATE64(int32_t in) { return castDATE_date32(in); }
int64_t extractYear(int64_t millis) { return extractYear_timestamp(millis); }
template <typename T>
T round2(T val, int precision = 2) {
  int charsNeeded = 1 + snprintf(NULL, 0, "%.*f", (int)precision, val);
  char* buffer = reinterpret_cast<char*>(malloc(charsNeeded));
  snprintf(buffer, charsNeeded, "%.*f", (int)precision, nextafter(val, val + 0.5));
  double result = atof(buffer);
  free(buffer);
  return static_cast<T>(result);
}
arrow::Decimal128 castDECIMAL(double val, int32_t precision, int32_t scale) {
  int charsNeeded = 1 + snprintf(NULL, 0, "%.*f", (int)scale, val);
  char* buffer = reinterpret_cast<char*>(malloc(charsNeeded));
  snprintf(buffer, charsNeeded, "%.*f", (int)scale, nextafter(val, val + 0.5));
  auto decimal_str = std::string(buffer);
  free(buffer);
  return arrow::Decimal128::FromString(decimal_str).ValueOrDie();
}

double castFloatFromDecimal(arrow::Decimal128 val, int32_t scale) {
  std::string str = val.ToString(scale);
  return atof(str.c_str());
}

arrow::Decimal128 castDECIMAL(arrow::Decimal128 in, int32_t original_precision,
                              int32_t original_scale, int32_t new_precision, 
                              int32_t new_scale) {
  bool overflow;
  gandiva::BasicDecimalScalar128 val(in, original_precision, original_scale);
  auto out = gandiva::decimalops::Convert(val, new_precision, new_scale, &overflow);
  return arrow::Decimal128(out);
}

arrow::Decimal128 divide(arrow::Decimal128 left, int32_t left_precision, 
                         int32_t left_scale, arrow::Decimal128 right, 
                         int32_t right_precision, int32_t right_scale,
                         int32_t out_precision, int32_t out_scale) {
  gandiva::BasicDecimalScalar128 x(left, left_precision, left_scale);
  gandiva::BasicDecimalScalar128 y(right, right_precision, right_scale);
  bool overflow;
  int64_t context = 0;
  arrow::BasicDecimal128 out =
      gandiva::decimalops::Divide(context, x, y, out_precision, out_scale, &overflow);
  return arrow::Decimal128(out);
}
