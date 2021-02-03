#pragma once

#include <arrow/util/decimal.h>
#include <math.h>

#include <cstdint>
#include <type_traits>

#include "third_party/gandiva/types.h"

int64_t castDATE(int32_t in) { return castDATE_date32(in); }
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
  return arrow::Decimal128(decimal_str);
}

arrow::Decimal128 castDECIMAL(arrow::Decimal128 in, int32_t original_scale,
                              int32_t new_scale) {
  return in.Rescale(original_scale, new_scale).ValueOrDie();
}
