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

#include <arrow/type.h>

#include "precompile/array.h"

namespace sparkcolumnarplugin {
namespace precompile {

template <typename T>
using is_number_type = std::is_base_of<arrow::NumberType, T>;

template <typename T>
using is_boolean_type = std::is_same<arrow::BooleanType, T>;

template <typename T>
using is_date_type = std::is_base_of<arrow::DateType, T>;

template <typename T>
using is_number_like =
    std::integral_constant<bool, is_number_type<T>::value || is_boolean_type<T>::value>;

template <typename T>
using is_number_like_type =
    std::integral_constant<bool, is_number_like<T>::value || is_date_type<T>::value>;

template <typename T>
using enable_if_number = std::enable_if_t<is_number_like_type<T>::value>;

template <typename T>
using is_base_binary_type = std::is_base_of<arrow::BaseBinaryType, T>;

template <typename T>
using is_string_like_type =
    std::integral_constant<bool, is_base_binary_type<T>::value && T::is_utf8>;

template <typename T>
using enable_if_string_like = std::enable_if_t<is_string_like_type<T>::value>;

struct PrecompileType {
  /// \brief Main data type enumeration
  ///
  /// This enumeration provides a quick way to interrogate the category
  /// of a DataType instance.
  enum type {
    /// A NULL type having no physical storage
    NA,

    /// Boolean as 1 bit, LSB bit-packed ordering
    BOOL,

    /// Unsigned 8-bit little-endian integer
    UINT8,

    /// Signed 8-bit little-endian integer
    INT8,

    /// Unsigned 16-bit little-endian integer
    UINT16,

    /// Signed 16-bit little-endian integer
    INT16,

    /// Unsigned 32-bit little-endian integer
    UINT32,

    /// Signed 32-bit little-endian integer
    INT32,

    /// Unsigned 64-bit little-endian integer
    UINT64,

    /// Signed 64-bit little-endian integer
    INT64,

    /// 2-byte floating point value
    HALF_FLOAT,

    /// 4-byte floating point value
    FLOAT,

    /// 8-byte floating point value
    DOUBLE,

    /// UTF8 variable-length string as List<Char>
    STRING,

    /// Variable-length bytes (no guarantee of UTF8-ness)
    BINARY,

    /// Fixed-size binary. Each value occupies the same number of bytes
    FIXED_SIZE_BINARY,

    /// int32_t days since the UNIX epoch
    DATE32,

    /// int64_t milliseconds since the UNIX epoch
    DATE64,

    /// Exact timestamp encoded with int64 since UNIX epoch
    /// Default unit millisecond
    TIMESTAMP,

    /// Time as signed 32-bit integer, representing either seconds or
    /// milliseconds since midnight
    TIME32,

    /// Time as signed 64-bit integer, representing either microseconds or
    /// nanoseconds since midnight
    TIME64,

    /// YEAR_MONTH or DAY_TIME interval in SQL style
    INTERVAL,

    /// Precision- and scale-based decimal type. Storage type depends on the
    /// parameters.
    DECIMAL,

    /// A list of some logical data type
    LIST,

    /// Struct of logical types
    STRUCT,

    /// Unions of logical types
    UNION,

    /// Dictionary-encoded type, also called "categorical" or "factor"
    /// in other programming languages. Holds the dictionary value
    /// type but not the dictionary itself, which is part of the
    /// ArrayData struct
    DICTIONARY,

    /// Map, a repeated struct logical type
    MAP,

    /// Custom data type, implemented by user
    EXTENSION,

    /// Fixed size list of some logical type
    FIXED_SIZE_LIST,

    /// Measure of elapsed time in either seconds, milliseconds, microseconds
    /// or nanoseconds.
    DURATION,

    /// Like STRING, but with 64-bit offsets
    LARGE_STRING,

    /// Like BINARY, but with 64-bit offsets
    LARGE_BINARY,

    /// Like LIST, but with 64-bit offsets
    LARGE_LIST
  };
};

template <typename T>
struct TypeTraits {};

template <>
struct TypeTraits<arrow::BooleanType> {
  static constexpr PrecompileType::type type_id = PrecompileType::BOOL;
  using ArrayType = BooleanArray;
  using CType = bool;
};
template <>
struct TypeTraits<arrow::UInt8Type> {
  static constexpr PrecompileType::type type_id = PrecompileType::UINT8;
  using ArrayType = UInt8Array;
  using CType = uint8_t;
};
template <>
struct TypeTraits<arrow::Int8Type> {
  static constexpr PrecompileType::type type_id = PrecompileType::INT8;
  using ArrayType = Int8Array;
  using CType = int8_t;
};
template <>
struct TypeTraits<arrow::UInt16Type> {
  static constexpr PrecompileType::type type_id = PrecompileType::UINT16;
  using ArrayType = UInt16Array;
  using CType = uint16_t;
};
template <>
struct TypeTraits<arrow::Int16Type> {
  static constexpr PrecompileType::type type_id = PrecompileType::INT16;
  using ArrayType = Int16Array;
  using CType = int16_t;
};
template <>
struct TypeTraits<arrow::UInt32Type> {
  static constexpr PrecompileType::type type_id = PrecompileType::UINT32;
  using ArrayType = UInt32Array;
  using CType = uint32_t;
};
template <>
struct TypeTraits<arrow::Int32Type> {
  static constexpr PrecompileType::type type_id = PrecompileType::INT32;
  using ArrayType = Int32Array;
  using CType = int32_t;
};
template <>
struct TypeTraits<arrow::UInt64Type> {
  static constexpr PrecompileType::type type_id = PrecompileType::UINT64;
  using ArrayType = UInt64Array;
  using CType = uint64_t;
};
template <>
struct TypeTraits<arrow::Int64Type> {
  static constexpr PrecompileType::type type_id = PrecompileType::INT64;
  using ArrayType = Int64Array;
  using CType = int64_t;
};
template <>
struct TypeTraits<arrow::FloatType> {
  static constexpr PrecompileType::type type_id = PrecompileType::FLOAT;
  using ArrayType = FloatArray;
  using CType = float;
};
template <>
struct TypeTraits<arrow::DoubleType> {
  static constexpr PrecompileType::type type_id = PrecompileType::DOUBLE;
  using ArrayType = DoubleArray;
  using CType = double;
};
template <>
struct TypeTraits<arrow::Date32Type> {
  static constexpr PrecompileType::type type_id = PrecompileType::DATE32;
  using ArrayType = Date32Array;
  using CType = int32_t;
};
template <>
struct TypeTraits<arrow::Date64Type> {
  static constexpr PrecompileType::type type_id = PrecompileType::DATE64;
  using ArrayType = Date64Array;
  using CType = int64_t;
};
template <>
struct TypeTraits<arrow::FixedSizeBinaryType> {
  static constexpr PrecompileType::type type_id = PrecompileType::FIXED_SIZE_BINARY;
  using ArrayType = FixedSizeBinaryArray;
  using CType = uint8_t;
};
template <>
struct TypeTraits<arrow::StringType> {
  static constexpr PrecompileType::type type_id = PrecompileType::STRING;
  using ArrayType = StringArray;
  using CType = std::string;
};

}  // namespace precompile
}  // namespace sparkcolumnarplugin