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
#include <arrow/type.h>

#include "precompile/array.h"
#include "utils/macros.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
using namespace sparkcolumnarplugin::precompile;

class ComparatorBase {
 public:
  ComparatorBase() {}

  ~ComparatorBase() {}

  virtual std::function<void(int, int, int64_t, int64_t, int&)> GetCompareFunc(
      const arrow::ArrayVector& arrays, bool asc, bool nulls_first) {
    throw std::runtime_error("ComparatorBase GetCompareFunc is abstract");
  }

  virtual arrow::Status PrepareCompareFunc(bool asc = true, bool nulls_first = true,
                                           bool nan_check = false,
                                           bool always_check_null = false) {
    throw std::runtime_error("ComparatorBase PrepareCompareFunc is abstract");
  }

  virtual int64_t GetCacheSize() { return 0; }

  virtual arrow::Status AddArray(const std::shared_ptr<arrow::Array>& arr) {
    return arrow::Status::NotImplemented("ComparatorBase AddArray is abstract.");
  }

  virtual arrow::Status ReleaseArray(const uint16_t& array_id) {
    return arrow::Status::NotImplemented("ComparatorBase ReleaseArray is abstract.");
  }

  virtual int Compare(int left_array_id, int right_array_id, int64_t left_id,
                      int64_t right_id) {
    return 0;
  }
};

template <typename DataType, typename CType>
class TypedComparator : public ComparatorBase {
 public:
  TypedComparator() {}

  ~TypedComparator() {}

  int64_t GetCacheSize() { return cached_arr_.size(); }

  arrow::Status AddArray(const std::shared_ptr<arrow::Array>& arr) {
    auto typed_arr_ = std::dynamic_pointer_cast<ArrayType>(arr);
    cached_arr_.push_back(typed_arr_);
    null_total_count_ += typed_arr_->null_count();
    return arrow::Status::OK();
  }

  arrow::Status ReleaseArray(const uint16_t& array_id) {
    if (cached_arr_.size() > array_id) {
      null_total_count_ -= cached_arr_[array_id]->null_count();
      cached_arr_[array_id] = nullptr;
    }
    return arrow::Status::OK();
  }

  int Compare(int left_array_id, int right_array_id, int64_t left_id, int64_t right_id) {
    int res = 2;
    compare_func_(left_array_id, right_array_id, left_id, right_id, res);
    return res;
  }

  arrow::Status PrepareCompareFunc(bool asc = true, bool nulls_first = true,
                                   bool nan_check = false,
                                   bool always_check_null = false) {
    if (null_total_count_ == 0 && !always_check_null) {
      if (asc) {
        compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                               int64_t right_id, int& cmp_res) {
          CType left = cached_arr_[left_array_id]->GetView(left_id);
          CType right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
          std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
          if (left != right) {
            cmp_res = left < right;
          }
        };
      } else {
        compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                               int64_t right_id, int& cmp_res) {
          CType left = cached_arr_[left_array_id]->GetView(left_id);
          CType right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
          std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
          if (left != right) {
            cmp_res = left > right;
          }
        };
      }
    } else if (asc) {
      if (nulls_first) {
        compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                               int64_t right_id, int& cmp_res) {
          if (null_total_count_ > 0) {
            bool is_left_null = cached_arr_[left_array_id]->null_count() > 0 &&
                                cached_arr_[left_array_id]->IsNull(left_id);
            bool is_right_null = cached_arr_[right_array_id]->null_count() > 0 &&
                                 cached_arr_[right_array_id]->IsNull(right_id);
            if (!is_left_null || !is_right_null) {
              if (is_left_null) {
#ifdef DEBUG
                std::cout << "left is null, right is not null" << std::endl;
#endif
                cmp_res = 1;
              } else if (is_right_null) {
#ifdef DEBUG
                std::cout << "left is not null, right is null" << std::endl;
#endif
                cmp_res = 0;
              } else {
                CType left = cached_arr_[left_array_id]->GetView(left_id);
                CType right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
                std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
                if (left != right) {
                  cmp_res = left < right;
                }
              }
#ifdef DEBUG
            } else {
              std::cout << "left is null, right is null" << std::endl;
#endif
            }
          } else {
            CType left = cached_arr_[left_array_id]->GetView(left_id);
            CType right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
            std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
            if (left != right) {
              cmp_res = left < right;
            }
          }
        };
      } else {
        compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                               int64_t right_id, int& cmp_res) {
          if (null_total_count_ > 0) {
            bool is_left_null = cached_arr_[left_array_id]->null_count() > 0 &&
                                cached_arr_[left_array_id]->IsNull(left_id);
            bool is_right_null = cached_arr_[right_array_id]->null_count() > 0 &&
                                 cached_arr_[right_array_id]->IsNull(right_id);
            if (!is_left_null || !is_right_null) {
              if (is_left_null) {
#ifdef DEBUG
                std::cout << "left is null, right is not null" << std::endl;
#endif
                cmp_res = 0;
              } else if (is_right_null) {
#ifdef DEBUG
                std::cout << "left is not null, right is null" << std::endl;
#endif
                cmp_res = 1;
              } else {
                CType left = cached_arr_[left_array_id]->GetView(left_id);
                CType right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
                std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
                if (left != right) {
                  cmp_res = left < right;
                }
              }
#ifdef DEBUG
            } else {
              std::cout << "left is null, right is null" << std::endl;
#endif
            }
          } else {
            CType left = cached_arr_[left_array_id]->GetView(left_id);
            CType right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
            std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
            if (left != right) {
              cmp_res = left < right;
            }
          }
        };
      }
    } else if (nulls_first) {
      compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                             int64_t right_id, int& cmp_res) {
        if (null_total_count_ > 0) {
          bool is_left_null = cached_arr_[left_array_id]->null_count() > 0 &&
                              cached_arr_[left_array_id]->IsNull(left_id);
          bool is_right_null = cached_arr_[right_array_id]->null_count() > 0 &&
                               cached_arr_[right_array_id]->IsNull(right_id);
          if (!is_left_null || !is_right_null) {
            if (is_left_null) {
#ifdef DEBUG
              std::cout << "left is null, right is not null" << std::endl;
#endif
              cmp_res = 1;
            } else if (is_right_null) {
#ifdef DEBUG
              std::cout << "left is not null, right is null" << std::endl;
#endif
              cmp_res = 0;
            } else {
              CType left = cached_arr_[left_array_id]->GetView(left_id);
              CType right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
              std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
              if (left != right) {
                cmp_res = left > right;
              }
            }
#ifdef DEBUG
          } else {
            std::cout << "left is null, right is null" << std::endl;
#endif
          }
        } else {
          CType left = cached_arr_[left_array_id]->GetView(left_id);
          CType right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
          std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
          if (left != right) {
            cmp_res = left > right;
          }
        }
      };
    } else {
      compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                             int64_t right_id, int& cmp_res) {
        if (null_total_count_ > 0) {
          bool is_left_null = cached_arr_[left_array_id]->null_count() > 0 &&
                              cached_arr_[left_array_id]->IsNull(left_id);
          bool is_right_null = cached_arr_[right_array_id]->null_count() > 0 &&
                               cached_arr_[right_array_id]->IsNull(right_id);
          if (!is_left_null || !is_right_null) {
            if (is_left_null) {
#ifdef DEBUG
              std::cout << "left is null, right is not null" << std::endl;
#endif
              cmp_res = 0;
            } else if (is_right_null) {
#ifdef DEBUG
              std::cout << "left is not null, right is null" << std::endl;
#endif
              cmp_res = 1;
            } else {
              CType left = cached_arr_[left_array_id]->GetView(left_id);
              CType right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
              std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
              if (left != right) {
                cmp_res = left > right;
              }
            }
#ifdef DEBUG
          } else {
            std::cout << "left is null, right is null" << std::endl;
#endif
          }
        } else {
          CType left = cached_arr_[left_array_id]->GetView(left_id);
          CType right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
          std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
          if (left != right) {
            cmp_res = left > right;
          }
        }
      };
    }
    return arrow::Status::OK();
  }

  std::function<void(int, int, int64_t, int64_t, int&)> GetCompareFunc(
      const arrow::ArrayVector& arrays, bool asc, bool nulls_first) {
    null_total_count_ = 0;
    cached_arr_.clear();
    for (int array_id = 0; array_id < arrays.size(); array_id++) {
      null_total_count_ += arrays[array_id]->null_count();
      auto typed_array = std::dynamic_pointer_cast<ArrayType>(arrays[array_id]);
      cached_arr_.push_back(typed_array);
    }
    PrepareCompareFunc(asc, nulls_first);
    return compare_func_;
  }

 private:
  using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
  std::vector<std::shared_ptr<ArrayType>> cached_arr_;
  std::shared_ptr<arrow::DataType> data_type_;
  uint64_t null_total_count_ = 0;
  std::function<void(int, int, int64_t, int64_t, int&)> compare_func_;
};

template <typename DataType, typename CType>
class FloatingComparator : public ComparatorBase {
 public:
  FloatingComparator() {}

  ~FloatingComparator() {}

  int64_t GetCacheSize() { return cached_arr_.size(); }

  arrow::Status AddArray(const std::shared_ptr<arrow::Array>& arr) {
    auto typed_arr_ = std::dynamic_pointer_cast<ArrayType>(arr);
    cached_arr_.push_back(typed_arr_);
    null_total_count_ += typed_arr_->null_count();
    return arrow::Status::OK();
  }

  arrow::Status ReleaseArray(const uint16_t& array_id) {
    if (cached_arr_.size() > array_id) {
      null_total_count_ -= cached_arr_[array_id]->null_count();
      cached_arr_[array_id] = nullptr;
    }
    return arrow::Status::OK();
  }

  int Compare(int left_array_id, int right_array_id, int64_t left_id, int64_t right_id) {
    int res = 2;
    compare_func_(left_array_id, right_array_id, left_id, right_id, res);
    return res;
  }

  arrow::Status PrepareCompareFunc(bool asc = true, bool nulls_first = true,
                                   bool nan_check = false,
                                   bool always_check_null = false) {
    if (null_total_count_ == 0 && !always_check_null) {
      if (asc) {
        if (nan_check) {
          // null_total_count_ == 0, asc, nan_check
          compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                                 int64_t right_id, int& cmp_res) {
            CType left = cached_arr_[left_array_id]->GetView(left_id);
            CType right = cached_arr_[right_array_id]->GetView(right_id);
            bool is_left_nan = std::isnan(left);
            bool is_right_nan = std::isnan(right);
#ifdef DEBUG
            std::cout << "left is " << (is_left_nan ? "nan" : std::to_string(left))
                      << ", right is " << (is_right_nan ? "nan" : std::to_string(right))
                      << std::endl;
#endif
            if (!is_left_nan || !is_right_nan) {
              if (is_left_nan) {
                cmp_res = 0;
              } else if (is_right_nan) {
                cmp_res = 1;
              } else {
                if (left != right) {
                  cmp_res = left < right;
                }
              }
            }
          };
        } else {
          // null_total_count_ == 0, asc, !nan_check
          compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                                 int64_t right_id, int& cmp_res) {
            CType left = cached_arr_[left_array_id]->GetView(left_id);
            CType right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
            std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
            if (left != right) {
              cmp_res = left < right;
            }
          };
        }
      } else {
        if (nan_check) {
          // null_total_count_ == 0, desc, nan_check
          compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                                 int64_t right_id, int& cmp_res) {
            CType left = cached_arr_[left_array_id]->GetView(left_id);
            CType right = cached_arr_[right_array_id]->GetView(right_id);
            bool is_left_nan = std::isnan(left);
            bool is_right_nan = std::isnan(right);
#ifdef DEBUG
            std::cout << "left is " << (is_left_nan ? "nan" : std::to_string(left))
                      << ", right is " << (is_right_nan ? "nan" : std::to_string(right))
                      << std::endl;
#endif
            if (!is_left_nan || !is_right_nan) {
              if (is_left_nan) {
                cmp_res = 1;
              } else if (is_right_nan) {
                cmp_res = 0;
              } else {
                if (left != right) {
                  cmp_res = left > right;
                }
              }
            }
          };
        } else {
          // null_total_count_ == 0, desc, !nan_check
          compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                                 int64_t right_id, int& cmp_res) {
            CType left = cached_arr_[left_array_id]->GetView(left_id);
            CType right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
            std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
            if (left != right) {
              cmp_res = left > right;
            }
          };
        }
      }
    } else if (asc) {
      if (nulls_first) {
        if (nan_check) {
          // nulls_first, asc, nan_check
          compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                                 int64_t right_id, int& cmp_res) {
            if (null_total_count_ > 0) {
              bool is_left_null = cached_arr_[left_array_id]->null_count() > 0 &&
                                  cached_arr_[left_array_id]->IsNull(left_id);
              bool is_right_null = cached_arr_[right_array_id]->null_count() > 0 &&
                                   cached_arr_[right_array_id]->IsNull(right_id);
              if (!is_left_null || !is_right_null) {
                if (is_left_null) {
#ifdef DEBUG
                  std::cout << "left is null, right is not null" << std::endl;
#endif
                  cmp_res = 1;
                } else if (is_right_null) {
#ifdef DEBUG
                  std::cout << "left is not null, right is null" << std::endl;
#endif
                  cmp_res = 0;
                } else {
                  CType left = cached_arr_[left_array_id]->GetView(left_id);
                  CType right = cached_arr_[right_array_id]->GetView(right_id);
                  bool is_left_nan = std::isnan(left);
                  bool is_right_nan = std::isnan(right);
#ifdef DEBUG
                  std::cout << "left is " << (is_left_nan ? "nan" : std::to_string(left))
                            << ", right is "
                            << (is_right_nan ? "nan" : std::to_string(right))
                            << std::endl;
#endif
                  if (!is_left_nan || !is_right_nan) {
                    if (is_left_nan) {
                      cmp_res = 0;
                    } else if (is_right_nan) {
                      cmp_res = 1;
                    } else {
                      if (left != right) {
                        cmp_res = left < right;
                      }
                    }
                  }
                }
#ifdef DEBUG
              } else {
                std::cout << "left is null"
                          << ", right is null" << std::endl;
#endif
              }
            } else {
              CType left = cached_arr_[left_array_id]->GetView(left_id);
              CType right = cached_arr_[right_array_id]->GetView(right_id);
              bool is_left_nan = std::isnan(left);
              bool is_right_nan = std::isnan(right);
#ifdef DEBUG
              std::cout << "left is " << (is_left_nan ? "nan" : std::to_string(left))
                        << ", right is " << (is_right_nan ? "nan" : std::to_string(right))
                        << std::endl;
#endif
              if (!is_left_nan || !is_right_nan) {
                if (is_left_nan) {
                  cmp_res = 0;
                } else if (is_right_nan) {
                  cmp_res = 1;
                } else {
                  if (left != right) {
                    cmp_res = left < right;
                  }
                }
              }
            }
          };
        } else {
          // nulls_first, asc, !nan_check
          compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                                 int64_t right_id, int& cmp_res) {
            if (null_total_count_ > 0) {
              bool is_left_null = cached_arr_[left_array_id]->null_count() > 0 &&
                                  cached_arr_[left_array_id]->IsNull(left_id);
              bool is_right_null = cached_arr_[right_array_id]->null_count() > 0 &&
                                   cached_arr_[right_array_id]->IsNull(right_id);
              if (!is_left_null || !is_right_null) {
                if (is_left_null) {
#ifdef DEBUG
                  std::cout << "left is null, right is not null" << std::endl;
#endif
                  cmp_res = 1;
                } else if (is_right_null) {
#ifdef DEBUG
                  std::cout << "left is not null, right is null" << std::endl;
#endif
                  cmp_res = 0;
                } else {
                  CType left = cached_arr_[left_array_id]->GetView(left_id);
                  CType right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
                  std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
                  if (left != right) {
                    cmp_res = left < right;
                  }
                }
#ifdef DEBUG
              } else {
                std::cout << "left is null"
                          << ", right is null" << std::endl;
#endif
              }
            } else {
              CType left = cached_arr_[left_array_id]->GetView(left_id);
              CType right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
              std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
              if (left != right) {
                cmp_res = left < right;
              }
            }
          };
        }
      } else {
        if (nan_check) {
          // nulls_last, asc, nan_check
          compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                                 int64_t right_id, int& cmp_res) {
            if (null_total_count_ > 0) {
              bool is_left_null = cached_arr_[left_array_id]->null_count() > 0 &&
                                  cached_arr_[left_array_id]->IsNull(left_id);
              bool is_right_null = cached_arr_[right_array_id]->null_count() > 0 &&
                                   cached_arr_[right_array_id]->IsNull(right_id);
              if (!is_left_null || !is_right_null) {
                if (is_left_null) {
#ifdef DEBUG
                  std::cout << "left is null, right is not null" << std::endl;
#endif
                  cmp_res = 0;
                } else if (is_right_null) {
#ifdef DEBUG
                  std::cout << "left is not null, right is null" << std::endl;
#endif
                  cmp_res = 1;
                } else {
                  CType left = cached_arr_[left_array_id]->GetView(left_id);
                  CType right = cached_arr_[right_array_id]->GetView(right_id);
                  bool is_left_nan = std::isnan(left);
                  bool is_right_nan = std::isnan(right);
#ifdef DEBUG
                  std::cout << "left is " << (is_left_nan ? "nan" : std::to_string(left))
                            << ", right is "
                            << (is_right_nan ? "nan" : std::to_string(right))
                            << std::endl;
#endif
                  if (!is_left_nan || !is_right_nan) {
                    if (is_left_nan) {
                      cmp_res = 0;
                    } else if (is_right_nan) {
                      cmp_res = 1;
                    } else {
                      if (left != right) {
                        cmp_res = left < right;
                      }
                    }
                  }
                }
#ifdef DEBUG
              } else {
                std::cout << "left is null"
                          << ", right is null" << std::endl;
#endif
              }
            } else {
              CType left = cached_arr_[left_array_id]->GetView(left_id);
              CType right = cached_arr_[right_array_id]->GetView(right_id);
              bool is_left_nan = std::isnan(left);
              bool is_right_nan = std::isnan(right);
#ifdef DEBUG
              std::cout << "left is " << (is_left_nan ? "nan" : std::to_string(left))
                        << ", right is " << (is_right_nan ? "nan" : std::to_string(right))
                        << std::endl;
#endif
              if (!is_left_nan || !is_right_nan) {
                if (is_left_nan) {
                  cmp_res = 0;
                } else if (is_right_nan) {
                  cmp_res = 1;
                } else {
                  if (left != right) {
                    cmp_res = left < right;
                  }
                }
              }
            }
          };
        } else {
          // nulls_last, asc, !nan_check
          compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                                 int64_t right_id, int& cmp_res) {
            if (null_total_count_ > 0) {
              bool is_left_null = cached_arr_[left_array_id]->null_count() > 0 &&
                                  cached_arr_[left_array_id]->IsNull(left_id);
              bool is_right_null = cached_arr_[right_array_id]->null_count() > 0 &&
                                   cached_arr_[right_array_id]->IsNull(right_id);
              if (!is_left_null || !is_right_null) {
                if (is_left_null) {
#ifdef DEBUG
                  std::cout << "left is null, right is not null" << std::endl;
#endif
                  cmp_res = 0;
                } else if (is_right_null) {
#ifdef DEBUG
                  std::cout << "left is not null, right is null" << std::endl;
#endif
                  cmp_res = 1;
                } else {
                  CType left = cached_arr_[left_array_id]->GetView(left_id);
                  CType right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
                  std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
                  if (left != right) {
                    cmp_res = left < right;
                  }
                }
#ifdef DEBUG
              } else {
                std::cout << "left is null"
                          << ", right is null" << std::endl;
#endif
              }
            } else {
              CType left = cached_arr_[left_array_id]->GetView(left_id);
              CType right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
              std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
              if (left != right) {
                cmp_res = left < right;
              }
            }
          };
        }
      }
    } else if (nulls_first) {
      if (nan_check) {
        // nulls_first, desc, nan_check
        compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                               int64_t right_id, int& cmp_res) {
          if (null_total_count_ > 0) {
            bool is_left_null = cached_arr_[left_array_id]->null_count() > 0 &&
                                cached_arr_[left_array_id]->IsNull(left_id);
            bool is_right_null = cached_arr_[right_array_id]->null_count() > 0 &&
                                 cached_arr_[right_array_id]->IsNull(right_id);
            if (!is_left_null || !is_right_null) {
              if (is_left_null) {
#ifdef DEBUG
                std::cout << "left is null, right is not null" << std::endl;
#endif
                cmp_res = 1;
              } else if (is_right_null) {
#ifdef DEBUG
                std::cout << "left is not null, right is null" << std::endl;
#endif
                cmp_res = 0;
              } else {
                CType left = cached_arr_[left_array_id]->GetView(left_id);
                CType right = cached_arr_[right_array_id]->GetView(right_id);
                bool is_left_nan = std::isnan(left);
                bool is_right_nan = std::isnan(right);
#ifdef DEBUG
                std::cout << "left is " << (is_left_nan ? "nan" : std::to_string(left))
                          << ", right is "
                          << (is_right_nan ? "nan" : std::to_string(right)) << std::endl;
#endif
                if (!is_left_nan || !is_right_nan) {
                  if (is_left_nan) {
                    cmp_res = 1;
                  } else if (is_right_nan) {
                    cmp_res = 0;
                  } else {
                    if (left != right) {
                      cmp_res = left > right;
                    }
                  }
                }
              }
#ifdef DEBUG
            } else {
              std::cout << "left is null"
                        << ", right is null" << std::endl;
#endif
            }
          } else {
            CType left = cached_arr_[left_array_id]->GetView(left_id);
            CType right = cached_arr_[right_array_id]->GetView(right_id);
            bool is_left_nan = std::isnan(left);
            bool is_right_nan = std::isnan(right);
#ifdef DEBUG
            std::cout << "left is " << (is_left_nan ? "nan" : std::to_string(left))
                      << ", right is " << (is_right_nan ? "nan" : std::to_string(right))
                      << std::endl;
#endif
            if (!is_left_nan || !is_right_nan) {
              if (is_left_nan) {
                cmp_res = 1;
              } else if (is_right_nan) {
                cmp_res = 0;
              } else {
                if (left != right) {
                  cmp_res = left > right;
                }
              }
            }
          }
        };
      } else {
        // nulls_first, desc, !nan_check
        compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                               int64_t right_id, int& cmp_res) {
          if (null_total_count_ > 0) {
            bool is_left_null = cached_arr_[left_array_id]->null_count() > 0 &&
                                cached_arr_[left_array_id]->IsNull(left_id);
            bool is_right_null = cached_arr_[right_array_id]->null_count() > 0 &&
                                 cached_arr_[right_array_id]->IsNull(right_id);
            if (!is_left_null || !is_right_null) {
              if (is_left_null) {
#ifdef DEBUG
                std::cout << "left is null, right is not null" << std::endl;
#endif
                cmp_res = 1;
              } else if (is_right_null) {
#ifdef DEBUG
                std::cout << "left is not null, right is null" << std::endl;
#endif
                cmp_res = 0;
              } else {
                CType left = cached_arr_[left_array_id]->GetView(left_id);
                CType right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
                std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
                if (left != right) {
                  cmp_res = left > right;
                }
              }
#ifdef DEBUG
            } else {
              std::cout << "left is null"
                        << ", right is null" << std::endl;
#endif
            }
          } else {
            CType left = cached_arr_[left_array_id]->GetView(left_id);
            CType right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
            std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
            if (left != right) {
              cmp_res = left > right;
            }
          }
        };
      }
    } else {
      if (nan_check) {
        // nulls_last, desc, nan_check
        compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                               int64_t right_id, int& cmp_res) {
          if (null_total_count_ > 0) {
            bool is_left_null = cached_arr_[left_array_id]->null_count() > 0 &&
                                cached_arr_[left_array_id]->IsNull(left_id);
            bool is_right_null = cached_arr_[right_array_id]->null_count() > 0 &&
                                 cached_arr_[right_array_id]->IsNull(right_id);
            if (!is_left_null || !is_right_null) {
              if (is_left_null) {
#ifdef DEBUG
                std::cout << "left is null, right is not null" << std::endl;
#endif
                cmp_res = 0;
              } else if (is_right_null) {
#ifdef DEBUG
                std::cout << "left is not null, right is null" << std::endl;
#endif
                cmp_res = 1;
              } else {
                CType left = cached_arr_[left_array_id]->GetView(left_id);
                CType right = cached_arr_[right_array_id]->GetView(right_id);
                bool is_left_nan = std::isnan(left);
                bool is_right_nan = std::isnan(right);
#ifdef DEBUG
                std::cout << "left is " << (is_left_nan ? "nan" : std::to_string(left))
                          << ", right is "
                          << (is_right_nan ? "nan" : std::to_string(right)) << std::endl;
#endif
                if (!is_left_nan || !is_right_nan) {
                  if (is_left_nan) {
                    cmp_res = 1;
                  } else if (is_right_nan) {
                    cmp_res = 0;
                  } else {
                    if (left != right) {
                      cmp_res = left > right;
                    }
                  }
                }
              }
#ifdef DEBUG
            } else {
              std::cout << "left is null"
                        << ", right is null" << std::endl;
#endif
            }
          } else {
            CType left = cached_arr_[left_array_id]->GetView(left_id);
            CType right = cached_arr_[right_array_id]->GetView(right_id);
            bool is_left_nan = std::isnan(left);
            bool is_right_nan = std::isnan(right);
#ifdef DEBUG
            std::cout << "left is " << (is_left_nan ? "nan" : std::to_string(left))
                      << ", right is " << (is_right_nan ? "nan" : std::to_string(right))
                      << std::endl;
#endif
            if (!is_left_nan || !is_right_nan) {
              if (is_left_nan) {
                cmp_res = 1;
              } else if (is_right_nan) {
                cmp_res = 0;
              } else {
                if (left != right) {
                  cmp_res = left > right;
                }
              }
            }
          }
        };
      } else {
        // nulls_last, desc, !nan_check
        compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                               int64_t right_id, int& cmp_res) {
          if (null_total_count_ > 0) {
            bool is_left_null = cached_arr_[left_array_id]->null_count() > 0 &&
                                cached_arr_[left_array_id]->IsNull(left_id);
            bool is_right_null = cached_arr_[right_array_id]->null_count() > 0 &&
                                 cached_arr_[right_array_id]->IsNull(right_id);
            if (!is_left_null || !is_right_null) {
              if (is_left_null) {
#ifdef DEBUG
                std::cout << "left is null, right is not null" << std::endl;
#endif
                cmp_res = 0;
              } else if (is_right_null) {
#ifdef DEBUG
                std::cout << "left is not null, right is null" << std::endl;
#endif
                cmp_res = 1;
              } else {
                CType left = cached_arr_[left_array_id]->GetView(left_id);
                CType right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
                std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
                if (left != right) {
                  cmp_res = left > right;
                }
              }
#ifdef DEBUG
            } else {
              std::cout << "left is null"
                        << ", right is null" << std::endl;
#endif
            }
          } else {
            CType left = cached_arr_[left_array_id]->GetView(left_id);
            CType right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
            std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
            if (left != right) {
              cmp_res = left > right;
            }
          }
        };
      }
    }
    return arrow::Status::OK();
  }

  std::function<void(int, int, int64_t, int64_t, int&)> GetCompareFunc(
      const arrow::ArrayVector& arrays, bool asc, bool nulls_first, bool nan_check) {
    null_total_count_ = 0;
    cached_arr_.clear();
    for (int array_id = 0; array_id < arrays.size(); array_id++) {
      null_total_count_ += arrays[array_id]->null_count();
      auto typed_array = std::dynamic_pointer_cast<ArrayType>(arrays[array_id]);
      cached_arr_.push_back(typed_array);
    }
    PrepareCompareFunc(asc, nulls_first, nan_check);
    return compare_func_;
  }

 private:
  using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
  std::vector<std::shared_ptr<ArrayType>> cached_arr_;
  std::shared_ptr<arrow::DataType> data_type_;
  uint64_t null_total_count_ = 0;
  std::function<void(int, int, int64_t, int64_t, int&)> compare_func_;
};

class StringComparator : public ComparatorBase {
 public:
  StringComparator() {}

  ~StringComparator() {}

  int64_t GetCacheSize() { return cached_arr_.size(); }

  arrow::Status AddArray(const std::shared_ptr<arrow::Array>& arr) {
    auto typed_arr_ = std::dynamic_pointer_cast<arrow::StringArray>(arr);
    cached_arr_.push_back(typed_arr_);
    null_total_count_ += typed_arr_->null_count();
    return arrow::Status::OK();
  }

  arrow::Status ReleaseArray(const uint16_t& array_id) {
    if (cached_arr_.size() > array_id) {
      null_total_count_ -= cached_arr_[array_id]->null_count();
      cached_arr_[array_id] = nullptr;
    }
    return arrow::Status::OK();
  }

  int Compare(int left_array_id, int right_array_id, int64_t left_id, int64_t right_id) {
    int res = 2;
    compare_func_(left_array_id, right_array_id, left_id, right_id, res);
    return res;
  }

  arrow::Status PrepareCompareFunc(bool asc = true, bool nulls_first = true,
                                   bool nan_check = false,
                                   bool always_check_null = false) {
    if (null_total_count_ == 0 && !always_check_null) {
      if (asc) {
        compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                               int64_t right_id, int& cmp_res) {
          std::string left = cached_arr_[left_array_id]->GetString(left_id);
          std::string right = cached_arr_[right_array_id]->GetString(right_id);
#ifdef DEBUG
          std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
          if (left != right) {
            cmp_res = left < right;
          }
        };
      } else {
        compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                               int64_t right_id, int& cmp_res) {
          std::string left = cached_arr_[left_array_id]->GetString(left_id);
          std::string right = cached_arr_[right_array_id]->GetString(right_id);
#ifdef DEBUG
          std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
          if (left != right) {
            cmp_res = left > right;
          }
        };
      }
    } else if (asc) {
      if (nulls_first) {
        compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                               int64_t right_id, int& cmp_res) {
          if (null_total_count_ > 0) {
            bool is_left_null = cached_arr_[left_array_id]->null_count() > 0 &&
                                cached_arr_[left_array_id]->IsNull(left_id);
            bool is_right_null = cached_arr_[right_array_id]->null_count() > 0 &&
                                 cached_arr_[right_array_id]->IsNull(right_id);
            if (!is_left_null || !is_right_null) {
              if (is_left_null) {
#ifdef DEBUG
                std::cout << "left is null, right is not null" << std::endl;
#endif
                cmp_res = 1;
              } else if (is_right_null) {
#ifdef DEBUG
                std::cout << "left is not null, right is null" << std::endl;
#endif
                cmp_res = 0;
              } else {
                std::string left = cached_arr_[left_array_id]->GetString(left_id);
                std::string right = cached_arr_[right_array_id]->GetString(right_id);
#ifdef DEBUG
                std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
                if (left != right) {
                  cmp_res = left < right;
                }
              }
#ifdef DEBUG
            } else {
              std::cout << "left is null, right is null" << std::endl;
#endif
            }
          } else {
            std::string left = cached_arr_[left_array_id]->GetString(left_id);
            std::string right = cached_arr_[right_array_id]->GetString(right_id);
#ifdef DEBUG
            std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
            if (left != right) {
              cmp_res = left < right;
            }
          }
        };
      } else {
        compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                               int64_t right_id, int& cmp_res) {
          if (null_total_count_ > 0) {
            bool is_left_null = cached_arr_[left_array_id]->null_count() > 0 &&
                                cached_arr_[left_array_id]->IsNull(left_id);
            bool is_right_null = cached_arr_[right_array_id]->null_count() > 0 &&
                                 cached_arr_[right_array_id]->IsNull(right_id);
            if (!is_left_null || !is_right_null) {
              if (is_left_null) {
#ifdef DEBUG
                std::cout << "left is null, right is not null" << std::endl;
#endif
                cmp_res = 0;
              } else if (is_right_null) {
#ifdef DEBUG
                std::cout << "left is not null, right is null" << std::endl;
#endif
                cmp_res = 1;
              } else {
                std::string left = cached_arr_[left_array_id]->GetString(left_id);
                std::string right = cached_arr_[right_array_id]->GetString(right_id);
#ifdef DEBUG
                std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
                if (left != right) {
                  cmp_res = left < right;
                }
              }
#ifdef DEBUG
            } else {
              std::cout << "left is null"
                        << ", right is null" << std::endl;
#endif
            }
          } else {
            std::string left = cached_arr_[left_array_id]->GetString(left_id);
            std::string right = cached_arr_[right_array_id]->GetString(right_id);
#ifdef DEBUG
            std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
            if (left != right) {
              cmp_res = left < right;
            }
          }
        };
      }
    } else if (nulls_first) {
      compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                             int64_t right_id, int& cmp_res) {
        if (null_total_count_ > 0) {
          bool is_left_null = cached_arr_[left_array_id]->null_count() > 0 &&
                              cached_arr_[left_array_id]->IsNull(left_id);
          bool is_right_null = cached_arr_[right_array_id]->null_count() > 0 &&
                               cached_arr_[right_array_id]->IsNull(right_id);
          if (!is_left_null || !is_right_null) {
            if (is_left_null) {
#ifdef DEBUG
              std::cout << "left is null, right is null" << std::endl;
#endif
              cmp_res = 1;
            } else if (is_right_null) {
#ifdef DEBUG
              std::cout << "left is not null, right is null" << std::endl;
#endif
              cmp_res = 0;
            } else {
              std::string left = cached_arr_[left_array_id]->GetString(left_id);
              std::string right = cached_arr_[right_array_id]->GetString(right_id);
#ifdef DEBUG
              std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
              if (left != right) {
                cmp_res = left > right;
              }
            }
#ifdef DEBUG
          } else {
            std::cout << "left is null, right is null" << std::endl;
#endif
          }
        } else {
          std::string left = cached_arr_[left_array_id]->GetString(left_id);
          std::string right = cached_arr_[right_array_id]->GetString(right_id);
#ifdef DEBUG
          std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
          if (left != right) {
            cmp_res = left > right;
          }
        }
      };
    } else {
      compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                             int64_t right_id, int& cmp_res) {
        if (null_total_count_ > 0) {
          bool is_left_null = cached_arr_[left_array_id]->null_count() > 0 &&
                              cached_arr_[left_array_id]->IsNull(left_id);
          bool is_right_null = cached_arr_[right_array_id]->null_count() > 0 &&
                               cached_arr_[right_array_id]->IsNull(right_id);
          if (!is_left_null || !is_right_null) {
            if (is_left_null) {
#ifdef DEBUG
              std::cout << "left is null, right is null" << std::endl;
#endif
              cmp_res = 0;
            } else if (is_right_null) {
#ifdef DEBUG
              std::cout << "left is not null, right is null" << std::endl;
#endif
              cmp_res = 1;
            } else {
              std::string left = cached_arr_[left_array_id]->GetString(left_id);
              std::string right = cached_arr_[right_array_id]->GetString(right_id);
#ifdef DEBUG
              std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
              if (left != right) {
                cmp_res = left > right;
              }
            }
#ifdef DEBUG
          } else {
            std::cout << "left is null, right is null" << std::endl;
#endif
          }
        } else {
          std::string left = cached_arr_[left_array_id]->GetString(left_id);
          std::string right = cached_arr_[right_array_id]->GetString(right_id);
#ifdef DEBUG
          std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
          if (left != right) {
            cmp_res = left > right;
          }
        }
      };
    }
    return arrow::Status::OK();
  }

  std::function<void(int, int, int64_t, int64_t, int&)> GetCompareFunc(
      const arrow::ArrayVector& arrays, bool asc, bool nulls_first) {
    null_total_count_ = 0;
    cached_arr_.clear();
    for (int array_id = 0; array_id < arrays.size(); array_id++) {
      null_total_count_ += arrays[array_id]->null_count();
      auto typed_array = std::dynamic_pointer_cast<arrow::StringArray>(arrays[array_id]);
      cached_arr_.push_back(typed_array);
    }
    PrepareCompareFunc(asc, nulls_first);
    return compare_func_;
  }

 private:
  std::vector<std::shared_ptr<arrow::StringArray>> cached_arr_;
  std::shared_ptr<arrow::DataType> data_type_;
  uint64_t null_total_count_ = 0;
  std::function<void(int, int, int64_t, int64_t, int&)> compare_func_;
};

class DecimalComparator : public ComparatorBase {
 public:
  DecimalComparator() {}

  ~DecimalComparator() {}

  int64_t GetCacheSize() { return cached_arr_.size(); }

  arrow::Status AddArray(const std::shared_ptr<arrow::Array>& arr) {
    auto typed_arr_ = std::make_shared<Decimal128Array>(arr);
    cached_arr_.push_back(typed_arr_);
    null_total_count_ += typed_arr_->null_count();
    return arrow::Status::OK();
  }

  arrow::Status ReleaseArray(const uint16_t& array_id) {
    if (cached_arr_.size() > array_id) {
      null_total_count_ -= cached_arr_[array_id]->null_count();
      cached_arr_[array_id] = nullptr;
    }
    return arrow::Status::OK();
  }

  int Compare(int left_array_id, int right_array_id, int64_t left_id, int64_t right_id) {
    int res = 2;
    compare_func_(left_array_id, right_array_id, left_id, right_id, res);
    return res;
  }

  arrow::Status PrepareCompareFunc(bool asc = true, bool nulls_first = true,
                                   bool nan_check = true,
                                   bool always_check_null = false) {
    if (null_total_count_ == 0 && !always_check_null) {
      if (asc) {
        compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                               int64_t right_id, int& cmp_res) {
          arrow::Decimal128 left = cached_arr_[left_array_id]->GetView(left_id);
          arrow::Decimal128 right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
          std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
          if (left != right) {
            cmp_res = left < right;
          }
        };
      } else {
        compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                               int64_t right_id, int& cmp_res) {
          arrow::Decimal128 left = cached_arr_[left_array_id]->GetView(left_id);
          arrow::Decimal128 right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
          std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
          if (left != right) {
            cmp_res = left > right;
          }
        };
      }
    } else if (asc) {
      if (nulls_first) {
        compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                               int64_t right_id, int& cmp_res) {
          if (null_total_count_ > 0) {
            bool is_left_null = cached_arr_[left_array_id]->null_count() > 0 &&
                                cached_arr_[left_array_id]->IsNull(left_id);
            bool is_right_null = cached_arr_[right_array_id]->null_count() > 0 &&
                                 cached_arr_[right_array_id]->IsNull(right_id);
            if (!is_left_null || !is_right_null) {
              if (is_left_null) {
#ifdef DEBUG
                std::cout << "left is null, right is not null" << std::endl;
#endif
                cmp_res = 1;
              } else if (is_right_null) {
#ifdef DEBUG
                std::cout << "left is not null, right is null" << std::endl;
#endif
                cmp_res = 0;
              } else {
                arrow::Decimal128 left = cached_arr_[left_array_id]->GetView(left_id);
                arrow::Decimal128 right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
                std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
                if (left != right) {
                  cmp_res = left < right;
                }
              }
#ifdef DEBUG
            } else {
              std::cout << "left is null"
                        << ", right is null" << std::endl;
#endif
            }
          } else {
            arrow::Decimal128 left = cached_arr_[left_array_id]->GetView(left_id);
            arrow::Decimal128 right = cached_arr_[right_array_id]->GetView(right_id);
            if (left != right) {
              cmp_res = left < right;
            }
          }
        };
      } else {
        compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                               int64_t right_id, int& cmp_res) {
          if (null_total_count_ > 0) {
            bool is_left_null = cached_arr_[left_array_id]->null_count() > 0 &&
                                cached_arr_[left_array_id]->IsNull(left_id);
            bool is_right_null = cached_arr_[right_array_id]->null_count() > 0 &&
                                 cached_arr_[right_array_id]->IsNull(right_id);
            if (!is_left_null || !is_right_null) {
              if (is_left_null) {
#ifdef DEBUG
                std::cout << "left is null, right is not null" << std::endl;
#endif
                cmp_res = 0;
              } else if (is_right_null) {
#ifdef DEBUG
                std::cout << "left is not null, right is null" << std::endl;
#endif
                cmp_res = 1;
              } else {
                arrow::Decimal128 left = cached_arr_[left_array_id]->GetView(left_id);
                arrow::Decimal128 right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
                std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
                if (left != right) {
                  cmp_res = left < right;
                }
              }
#ifdef DEBUG
            } else {
              std::cout << "left is null"
                        << ", right is null" << std::endl;
#endif
            }
          } else {
            arrow::Decimal128 left = cached_arr_[left_array_id]->GetView(left_id);
            arrow::Decimal128 right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
            std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
            if (left != right) {
              cmp_res = left < right;
            }
          }
        };
      }
    } else if (nulls_first) {
      compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                             int64_t right_id, int& cmp_res) {
        if (null_total_count_ > 0) {
          bool is_left_null = cached_arr_[left_array_id]->null_count() > 0 &&
                              cached_arr_[left_array_id]->IsNull(left_id);
          bool is_right_null = cached_arr_[right_array_id]->null_count() > 0 &&
                               cached_arr_[right_array_id]->IsNull(right_id);
          if (!is_left_null || !is_right_null) {
            if (is_left_null) {
#ifdef DEBUG
              std::cout << "left is null, right is not null" << std::endl;
#endif
              cmp_res = 1;
            } else if (is_right_null) {
#ifdef DEBUG
              std::cout << "left is not null, right is null" << std::endl;
#endif
              cmp_res = 0;
            } else {
              arrow::Decimal128 left = cached_arr_[left_array_id]->GetView(left_id);
              arrow::Decimal128 right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
              std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
              if (left != right) {
                cmp_res = left > right;
              }
            }
#ifdef DEBUG
          } else {
            std::cout << "left is null"
                      << ", right is null" << std::endl;
#endif
          }
        } else {
          arrow::Decimal128 left = cached_arr_[left_array_id]->GetView(left_id);
          arrow::Decimal128 right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
          std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
          if (left != right) {
            cmp_res = left > right;
          }
        }
      };
    } else {
      compare_func_ = [this](int left_array_id, int right_array_id, int64_t left_id,
                             int64_t right_id, int& cmp_res) {
        if (null_total_count_ > 0) {
          bool is_left_null = cached_arr_[left_array_id]->null_count() > 0 &&
                              cached_arr_[left_array_id]->IsNull(left_id);
          bool is_right_null = cached_arr_[right_array_id]->null_count() > 0 &&
                               cached_arr_[right_array_id]->IsNull(right_id);
          if (!is_left_null || !is_right_null) {
            if (is_left_null) {
#ifdef DEBUG
              std::cout << "left is null, right is not null" << std::endl;
#endif
              cmp_res = 0;
            } else if (is_right_null) {
#ifdef DEBUG
              std::cout << "left is not null, right is null" << std::endl;
#endif
              cmp_res = 1;
            } else {
              arrow::Decimal128 left = cached_arr_[left_array_id]->GetView(left_id);
              arrow::Decimal128 right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
              std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
              if (left != right) {
                cmp_res = left > right;
              }
            }
#ifdef DEBUG
          } else {
            std::cout << "left is null"
                      << ", right is null" << std::endl;
#endif
          }
        } else {
          arrow::Decimal128 left = cached_arr_[left_array_id]->GetView(left_id);
          arrow::Decimal128 right = cached_arr_[right_array_id]->GetView(right_id);
#ifdef DEBUG
          std::cout << "left is " << left << ", right is " << right << std::endl;
#endif
          if (left != right) {
            cmp_res = left > right;
          }
        }
      };
    }
    return arrow::Status::OK();
  }

  std::function<void(int, int, int64_t, int64_t, int&)> GetCompareFunc(
      const arrow::ArrayVector& arrays, bool asc, bool nulls_first) {
    null_total_count_ = 0;
    cached_arr_.clear();
    for (int array_id = 0; array_id < arrays.size(); array_id++) {
      null_total_count_ += arrays[array_id]->null_count();
      auto typed_array = std::make_shared<Decimal128Array>(arrays[array_id]);
      cached_arr_.push_back(typed_array);
    }
    PrepareCompareFunc(asc, nulls_first);
    return compare_func_;
  }

 private:
  std::vector<std::shared_ptr<Decimal128Array>> cached_arr_;
  std::shared_ptr<arrow::DataType> data_type_;
  uint64_t null_total_count_ = 0;
  std::function<void(int, int, int64_t, int64_t, int&)> compare_func_;
};

template <typename T>
struct ComparatorTypeTraits {};

template <>
struct ComparatorTypeTraits<arrow::StringType> {
  using ComparatorType = StringComparator;
};

template <>
struct ComparatorTypeTraits<arrow::DoubleType> {
  using ComparatorType = FloatingComparator<arrow::DoubleType, double>;
};

template <>
struct ComparatorTypeTraits<arrow::FloatType> {
  using ComparatorType = FloatingComparator<arrow::FloatType, double>;
};

template <>
struct ComparatorTypeTraits<arrow::Decimal128Type> {
  using ComparatorType = DecimalComparator;
};

class ComparatorKeyProjector {
 public:
  ComparatorKeyProjector(std::vector<int> idx_list)
      : key_idx_list_(idx_list), num_columns_(idx_list.size()) {}
  ComparatorKeyProjector(int num_columns,
                         std::shared_ptr<gandiva::Projector> key_projector)
      : key_projector_(key_projector),
        need_projection_(true),
        num_columns_(num_columns) {}
  int num_columns_ = 0;
  bool need_projection_ = false;
  std::shared_ptr<gandiva::Projector> key_projector_;
  std::vector<int> key_idx_list_;
};

#define PROCESS(DATATYPE, CTYPE)                             \
  template <>                                                \
  struct ComparatorTypeTraits<DATATYPE> {                    \
    using ComparatorType = TypedComparator<DATATYPE, CTYPE>; \
  };
PROCESS(arrow::BooleanType, bool)
PROCESS(arrow::UInt8Type, uint8_t)
PROCESS(arrow::Int8Type, int8_t)
PROCESS(arrow::UInt16Type, uint16_t)
PROCESS(arrow::Int16Type, int16_t)
PROCESS(arrow::UInt32Type, uint32_t)
PROCESS(arrow::Int32Type, int32_t)
PROCESS(arrow::UInt64Type, uint64_t)
PROCESS(arrow::Int64Type, int64_t)
PROCESS(arrow::Date32Type, int32_t)
PROCESS(arrow::Date64Type, int64_t)
PROCESS(arrow::TimestampType, int64_t)
#undef PROCESS

template <typename DATATYPE>
static arrow::Status MakeComparator(bool asc, bool null_first, bool nan_check,
                                    std::shared_ptr<ComparatorBase>* out) {
  using ComparatorType = typename ComparatorTypeTraits<DATATYPE>::ComparatorType;
  auto cmp = std::make_shared<ComparatorType>();
  cmp->PrepareCompareFunc(asc, null_first, nan_check, true);
  *out = std::dynamic_pointer_cast<ComparatorBase>(cmp);
  return arrow::Status::OK();
}

#define PROCESS_SUPPORTED_TYPES(PROCESS) \
  PROCESS(arrow::BooleanType)            \
  PROCESS(arrow::UInt8Type)              \
  PROCESS(arrow::Int8Type)               \
  PROCESS(arrow::UInt16Type)             \
  PROCESS(arrow::Int16Type)              \
  PROCESS(arrow::UInt32Type)             \
  PROCESS(arrow::Int32Type)              \
  PROCESS(arrow::UInt64Type)             \
  PROCESS(arrow::Int64Type)              \
  PROCESS(arrow::Date32Type)             \
  PROCESS(arrow::Date64Type)             \
  PROCESS(arrow::TimestampType)
static arrow::Status MakeComparator(std::shared_ptr<arrow::DataType> type, bool asc,
                                    bool null_first, bool nan_check,
                                    std::shared_ptr<ComparatorBase>* out) {
  if (type->id() == arrow::Type::STRING) {
    auto cmp = std::make_shared<StringComparator>();
    cmp->PrepareCompareFunc(asc, null_first, nan_check, true);
    *out = std::dynamic_pointer_cast<ComparatorBase>(cmp);
  } else if (type->id() == arrow::Type::DOUBLE) {
    auto cmp = std::make_shared<FloatingComparator<arrow::DoubleType, double>>();
    cmp->PrepareCompareFunc(asc, null_first, nan_check, true);
    *out = std::dynamic_pointer_cast<ComparatorBase>(cmp);
  } else if (type->id() == arrow::Type::FLOAT) {
    auto cmp = std::make_shared<FloatingComparator<arrow::FloatType, float>>();
    cmp->PrepareCompareFunc(asc, null_first, nan_check, true);
    *out = std::dynamic_pointer_cast<ComparatorBase>(cmp);
  } else if (type->id() == arrow::Type::DECIMAL128) {
    auto cmp = std::make_shared<DecimalComparator>();
    cmp->PrepareCompareFunc(asc, null_first, nan_check, true);
    *out = std::dynamic_pointer_cast<ComparatorBase>(cmp);
  } else {
    switch (type->id()) {
#define PROCESS(InType)                                            \
  case InType::type_id: {                                          \
    using CType = typename arrow::TypeTraits<InType>::CType;       \
    auto cmp = std::make_shared<TypedComparator<InType, CType>>(); \
    cmp->PrepareCompareFunc(asc, null_first, nan_check, true);     \
    *out = std::dynamic_pointer_cast<ComparatorBase>(cmp);         \
  } break;
      PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
      default: {
        std::cout << "MakeComparator type not supported, type is " << type << std::endl;
      } break;
    }
  }
  return arrow::Status::OK();
}
#undef PROCESS_SUPPORTED_TYPES

#define PROCESS_SUPPORTED_TYPES(PROCESS) \
  PROCESS(arrow::BooleanType)            \
  PROCESS(arrow::UInt8Type)              \
  PROCESS(arrow::Int8Type)               \
  PROCESS(arrow::UInt16Type)             \
  PROCESS(arrow::Int16Type)              \
  PROCESS(arrow::UInt32Type)             \
  PROCESS(arrow::Int32Type)              \
  PROCESS(arrow::UInt64Type)             \
  PROCESS(arrow::Int64Type)              \
  PROCESS(arrow::Date32Type)             \
  PROCESS(arrow::Date64Type)             \
  PROCESS(arrow::TimestampType)
static arrow::Status MakeCmpFunction(
    const std::vector<arrow::ArrayVector>& array_vectors,
    const std::vector<std::shared_ptr<arrow::Field>>& key_field_list,
    const std::vector<int>& key_index_list, const std::vector<bool>& sort_directions,
    const std::vector<bool>& nulls_order, const bool& nan_check,
    std::vector<std::shared_ptr<ComparatorBase>>& comparators,
    std::vector<std::function<void(int, int, int64_t, int64_t, int&)>>& cmp_functions) {
  for (int i = 0; i < key_field_list.size(); i++) {
    auto type = key_field_list[i]->type();
    int key_col_id = key_index_list[i];
    arrow::ArrayVector col = array_vectors[key_col_id];
    bool asc = sort_directions[i];
    bool nulls_first = nulls_order[i];
    if (type->id() == arrow::Type::STRING) {
      auto comparator_ptr = std::make_shared<StringComparator>();
      comparators.push_back(std::dynamic_pointer_cast<ComparatorBase>(comparator_ptr));
      cmp_functions.push_back(comparator_ptr->GetCompareFunc(col, asc, nulls_first));
    } else if (type->id() == arrow::Type::DOUBLE) {
      auto comparator_ptr =
          std::make_shared<FloatingComparator<arrow::DoubleType, double>>();
      comparators.push_back(std::dynamic_pointer_cast<ComparatorBase>(comparator_ptr));
      cmp_functions.push_back(
          comparator_ptr->GetCompareFunc(col, asc, nulls_first, nan_check));
    } else if (type->id() == arrow::Type::FLOAT) {
      auto comparator_ptr =
          std::make_shared<FloatingComparator<arrow::FloatType, float>>();
      comparators.push_back(std::dynamic_pointer_cast<ComparatorBase>(comparator_ptr));
      cmp_functions.push_back(
          comparator_ptr->GetCompareFunc(col, asc, nulls_first, nan_check));
    } else if (type->id() == arrow::Type::DECIMAL128) {
      auto comparator_ptr = std::make_shared<DecimalComparator>();
      comparators.push_back(std::dynamic_pointer_cast<ComparatorBase>(comparator_ptr));
      cmp_functions.push_back(comparator_ptr->GetCompareFunc(col, asc, nulls_first));
    } else {
      switch (type->id()) {
#define PROCESS(InType)                                                               \
  case InType::type_id: {                                                             \
    using CType = typename arrow::TypeTraits<InType>::CType;                          \
    auto comparator_ptr = std::make_shared<TypedComparator<InType, CType>>();         \
    comparators.push_back(std::dynamic_pointer_cast<ComparatorBase>(comparator_ptr)); \
    cmp_functions.push_back(comparator_ptr->GetCompareFunc(col, asc, nulls_first));   \
  } break;
        PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
        default: {
          std::cout << "MakeCmpFunction type not supported, type is " << type
                    << std::endl;
        } break;
      }
    }
  }
  return arrow::Status::OK();
}
#undef PROCESS_SUPPORTED_TYPES

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
