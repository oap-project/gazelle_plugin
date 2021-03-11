#include "precompile/type.h"

#include <arrow/type.h>

namespace sparkcolumnarplugin {
namespace precompile {

arrow::Status MakeFixedSizeBinaryType(int32_t byte_width,
                                      std::shared_ptr<arrow::FixedSizeBinaryType>* out) {
  *out = std::make_shared<arrow::FixedSizeBinaryType>(byte_width);
  return arrow::Status::OK();
}
}  // namespace precompile
}  // namespace sparkcolumnarplugin
