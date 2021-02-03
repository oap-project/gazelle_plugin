#pragma once

#include <arrow/type_fwd.h>

namespace sparkcolumnarplugin {
namespace precompile {

arrow::Status MakeFixedSizeBinaryType(int32_t byte_width,
                                      std::shared_ptr<arrow::FixedSizeBinaryType>*);

}  // namespace precompile
}  // namespace sparkcolumnarplugin