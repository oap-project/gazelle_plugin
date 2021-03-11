#include <cstdint>
#include <functional>
#include <string>

#include "codegen/arrow_compute/ext/array_item_index.h"

using namespace sparkcolumnarplugin::codegen::arrowcompute::extra;
namespace sparkcolumnarplugin {
namespace precompile {
#define TYPED_ASC_SORT_DEFINE(CTYPE) \
  void sort_asc(ArrayItemIndex*, ArrayItemIndex*, std::function<CTYPE(ArrayItemIndex)>);

TYPED_ASC_SORT_DEFINE(int32_t)
TYPED_ASC_SORT_DEFINE(uint32_t)
TYPED_ASC_SORT_DEFINE(int64_t)
TYPED_ASC_SORT_DEFINE(uint64_t)
TYPED_ASC_SORT_DEFINE(float)
TYPED_ASC_SORT_DEFINE(double)
TYPED_ASC_SORT_DEFINE(std::string)

void sort_desc(ArrayItemIndex*, ArrayItemIndex*,
               std::function<bool(ArrayItemIndex, ArrayItemIndex)>);

}  // namespace precompile
}  // namespace sparkcolumnarplugin