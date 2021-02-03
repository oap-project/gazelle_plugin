#include "precompile/sort.h"

#include "third_party/ska_sort.hpp"

using namespace sparkcolumnarplugin::codegen::arrowcompute::extra;
namespace sparkcolumnarplugin {
namespace precompile {
#define TYPED_ASC_SORT_IMPL(CTYPE)                                  \
  void sort_asc(ArrayItemIndex* begin, ArrayItemIndex* end,         \
                std::function<CTYPE(ArrayItemIndex)> extract_key) { \
    ska_sort(begin, end, extract_key);                              \
  }

TYPED_ASC_SORT_IMPL(int32_t)
TYPED_ASC_SORT_IMPL(uint32_t)
TYPED_ASC_SORT_IMPL(int64_t)
TYPED_ASC_SORT_IMPL(uint64_t)
TYPED_ASC_SORT_IMPL(float)
TYPED_ASC_SORT_IMPL(double)
TYPED_ASC_SORT_IMPL(std::string)

void sort_desc(ArrayItemIndex* begin, ArrayItemIndex* end,
               std::function<bool(ArrayItemIndex, ArrayItemIndex)> comp) {
  // std::sort(begin, end, *comp.target<bool (*)(ArrayItemIndex, ArrayItemIndex)>());
  std::sort(begin, end, comp);
}
}  // namespace precompile
}  // namespace sparkcolumnarplugin
