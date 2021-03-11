#include <arrow/type_fwd.h>
namespace sparkcolumnarplugin {
namespace precompile {

class HashArraysKernel {
 public:
  HashArraysKernel(arrow::MemoryPool* pool,
                   const std::vector<std::shared_ptr<arrow::Field>>& field_list);
  arrow::Status Evaluate(const std::vector<std::shared_ptr<arrow::Array>>& in,
                         std::shared_ptr<arrow::Array>* out);

 private:
  class Impl;
  std::shared_ptr<Impl> impl_;
};
}  // namespace precompile
}  // namespace sparkcolumnarplugin