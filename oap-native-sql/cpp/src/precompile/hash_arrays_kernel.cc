#include "precompile/hash_arrays_kernel.h"

#include <gandiva/node.h>
#include <gandiva/projector.h>
#include <gandiva/tree_expr_builder.h>

namespace sparkcolumnarplugin {
namespace precompile {

class HashArraysKernel::Impl {
 public:
  Impl(arrow::MemoryPool* pool,
       const std::vector<std::shared_ptr<arrow::Field>>& field_list)
      : pool_(pool) {
    int index = 0;
    std::vector<std::shared_ptr<gandiva::Node>> func_node_list = {};
    for (auto field : field_list) {
      auto field_node = gandiva::TreeExprBuilder::MakeField(field);
      auto func_node =
          gandiva::TreeExprBuilder::MakeFunction("hash32", {field_node}, arrow::int32());
      func_node_list.push_back(func_node);
      if (func_node_list.size() == 2) {
        auto shift_func_node = gandiva::TreeExprBuilder::MakeFunction(
            "multiply",
            {func_node_list[0], gandiva::TreeExprBuilder::MakeLiteral((int32_t)10)},
            arrow::int32());
        auto tmp_func_node = gandiva::TreeExprBuilder::MakeFunction(
            "add", {shift_func_node, func_node_list[1]}, arrow::int32());
        func_node_list.clear();
        func_node_list.push_back(tmp_func_node);
      }
      index++;
    }
    auto expr = gandiva::TreeExprBuilder::MakeExpression(
        func_node_list[0], arrow::field("projection_key", arrow::int32()));
    schema_ = arrow::schema(field_list);
    auto configuration = gandiva::ConfigurationBuilder().DefaultConfiguration();
    auto status = gandiva::Projector::Make(schema_, {expr}, configuration, &projector_);
  }

  arrow::Status Evaluate(const std::vector<std::shared_ptr<arrow::Array>>& in,
                         std::shared_ptr<arrow::Array>* out) {
    auto length = in[0]->length();
    auto num_columns = in.size();

    auto in_batch = arrow::RecordBatch::Make(schema_, length, in);

    arrow::ArrayVector outputs;
    RETURN_NOT_OK(projector_->Evaluate(*in_batch.get(), pool_, &outputs));
    *out = outputs[0];

    return arrow::Status::OK();
  }

 private:
  std::shared_ptr<gandiva::Projector> projector_;
  std::shared_ptr<arrow::Schema> schema_;
  arrow::MemoryPool* pool_;
};

HashArraysKernel::HashArraysKernel(
    arrow::MemoryPool* pool,
    const std::vector<std::shared_ptr<arrow::Field>>& type_list) {
  impl_ = std::make_shared<Impl>(pool, type_list);
}

arrow::Status HashArraysKernel::Evaluate(
    const std::vector<std::shared_ptr<arrow::Array>>& in,
    std::shared_ptr<arrow::Array>* out) {
  return impl_->Evaluate(in, out);
}
}  // namespace precompile
}  // namespace sparkcolumnarplugin