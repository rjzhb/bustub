//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  exec_ctx_ = exec_ctx;
  child_executor_ = std::move(child_executor);
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan->TableOid());
  index_info_array_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  bool is_delete = false;
  if (child_executor_->Next(tuple, rid)) {
    is_delete = table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
  }
  if (is_delete && !index_info_array_) {
    for (auto &index_info : index_info_array_) {
      const auto tuple_key =
          tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(tuple_key, *rid, exec_ctx_->GetTransaction());
    }
  }
  return is_delete;
}

}  // namespace bustub
