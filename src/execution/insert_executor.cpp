//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), child_executor_(std::move(child_executor)) {
  plan_ = plan;
  exec_ctx_ = exec_ctx;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_array_ = exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void InsertExecutor::Init() {
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
  next_insert_ = 0;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  bool is_insert = false;
  if (plan_->IsRawInsert()) {
    std::vector<std::vector<Value>> raw_vals = plan_->RawValues();
    if (next_insert_ != raw_vals.size()) {
      *tuple = Tuple(raw_vals[next_insert_++], &table_info_->schema_);
      is_insert = table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
    }
  } else if (child_executor_->Next(tuple, rid)) {
    is_insert = table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
  }

  // 插入索引
  if (is_insert && !index_array_.empty()) {
    for (auto &index_info : index_array_) {
      const auto key_tuple =
          tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
    }
  }
  return is_insert;
}

}  // namespace bustub
