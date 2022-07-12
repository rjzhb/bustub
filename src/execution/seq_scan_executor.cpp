//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), iter_(nullptr, RID{}, nullptr) {
  plan_ = plan;
  exec_ctx_ = exec_ctx;
  Init();
}

void SeqScanExecutor::Init() {
  tableInfo_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  iter_ = tableInfo_->table_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ != tableInfo_->table_->End()) {
    std::vector<Value> values;
    const Schema *schema = plan_->OutputSchema();
    values.reserve(schema->GetColumnCount());
    auto value = plan_->GetPredicate()->Evaluate(&(*iter_), schema);
    if (value.GetAs<bool>()) {
      for (const Column &column : schema->GetColumns()) {
        values.push_back(column.GetExpr()->Evaluate(&(*iter_), schema));
      }
      *tuple = Tuple(values, schema);
      *rid = (*iter_).GetRid();
      iter_++;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
