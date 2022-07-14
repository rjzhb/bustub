//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  exec_ctx_ = exec_ctx;
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void DistinctExecutor::Init() {
  child_executor_->Init();
  set_.clear();
}

auto DistinctExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (child_executor_->Next(tuple, rid)) {
    DistinctKey key;
    for (size_t i = 0; i < plan_->OutputSchema()->GetColumnCount(); i++) {
      key.values_.emplace_back(tuple->GetValue(plan_->OutputSchema(), i));
    }
    if (set_.count(key) <= 0) {
      set_.insert(std::move(key));
      return true;
    }
  }
  return false;
}

}  // namespace bustub