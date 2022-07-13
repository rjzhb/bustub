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

void DistinctExecutor::Init() { child_executor_->Init(); }

auto DistinctExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (child_executor_->Next(tuple, rid)) {
    if (set_.empty() || set_.find(tuple->GetData()) != set_.end()) {
      set_.insert(tuple->GetData());
      return true;
    }
  }
  return false;
}

}  // namespace bustub
