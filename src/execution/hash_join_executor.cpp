//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx) {
  exec_ctx_ = exec_ctx;
  plan_ = plan;
  left_child_executor_ = std::move(left_child);
  right_child_executor_ = std::move(right_child);
  Tuple tuple;
  RID rid;
  if(left_child_executor_->Next(&tuple, &rid)) {
    std::vector<Value> values;
    HashJoinKey key{};
  }
}

void HashJoinExecutor::Init() {
  left_child_executor_->Init();
  right_child_executor_->Init();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while(right_child_executor_->Next(tuple,rid)){

    for(const Column column:plan_->OutputSchema()->GetColumns()){

    }
  }
  return false;
}

}  // namespace bustub
