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
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx) {
  exec_ctx_ = exec_ctx;
  plan_ = plan;
  left_child_executor_ = std::move(left_child);
  right_child_executor_ = std::move(right_child);
  Tuple left_tuple;
  RID left_rid;
  left_child_executor_->Init();
  right_child_executor_->Init();
  while (left_child_executor_->Next(&left_tuple, &left_rid)) {
    Value value = plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple, plan_->GetLeftPlan()->OutputSchema());
    std::vector<Value> values;
    HashJoinKey key{value};
    uint32_t column_count = plan_->GetLeftPlan()->OutputSchema()->GetColumnCount();
    for (size_t i = 0; i < column_count; i++) {
      values.emplace_back(left_tuple.GetValue(plan_->GetLeftPlan()->OutputSchema(), i));
    }
    if (hash_table_.count(key) > 0) {
      hash_table_[key].emplace_back(values);
    } else {
      hash_table_.insert({key, {values}});
    }
  }
}

void HashJoinExecutor::Init() {
  left_child_executor_->Init();
  right_child_executor_->Init();
  next_pos_ = 0;
  outer_buffer_table_.clear();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (next_pos_ >= outer_buffer_table_.size()) {
    bool is_find = false;
    while (right_child_executor_->Next(tuple, rid)) {
      Tuple right_tuple;
      RID right_rid;
      Value value = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple, plan_->OutputSchema());
      HashJoinKey key{value};
      if (hash_table_.find(key) != hash_table_.end()) {
        is_find = true;
        outer_buffer_table_ = hash_table_[key];
        next_pos_ = 0;
        break;
      }
    }
    if (!is_find) {
      return false;
    }
  }
  std::vector<Value> values;
  for (const Column &column : plan_->GetRightPlan()->OutputSchema()->GetColumns()) {
    ColumnValueExpression *expr = reinterpret_cast<ColumnValueExpression *>(column.GetExpr());
    if (expr->GetTupleIdx() == 0) {
      values.emplace_back(outer_buffer_table_[next_pos_][expr->GetColIdx()]);
    } else {
      values.emplace_back(tuple->GetValue(plan_->GetRightPlan()->OutputSchema(), expr->GetColIdx()));
    }
  }
  *tuple = Tuple(values, plan_->OutputSchema());
  next_pos_++;
  return true;
}

}  // namespace bustub
