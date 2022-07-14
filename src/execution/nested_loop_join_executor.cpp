//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "execution/expressions/constant_value_expression.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  left_executor_ = std::move(left_executor);
  right_executor_ = std::move(right_executor);
  exec_ctx_ = exec_ctx;
  plan_ = plan;
}

NestedLoopJoinExecutor::~NestedLoopJoinExecutor() {
  if (is_alloc_) {
    delete predicate_;
  }
  predicate_ = nullptr;
}
void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  if (plan_->Predicate()) {
    predicate_ = plan_->Predicate();
  } else {
    is_alloc_ = true;
    predicate_ = new ConstantValueExpression(ValueFactory::GetBooleanValue(true));
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple left_tuple;
  RID left_rid;
  if (!left_executor_->Next(&left_tuple, &left_rid)) {
    return false;
  }
  Tuple right_tuple;
  RID right_rid;
  if (!right_executor_->Next(&right_tuple, &right_rid)) {
    return false;
  }
  auto value = predicate_->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                                        right_executor_->GetOutputSchema());
  if (value.GetAs<bool>()) {
    std::vector<Value> values;
    for (const auto &column : plan_->OutputSchema()->GetColumns()) {
      values.emplace_back(column.GetExpr()->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                                                         right_executor_->GetOutputSchema()));
    }
    *tuple=Tuple(values,plan_->OutputSchema());
    *rid = tuple->GetRid();
    return true;
  }
  return false;
}

}  // namespace bustub
