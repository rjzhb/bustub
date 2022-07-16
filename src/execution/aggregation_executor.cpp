//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      table_(plan->GetAggregates(), plan->GetAggregateTypes()),
      iter_(table_.Begin()) {
  plan_ = plan;
  child_ = std::move(child);
  const auto &agg_exprs_ = plan_->GetAggregates();
  Tuple tuple;
  RID rid;
  auto keys = std::move(table_.GenerateInitialAggregateValue().aggregates_);
  child_->Init();
  // 含有groupby字段的时候，就进行key，value映射
  while (child_->Next(&tuple, &rid)) {
    if (!plan_->GetGroupBys().empty()) {
      const auto &group_bys = plan_->GetGroupBys();
      keys.clear();
      keys.reserve(group_bys.size());
      for (auto group_by : group_bys) {
        keys.emplace_back(group_by->Evaluate(&tuple, child_->GetOutputSchema()));
      }
    }
    std::vector<Value> values;
    for (auto agg_expr : agg_exprs_) {
      values.emplace_back(agg_expr->Evaluate(&tuple, child_->GetOutputSchema()));
    }
    table_.InsertCombine(AggregateKey{keys}, AggregateValue{values});
  }
}

void AggregationExecutor::Init() {
  iter_ = table_.Begin();
  child_->Init();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 迭代器进行遍历
  while (iter_ != table_.End()) {
   auto temp = iter_;
    ++iter_;
    // 用having过滤
    if (plan_->GetHaving() != nullptr) {
      Value value = plan_->GetHaving()->EvaluateAggregate(temp.Key().group_bys_, temp.Val().aggregates_);
      if (!value.GetAs<bool>()) {
        continue;
      }
    }
    std::vector<Value> values;
    values.reserve(plan_->OutputSchema()->GetColumnCount());
    // 拿到tuple
    for (const Column &column : plan_->OutputSchema()->GetColumns()) {
      values.emplace_back(column.GetExpr()->EvaluateAggregate(temp.Key().group_bys_, temp.Val().aggregates_));
    }
    *tuple = Tuple(values, plan_->OutputSchema());
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
