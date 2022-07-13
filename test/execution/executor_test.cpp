//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// executor_test.cpp
//
// Identification: test/execution/executor_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <numeric>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_engine.h"
#include "execution/executor_context.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/executors/insert_executor.h"
#include "execution/executors/nested_loop_join_executor.h"
#include "execution/expressions/aggregate_value_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/delete_plan.h"
#include "execution/plans/distinct_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/update_plan.h"
#include "executor_test_util.h"  // NOLINT
#include "gtest/gtest.h"
#include "storage/table/tuple.h"
#include "test_util.h"  // NOLINT
#include "type/value_factory.h"

/**
 * This file contains basic tests for the functionality of all nine
 * executors required for Fall 2021 Project 3: Query Execution. In
 * particular, the tests in this file include:
 *
 * - Sequential Scan
 * - Insert (Raw)
 * - Insert (Select)
 * - Update
 * - Delete
 * - Nested Loop Join
 * - Hash Join
 * - Aggregation
 * - Limit
 * - Distinct
 *
 * Each of the tests demonstrates how to construct a query plan for
 * a particular executors. Students should be able to learn from and
 * extend these example usages to write their own tests for the
 * correct functionality of their executors.
 *
 * Each of the tests in this file uses the `ExecutorTest` unit test
 * fixture. This class is defined in the header:
 *
 * `test/execution/executor_test_util.h`
 *
 * This text fixture takes care of many of the steps required to set
 * up the system for execution engine tests. For example, it initializes
 * key DBMS components, such as the disk manager, the  buffer pool manager,
 * and the catalog, among others. Furthermore, this text fixture also
 * populates the test tables used by all unit tests. This is accomplished
 * with the help of the `TableGenerator` class via a call to `GenerateTestTables()`.
 *
 * See the definition of `TableGenerator::GenerateTestTables()` for the
 * schema of each of the tables used in the tests below. The definition of
 * this function is in `src/catalog/table_generator.cpp`.
 */

namespace bustub {

// Parameters for index construction
using KeyType = GenericKey<8>;
using ValueType = RID;
using ComparatorType = GenericComparator<8>;
using HashFunctionType = HashFunction<KeyType>;

// SELECT col_a, col_b FROM test_1 WHERE col_a < 500
TEST_F(ExecutorTest, SimpleSeqScanTest) {
  // Construct query plan
  TableInfo *table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
  const Schema &schema = table_info->schema_;
  auto *col_a = MakeColumnValueExpression(schema, 0, "colA");
  auto *col_b = MakeColumnValueExpression(schema, 0, "colB");
  auto *const500 = MakeConstantValueExpression(ValueFactory::GetIntegerValue(500));
  auto *predicate = MakeComparisonExpression(col_a, const500, ComparisonType::LessThan);
  auto *out_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
  SeqScanPlanNode plan{out_schema, predicate, table_info->oid_};

  // Execute
  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(&plan, &result_set, GetTxn(), GetExecutorContext());

  // Verify
  ASSERT_EQ(result_set.size(), 500);
  for (const auto &tuple : result_set) {
    ASSERT_TRUE(tuple.GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>() < 500);
    ASSERT_TRUE(tuple.GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>() < 10);
  }
}

// INSERT INTO empty_table2 VALUES (100, 10), (101, 11), (102, 12)
TEST_F(ExecutorTest, SimpleRawInsertTest) {
  // Create Values to insert
  std::vector<Value> val1{ValueFactory::GetIntegerValue(100), ValueFactory::GetIntegerValue(10)};
  std::vector<Value> val2{ValueFactory::GetIntegerValue(101), ValueFactory::GetIntegerValue(11)};
  std::vector<Value> val3{ValueFactory::GetIntegerValue(102), ValueFactory::GetIntegerValue(12)};
  std::vector<std::vector<Value>> raw_vals{val1, val2, val3};

  // Create insert plan node
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table2");
  InsertPlanNode insert_plan{std::move(raw_vals), table_info->oid_};

  GetExecutionEngine()->Execute(&insert_plan, nullptr, GetTxn(), GetExecutorContext());

  // Iterate through table make sure that values were inserted.

  // SELECT * FROM empty_table2;
  const auto &schema = table_info->schema_;
  auto col_a = MakeColumnValueExpression(schema, 0, "colA");
  auto col_b = MakeColumnValueExpression(schema, 0, "colB");
  auto out_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
  SeqScanPlanNode scan_plan{out_schema, nullptr, table_info->oid_};

  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(&scan_plan, &result_set, GetTxn(), GetExecutorContext());

  // Size
  ASSERT_EQ(result_set.size(), 3);

  // First value
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 100);
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 10);

  // Second value
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 101);
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 11);

  // Third value
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 102);
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 12);
}

}  // namespace bustub