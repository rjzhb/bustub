//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

namespace bustub {

auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  /**
   * 如果已经 abort，直接 return false。
如果 IsolationLevel 是 READ_UNCOMMITTED，直接 abort，它不需要读锁。
如果不是处于 2PL 的 GROWING 阶段，直接 abort。
如果已经获取过 shared lock，直接 return true。
添加锁到 RequestQueue 和 txn 的 SharedLockSet 中，之后尝试等待获取锁。
获得锁成功后，将锁的 granted_ 设置为 true。
   */
  std::unique_lock<std::mutex> lock(latch_);
  auto state = txn->GetState();
  if (state == TransactionState::ABORTED || state == TransactionState::SHRINKING) {
    return false;
  }
  auto isolation_level = txn->GetIsolationLevel();
  if (isolation_level == IsolationLevel::READ_UNCOMMITTED || state != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  if (txn->IsSharedLocked(rid)) {
    return true;
  }
  LockRequest lock_request = LockRequest(txn->GetTransactionId(), LockMode::SHARED);

  auto request_queue = lock_table_[rid].request_queue_;
  request_queue.emplace_back(lock_request);
  if (request_queue.empty()) {
    lock_request.granted_ = true;
    txn->GetSharedLockSet()->emplace(rid);
  }

  LockRequestQueue *lock_request_queue = &lock_table_.find(rid)->second;
  if (lock_request_queue->is_writing_) {
    lock_table_[rid].cv_.wait(lock, [txn, lock_request_queue]() -> bool {
      return txn->GetState() == TransactionState::ABORTED || !lock_request_queue->is_writing_;
    });
  }
  lock_request.granted_ = true;
  lock_request_queue->sharing_count_++;
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  auto state = txn->GetState();
  if (state == TransactionState::ABORTED || state == TransactionState::SHRINKING) {
    return false;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED || state != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  auto request = LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  auto request_queue = lock_table_[rid].request_queue_;
  request_queue.emplace_back(request);
  LockRequestQueue *lock_request_queue = &lock_table_.find(rid)->second;
  if (lock_request_queue->is_writing_ || lock_request_queue->sharing_count_ > 0) {
    lock_request_queue->cv_.wait(lock, [txn, lock_request_queue]() -> bool {
      return txn->GetState() == TransactionState::ABORTED ||
             (!lock_request_queue->is_writing_ && lock_request_queue->sharing_count_ == 0);
    });
  }
  request.granted_ = true;
  lock_request_queue->is_writing_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  LockRequestQueue *lock_request_queue = &lock_table_.find(rid)->second;
  if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
  txn->GetSharedLockSet()->erase(rid);
  auto request = LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  if (lock_request_queue->is_writing_ || lock_request_queue->sharing_count_ > 0) {
    lock_request_queue->upgrading_ = txn->GetTransactionId();
    lock_request_queue->cv_.wait(lock, [txn, lock_request_queue]() -> bool {
      return txn->GetState() == TransactionState::ABORTED ||
             (!lock_request_queue->is_writing_ && lock_request_queue->sharing_count_ == 0);
    });
  }
  lock_request_queue->is_writing_ = true;
  request.granted_ = true;
  lock_request_queue->upgrading_ = INVALID_TXN_ID;
  lock_request_queue->request_queue_.emplace_back(request);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  LockRequestQueue *request_queue = &lock_table_.find(rid)->second;

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  auto iter = request_queue->request_queue_.begin();
  for (; iter != request_queue->request_queue_.end(); ++iter) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      break;
    }
  }
  LockMode mode = iter->lock_mode_;
  request_queue->request_queue_.erase(iter);

  if (!(mode == LockMode::SHARED && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) &&
      txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  if (mode == LockMode::SHARED) {
    if (--request_queue->sharing_count_ == 0) {
      request_queue->cv_.notify_all();
    }
  } else {
    request_queue->is_writing_ = false;
    request_queue->cv_.notify_all();
  }
  return true;
}

}  // namespace bustub
