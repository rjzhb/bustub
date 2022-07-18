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
  id_2_txn_.emplace(txn->GetTransactionId(), txn);
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }
  if (!LockPrepare(txn, rid)) {
    return false;
  }
  //  LockRequest lock_request = LockRequest(txn->GetTransactionId(), LockMode::SHARED);
  LockRequestQueue *lock_request_queue = &lock_table_.find(rid)->second;
  auto &request_queue = lock_request_queue->request_queue_;
  request_queue.emplace_back(txn->GetTransactionId(), LockMode::SHARED);
//  lock_request_queue->request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);

  if (lock_request_queue->is_writing_) {
    DeadlockPrevent(txn, lock_request_queue);
    lock_table_[rid].cv_.wait(lock, [txn, lock_request_queue]() -> bool {
      return txn->GetState() == TransactionState::ABORTED || !lock_request_queue->is_writing_;
    });
  }
  CheckAborted(txn, lock_request_queue);
  auto iter = GetIterator(&lock_request_queue->request_queue_, txn->GetTransactionId());
  iter->granted_ = true;
  lock_request_queue->sharing_count_++;
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  id_2_txn_.emplace(txn->GetTransactionId(), txn);
  if (!LockPrepare(txn, rid)) {
    return false;
  }
  //  auto request = LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  LockRequestQueue *lock_request_queue = &lock_table_.find(rid)->second;
  lock_request_queue->request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  if (lock_request_queue->is_writing_ || lock_request_queue->sharing_count_ > 0) {
    DeadlockPrevent(txn, lock_request_queue);
    lock_request_queue->cv_.wait(lock, [txn, lock_request_queue]() -> bool {
      return txn->GetState() == TransactionState::ABORTED ||
             (!lock_request_queue->is_writing_ && lock_request_queue->sharing_count_ == 0);
    });
  }
  CheckAborted(txn, lock_request_queue);
  auto iter = GetIterator(&lock_request_queue->request_queue_, txn->GetTransactionId());
  iter->granted_ = true;
  lock_request_queue->is_writing_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  id_2_txn_.emplace(txn->GetTransactionId(), txn);
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
  lock_request_queue->sharing_count_--;
  auto iter = GetIterator(&lock_request_queue->request_queue_, txn->GetTransactionId());
  iter->lock_mode_ = LockMode::EXCLUSIVE;
  iter->granted_ = false;
  //  auto request = LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  lock_request_queue->request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  if (lock_request_queue->is_writing_ || lock_request_queue->sharing_count_ > 0) {
    lock_request_queue->upgrading_ = txn->GetTransactionId();
    DeadlockPrevent(txn, lock_request_queue);
    lock_request_queue->cv_.wait(lock, [txn, lock_request_queue]() -> bool {
      return txn->GetState() == TransactionState::ABORTED ||
             (!lock_request_queue->is_writing_ && lock_request_queue->sharing_count_ == 0);
    });
  }
  CheckAborted(txn, lock_request_queue);
  lock_request_queue->is_writing_ = true;
  iter = GetIterator(&lock_request_queue->request_queue_, txn->GetTransactionId());
  iter->granted_ = true;
  lock_request_queue->upgrading_ = INVALID_TXN_ID;
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

void LockManager::CheckAborted(Transaction *txn, LockRequestQueue *request_queue) {
  if (txn->GetState() == TransactionState::ABORTED) {
    auto iter = GetIterator(&request_queue->request_queue_, txn->GetTransactionId());
    request_queue->request_queue_.erase(iter);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
}

std::list<LockManager::LockRequest>::iterator LockManager::GetIterator(std::list<LockRequest> *request_queue,
                                                                       txn_id_t txn_id) {
  for (auto iter = request_queue->begin(); iter != request_queue->end(); ++iter) {
    if (iter->txn_id_ == txn_id) {
      return iter;
    }
  }
  return request_queue->end();
}

void LockManager::DeadlockPrevent(Transaction *txn, LockRequestQueue *request_queue) {
  for (const auto &request : request_queue->request_queue_) {
    if (request.granted_ && request.txn_id_ > txn->GetTransactionId()) {
      id_2_txn_[request.txn_id_]->SetState(TransactionState::ABORTED);
      if (request.lock_mode_ == LockMode::SHARED) {
        request_queue->sharing_count_--;
      } else {
        request_queue->is_writing_ = false;
      }
    }
  }
}

bool LockManager::LockPrepare(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  if (lock_table_.find(rid) == lock_table_.end()) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }
  return true;
}

}  // namespace bustub
