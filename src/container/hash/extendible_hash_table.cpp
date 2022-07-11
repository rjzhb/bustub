//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  auto dir_page = buffer_pool_manager_->NewPage(&directory_page_id_);
  auto dir_page_data = reinterpret_cast<HashTableDirectoryPage *>(dir_page->GetData());
  page_id_t bucket_0_page_id;
  page_id_t bucket_1_page_id;
  buffer_pool_manager_->NewPage(&bucket_0_page_id);
  buffer_pool_manager_->NewPage(&bucket_1_page_id);
  dir_page_data->SetBucketPageId(0, bucket_0_page_id);
  dir_page_data->SetLocalDepth(0, 1);
  dir_page_data->SetBucketPageId(1, bucket_1_page_id);
  dir_page_data->SetLocalDepth(1, 1);

  // remeber update directory page
  dir_page_data->IncrGlobalDepth();
  dir_page_data->SetPageId(directory_page_id_);

  // unpin the pages
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_0_page_id, false);
  buffer_pool_manager_->UnpinPage(bucket_1_page_id, false);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  uint32_t mask = dir_page->GetGlobalDepthMask();
  return Hash(key) & mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
std::pair<Page *, HASH_TABLE_BUCKET_TYPE *> HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  auto bucket_page = buffer_pool_manager_->FetchPage(bucket_page_id);
  auto bucket_page_data = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page->GetData());
  return std::pair<Page *, HASH_TABLE_BUCKET_TYPE *>(bucket_page, bucket_page_data);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  table_latch_.RLock();
  HashTableDirectoryPage *hash_table_directory_page = FetchDirectoryPage();
  uint32_t bucket_page_id = KeyToPageId(key, hash_table_directory_page);
  auto [page, hash_table_bucket_page] = FetchBucketPage(bucket_page_id);
  page->RLatch();
  // do I need unpin?
  buffer_pool_manager_->UnpinPage(hash_table_directory_page->GetPageId(), false);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  bool is_get = hash_table_bucket_page->GetValue(key, comparator_, result);
  table_latch_.RUnlock();
  return is_get;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  // 1.Get DiretoryPage
  HashTableDirectoryPage *hash_table_directory_page = FetchDirectoryPage();
  // 2. transfer key to bucket_index
  uint32_t bucket_index = KeyToDirectoryIndex(key, hash_table_directory_page);
  // 3.Get BucketPage
  uint32_t bucket_page_id = hash_table_directory_page->GetBucketPageId(bucket_index);
  auto [page, hash_table_bucket_page] = FetchBucketPage(bucket_page_id);
  page->WLatch();
  // insert k,v to bucketPage
  // judge if bucket is full
  if (!hash_table_bucket_page->IsFull()) {
    bool is_insert = hash_table_bucket_page->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    table_latch_.RUnlock();
    return is_insert;
  }
  page->WUnlatch();
  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  // 1.Get DiretoryPage
  table_latch_.WLock();
  HashTableDirectoryPage *hash_table_directory_page = FetchDirectoryPage();
  // 2. transfer key to bucket_index
  uint32_t bucket_index = KeyToDirectoryIndex(key, hash_table_directory_page);
  // 3.Get BucketPage
  uint32_t bucket_page_id = hash_table_directory_page->GetBucketPageId(bucket_index);
  auto [page_latch, hash_table_bucket_page] = FetchBucketPage(bucket_page_id);
  page_latch->WLatch();
  // 4.judge

  if (hash_table_directory_page->GetLocalDepth(bucket_index) == hash_table_directory_page->GetGlobalDepth()) {
    hash_table_directory_page->IncrGlobalDepth();
  }
  // 5.incr LocalDepth
  hash_table_directory_page->IncrLocalDepth(bucket_index);
  uint32_t split_image_index = hash_table_directory_page->GetSplitImageIndex(bucket_index);
  // 6. create a new bucket page
  page_id_t *split_image_pid = new page_id_t;
  Page *page = buffer_pool_manager_->NewPage(split_image_pid);
  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *split_image_bucket = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  hash_table_directory_page->SetBucketPageId(split_image_index, *split_image_pid);
  hash_table_directory_page->SetLocalDepth(split_image_index, hash_table_directory_page->GetLocalDepth(bucket_index));
  // 7. get copy Array
  MappingType *array = hash_table_bucket_page->GetCopyArray();
  // 8.remove all data and reset to two bucket page(split_image_bucket_page,hash_table_bucket_page)
  hash_table_bucket_page->ResetMemory();
  for (uint32_t i = 0; i < hash_table_bucket_page->NumReadable(); i++) {
    auto k = array[i].first;
    auto v = array[i].second;
    uint32_t index = Hash(k) & hash_table_directory_page->GetLocalDepth(split_image_index);
    page_id_t bucket_index_page = hash_table_directory_page->GetBucketPageId(index);
    if (bucket_index_page == hash_table_directory_page->GetBucketPageId(split_image_index)) {
      split_image_bucket->Insert(k, v, comparator_);
    } else {
      hash_table_bucket_page->Insert(k, v, comparator_);
    }
  }
  delete[] array;
  // 9.Set all localDepth
  uint32_t diff = 1 << hash_table_directory_page->GetLocalDepth(split_image_index);
  for (uint32_t i = split_image_index; i >= 0; i -= diff) {
    hash_table_directory_page->SetBucketPageId(i, *split_image_pid);
    hash_table_directory_page->SetLocalDepth(i, hash_table_directory_page->GetLocalDepth(split_image_index));
  }
  for (uint32_t i = split_image_index; i < hash_table_directory_page->Size(); i += diff) {
    hash_table_directory_page->SetBucketPageId(i, *split_image_pid);
    hash_table_directory_page->SetLocalDepth(i, hash_table_directory_page->GetLocalDepth(split_image_index));
  }
  for (uint32_t i = bucket_index; i >= 0; i -= diff) {
    hash_table_directory_page->SetBucketPageId(i, bucket_page_id);
    hash_table_directory_page->SetLocalDepth(i, hash_table_directory_page->GetLocalDepth(split_image_index));
  }
  for (uint32_t i = bucket_index; i < hash_table_directory_page->Size(); i += diff) {
    hash_table_directory_page->SetBucketPageId(i, bucket_page_id);
    hash_table_directory_page->SetLocalDepth(i, hash_table_directory_page->GetLocalDepth(split_image_index));
  }
  page->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(*split_image_pid, true));
  page_latch->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(hash_table_directory_page->GetPageId(), true));
  table_latch_.WUnlock();
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  auto dir_page_data = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page_data);
  auto [bucket_page, bucket_page_data] = FetchBucketPage(bucket_page_id);
  bucket_page->WLatch();
  auto success = bucket_page_data->Remove(key, value, comparator_);

  // if the bucket is empty after removing, call Merge().
  if (success && bucket_page_data->IsEmpty()) {
    buffer_pool_manager_->UnpinPage(bucket_page_id, success);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    bucket_page->WUnlatch();
    table_latch_.RUnlock();
    Merge(transaction, key, value);
    return success;
  }
  buffer_pool_manager_->UnpinPage(bucket_page_id, success);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  bucket_page->WUnlatch();

  table_latch_.RUnlock();
  return success;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();

  auto dir_page_data = FetchDirectoryPage();

  // traverse the directory page and merge all empty buckets.
  for (uint32_t i = 0;; i++) {
    // after merging the buckets, the directory page may shrink.
    // so we have to check every time whether it is out of bounds.
    if (i >= dir_page_data->Size()) {
      break;
    }
    auto old_local_depth = dir_page_data->GetLocalDepth(i);
    auto bucket_page_id = dir_page_data->GetBucketPageId(i);
    auto [bucket_page, bucket_page_data] = FetchBucketPage(bucket_page_id);
    bucket_page->RLatch();
    if (old_local_depth > 1 && bucket_page_data->IsEmpty()) {
      auto split_bucket_idx = dir_page_data->GetSplitImageIndex(i);
      if (dir_page_data->GetLocalDepth(split_bucket_idx) == old_local_depth) {
        dir_page_data->DecrLocalDepth(i);
        dir_page_data->DecrLocalDepth(split_bucket_idx);
        dir_page_data->SetBucketPageId(i, dir_page_data->GetBucketPageId(split_bucket_idx));
        auto new_bucket_page_id = dir_page_data->GetBucketPageId(i);

        // after merging the buckets, all buckets with the same page id as the bucket pair need to be updated.
        //! For more info, see VerifyIntegrity().
        for (uint32_t j = 0; j < dir_page_data->Size(); j++) {
          if (j == i || j == split_bucket_idx) {
            continue;
          }
          auto cur_bucket_page_id = dir_page_data->GetBucketPageId(j);
          if (cur_bucket_page_id == bucket_page_id || cur_bucket_page_id == new_bucket_page_id) {
            dir_page_data->SetLocalDepth(j, dir_page_data->GetLocalDepth(i));
            dir_page_data->SetBucketPageId(j, new_bucket_page_id);
          }
        }
      }
      if (dir_page_data->CanShrink()) {
        dir_page_data->DecrGlobalDepth();
      }
    }
    bucket_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  }
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);

  table_latch_.WUnlock();
}


/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
