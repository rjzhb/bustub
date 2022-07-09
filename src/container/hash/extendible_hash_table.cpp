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
  directory_page_id_ = INVALID_PAGE_ID;
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
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  // if pageId is INVALID
  Page *page = new Page;
  if (directory_page_id_ == INVALID_PAGE_ID) {
    page_id_t *page_id = new page_id_t;
    page = buffer_pool_manager_->NewPage(page_id);
    HashTableDirectoryPage *hash_table_directory_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
    directory_page_id_ = *page_id;
    hash_table_directory_page->SetPageId(directory_page_id_);
    page_id_t *bucket_page_id = new page_id_t;
    buffer_pool_manager_->NewPage(bucket_page_id);
    hash_table_directory_page->SetBucketPageId(0, *bucket_page_id);
    buffer_pool_manager_->UnpinPage(*page_id, false);
    buffer_pool_manager_->UnpinPage(*bucket_page_id, false);
  }
  // if pageID is VALID
  assert(directory_page_id_ != INVALID_PAGE_ID);
  page = buffer_pool_manager_->FetchPage(directory_page_id_);
  // Do I need Unpin?
  return reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  assert(page != nullptr);
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  HashTableDirectoryPage *hash_table_directory_page = FetchDirectoryPage();
  uint32_t bucket_page_id = KeyToPageId(key, hash_table_directory_page);
  HASH_TABLE_BUCKET_TYPE *hash_table_bucket_page = FetchBucketPage(bucket_page_id);
  // do I need unpin?
  buffer_pool_manager_->UnpinPage(hash_table_directory_page->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  return hash_table_bucket_page->GetValue(key, comparator_, result);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  // 1.Get DiretoryPage
  HashTableDirectoryPage *hash_table_directory_page = FetchDirectoryPage();
  // 2. transfer key to bucket_index
  uint32_t bucket_index = KeyToDirectoryIndex(key, hash_table_directory_page);
  // 3.Get BucketPage
  uint32_t bucket_page_id = hash_table_directory_page->GetBucketPageId(bucket_index);
  HashTableBucketPage<KeyType, ValueType, KeyComparator> *hash_table_bucket_page = FetchBucketPage(bucket_page_id);
  // insert k,v to bucketPage
  // judge if bucket is full
  if (!hash_table_bucket_page->IsFull()) {
    bool is_insert = hash_table_bucket_page->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    return is_insert;
  }
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  // 1.Get DiretoryPage
  HashTableDirectoryPage *hash_table_directory_page = FetchDirectoryPage();
  // 2. transfer key to bucket_index
  uint32_t bucket_index = KeyToDirectoryIndex(key, hash_table_directory_page);
  // 3.Get BucketPage
  uint32_t bucket_page_id = hash_table_directory_page->GetBucketPageId(bucket_index);
  HashTableBucketPage<KeyType, ValueType, KeyComparator> *hash_table_bucket_page = FetchBucketPage(bucket_page_id);
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
    if (index == split_image_index) {
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
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  // 1.Get DiretoryPage
  HashTableDirectoryPage *hash_table_directory_page = FetchDirectoryPage();
  // 2. transfer key to bucket_index
  uint32_t bucket_index = KeyToDirectoryIndex(key, hash_table_directory_page);
  // 3.Get BucketPage
  uint32_t bucket_page_id = hash_table_directory_page->GetBucketPageId(bucket_index);
  HashTableBucketPage<KeyType, ValueType, KeyComparator> *hash_table_bucket_page = FetchBucketPage(bucket_page_id);

  bool res = hash_table_bucket_page->Remove(key, value, comparator_);
  if (hash_table_bucket_page->IsEmpty()) {
    Merge(transaction, key, value);
  }
  return res;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_index = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_pid = dir_page->GetBucketPageId(bucket_index);
  uint32_t local_depth = dir_page->GetLocalDepth(bucket_pid);
  uint32_t image_bucket_index = dir_page->GetSplitImageIndex(bucket_index);
  if (local_depth == 0) {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    return;
  }
  // 如果该bucket与其split image深度不同，也不收缩
  if (local_depth != dir_page->GetLocalDepth(image_bucket_index)) {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    return;
  }

  // 如果target bucket不为空，则不收缩
  HASH_TABLE_BUCKET_TYPE *target_bucket = FetchBucketPage(bucket_pid);
  if (!target_bucket->IsEmpty()) {
    assert(buffer_pool_manager_->UnpinPage(bucket_pid, false));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    return;
  }

  // 删除target bucket，此时该bucket已经为空
  assert(buffer_pool_manager_->UnpinPage(bucket_pid, false));
  assert(buffer_pool_manager_->DeletePage(bucket_pid));

  // 设置target bucket的page为split image的page，即合并target和split
  page_id_t image_bucket_page_id = dir_page->GetBucketPageId(image_bucket_index);
  dir_page->SetBucketPageId(bucket_index, image_bucket_page_id);
  dir_page->DecrLocalDepth(bucket_index);
  dir_page->DecrLocalDepth(image_bucket_index);
  assert(dir_page->GetLocalDepth(bucket_index) == dir_page->GetLocalDepth(image_bucket_index));

  // 遍历整个directory，将所有指向target bucket page的bucket全部重新指向split image bucket的page
  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    if (dir_page->GetBucketPageId(i) == bucket_pid || dir_page->GetBucketPageId(i) == image_bucket_page_id) {
      dir_page->SetBucketPageId(i, image_bucket_page_id);
      dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(bucket_index));
    }
  }

  // 尝试收缩Directory
  // 这里要循环，不能只收缩一次
  while (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }

  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
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
