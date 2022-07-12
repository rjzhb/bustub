//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.h
//
// Identification: src/include/storage/page/hash_table_bucket_page.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <utility>
#include <vector>

#include "common/config.h"
#include "storage/index/int_comparator.h"
#include "storage/page/hash_table_page_defs.h"

namespace bustub {
/**
 * Store indexed key and and value together within bucket page. Supports
 * non-unique keys.
 *
 * Bucket page format (keys are stored in order):
 *  ----------------------------------------------------------------
 * | KEY(1) + VALUE(1) | KEY(2) + VALUE(2) | ... | KEY(n) + VALUE(n)
 *  ----------------------------------------------------------------
 *
 *  Here '+' means concatenation.
 *  The above format omits the space required for the occupied_ and
 *  readable_ arrays. More information is in storage/page/hash_table_page_defs.h.
 *
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
class HashTableBucketPage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  HashTableBucketPage() = delete;

  /**
   * Scan the bucket and collect values that have the matching key
   *
   * @return true if at least one key matched
   */
  bool GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result);

  /**
   * Attempts to insert a key and value in the bucket.  Uses the occupied_
   * and readable_ arrays to keep track of each slot's availability.
   *
   * @param key key to insert
   * @param value value to insert
   * @return true if inserted, false if duplicate KV pair or bucket is full
   */
  bool Insert(KeyType key, ValueType value, KeyComparator cmp);

  /**
   * Removes a key and value.
   *
   * @return true if removed, false if not found
   */
  bool Remove(KeyType key, ValueType value, KeyComparator cmp);

  /**
   * Gets the key at an index in the bucket.
   *
   * @param bucket_idx the index in the bucket to get the key at
   * @return key at index bucket_idx of the bucket
   */
  KeyType KeyAt(uint32_t bucket_idx) const;

  /**
   * Gets the value at an index in the bucket.
   *
   * @param bucket_idx the index in the bucket to get the value at
   * @return value at index bucket_idx of the bucket
   */
  ValueType ValueAt(uint32_t bucket_idx) const;

  /**
   * Remove the KV pair at bucket_idx
   */
  void RemoveAt(uint32_t bucket_idx);

  /**
   * Returns whether or not an index is occupied (key/value pair or tombstone)
   *
   * @param bucket_idx index to look at
   * @return true if the index is occupied, false otherwise
   */
  bool IsOccupied(uint32_t bucket_idx) const;

  /**
   * SetOccupied - Updates the bitmap to indicate that the entry at
   * bucket_idx is occupied.
   *
   * @param bucket_idx the index to update
   * @param bit the bit that you want to set, 0 or 1
   */
  void SetOccupied(uint32_t bucket_idx, int bit);

  /**
   * Returns whether or not an index is readable (valid key/value pair)
   *
   * @param bucket_idx index to lookup
   * @return true if the index is readable, false otherwise
   */
  bool IsReadable(uint32_t bucket_idx) const;

  /**
   * SetReadable - Updates the bitmap to indicate that the entry at
   * bucket_idx is readable.
   *
   * @param bucket_idx the index to update
   * @param bit the bit that you want to set, 0 or 1
   */
  void SetReadable(uint32_t bucket_idx, int bit);

  /**
   * @return the number of readable elements, i.e. current size
   */
  uint32_t NumReadable();

  /**
   * @return whether the bucket is full
   */
  bool IsFull();

  /**
   * @return whether the bucket is empty
   */
  bool IsEmpty();

  /**
   * Prints the bucket's occupancy information
   */
  void PrintBucket();

  /**
   * @return the length of occupied_ or readable_
   */
  int GetNumofChars() const { return (BUCKET_ARRAY_SIZE - 1) / 8 + 1; }

  /**
   * @return first is bucket_idx / 8, second is bucket_idx % 8
   */
  std::pair<int, int> GetLocation(uint32_t bucket_idx) const {
    auto ret = std::pair<int, int>(0, 0);
    ret.first = bucket_idx / 8;
    ret.second = bucket_idx % 8;
    return ret;
  }

  /**
   * Set the bit at the specified position
   *
   * @param which which bit you want to update
   * @param bit the bit yout want to set, 0 or 1
   */
  char GetMask(int which, int bit) const {
    char mask = 0;
    if (bit == 0) {
      mask = static_cast<char>(~(1 << which));
    } else {
      mask = static_cast<char>(1 << which);
    }
    return mask;
  }

 private:
  //  For more on BUCKET_ARRAY_SIZE see storage/page/hash_table_page_defs.h
  char occupied_[(BUCKET_ARRAY_SIZE - 1) / 8 + 1]{0};
  // 0 if tombstone/brand new (never occupied), 1 otherwise.
  char readable_[(BUCKET_ARRAY_SIZE - 1) / 8 + 1]{0};
  MappingType array_[BUCKET_ARRAY_SIZE];
};

}  // namespace bustub
