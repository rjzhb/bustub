//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { capacity_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  /**
   *  Remove the object that was accessed the least recently compared to all the elements being tracked by the Replacer,
   *  store its contents in the output parameter and return True.
   *  If the Replacer is empty return False.
   * @param frame_id
   */
  latch_.lock();
  if (lru_map_.empty()) {
    latch_.unlock();
    return false;
  }

  // 选择列表尾部 也就是最少使用的frame
  frame_id_t lru_frame = lru_list_.back();
  lru_map_.erase(lru_frame);
  // 列表删除
  lru_list_.pop_back();
  *frame_id = lru_frame;
  latch_.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  /**
   * This method should be called after a page is pinned to a frame in the BufferPoolManager.
   * It should remove the frame containing the pinned page from the LRUReplacer.
   */
  latch_.lock();

  if (lru_map_.count(frame_id) != 0) {
    lru_list_.erase(lru_map_[frame_id]);
    lru_map_.erase(frame_id);
  }

  latch_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  /**
   * This method should be called when the pin_count of a page becomes 0.
   * This method should add the frame containing the unpinned page to the LRUReplacer.
   * @return
   */
  latch_.lock();
  if (lru_map_.count(frame_id) != 0) {
    latch_.unlock();
    return;
  }
  // if list size >= capacity
  // while {delete front}
  while (Size() >= capacity_) {
    frame_id_t need_del = lru_list_.back();
    lru_list_.pop_back();
    lru_map_.erase(need_del);
  }
  // insert
  lru_list_.push_front(frame_id);
  lru_map_[frame_id] = lru_list_.begin();
  latch_.unlock();
}

auto LRUReplacer::Size() -> size_t { return lru_list_.size(); }

}  // namespace bustub
