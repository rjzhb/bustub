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

LRUReplacer::LRUReplacer(size_t num_pages) { capacity = num_pages; }

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  /**
   *  Remove the object that was accessed the least recently compared to all the elements being tracked by the Replacer,
   *  store its contents in the output parameter and return True.
   *  If the Replacer is empty return False.
   * @param frame_id
   */
  std::scoped_lock lru_clk{lru_mutex};
  if (frame_list_.empty()) {
    return false;
  }
  *frame_id = frame_list_.back();
  frame_list_.pop_back();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  /**
   * This method should be called after a page is pinned to a frame in the BufferPoolManager.
   * It should remove the frame containing the pinned page from the LRUReplacer.
   */
  std::scoped_lock lru_clk{lru_mutex};
  if (pin_count_map_[frame_id] == 0) {
    frame_list_.remove(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  /**
   * This method should be called when the pin_count of a page becomes 0.
   * This method should add the frame containing the unpinned page to the LRUReplacer.
   * @return
   */
  std::scoped_lock lru_clk{lru_mutex};
  if (pin_count_map_.find(frame_id) == pin_count_map_.end()) {
    if (frame_list_.size() >= capacity) {
      // no free position
      frame_list_.pop_back();
    }
    frame_list_.push_front(frame_id);
    pin_count_map_[frame_id] = 0;
  }
}

auto LRUReplacer::Size() -> size_t { return frame_list_.size(); }

}  // namespace bustub
