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
  latch.lock();
  if (lruMap.empty()) {
    latch.unlock();
    return false;
  }

  // 选择列表尾部 也就是最少使用的frame
  frame_id_t lru_frame = lru_list.back();
  lruMap.erase(lru_frame);
  // 列表删除
  lru_list.pop_back();
  *frame_id = lru_frame;
  latch.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  /**
   * This method should be called after a page is pinned to a frame in the BufferPoolManager.
   * It should remove the frame containing the pinned page from the LRUReplacer.
   */
  latch.lock();

  if (lruMap.count(frame_id) != 0) {
    lru_list.erase(lruMap[frame_id]);
    lruMap.erase(frame_id);
  }

  latch.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  /**
   * This method should be called when the pin_count of a page becomes 0.
   * This method should add the frame containing the unpinned page to the LRUReplacer.
   * @return
   */
  latch.lock();
  if (lruMap.count(frame_id) != 0) {
    latch.unlock();
    return;
  }
  // if list size >= capacity
  // while {delete front}
  while (Size() >= capacity) {
    frame_id_t need_del = lru_list.back();
    lru_list.pop_back();
    lruMap.erase(need_del);
  }
  // insert
  lru_list.push_front(frame_id);
  lruMap[frame_id] = lru_list.begin();
  latch.unlock();
}

auto LRUReplacer::Size() -> size_t { lru_list.size(); }

}  // namespace bustub
