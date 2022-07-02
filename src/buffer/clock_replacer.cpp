//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {}

ClockReplacer::~ClockReplacer() = default;

auto ClockReplacer::Victim(frame_id_t *frame_id) -> bool {
  /**
   *  Starting from the current position of clock hand, find the first frame that is both in the `ClockReplacer`
   *  and with its ref flag set to false. If a frame is in the `ClockReplacer`,
   *  but its ref flag is set to true, change it to false instead.
   *  This should be the only method that updates the clock hand.
   */
  auto iterator_1 = frame_list_.begin();
  // find the current position
  while (iterator_1 != frame_list_.end()) {
    if (*iterator_1 == *frame_id) {
      break;
    }
    iterator_1++;
  }
  auto iterator_2 = iterator_1;
  // find the first frame that is both in the `ClockReplacer` and with its ref flag set to false
  do {
    if (ref_map_[*iterator_2]) {
      ref_map_[*iterator_2] = false;
    } else {
      frame_list_.remove(*iterator_2);
      ref_map_.erase(*iterator_2);
      return true;
    }
    iterator_2++;
  } while (iterator_2 != iterator_1);
  return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  /**
   * This method should be called after a page is pinned to a frame in the BufferPoolManager.
   * It should remove the frame containing the pinned page from the ClockReplacer.
   */
  frame_list_.remove(frame_id);
  ref_map_.erase(frame_id);
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  /**
   * This method should be called when the pin_count of a page becomes 0.
   * This method should add the frame containing the unpinned page to the ClockReplacer.
   */
}

auto ClockReplacer::Size() -> size_t { return frame_list_.size(); }

}  // namespace bustub
