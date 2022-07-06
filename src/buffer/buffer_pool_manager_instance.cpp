//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // Make sure you call DiskManager::WritePage!
  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end() || page_id == INVALID_PAGE_ID) {
    latch_.unlock();
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  disk_manager_->WritePage(page_id, page->GetData());
  latch_.unlock();
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  latch_.lock();
  Page *page_head = pages_;
  for (size_t i = 0; i < pool_size_; i++) {
    if (page_table_.find(page_head->page_id_) == page_table_.end() || page_head->page_id_ == INVALID_PAGE_ID) {
      latch_.unlock();
      return;
    }
    disk_manager_->WritePage(page_head->page_id_, page_head->GetData());
    page_head++;
  }
  latch_.unlock();
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // 0.   Make sure you call AllocatePage!
  latch_.lock();
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  Page *page_head = pages_;
  bool is_all = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (page_head->pin_count_ == 0) {
      is_all = false;
      break;
    }
    page_head++;
  }
  if (is_all) {
    latch_.unlock();
    return nullptr;
  }
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t *new_frame_id = new frame_id_t;
  if (free_list_.empty()) {
    if (!replacer_->Victim(new_frame_id)) {
      latch_.unlock();
      return nullptr;
    }
  } else {
    *new_frame_id = free_list_.front();
    free_list_.pop_front();
  }
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  Page *new_page = &pages_[*new_frame_id];
  if (new_page->IsDirty()) {
    disk_manager_->WritePage(new_page->page_id_, new_page->data_);  // 写回磁盘
    new_page->is_dirty_ = false;
  }
  page_table_.erase(new_page->page_id_);
  //这里才开始分配内存，为了后面的并行bufferpool的实现
  page_id_t new_page_id = AllocatePage();
  new_page->page_id_ = new_page_id;
  page_table_[new_page_id] = *new_frame_id;
  new_page->is_dirty_ = false;
  new_page->pin_count_++;
  replacer_->Pin(*new_frame_id);
  // 4.   Set the page ID output parameter. Return a pointer to P.
  *page_id = new_page_id;
  latch_.unlock();
  return new_page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  latch_.lock();
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    Page *page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->Pin(frame_id);
    latch_.unlock();
    return page;
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  frame_id_t *rep_fid = new frame_id_t;
  if (free_list_.empty()) {
    if (!replacer_->Victim(rep_fid)) {
      latch_.unlock();
      return nullptr;
    }
  } else {
    *rep_fid = free_list_.front();
    free_list_.pop_front();
  }
  // 3.     Delete R from the page table and insert P.
  Page *fetch_page = &pages_[*rep_fid];
  if (fetch_page->IsDirty()) {
    disk_manager_->WritePage(fetch_page->page_id_, fetch_page->data_);
    fetch_page->is_dirty_ = false;
  }
  page_table_.erase(fetch_page->page_id_);
  page_table_[page_id] = *rep_fid;
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  fetch_page->page_id_ = page_id;
  fetch_page->pin_count_++;
  replacer_->Pin(*rep_fid);
  fetch_page->is_dirty_ = false;
  disk_manager_->ReadPage(page_id, fetch_page->GetData());
  latch_.unlock();
  return fetch_page;
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return true;
  }
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  frame_id_t fid = page_table_[page_id];
  Page *page = &pages_[fid];
  if (page->pin_count_ != 0) {
    latch_.unlock();
    return false;
  }
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list
  DeallocatePage(page_id);
  page_table_.erase(page_id);
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page->page_id_ = INVALID_PAGE_ID;
  free_list_.push_back(fid);
  latch_.unlock();
  return true;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (is_dirty) {
    page->is_dirty_ = true;
  }
  if (page->pin_count_ == 0) {
    latch_.unlock();
    return false;
  }
  page->pin_count_--;
  if (page->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  latch_.unlock();
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}
}  // namespace bustub
