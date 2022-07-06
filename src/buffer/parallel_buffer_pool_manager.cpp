//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
#include "buffer/buffer_pool_manager_instance.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstances
  log_manager_ = log_manager;
  disk_manager_ = disk_manager;
  num_instances_ = num_instances;
  pool_size_ = pool_size;
  for (uint32_t i = 0; i < num_instances; i++) {
    bpmi_.push_back(new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager));
  }
}
// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() = default;

auto ParallelBufferPoolManager::GetPoolSize() -> size_t {
  // Get size of all BufferPoolManagerInstances
  return num_instances_ * pool_size_;
}

auto ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) -> BufferPoolManager * {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  // range [0, num_instances]
  uint32_t index = page_id % num_instances_;
  return bpmi_[index];
}

auto ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) -> Page * {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *buffer_pool_manager = GetBufferPoolManager(page_id);
  return buffer_pool_manager->FetchPage(page_id);
  ;
}

auto ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  // Unpin page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *buffer_pool_manager = GetBufferPoolManager(page_id);
  if (buffer_pool_manager == nullptr) return true;
  return buffer_pool_manager->UnpinPage(page_id, is_dirty);
}

auto ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) -> bool {
  // Flush page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *buffer_pool_manager = GetBufferPoolManager(page_id);
  if (buffer_pool_manager == nullptr) return true;
  return buffer_pool_manager->FlushPage(page_id);
}

auto ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) -> Page * {
  /**
   *create new page. We will request page allocation in a round robin manner
     from the underlyingBufferPoolManagerInstances
      1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around
      to starting index and return nullptr
      2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this
      function is called
   */
  for (uint32_t i = 0; i < num_instances_; i++) {
    Page *page = bpmi_[next_instance_]->NewPage(page_id);
    next_instance_ = (next_instance_ + 1) % num_instances_;
    if (page != nullptr) {
      return page;
    }
  }
  return nullptr;
}

auto ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) -> bool {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *buffer_pool_manager = GetBufferPoolManager(page_id);
  if (buffer_pool_manager == nullptr) return true;
  return buffer_pool_manager->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (uint32_t i = 0; i < num_instances_; i++) {
    bpmi_[i]->FlushAllPages();
  }
}

}  // namespace bustub
