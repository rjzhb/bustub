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
  BufferPoolManagerInstance *bpmi = buffer_pool_manager_instance_;
  for (size_t i = 0; i < num_instances; i++) {
    bpmi = new BufferPoolManagerInstance(pool_size_, disk_manager_, log_manager_);
    bpmi++;
  }
}
// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() = default;

auto ParallelBufferPoolManager::GetPoolSize() -> size_t {
  // Get size of all BufferPoolManagerInstances
  return pool_size_;
}

auto ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) -> BufferPoolManager * {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  // range [0, num_instances]
  int index = page_id % num_instances_;
  return &buffer_pool_manager_instance_[index];
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
  return buffer_pool_manager->UnpinPage(page_id, is_dirty);
}

auto ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) -> bool {
  // Flush page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *buffer_pool_manager = GetBufferPoolManager(page_id);
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
  int index = *page_id % num_instances_;
  size_t i = index;
  do {
    Page *page = buffer_pool_manager_instance_[i].NewPage(page_id);
    if (page != nullptr) {
      return page;
    }
    i++;
    if (i >= num_instances_) {
      i = 0;
      continue;
    }
  } while (i != index);
  return nullptr;
}

auto ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) -> bool {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *buffer_pool_manager = GetBufferPoolManager(page_id);
  return buffer_pool_manager->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  BufferPoolManagerInstance *head = buffer_pool_manager_instance_;
  for (size_t i = 0; i < num_instances_; i++) {
    head->FlushAllPages();
    head++;
  }
}

}  // namespace bustub
