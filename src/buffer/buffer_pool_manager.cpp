//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
//  throw NotImplementedException(
//      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
//      "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {

  frame_id_t  frame_id;
  bool is_have_free_frame = GetFrameIDFromFreeList(frame_id);
  // 没有空闲帧，
  if(!is_have_free_frame ){
    bool is_allocatable = IsAllocateFromReplacer();
    // 没有可驱逐的帧
    if(!is_allocatable) {
      page_id = nullptr;
      return nullptr;
    }else{
      // 可驱逐的帧，将其驱逐并设定为不可驱逐

      replacer_->Evict(&frame_id);
      replacer_->SetEvictable(frame_id, false);
    }
  }

  // 是否需要刷盘
  if(pages_[frame_id].is_dirty_){
    FlushPage(pages_[frame_id].page_id_);
    pages_[frame_id].ResetMemory();
  }

  replacer_->SetEvictable(frame_id, false);
  *page_id = AllocatePage();
  page_table_[*page_id] = frame_id;

  // record access
  replacer_->RecordAccess(frame_id);
  pages_[frame_id].pin_count_++;
  pages_[frame_id].page_id_ = *page_id;

  return &pages_[frame_id];

}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {

  // 取得page 从freelist 或 replacer获得
  auto iter = page_table_.find(page_id);
  Page * fetchPage = nullptr;
  frame_id_t frame_id = -1;
  if(iter == page_table_.end()){

    // 从freelist取得可食用frameId
    frame_id = -1;
    bool is_frame_id_allocatable = AllocateFrameId(0, frame_id);
    // 可分配 帧
    if(is_frame_id_allocatable )
    {
      page_table_[page_id] = frame_id;
    }else{
      // 帧id无法被分配
      return nullptr;
    }
  }else{
    frame_id = iter->second;
  }

  char  page_content[BUSTUB_PAGE_SIZE] ;
  disk_manager_->ReadPage(page_id, page_content);
  *pages_[frame_id].data_ = *page_content;
  // 更新被固定的个数
  //frame_infos_[frame_id].pin_count++;

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  fetchPage = &pages_[frame_id];
  pages_[frame_id].pin_count_++;
  return fetchPage;
}

auto BufferPoolManager::  UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {

  auto iter = page_table_.find(page_id);
  // 在缓冲区池里没有 ，返回false

  if(iter == page_table_.end())
    return false;
  else {
    // pin count 已经小于等于0
    frame_id_t  frame_id =  iter->second;
    if(pages_[frame_id].pin_count_ <=0 )
    {
      return false;
    }
    pages_[frame_id].pin_count_ --;
    if(pages_[frame_id].pin_count_ == 0){
      if(is_dirty )
        pages_[frame_id].is_dirty_ = is_dirty;
      replacer_->SetEvictable(frame_id, true);
    }

  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {

  auto iter = page_table_.find(page_id);
  // 在页表中没有
  if(iter == page_table_.end()){
    return false;
  }

  frame_id_t  frame_id = iter->second;

  disk_manager_->WritePage(page_id,pages_[frame_id].data_ );
  pages_[frame_id].is_dirty_ = false;
  return true;

}

void BufferPoolManager::FlushAllPages() {

  for(auto iter = page_table_.begin(); iter != page_table_.end(); iter++)
  {
    FlushPage(iter->first);
  }

}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {

  auto iter = page_table_.find(page_id);
  if(iter == page_table_.end()){
    return true;
  }

  frame_id_t  frame_id = iter->second;
  if(pages_[frame_id].pin_count_ > 0)
    return false;

  page_table_.erase(iter);
  free_list_.push_back(frame_id);
  pages_[frame_id].ResetMemory();
  replacer_->Remove(frame_id);
  DeallocatePage(page_id);
  return true;

}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }
auto BufferPoolManager::AllocateFrameId(page_id_t page_id, frame_id_t &frame_id) -> bool {

  // 无可分配
  if(free_list_.empty())
  {
    frame_id_t * evicted_frame_id = nullptr;
    bool res = replacer_->Evict(evicted_frame_id);
    if(res == false) {
      frame_id = -1;
      return false;
    }
    else
    {
      frame_id = * evicted_frame_id;
       // if page dirty flush page and reset pin count;
      if(pages_[frame_id].is_dirty_) {
        FlushPage(page_id);
        pages_[frame_id].page_id_ = page_id;
        pages_[frame_id].pin_count_ = 0;
      }
      return true;
    }
  }
  else
  {
    frame_id_t  id = free_list_.front();
    free_list_.pop_front();
    return id;
  }
}

auto BufferPoolManager::IsAllocateFromReplacer() -> bool {

  for(size_t i = 0; i < pool_size_; i ++ )
  {
    if(pages_[i].pin_count_ == 0 ){
      return true;
    }
  }

  return false;
}


auto BufferPoolManager::GetFrameIDFromFreeList(frame_id_t &frame_id) -> bool {

  if(free_list_.empty()){
    frame_id = -1;
    return false;
  }

  frame_id = free_list_.front();
  free_list_.pop_front();

  return true;
}
}  // namespace bustub
