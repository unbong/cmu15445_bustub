//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {

  frame_id = nullptr;                           // set null
  size_t maxKthDistance = 0;                    // init max distance
  size_t currentTime = GetCurrentTimeStamp();   //
  size_t  earliestFrameTime = INT64_MAX;

  // loop frame map to get max kth distance
  for(auto iter = node_store_.begin(); iter != node_store_.end(); iter++){
    LRUKNode node = iter->second;
    if(node.isEvictable())
    {
      if(node.getK() < k_)
      {
        maxKthDistance = INT64_MAX;
        if(earliestFrameTime > node.getHistory().front())
        {
          earliestFrameTime = node.getHistory().front();
          * frame_id = iter->first;
        }
      }
      else if( maxKthDistance < (currentTime  - node.getHistory().front())){
        maxKthDistance = currentTime  - node.getHistory().front();
        * frame_id = iter->first;
      }
    }
  }

  if(frame_id != nullptr)
  {
    Remove(*frame_id);
    return true;
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {

  if(replacer_size_ >= curr_size_) throw  std::runtime_error("curr");
  auto iter = node_store_.find(frame_id);
  // 在replacer中不存在则新追加
  if(iter == node_store_.end())
  {
    LRUKNode node;
    node.setFid(frame_id);
    node.addOneToK();
    node.setIsEvictable(true);
    std::list<size_t> hist{GetCurrentTimeStamp()};
    node.setHistory(hist);
    latch_.lock();
    node_store_[frame_id] = node;
    latch_.unlock();
  }
  // 存在则，更新第更新时间戳
  else{
    LRUKNode node = iter->second;
    // check k
    latch_.lock();
    if(node.getK() < k_ ){
      node.addOneToK();
      node.getHistory().push_back(GetCurrentTimeStamp());
    }
    else
    {
      node.getHistory().pop_front();
      node.getHistory().push_back(GetCurrentTimeStamp());
    }
    node_store_[frame_id] = node;
    latch_.unlock();
  }

}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  auto iter = node_store_.find(frame_id);
  if(iter == node_store_.end())
    return ;
  LRUKNode node =iter->second;
  latch_.lock();
  if(node.isEvictable() && !set_evictable){
    curr_size_--;
  }
  else if(!node.isEvictable() && set_evictable){
    curr_size_++;
  }
  node.setIsEvictable(set_evictable);
  node_store_[frame_id] = node;
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  auto iter = node_store_.find(frame_id);
  if(iter == node_store_.end())
    return ;

  if(!iter->second.isEvictable()) throw std::runtime_error("Can't remove inevitable frame/");
  latch_.lock();
  node_store_.erase(iter);
  curr_size_--;
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

auto LRUKReplacer::GetCurrentTimeStamp() -> size_t {

  std::chrono::system_clock::time_point now = std::chrono::system_clock::now();

  // Convert the system time to a time_t object
  std::time_t currentTime = std::chrono::system_clock::to_time_t(now);

  return  static_cast<size_t>(currentTime);

}

std::list<size_t> &LRUKNode::getHistory()  { return history_; }
void LRUKNode::setHistory(const std::list<size_t> &history) { history_ = history; }
size_t LRUKNode::getK() const { return k_; }
void LRUKNode::setK(size_t k) { k_ = k; }
frame_id_t LRUKNode::getFid() const { return fid_; }
void LRUKNode::setFid(frame_id_t fid) { fid_ = fid; }
bool LRUKNode::isEvictable() const { return is_evictable_; }
void LRUKNode::setIsEvictable(bool isEvictable) { is_evictable_ = isEvictable; }
void LRUKNode::addOneToK(){k_++;};
}  // namespace bustub
