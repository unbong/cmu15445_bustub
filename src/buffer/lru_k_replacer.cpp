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

  bool res = false;
  size_t max_kth_distance = 0;                  // init max distance
  size_t earliest_frame_time = INT64_MAX;

  latch_.lock();
  size_t current_time = GetCurrentTimeStamp();  //
  // loop frame map to get max kth distance
  for (auto &iter : node_store_) {
    LRUKNode node = iter.second;
    if (node.IsEvictable()) {
      if (node.GetK() < k_) {
        max_kth_distance = INT64_MAX;
        if (earliest_frame_time > node.GetHistory().front()) {
          earliest_frame_time = node.GetHistory().front();
          *frame_id = iter.first;
          res = true;
        }
      } else if (max_kth_distance < (current_time - node.GetHistory().front())) {
        max_kth_distance = current_time - node.GetHistory().front();
        *frame_id = iter.first;
        res = true;
      }
    }
  }

  if (res) {
    latch_.unlock();
    Remove(*frame_id);
    return true;
  }
  latch_.unlock();
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  latch_.lock();

  if (replacer_size_ < static_cast<size_t>(frame_id)) {
    latch_.unlock();
    throw std::runtime_error("frame id is invalid.");
  }

  auto iter = node_store_.find(frame_id);
  // 在replacer中不存在则新追加
  if (iter == node_store_.end()) {
    //  如果不在缓存池中则追加
    LRUKNode node;
    node.SetFid(frame_id);
    node.AddOneToK();
    node.SetIsEvictable(true);
    std::list<size_t> hist{GetCurrentTimeStamp()};
    node.SetHistory(hist);
    node_store_[frame_id] = node;
    curr_size_++;
  } else {
    // 存在则，更新第更新时间戳
    LRUKNode node = iter->second;
    // check k
    if (node.GetK() < k_) {
      node.AddOneToK();
      node.GetHistory().push_back(GetCurrentTimeStamp());
    } else {
      node.GetHistory().pop_front();
      node.GetHistory().push_back(GetCurrentTimeStamp());
    }
    node_store_[frame_id] = node;
  }
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();

  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    latch_.unlock();
    return;
  }
  LRUKNode node = iter->second;
  if (node.IsEvictable() && !set_evictable) {
    curr_size_--;
  } else if (!node.IsEvictable() && set_evictable) {
    curr_size_++;
  }

  node.SetIsEvictable(set_evictable);
  node_store_[frame_id] = node;
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();

  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    latch_.unlock();
    return;
  }

  if (!iter->second.IsEvictable()) {
    latch_.unlock();
    throw std::runtime_error("Can't remove inevitable frame/");
  }
  node_store_.erase(iter);
  curr_size_--;
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

auto LRUKReplacer::GetCurrentTimeStamp() -> size_t {
  // Convert the system time to a time_t object
  size_t current_time = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::high_resolution_clock::now().time_since_epoch())
                            .count();

  return static_cast<size_t>(current_time);
}

auto LRUKNode::GetHistory() -> std::list<size_t> & { return history_; }
void LRUKNode::SetHistory(const std::list<size_t> &history) { history_ = history; }
auto LRUKNode::GetK() const -> size_t { return k_; }
void LRUKNode::SetK(size_t k) { k_ = k; }
auto LRUKNode::GetFid() const -> frame_id_t { return fid_; }
void LRUKNode::SetFid(frame_id_t fid) { fid_ = fid; }
auto LRUKNode::IsEvictable() const -> bool { return is_evictable_; }
void LRUKNode::SetIsEvictable(bool isEvictable) { is_evictable_ = isEvictable; }
void LRUKNode::AddOneToK() { k_++; }
}  // namespace bustub
