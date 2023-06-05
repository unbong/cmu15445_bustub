#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {

  this->is_dirty_ = that.is_dirty_;
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_own_page_ = that.is_own_page_;
  that.is_own_page_ = false;
}

void BasicPageGuard::Drop() {

  // clear memory
  memset(page_->GetData(), 0, BUSTUB_PAGE_SIZE);
  this->bpm_->UnpinPage(this->page_->GetPageId(), this->is_dirty_);
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  this->bpm_->UnpinPage(this->page_->GetPageId(), this->is_dirty_);
  this->is_dirty_ = that.is_dirty_;
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_own_page_ = that.is_own_page_;
  that.is_own_page_ = false;
  return *this;

}

BasicPageGuard::~BasicPageGuard(){

    if(this->is_own_page_){
      this->bpm_->UnpinPage(this->page_->GetPageId(), this->is_dirty_);
    }

};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
    this->guard_ = BasicPageGuard(std::move(that.guard_));
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
    this->guard_ = std::move(that.guard_);
    return *this; }

void ReadPageGuard::Drop() {this->guard_.Drop();}

ReadPageGuard::~ReadPageGuard() {
    //this->guard_.
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
    this->guard_ = BasicPageGuard(std::move(that.guard_));
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
    this->guard_ = std::move(that.guard_);
    return *this;
}

void WritePageGuard::Drop() {this->guard_.Drop();}

WritePageGuard::~WritePageGuard() {}  // NOLINT

}  // namespace bustub
