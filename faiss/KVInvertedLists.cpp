/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/InvertedLists.h>
#include <faiss/impl/FaissAssert.h>
#include <cassert>
#include <cstring>
namespace faiss {

KVInvertedLists::KVInvertedLists(size_t nlist,
                                 size_t code_size,
                                 std::function<bool(const std::string &key, const std::string &value)> put,
                                 std::function<bool(const std::string &key, std::string &value)> get
)
    : InvertedLists(nlist, code_size), put_(std::move(put)), get_(std::move(get)) {
  ids_.resize(nlist, nullptr);
  codes_.resize(nlist, nullptr);
}

KVInvertedLists::~KVInvertedLists() noexcept {
  for (auto p:ids_) delete p;
  for (auto p:codes_) delete p;
}

size_t KVInvertedLists::list_size(size_t list_no) const {
  assert(list_no < nlist);
  FAISS_THROW_IF_NOT_FMT(ids_[list_no] != nullptr, "have not prefetch list %zu", list_no);
  return ids_[list_no]->size() / idx_t_size;
}

const uint8_t *KVInvertedLists::get_codes(size_t list_no) const {
  assert(list_no < nlist);
  FAISS_THROW_IF_NOT_FMT(codes_[list_no] != nullptr, "have not prefetch list %zu", list_no);
  return reinterpret_cast<const uint8_t *>(codes_[list_no]->data());
}

const faiss::Index::idx_t *KVInvertedLists::get_ids(size_t list_no) const {
  assert(list_no < nlist);
  FAISS_THROW_IF_NOT_FMT(ids_[list_no] != nullptr, "have not prefetch list %zu", list_no);
  return reinterpret_cast<const faiss::Index::idx_t *>(ids_[list_no]->data());
}

void KVInvertedLists::prefetch_lists(const idx_t *list_nos, int nlist) const {
  for (int i = 0; i < nlist; i++) {
    get_lists(list_nos[i]);
  }
}

size_t KVInvertedLists::add_entries(size_t list_no,
                                    size_t n_entry,
                                    const faiss::Index::idx_t *ids,
                                    const uint8_t *code) {
  if (n_entry == 0) return 0;
  assert(list_no < nlist);
  get_lists(list_no);
  auto o = ids_[list_no]->size() / idx_t_size;
  ids_[list_no]->resize((o + n_entry) * idx_t_size);
  memcpy(&ids_[list_no]->at(o * idx_t_size), ids, n_entry * idx_t_size);
  codes_[list_no]->resize((o + n_entry) * code_size);
  memcpy(&codes_[list_no]->at(o * code_size), code, n_entry * code_size);
  put_lists(list_no);
  return o;
}

void KVInvertedLists::update_entries(size_t list_no,
                                     size_t offset,
                                     size_t n_entry,
                                     const faiss::Index::idx_t *ids,
                                     const uint8_t *code) {
  assert(list_no < nlist);
  get_lists(list_no);
  auto o = ids_[list_no]->size() / idx_t_size;
  assert(offset + n_entry <= o);
  memcpy(&ids_[list_no]->at(offset * idx_t_size), ids, n_entry * idx_t_size);
  memcpy(&codes_[list_no]->at(offset * code_size), code, n_entry * code_size);
  put_lists(list_no);
}

void KVInvertedLists::resize(size_t list_no, size_t new_size) {
  assert(list_no < nlist);
  get_lists(list_no);
  ids_[list_no]->resize(new_size * idx_t_size);
  codes_[list_no]->resize(new_size * code_size);
}

void KVInvertedLists::reset() {
  for (size_t i = 0; i < nlist; i++) {
    delete ids_[i];
    ids_[i] = nullptr;
    delete codes_[i];
    codes_[i] = nullptr;
  }
}

void KVInvertedLists::merge_from(InvertedLists *oivf, size_t add_id) {
  std::vector<faiss::Index::idx_t> list_nos(nlist);
  for (size_t i = 0; i < nlist; i++) list_nos[i] = i;
  oivf->prefetch_lists(list_nos.data(), nlist);
  prefetch_lists(list_nos.data(), nlist);
  InvertedLists::merge_from(oivf, add_id);
  for (size_t i = 0; i < nlist; i++) if (ids_[i] != nullptr) put_lists(i);
}

void KVInvertedLists::copy_lists(KVInvertedLists &&lists) {
  assert(nlist == lists.nlist);
  assert(code_size == lists.code_size);
  reset();
  ids_ = std::move(lists.ids_);
  codes_ = std::move(lists.codes_);
}
void KVInvertedLists::copy_lists(const KVInvertedLists &lists) {
  assert(nlist == lists.nlist);
  assert(code_size == lists.code_size);
  reset();

  for (size_t i = 0; i < nlist; i++) {
    if (lists.ids_[i] != nullptr) ids_[i] = new std::string(*lists.ids_[i]);
    else ids_[i] = nullptr;

    if (lists.codes_[i] != nullptr) codes_[i] = new std::string(*lists.codes_[i]);
    else codes_[i] = nullptr;
  }
}

void KVInvertedLists::put_lists(size_t list_no) {
  if (ids_[list_no] == nullptr || codes_[list_no] == nullptr) {
    FAISS_THROW_FMT("can't put list while list is null, list_no = %zu", list_no);
  }
  auto b = put_(to_ids_key(list_no), *ids_[list_no]);
  FAISS_THROW_IF_NOT_FMT(b, "put list ids faild, list_no = %zu", list_no);
  b = put_(to_codes_key(list_no), *codes_[list_no]);
  FAISS_THROW_IF_NOT_FMT(b, "put list codes failed, list_no = %zu", list_no);
}

void KVInvertedLists::get_lists(size_t list_no) const {
  auto id = new std::string;
  auto b = get_(to_ids_key(list_no), *id);
  FAISS_THROW_IF_NOT_FMT(b, "prefetch list ids failed, list_no = %zu", list_no);
  auto code = new std::string;
  b = get_(to_codes_key(list_no), *code);
  FAISS_THROW_IF_NOT_FMT(b, "prefetch list codes failed, list_no = %zu", list_no);

  //std::lock_guard<std::mutex> lock(mux_);
  delete ids_[list_no];
  delete codes_[list_no];
  ids_[list_no] = id;
  codes_[list_no] = code;
}

std::string KVInvertedLists::to_ids_key(size_t list_no) {
  return "list-" + std::to_string(list_no) + "/ids";
}

std::string KVInvertedLists::to_codes_key(size_t list_no) {
  return "list-" + std::to_string(list_no) + "/codes";
}

std::pair<KVInvertedLists::KeyType, size_t> KVInvertedLists::parse_key(const std::string &key) {
  size_t list_no = 0;
  auto vector_slashe_pos = key.find_last_of('/');
  if (vector_slashe_pos == std::string::npos) return std::make_pair(KeyType::Others, list_no);

  auto list_slashe_pos = key.find_last_of('/', vector_slashe_pos - 1);
  if (list_slashe_pos == std::string::npos) list_slashe_pos = 0;
  else list_slashe_pos++;

  auto list_str = key.substr(list_slashe_pos, vector_slashe_pos - list_slashe_pos);
  if (list_str.length() < 6) return std::make_pair(KeyType::Others, list_no);
  if (list_str.substr(0, 5) != "list-")return std::make_pair(KeyType::Others, list_no);
  for (size_t i = 5; i < list_str.length(); i++) {
    if (list_str.at(i) > '9' || list_str.at(i) < '0') return std::make_pair(KeyType::Others, list_no);
    list_no = list_no * 10 + static_cast<size_t>(list_str.at(i) - '0');
  }

  auto vector_str = key.substr(vector_slashe_pos + 1);

  if (vector_str == "ids") return std::make_pair(KeyType::IDS, list_no);
  else if (vector_str == "codes") return std::make_pair(KeyType::CODES, list_no);
  else return std::make_pair(KeyType::Others, list_no);
}

}