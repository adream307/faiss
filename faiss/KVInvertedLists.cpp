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
                                 std::function<Status(size_t list_no,
                                                      size_t n_entry,
                                                      const idx_t *ids,
                                                      const uint8_t *codes)> put,
                                 std::function<Status(size_t list_no, std::string *ids, std::string *codes)> get,
                                 bool cache)
    : InvertedLists(nlist, code_size), put_(put), get_(get), cache_(cache) {
  ids_.resize(nlist, nullptr);
  codes_.resize(nlist, nullptr);
  ref_.resize(nlist, 0);
}

KVInvertedLists::~KVInvertedLists() noexcept {
  release_all();
}

size_t KVInvertedLists::list_size(size_t list_no) const {
  assert(list_no < nlist);
  if (ids_[list_no] == nullptr) get_list(list_no);
  return ids_[list_no]->size() / idx_t_size;
}

const uint8_t *KVInvertedLists::get_codes(size_t list_no) const {
  assert(list_no < nlist);
  if (codes_[list_no] == nullptr) get_list(list_no);
  ref_[list_no]++;
  return reinterpret_cast<const uint8_t *>(codes_[list_no]->data());
}

const faiss::Index::idx_t *KVInvertedLists::get_ids(size_t list_no) const {
  assert(list_no < nlist);
  if (ids_[list_no] == nullptr) get_list(list_no);
  return reinterpret_cast<const faiss::Index::idx_t *>(ids_[list_no]->data());
}

void KVInvertedLists::release_codes(size_t list_no, const uint8_t *codes) const {
  if (cache_) return;
  ref_[list_no]--;
  if (ref_[list_no] > 0) return;
  delete ids_[list_no];
  delete codes_[list_no];
  ids_[list_no] = nullptr;
  codes_[list_no] = nullptr;
}

void KVInvertedLists::release_ids(size_t list_no, const idx_t *ids) const {
}

size_t KVInvertedLists::add_entries(size_t list_no,
                                    size_t n_entry,
                                    const faiss::Index::idx_t *ids,
                                    const uint8_t *code) {
  assert(list_no < nlist);
  get_list(list_no);
  auto o = ids_[list_no]->size() / idx_t_size;
  if (n_entry == 0) return o;
  ids_[list_no]->resize((o + n_entry) * idx_t_size);
  memcpy(&ids_[list_no]->at(o * idx_t_size), ids, n_entry * idx_t_size);
  codes_[list_no]->resize((o + n_entry) * code_size);
  memcpy(&codes_[list_no]->at(o * code_size), code, n_entry * code_size);
  put_list(list_no);
  return o;
}

void KVInvertedLists::update_entries(size_t list_no,
                                     size_t offset,
                                     size_t n_entry,
                                     const faiss::Index::idx_t *ids,
                                     const uint8_t *code) {
  assert(list_no < nlist);
  get_list(list_no);
  auto o = ids_[list_no]->size() / idx_t_size;
  assert(offset + n_entry <= o);
  memcpy(&ids_[list_no]->at(offset * idx_t_size), ids, n_entry * idx_t_size);
  memcpy(&codes_[list_no]->at(offset * code_size), code, n_entry * code_size);
  put_list(list_no);
}

void KVInvertedLists::resize(size_t list_no, size_t new_size) {
  assert(list_no < nlist);
  get_list(list_no);
  ids_[list_no]->resize(new_size * idx_t_size);
  codes_[list_no]->resize(new_size * code_size);
  put_list(list_no);
}

void KVInvertedLists::get_list(size_t list_no) const {
  if (cache_ && ids_[list_no] != nullptr) return;

  auto ids = new std::string;
  auto codes = new std::string;
  auto s = get_(list_no, ids, codes);
  FAISS_THROW_IF_NOT_MSG(s.ok(), "get list failed");
  FAISS_THROW_IF_NOT_MSG((ids->size() / idx_t_size == codes->size() / code_size), "get list failed");
  delete ids_[list_no];
  delete codes_[list_no];
  ids_[list_no] = ids;
  codes_[list_no] = codes;
}

void KVInvertedLists::put_list(size_t list_no) const {
  if (ids_[list_no] == nullptr && codes_[list_no] == nullptr) return;
  if (ids_[list_no] != nullptr && codes_[list_no] != nullptr) {
    auto s = put_(list_no,
                  ids_[list_no]->size() / idx_t_size,
                  reinterpret_cast<const faiss::Index::idx_t *>(ids_[list_no]->data()),
                  reinterpret_cast<const uint8_t *>(codes_[list_no]->data()));
    FAISS_THROW_IF_NOT_MSG(s.ok(), "put list failed");
  } else {
    FAISS_THROW_MSG("can't put list if data is nullptr");
  }
}

void KVInvertedLists::release_all() const {
  for (int i = 0; i < nlist; i++) {
    delete ids_[i];
    ids_[i] = nullptr;
    delete codes_[i];
    codes_[i] = nullptr;
    ref_[i] = 0;
  }
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