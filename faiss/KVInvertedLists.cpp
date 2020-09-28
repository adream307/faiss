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
                                 size_t code_size)
    : InvertedLists(nlist, code_size) {
}

KVInvertedLists::~KVInvertedLists() noexcept {
}

size_t KVInvertedLists::list_size(size_t list_no) const {
  assert(list_no < nlist);
  size_t size;
  auto p = get_(to_ids_key(list_no), size);
  FAISS_THROW_IF_NOT_FMT(p != nullptr, "get list ids failed, list_no = %zu", list_no);
  release_(p);
  return size / idx_t_size;
}

const uint8_t *KVInvertedLists::get_codes(size_t list_no) const {
  assert(list_no < nlist);
  size_t size;
  auto p = get_(to_codes_key(list_no), size);
  FAISS_THROW_IF_NOT_FMT(p != nullptr, "get list codes failed, list_no = %zu", list_no);
  return reinterpret_cast<const uint8_t *>(p);
}

const faiss::Index::idx_t *KVInvertedLists::get_ids(size_t list_no) const {
  assert(list_no < nlist);
  size_t size;
  auto p = get_(to_ids_key(list_no), size);
  FAISS_THROW_IF_NOT_FMT(p != nullptr, "get list ids failed, list_no = %zu", list_no);
  return reinterpret_cast<const faiss::Index::idx_t *>(p);
}

void KVInvertedLists::release_codes(size_t list_no, const uint8_t *codes) const {
  release_(codes);
}

void KVInvertedLists::release_ids(size_t list_no, const idx_t *ids) const {
  release_(ids);
}

size_t KVInvertedLists::add_entries(size_t list_no,
                                    size_t n_entry,
                                    const faiss::Index::idx_t *ids,
                                    const uint8_t *code) {
  assert(list_no < nlist);
  
}

void KVInvertedLists::update_entries(size_t list_no,
                                     size_t offset,
                                     size_t n_entry,
                                     const faiss::Index::idx_t *ids,
                                     const uint8_t *code) {
  assert(list_no < nlist);
}

void KVInvertedLists::resize(size_t list_no, size_t new_size) {
  assert(list_no < nlist);
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