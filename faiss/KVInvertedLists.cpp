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
                                 std::function<size_t(const std::string &key, const void *data, size_t size)> put,
                                 std::function<void *(const std::string &key, size_t &size)> get,
                                 std::function<void(const void *)> release
)
    : InvertedLists(nlist, code_size), put_(put), get_(get), release_(release) {
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
  size_t o, co;
  auto o_ids = get_(to_ids_key(list_no), o);
  FAISS_THROW_IF_NOT_FMT(o_ids != nullptr, "get list ids failed, list_no = %zu", list_no);
  ScopedData so_ids([this, o_ids] { this->release_(o_ids); });
  o /= idx_t_size;
  auto o_codes = get_(to_codes_key(list_no), co);
  FAISS_THROW_IF_NOT_FMT(o_codes != nullptr, "get list codes failed, list_no = %zu", list_no);
  ScopedData so_codes([this, o_codes] { this->release_(o_codes); });

  auto n_ids = new faiss::Index::idx_t[o + n_entry];
  ScopedData sn_ids([n_ids] { delete[] n_ids; });
  memcpy(n_ids, o_ids, o * idx_t_size);
  memcpy(n_ids + o, ids, n_entry * idx_t_size);
  auto n_put = put_(to_ids_key(list_no), n_ids, (o + n_entry) * idx_t_size);
  FAISS_THROW_IF_NOT_FMT(n_put != (o + n_entry) * idx_t_size, "put list ids failed, list_no = %zu", list_no);

  auto n_codes = new uint8_t[(o + n_entry) * code_size];
  ScopedData sn_codes([n_codes] { delete[] n_codes; });
  memcpy(n_codes, o_codes, o * code_size);
  memcpy(n_codes + o * code_size, code, n_entry * code_size);
  n_put = put_(to_codes_key(list_no), n_codes, (o + n_entry) * code_size);
  FAISS_THROW_IF_NOT_FMT(n_put != (o + n_entry) * code_size, "put list codes failed, list_no = %zu", list_no);
  return o;
}

void KVInvertedLists::update_entries(size_t list_no,
                                     size_t offset,
                                     size_t n_entry,
                                     const faiss::Index::idx_t *ids,
                                     const uint8_t *code) {
  assert(list_no < nlist);

  size_t o, co;
  auto o_ids = get_(to_ids_key(list_no), o);
  FAISS_THROW_IF_NOT_FMT(o_ids != nullptr, "get list ids failed, list_no = %zu", list_no);
  ScopedData so_ids([this, o_ids] { this->release_(o_ids); });
  o /= idx_t_size;
  auto o_codes = get_(to_codes_key(list_no), co);
  FAISS_THROW_IF_NOT_FMT(o_codes != nullptr, "get list codes failed, list_no = %zu", list_no);
  ScopedData so_codes([this, o_codes] { this->release_(o_codes); });

  assert(offset + n_entry <= o);
  memcpy((uint8_t *) o_ids + offset * idx_t_size, ids, n_entry * idx_t_size);
  auto n_put = put_(to_ids_key(list_no), o_ids, o * idx_t_size);
  FAISS_THROW_IF_NOT_FMT(n_put != o * idx_t_size, "put list ids failed, list_no = %zu", list_no);
  memcpy((uint8_t *) o_codes + offset * code_size, code, n_entry * code_size);
  n_put = put_(to_codes_key(list_no), o_codes, o * code_size);
  FAISS_THROW_IF_NOT_FMT(n_put != o * code_size, "put list codes faile, list_no = %zu", list_no);
}

void KVInvertedLists::resize(size_t list_no, size_t new_size) {
  assert(list_no < nlist);

  size_t o, co;
  auto o_ids = get_(to_ids_key(list_no), o);
  FAISS_THROW_IF_NOT_FMT(o_ids != nullptr, "get list ids failed, list_no = %zu", list_no);
  ScopedData so_ids([this, o_ids] { this->release_(o_ids); });
  o /= idx_t_size;
  auto o_codes = get_(to_codes_key(list_no), co);
  FAISS_THROW_IF_NOT_FMT(o_codes != nullptr, "get list codes failed, list_no = %zu", list_no);
  ScopedData so_codes([this, o_codes] { this->release_(o_codes); });

  auto n_ids = new faiss::Index::idx_t[new_size];
  ScopedData sn_ids([n_ids] { delete[] n_ids; });
  memcpy(n_ids, o_ids, new_size * idx_t_size);
  auto n_put = put_(to_ids_key(list_no), n_ids, new_size * idx_t_size);
  FAISS_THROW_IF_NOT_FMT(n_put != new_size * idx_t_size, "put list ids failed, list_no = %zu", list_no);

  auto n_codes = new uint8_t[new_size * code_size];
  ScopedData sn_codes([n_codes] { delete[] n_codes; });
  memcpy(n_codes, o_codes, new_size * code_size);
  n_put = put_(to_codes_key(list_no), n_codes, new_size * code_size);
  FAISS_THROW_IF_NOT_FMT(n_put != new_size * code_size, "put list failed, list_no = %zu", list_no);
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