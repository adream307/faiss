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

KVInvertedLists::KVInvertedLists(size_t nlist, size_t code_size)
    : InvertedLists(nlist, code_size) {
  ids_.resize(nlist, nullptr);
  codes_.resize(nlist, nullptr);
}

KVInvertedLists::~KVInvertedLists() noexcept {
  for (auto p : ids_) delete p;
  for (auto p : codes_) delete p;
}

size_t KVInvertedLists::list_size(size_t list_no) const {
  assert(list_no < nlist);
  if (ids_[list_no] == nullptr) get_list(list_no);
  return ids_[list_no]->size() / idx_t_size;
}

const uint8_t *KVInvertedLists::get_codes(size_t list_no) const {
  assert(list_no < nlist);
  if (codes_[list_no] == nullptr) get_list(list_no);
  return reinterpret_cast<uint8_t *>(codes_[list_no]->data());
}

const faiss::Index::idx_t *KVInvertedLists::get_ids(size_t list_no) const {
  assert(list_no < nlist);
  if (ids_[list_no] == nullptr) get_list(list_no);
  return reinterpret_cast<const faiss::Index::idx_t *>(ids_[list_no]->data());
}

void KVInvertedLists::release_codes(size_t list_no, const uint8_t *codes) const {
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
  if (n_entry == 0) {
    return 0;
  }
  assert (list_no < nlist);
  get_ids(list_no);
  get_codes(list_no);
  auto o = ids_[list_no]->size();

  ids_[list_no]->resize((o + n_entry) * idx_t_size);
  memcpy(&ids_[list_no]->at(o * idx_t_size), ids, idx_t_size * n_entry);
  auto s = put(to_ids_key(list_no), ids_[list_no]->data(), ids_[list_no]->size());
  assert(s.ok());

  codes_[list_no]->resize((o + n_entry) * code_size);
  memcpy(&codes_[list_no]->at(o * code_size), code, n_entry * code_size);
  auto s = put(to_codes_key(list_no), code)

  if (ids_[list_no] == nullptr) get_ids(list_no);
  auto o = ids_[list_no]->size();
  o /= idx_t_size;
  ids_[list_no]->resize((o + n_entry) * idx_t_size);
  memcpy(&ids_[list_no]->at(o * idx_t_size), ids, idx_t_size * n_entry);
  auto s = Put(to_ids_key(list_no), *ids_[list_no]);
  assert(s.ok());

  if (codes_[list_no] == nullptr) get_codes(list_no);
  codes_[list_no]->resize((o + n_entry) * code_size);
  memcpy(&codes_[list_no]->at(o * code_size), code, code_size * n_entry);
  return o;
}

void KVInvertedLists::update_entries(size_t list_no,
                                     size_t offset,
                                     size_t n_entry,
                                     const faiss::Index::idx_t *ids,
                                     const uint8_t *code) {
  assert (list_no < nlist);
  if (ids_[list_no] == nullptr) get_ids(list_no);
  assert(n_entry + offset <= ids_[list_no]->size() / idx_t_size);

  std::vector<uint8_t> data_buffer;
  data_buffer.resize(list_size_o * idx_t_size);
  memcpy(data_buffer.data(), ids_o, idx_t_size * offset);
  memcpy(&data_buffer[idx_t_size * offset], ids, idx_t_size * n_entry);
  memcpy(&data_buffer[idx_t_size * (offset + n_entry)],
         ids_o + offset + n_entry,
         idx_t_size * (list_size_o - offset - n_entry));
  s = put_ids(list_no, data_buffer);
  assert(s.ok());

  data_buffer.resize(codes_size_o);
  memcpy(data_buffer.data(), codes_o, code_size * offset);
  memcpy(&data_buffer[offset * code_size], code, code_size * n_entry);
  memcpy(&data_buffer[code_size * (offset + n_entry)],
         codes_o + (code_size * (offset + n_entry)),
         code_size * (list_size_o - offset - n_entry));
  s = put_codes(list_no, data_buffer);
  assert(s.ok());
}

void KVInvertedLists::resize(size_t list_no, size_t new_size) {
  assert (list_no < nlist);
  const faiss::Index::idx_t *ids_o = nullptr;
  size_t list_size_o;
  auto s = get_ids(list_no, ids_o, list_size_o);
  assert(s.ok());

  const uint8_t *codes_o = nullptr;
  size_t codes_size_o;
  s = get_codes(list_no, codes_o, codes_size_o);
  assert(s.ok());
  assert(codes_size_o == (list_size_o * code_size));

  size_t min_size = list_size_o;
  if (new_size < min_size) min_size = new_size;

  std::vector<uint8_t> data_buffer;
  data_buffer.resize(idx_t_size * new_size);
  memcpy(data_buffer.data(), ids_o, idx_t_size * min_size);
  s = put_ids(list_no, data_buffer);
  assert(s.ok());
  data_buffer.resize(new_size * code_size);
  memcpy(data_buffer.data(), codes_o, code_size * min_size);
  s = put_codes(list_no, data_buffer);
}

std::string KVInvertedLists::to_ids_key(size_t list_no) {
  return "list-" + std::to_string(list_no) + "/ids";
}

std::string KVInvertedLists::to_codes_key(size_t list_no) {
  return "list-" + std::to_string(list_no) + "/codes";
}

void KVInvertedLists::get_list(size_t list_no) const {
  auto ids = new std::string;
  auto codes = new std::string;
  auto s = get(list_no, ids, codes);
  FAISS_THROW_IF_NOT_MSG(s.ok(), "get list failed");
  delete ids_[list_no];
  delete codes_[list_no];
  ids_[list_no] = ids;
  codes_[list_no] = codes;
}

void KVInvertedLists::put_list(size_t list_no) const {
  if (ids_[list_no] == nullptr || codes_[list_no] == nullptr) {
    FAISS_THROW_MSG("can't put list if data is nullptr");
  }
  auto s = put(list_no,
               ids_[list_no]->size() / idx_t_size,
               reinterpret_cast<const faiss::Index::idx_t *>(ids_[list_no]->data()),
               reinterpret_cast<const uint8_t *>(codes_[list_no]->data()));
  FAISS_THROW_IF_NOT_MSG(s.ok(), "put list failed");
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