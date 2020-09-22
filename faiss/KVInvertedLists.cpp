/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/InvertedLists.h>
#include <cassert>
#include <cstring>
namespace faiss {

KVInvertedLists::KVInvertedLists(size_t nlist, size_t code_size, KVPutF p, KVGetF g)
    : InvertedLists(nlist, code_size), put(p), get(g) {
}

size_t KVInvertedLists::list_size(size_t list_no) const {
  assert(list_no < nlist);
  const faiss::Index::idx_t *data = nullptr;
  size_t size;
  auto s = get_ids(list_no, data, size);
  assert(s.ok());
  return size;
}

const uint8_t *KVInvertedLists::get_codes(size_t list_no) const {
  assert(list_no < nlist);
  const uint8_t *data = nullptr;
  size_t size;
  auto s = get_codes(list_no, data, size);
  assert(s.ok());
  return data;
}

const faiss::Index::idx_t *KVInvertedLists::get_ids(size_t list_no) const {
  assert(list_no < nlist);
  const faiss::Index::idx_t *data = nullptr;
  size_t size;
  auto s = get_ids(list_no, data, size);
  assert(s.ok());
  return data;
}

size_t KVInvertedLists::add_entries(size_t list_no,
                                    size_t n_entry,
                                    const faiss::Index::idx_t *ids,
                                    const uint8_t *code) {
  if (n_entry == 0) {
    return 0;
  }
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

  std::vector<faiss::Index::idx_t> ids_v(n_entry + list_size_o);
  memcpy(ids_v.data(), ids_o, sizeof(faiss::Index::idx_t) * list_size_o);
  memcpy(&ids_v[0], ids, sizeof(faiss::Index::idx_t) * n_entry);
  s = put_ids(list_no, ids_v.data(), ids_v.size());
  assert(s.ok());

  std::vector<uint8_t> codes_v((n_entry + list_size_o) * code_size);
  memcpy(codes_v.data(), codes_o, codes_size_o);
  memcpy(&codes_v[codes_size_o], code, code_size * n_entry);
  s = put_codes(list_no, codes_v.data(), codes_v.size());
  assert(s.ok());
  return list_size_o;
}

void KVInvertedLists::update_entries(size_t list_no,
                                     size_t offset,
                                     size_t n_entry,
                                     const faiss::Index::idx_t *ids,
                                     const uint8_t *code) {
  assert (list_no < nlist);

  const faiss::Index::idx_t *ids_o = nullptr;
  size_t list_size_o;
  auto s = get_ids(list_no, ids_o, list_size_o);
  assert(s.ok());
  assert(n_entry + offset <= list_size_o);

  const uint8_t *codes_o = nullptr;
  size_t codes_size_o;
  s = get_codes(list_no, codes_o, codes_size_o);
  assert(s.ok());
  assert(codes_size_o == (list_size_o * code_size));

  std::vector<faiss::Index::idx_t> ids_v(list_size_o);
  memcpy(ids_v.data(), ids_o, sizeof(faiss::Index::idx_t) * offset);
  memcpy(&ids_v[offset], ids, sizeof(faiss::Index::idx_t) * n_entry);
  memcpy(&ids_v[offset + n_entry],
         ids_o + offset + n_entry,
         sizeof(faiss::Index::idx_t) * (list_size_o - offset - n_entry));
  s = put_ids(list_no, ids_v.data(), ids_v.size());
  assert(s.ok());

  std::vector<uint8_t> codes_v(codes_size_o);
  memcpy(codes_v.data(), codes_o, code_size * offset);
  memcpy(&codes_v[offset * code_size], code, code_size * n_entry);
  memcpy(&codes_v[code_size * (offset + n_entry)],
         codes_o + (code_size * (offset + n_entry)),
         code_size * (list_size_o - offset - n_entry));
  s = put_codes(list_no, codes_v.data(), codes_v.size());
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

  std::vector<faiss::Index::idx_t> ids_v(new_size);
  memcpy(ids_v.data(), ids_o, sizeof(faiss::Index::idx_t) * min_size);
  std::vector<uint8_t> codes_v(new_size * code_size);
  memcpy(codes_v.data(), codes_o, code_size * min_size);
}

Status KVInvertedLists::get_ids(size_t list_no, const faiss::Index::idx_t *&ids, size_t &list_size) const {
  return ReadVector(ids, list_size, [this, list_no](KVEntry &en) {
    en.AddKeyPrefix("list-" + std::to_string(list_no) + "/ids");
    return this->get(en);
  });
}
Status KVInvertedLists::get_codes(size_t list_no, const uint8_t *&codes, size_t &codes_size) const {
  return ReadVector(codes, codes_size, [this, list_no](KVEntry &en) {
    en.AddKeyPrefix("list-" + std::to_string(list_no) + "/codes");
    return this->get(en);
  });
}
Status KVInvertedLists::put_ids(size_t list_no, const faiss::Index::idx_t *ids, size_t list_size) {
  return WriteVector(ids, list_size, [this, list_no](const KVEntry &en) {
    en.AddKeyPrefix("list-" + std::to_string(list_no) + "/ids");
    return this->put(en);
  });
}
Status KVInvertedLists::put_codes(size_t list_no, const uint8_t *codes, size_t codes_size) {
  return WriteVector(codes, codes_size, [this, list_no](const KVEntry &en) {
    en.AddKeyPrefix("list-" + std::to_string(list_no) + "/codes");
    return this->put(en);
  });
}


}