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

MapInvertedLists::MapInvertedLists(size_t nlist, size_t code_size) :
    KVInvertedLists(nlist, code_size, nullptr, nullptr, nullptr) {
  for (size_t i = 0; i < nlist; i++) {
    Entry en;
    en.list_no = i;
    datas[i] = en;
  }
  InitKV();
}

MapInvertedLists::MapInvertedLists(const MapInvertedLists &ivl) :
    KVInvertedLists(ivl.nlist, ivl.code_size, nullptr, nullptr, nullptr) {
  datas = ivl.datas;
  InitKV();
}

MapInvertedLists::MapInvertedLists(MapInvertedLists &&ivl) :
    KVInvertedLists(ivl.nlist, ivl.code_size, nullptr, nullptr, nullptr) {
  datas = std::move(ivl.datas);
  InitKV();
}

void MapInvertedLists::InitKV() {
  put_ = [this](const std::string &key, const void *data, size_t size) {
    auto kt = parse_key(key);
    switch (kt.first) {
      case KVInvertedLists::KeyType::IDS: {
        datas[kt.second].ids.resize(size / idx_t_size);
        memcpy(datas[kt.second].ids.data(), data, size);
        return size;
      }
      case KVInvertedLists::KeyType::CODES: {
        datas[kt.second].codes.resize(size);
        memcpy(datas[kt.second].codes.data(), data, size);
        return size;
      }
      default: return size_t(0);
    }
  };

  get_ = [this](const std::string &key, size_t &size) {
    auto kt = parse_key(key);
    switch (kt.first) {
      case KVInvertedLists::KeyType::IDS: {
        size = datas[kt.second].ids.size() * idx_t_size;
        auto data = malloc(size);
        memcpy(data, datas[kt.second].ids.data(), size);
        return data;
      }
      case KVInvertedLists::KeyType::CODES: {
        size = datas[kt.second].codes.size();
        auto data = malloc(size);
        memcpy(data, datas[kt.second].codes.data(), size);
        return data;
      }
      default: return (void *) nullptr;
    }
  };

  release_ = [](const void *p) { free((void *) p); };

}

//size_t MapInvertedLists::list_size(size_t list_no) const {
//  assert (list_no < nlist);
//  auto it = datas.find(list_no);
//  return it->second.ids.size();
//}
//
//const uint8_t *MapInvertedLists::get_codes(size_t list_no) const {
//  assert (list_no < nlist);
//  auto it = datas.find(list_no);
//  return it->second.codes.data();
//}
//
//const InvertedLists::idx_t *MapInvertedLists::get_ids(size_t list_no) const {
//  assert (list_no < nlist);
//  auto it = datas.find(list_no);
//  return it->second.ids.data();
//}

//size_t MapInvertedLists::add_entries(size_t list_no, size_t n_entry, const idx_t *ids, const uint8_t *code) {
//  if (n_entry == 0) {
//    return 0;
//  }
//  assert (list_no < nlist);
//  auto it = datas.find(list_no);
//  size_t o = it->second.ids.size();
//  it->second.ids.resize(o + n_entry);
//  memcpy(&it->second.ids[o], ids, sizeof(idx_t) * n_entry);
//  it->second.codes.resize((o + n_entry) * code_size);
//  memcpy(&it->second.codes[o * code_size], code, code_size * n_entry);
//  return o;
//}
//
//void MapInvertedLists::update_entries(size_t list_no,
//                                      size_t offset,
//                                      size_t n_entry,
//                                      const InvertedLists::idx_t *ids,
//                                      const uint8_t *code) {
//  assert (list_no < nlist);
//  auto it = datas.find(list_no);
//  assert(n_entry + offset <= it->second.ids.size());
//  memcpy(&it->second.ids[offset], ids, sizeof(idx_t) * n_entry);
//  memcpy(&it->second.codes[offset * code_size], code, code_size * n_entry);
//}
//
//void MapInvertedLists::resize(size_t list_no, size_t new_size) {
//  assert (list_no < nlist);
//  auto it = datas.find(list_no);
//  it->second.ids.resize(new_size);
//  it->second.codes.resize(code_size * new_size);
//}


}//namespace faiss
