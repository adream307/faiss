/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef FAISS_MAP_INVERTED_LISTS_H
#define FAISS_MAP_INVERTED_LISTS_H

#include <faiss/InvertedLists.h>
#include <map>
#include <cassert>
#include <cstring>

namespace faiss {
struct MapInvertedLists : InvertedLists {
  struct Entry {
    size_t list_no;
    std::vector<idx_t> ids;
    std::vector<uint8_t> codes;
  };

  std::map<size_t, Entry> datas;

  MapInvertedLists(size_t nlist, size_t code_size);
  virtual ~MapInvertedLists() noexcept = default;

  size_t list_size(size_t list_no) const override;
  const uint8_t *get_codes(size_t list_no) const override;
  const idx_t *get_ids(size_t list_no) const override;

  size_t add_entries(size_t list_no, size_t n_entry, const idx_t *ids, const uint8_t *code) override;
  void update_entries(size_t list_no, size_t offset, size_t n_entry, const idx_t *ids, const uint8_t *code) override;

};

}// namspace faiss

#endif //FAISS_FAISS_MAPINVERTEDLISTS_H
