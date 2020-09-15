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
    InvertedLists(nlist, code_size) {
    for (size_t i = 0; i < nlist; i++) {
        Entry en;
        en.list_no = i;
        datas[i] = en;
    }
}

size_t MapInvertedLists::list_size(size_t list_no) const {
    assert (list_no < nlist);
    auto it = datas.find(list_no);
    return it->second.ids.size();
}

const uint8_t *MapInvertedLists::get_codes(size_t list_no) const {
    assert (list_no < nlist);
    auto it = datas.find(list_no);
    return it->second.codes.data();
}

const InvertedLists::idx_t *MapInvertedLists::get_ids(size_t list_no) const {
    assert (list_no < nlist);
    auto it = datas.find(list_no);
    return it->second.ids.data();
}

size_t MapInvertedLists::add_entries(size_t list_no, size_t n_entry, const idx_t *ids, const uint8_t *code) {
    if (n_entry == 0) {
        return 0;
    }
    assert (list_no < nlist);
    auto it = datas.find(list_no);
    size_t o = it->second.ids.size();
    it->second.ids.resize(o + n_entry);
    memcpy(&it->second.ids[o], ids, sizeof(idx_t) * n_entry);
    it->second.codes.resize((o + n_entry) * code_size);
    memcpy(&it->second.codes[o], code, code_size * n_entry);
    return o;
}

void MapInvertedLists::update_entries(size_t list_no,
                                      size_t offset,
                                      size_t n_entry,
                                      const InvertedLists::idx_t *ids,
                                      const uint8_t *code) {
    assert (list_no < nlist);
    auto it = datas.find(list_no);
    assert(n_entry + offset <= it->second.ids.size());
    memcpy(&it->second.ids[offset], ids, sizeof(idx_t) * n_entry);
    memcpy(&it->second.codes[offset * code_size], code, code_size * n_entry);

}

void MapInvertedLists::resize(size_t list_no, size_t new_size) {
    assert (list_no < nlist);
    auto it = datas.find(list_no);
    it->second.ids.resize(new_size);
    it->second.codes.resize(code_size * new_size);
}

}//namespace faiss
