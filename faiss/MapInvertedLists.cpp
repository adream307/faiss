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
    KVInvertedLists(nlist, code_size,nullptr,nullptr) {
  for (size_t i = 0; i < nlist; i++) {
    Entry en;
    en.list_no = i;
    datas[i] = en;
  }
}


}//namespace faiss
