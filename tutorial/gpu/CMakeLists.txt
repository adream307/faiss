# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_executable(exp1 EXCLUDE_FROM_ALL exp1.cpp)
target_link_libraries(exp1 PRIVATE faiss)
