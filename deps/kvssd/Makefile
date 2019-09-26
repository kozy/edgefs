##
## Copyright (c) 2015-2018 Nexenta Systems, inc.
##
## This file is part of EdgeFS Project
## (see https://github.com/Nexenta/edgefs).
##
## Licensed to the Apache Software Foundation (ASF) under one
## or more contributor license agreements.  See the NOTICE file
## distributed with this work for additional information
## regarding copyright ownership.  The ASF licenses this file
## to you under the Apache License, Version 2.0 (the
## "License"); you may not use this file except in compliance
## with the License.  You may obtain a copy of the License at
##
##   http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing,
## software distributed under the License is distributed on an
## "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
## KIND, either express or implied.  See the License for the
## specific language governing permissions and limitations
## under the License.
##

SRCDIR=$(NEDGE_HOME)
NAME=KVSSD
KVS_PATH=$(NAME)/PDK/core
BUILD_DIR=${CURDIR}/../../

ifdef NEDGE_NDEBUG
DEBUG_FLAGS=-DUSE_JE_MALLOC
DEBUG_LDFLAGS=-ljemalloc -fno-omit-frame-pointer
else
DEBUG_FLAGS=-fsanitize=address -fno-omit-frame-pointer -fno-common

ifeq (${CC},clang)
DEBUG_LDFLAGS=-fsanitize=address
else
DEBUG_LDFLAGS=-lasan
endif
 
endif

.configure:
	cd ../.. && flock -e .gitmodules git submodule update --depth 1 --recursive --init deps/kvssd/$(NAME)
	$(KVS_PATH)/tools/install_deps.sh
	mkdir -p $(KVS_PATH)/build 
	touch $@

.build: .configure
	cd $(KVS_PATH)/build && cmake -DWITH_KDD=ON ../
	PREFIX=$(SRCDIR) EXTFLAGS="$(CFLAGS) $(DEBUG_FLAGS)" \
	LDFLAGS="$(LDFLAGS) $(DEBUG_LDFLAGS)" LIBS="$(DEBUG_LDFLAGS)" \
	make -C $(KVS_PATH)/build -j88
	touch $@

install: .build
	cp -f $(KVS_PATH)/build/libkvapi.so $(SRCDIR)/lib
	cp -rf $(KVS_PATH)/include $(BUILD_DIR)/src/ccow/src/include/kvssd
	cp -f $(KVS_PATH)/src/device_abstract_layer/include/kvs_adi.h $(BUILD_DIR)/src/ccow/src/include/kvssd

all: install

uninstall: .configure
	rm -f $(SRCDIR)/lib/libkvapi.so
	rm -rf $(BUILD_DIR)/src/ccow/src/include/kvssd

clean:
	rm -rf $(NAME); mkdir $(NAME)
	rm -f .configure .build
