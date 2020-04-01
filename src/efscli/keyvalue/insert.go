/*
 * Copyright (c) 2015-2018 Nexenta Systems, Inc.
 *
 * This file is part of EdgeFS Project
 * (see https://github.com/Nexenta/edgefs).
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package keyvalue

/*
#include <stdio.h>
#include "ccow.h"
#include "errno.h"
#include "msgpackalt.h"
#include "msgpackccow.h"
*/
import "C"
import "unsafe"

import (
	"fmt"
	"os"
	"strings"

	"github.com/Nexenta/edgefs/src/efscli/efsutil"
	"github.com/Nexenta/edgefs/src/efscli/validate"
	"github.com/spf13/cobra"
)

func keyValuePut(args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("Invalid input: key/value not provided")
	}

	opath := args[0]
	key := args[1]
	value := args[2]

	c_key := C.CString(key)
	defer C.free(unsafe.Pointer(c_key))

	c_value := C.CString(value)
	defer C.free(unsafe.Pointer(c_value))

	s := strings.SplitN(opath, "/", 4)

	c_cl := C.CString(s[0])
	defer C.free(unsafe.Pointer(c_cl))

	c_tn := C.CString(s[1])
	defer C.free(unsafe.Pointer(c_tn))

	c_bk := C.CString(s[2])
	defer C.free(unsafe.Pointer(c_bk))

	c_obj := C.CString(s[3])
	defer C.free(unsafe.Pointer(c_obj))

	conf, err := efsutil.GetLibccowConf()
	if err != nil {
		return err
	}

	c_conf := C.CString(string(conf))
	defer C.free(unsafe.Pointer(c_conf))

	var tc C.ccow_t
	ret := C.ccow_tenant_init(c_conf, c_cl, C.strlen(c_cl)+1,
		c_tn, C.strlen(c_tn)+1, &tc)
	if ret != 0 {
		return fmt.Errorf("ccow_tenant_init err=%d", ret)
	}
	defer C.ccow_tenant_term(tc)

	var comp C.ccow_completion_t
	ret = C.ccow_create_completion(tc, nil, nil, 1, &comp)
	if ret != 0 {
		return fmt.Errorf("%s: ccow_create_completion err=%d", efsutil.GetFUNC(), ret)
	}

	c_type := C.CString(string("btree_key_val"))
	defer C.free(unsafe.Pointer(c_type))

	ret = C.ccow_attr_modify_default(C.ccow_completion_t(comp), C.CCOW_ATTR_CHUNKMAP_TYPE, unsafe.Pointer(c_type), nil)
	if ret != 0 {
		return fmt.Errorf("set chunk map err=%d", ret)
	}

	var f C.uint16_t = C.RT_INLINE_DATA_TYPE_USER_KV
	ret = C.ccow_attr_modify_default(C.ccow_completion_t(comp), C.CCOW_ATTR_INLINE_DATA_FLAGS, unsafe.Pointer(&f), nil)
	if ret != 0 {
		return fmt.Errorf("set inline data type err=%d", ret)
	}

	p := C.msgpack_pack_init()

	var ver C.uint8_t = 2;
	C.msgpack_pack_uint8(p, ver)

	r := C.msgpack_pack_str(p, c_value)
	if r != 0 {
		return fmt.Errorf("%s: packing error=%d", efsutil.GetFUNC(), r)
	}

	var uv_b C.uv_buf_t
	C.msgpack_get_buffer(p, &uv_b)

	var iov [2]C.struct_iovec
	iov[0].iov_base = unsafe.Pointer(c_key)
	iov[0].iov_len = C.strlen(c_key) + 1
	iov[1].iov_base = unsafe.Pointer(uv_b.base)
	iov[1].iov_len = uv_b.len


	ret = C.ccow_insert_list(c_bk, C.strlen(c_bk) + 1, c_obj, C.strlen(c_obj) + 1, comp, &iov[0], 2)
	if ret != 0 {
		C.ccow_release(comp)
		return fmt.Errorf("%s: ccow_delete_list_cont err=%d", efsutil.GetFUNC(), ret)
	}

	ret = C.ccow_wait(comp, 0)
	if ret != 0 {
		return fmt.Errorf("%s: ccow_wait err=%d", efsutil.GetFUNC(), ret)
	}


	return nil
}

var (
	putCmd = &cobra.Command{
		Use:   "insert  <cluster>/<tenant>/<bucket>/<kvobject> <key> <value>",
		Short: "insert a new key/value",
		Long:  "insert a new key/value",
		Args:  validate.Object,
		Run: func(cmd *cobra.Command, args []string) {
			err := keyValuePut(args)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}
)

func init() {
	KeyValueCmd.AddCommand(putCmd)
}
