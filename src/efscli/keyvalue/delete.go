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
#include "ccow.h"
#include "errno.h"
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

func deleteByKey(opath string, key string) error {
	if key == "" {
		return fmt.Errorf("Key not provided")
	}

	c_key := C.CString(key)
	defer C.free(unsafe.Pointer(c_key))

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

	var iov_key C.struct_iovec
	iov_key.iov_base = unsafe.Pointer(c_key)
	iov_key.iov_len = C.strlen(c_key) + 1

	ret = C.ccow_delete_list(c_bk, C.strlen(c_bk) + 1, c_obj, C.strlen(c_obj) + 1, comp, &iov_key, 1)
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
	expunge bool

	deleteCmd = &cobra.Command{
		Use:   "delete  <cluster>/<tenant>/<bucket>/<kvobject> <key>",
		Short: "delete an existing key from kvobject",
		Long:  "delete an existing key from kvobject",
		Args:  validate.Object,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			err = deleteByKey(args[0], args[1])
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}
)

func init() {
	KeyValueCmd.AddCommand(deleteCmd)
}
