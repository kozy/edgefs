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
package object

/*
#include "ccow.h"
#include "ccowutil.h"
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
	"github.com/Nexenta/edgefs/src/efscli/edgex"
	"github.com/spf13/cobra"
)

func globalObjectDelete(cl string, tn string, bk string, obj string) error {
	c_cluster := C.CString(cl)
	defer C.free(unsafe.Pointer(c_cluster))

	c_tenant := C.CString(tn)
	defer C.free(unsafe.Pointer(c_tenant))

	c_bucket := C.CString(bk)
	defer C.free(unsafe.Pointer(c_bucket))

	c_object := C.CString(obj)
	defer C.free(unsafe.Pointer(c_object))

	conf, err := efsutil.GetLibccowConf()
	if err != nil {
		return err
	}

	c_conf := C.CString(string(conf))
	defer C.free(unsafe.Pointer(c_conf))

	var tc C.ccow_t

	ret := C.ccow_tenant_init(c_conf, c_cluster, C.strlen(c_cluster)+1,
		c_tenant, C.strlen(c_tenant)+1, &tc)
	if ret != 0 {
		return fmt.Errorf("ccow_tenant_init err=%d", ret)
	}
	defer C.ccow_tenant_term(tc)

	var c C.ccow_completion_t
	ret = C.ccow_create_completion(tc, nil, nil, 1, &c)
	if ret != 0 {
		return fmt.Errorf("ccow_create_completion err=%d", ret)
	}

	ret = C.ccow_delete_versioning(c_bucket, C.strlen(c_bucket)+1, c_object, C.strlen(c_object)+1, c)
	if ret != 0 {
		return fmt.Errorf("ccow_delete_versioning err=%d", ret)
	}

	ret = C.ccow_wait(c, 0)
	if ret != 0 {
		return fmt.Errorf("ccow_delete_versioning wait err=%d", ret)
	}

	return nil
}

func globalDelete(opath string) error {
	s := strings.SplitN(opath, "/", 4)

	err := edgex.GlobalVMAcquire(opath)
	if err != nil {
		return err
	}

	err = globalObjectDelete(s[0], s[1], s[2], s[3])


	e := edgex.GlobalVMRelease(opath)
	if e != nil {
		fmt.Printf("Put lock release error: %s, value : %v\n", opath, e)
	}
	return err
}


var (
	globalDeleteCmd = &cobra.Command{
		Use:   "globaldelete  <cluster>/<tenant>/<bucket>/<object>",
		Short: "globaldelete an existing object",
		Long:  "globaldelete an existing object",
		Args:  validate.Object,
		Run: func(cmd *cobra.Command, args []string) {
			err := globalDelete(args[0])
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}
)

func init() {
	ObjectCmd.AddCommand(globalDeleteCmd)
}
