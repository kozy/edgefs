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
#include "errno.h"
*/
import "C"
import "unsafe"

import (
	"fmt"
	"os"
	"strings"

	"github.com/Nexenta/edgefs/src/efscli/efsutil"
	//"github.com/Nexenta/edgefs/src/efscli/validate"
	"github.com/spf13/cobra"
)

func snapshotRollback(snapViewPath, snapshotName string) error {

	// SnapView path parts
	snapPathParts := strings.SplitN(snapViewPath, "/", 4)

	c_svCluster := C.CString(snapPathParts[0])
	defer C.free(unsafe.Pointer(c_svCluster))

	c_svTenant := C.CString(snapPathParts[1])
	defer C.free(unsafe.Pointer(c_svTenant))

	c_svBucket := C.CString(snapPathParts[2])
	defer C.free(unsafe.Pointer(c_svBucket))

	c_svObject := C.CString(snapPathParts[3])
	defer C.free(unsafe.Pointer(c_svObject))
	
	// Snapshot name
	c_shName := C.CString(snapshotName)

	// Libccow Init
	conf, err := efsutil.GetLibccowConf()
	if err != nil {
		return err
	}

	c_conf := C.CString(string(conf))
	defer C.free(unsafe.Pointer(c_conf))

	//SnapView ccow_t
	var svtc C.ccow_t
	ret := C.ccow_tenant_init(c_conf, c_svCluster, C.strlen(c_svCluster)+1,
		c_svTenant, C.strlen(c_svTenant)+1, &svtc)
	if ret != 0 {
		return fmt.Errorf("%: snapView ccow_tenant_init err=%d\n", efsutil.GetFUNC(), ret)
	}
	defer C.ccow_tenant_term(svtc)


	var snapview_t C.ccow_snapview_t
	ret = C.ccow_snapview_new(&snapview_t, c_svBucket, C.strlen(c_svBucket)+1, c_svObject, C.strlen(c_svObject)+1)
	if ret != 0 && ret != -C.EEXIST {
		return fmt.Errorf("%s: snapView ccow_snapview_create err=%d\n", efsutil.GetFUNC(), ret)
	}
	defer C.ccow_snapview_destroy(svtc, snapview_t)

	ret = C.ccow_snapshot_rollback(svtc, snapview_t, c_shName, C.strlen(c_shName)+1)
	if ret != 0 && ret != -C.EEXIST {
		return fmt.Errorf("%s: snapshot rollback error: %d\n", efsutil.GetFUNC(), ret)
	}
	return nil
}

var (

	snapshotRollbackCmd = &cobra.Command{
		Use:   "snapshot-rollback <snapview-path> <snapshot>",
		Long:  "rollback existing snapview's snapshot",
		Short: "rollback a snapshot",
		//Args:  validate.Object,
		Run: func(cmd *cobra.Command, args []string) {

			/*edgefs object snapshot-rollback <cl/tn/bk/ob>.snapview <snapshotName> */
			if len(args) != 2 {
				fmt.Printf("Wrong parameters: Should be 'edgefs object snapshot-rollback <cl/tn/bk/sv.snapview> <snapshotName>'\n")
				return
			}

			snapViewPathParts := strings.Split(args[0], "/")
			if len(snapViewPathParts) != 4 {
				fmt.Printf("Wrong snapview path: %s", args[0])
				return
			}

			if !strings.HasSuffix(args[0], EDGEFS_SNAPVIEW_SUFFIX) {
				fmt.Printf("Not a snapview path: %s", args[0])
				return
			}

			err := snapshotRollback(args[0], args[1])
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}
)

func init() {
	ObjectCmd.AddCommand(snapshotRollbackCmd)
}


