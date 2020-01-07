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
*/
import "C"

import (
	"fmt"
	"os"
	"strings"

	"github.com/Nexenta/edgefs/src/efscli/efsutil"
	"github.com/Nexenta/edgefs/src/efscli/validate"
	"github.com/Nexenta/edgefs/src/efscli/edgex"
	"github.com/spf13/cobra"
)

func globalGetByKey(opath string, key string) error {
	if key == "" {
		return fmt.Errorf("Key not provided")
	}

	ep := edgex.GlobalGetPrepare(opath)
	if ep != nil {
		fmt.Printf("Get prepare error: %v", ep)
	}


	s := strings.SplitN(opath, "/", 4)

	res, err := efsutil.GetKeyValue(s[0], s[1], s[2], s[3], key, 1024*1024)

	if err != nil {
		return fmt.Errorf("Get key error:  %v", err)
	}

	fmt.Printf("%s\n", res)
	return nil
}

var (
	globalGetCmd = &cobra.Command{
		Use:   "globalget  <cluster>/<tenant>/<bucket>/<object> <key>",
		Short: "globalget value by key",
		Long:  "globalget value from kvobject by key",
		Args:  validate.Object,
		Run: func(cmd *cobra.Command, args []string) {
			err := globalGetByKey(args[0], args[1])
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}
)

func init() {
	KeyValueCmd.AddCommand(globalGetCmd)
}
