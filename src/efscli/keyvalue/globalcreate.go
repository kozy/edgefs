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


import (
	"fmt"
	"os"

	"github.com/Nexenta/edgefs/src/efscli/efsutil"
	"github.com/Nexenta/edgefs/src/efscli/validate"
	"github.com/Nexenta/edgefs/src/efscli/edgex"
	"github.com/spf13/cobra"
)

func globalKeyValueCreate(opath string, flags []efsutil.FlagValue) error {
	err := edgex.GlobalVMAcquire(opath)
	if err != nil {
		return err
	}

	err = keyValueCreate(opath, flags)

	e := edgex.GlobalVMRelease(opath)
	if e != nil {
		fmt.Printf("Put lock release error: %s, value : %v\n", opath, e)
	}
	return err
}

var (
	globalCreateCmd = &cobra.Command{
		Use:   "globalcreate <cluster>/<tenant>/<bucket>/<kvobject>",
		Short: "globalcreate a new kvobject",
		Long:  "globalcreate a new kvobject",
		Args:  validate.Object,
		Run: func(cmd *cobra.Command, args []string) {
			err := globalKeyValueCreate(args[0], flagsCreate)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}
)

func init() {
	KeyValueCmd.AddCommand(globalCreateCmd)
}
