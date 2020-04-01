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
*/
import "C"

import (
	"github.com/Nexenta/edgefs/src/efscli/efsutil"
	"github.com/Nexenta/edgefs/src/efscli/validate"
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"strings"
)

func List(bpath string, name string) error {
	s := strings.Split(bpath, "/")
	var last string = ""
	var cmp int = 0
	var max_len int = 4096
	for {
		next, err := efsutil.PrintKeyStrValues(s[0], s[1], s[2], s[3], last, cmp, max_len, 1000)
		if err != nil {
			return err
		}
		if next == "" || next <= last {
			break
		}
		last = next
		cmp = 1
	}
	return nil
}

var (
	name string
	extended bool

	listCmd = &cobra.Command{
		Use:   "list  <cluster>/<tenant>/<bucket>/<kvobject>",
		Short: "list key/value pairs",
		Long:  "list key/value pairs from kvobject",
		Args:  validate.KeyValue,
		Run: func(cmd *cobra.Command, args []string) {
			err := List(args[0], name)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}
)

func init() {
	KeyValueCmd.AddCommand(listCmd)
}
