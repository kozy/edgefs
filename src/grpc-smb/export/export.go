/*
 * Copyright (c) 2020 Nexenta Systems, Inc.
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
package export

import (
	"../../efscli/efsutil"
	"bufio"
	"fmt"
	"golang.org/x/net/context"
	"io/ioutil"
	"log"
	"os"
	"regexp"
)

type ExportImpl struct {
}

func smbCommand(cmd string) ([]string, error) {
	var data []string

	// FIXME: implement

	return data, nil
}

func (s *ExportImpl) ExportAdd(ctx context.Context, msg *ExportRequest) (*GenericResponse, error) {
	err := LocalExportAdd(msg.Service, msg.Cluster, msg.Tenant, msg.Bucket, msg.ExportId, true)

	if err == nil {
		return &GenericResponse{}, nil
	} else {
		return nil, err
	}
}

func getServiceValue(Service string, Key string) (string) {
	str, err := efsutil.GetMDKey("", "svcs", Service, "", Key)
	if err != nil {
		return ""
	}
	return str
}

func getOptsBlock(Service string, Cluster string, Tenant string, Bucket string, export_config *string) error {
	var conf = ""
	var exportOpts = "" // "hosts allow = 192.168.3.;hosts deny = 10.244."

	// Check not standard value for compat
	opts := getServiceValue(Service, "X-SMB-OPTS-"+Tenant+"/"+Bucket)

	exportOpts = opts

	// default, high priority
	opts = getServiceValue(Service, "X-SMB-OPTS-"+Tenant+"-"+Bucket)
	if opts != "" {
		exportOpts = opts
	}

	// Service value, override per export, highest priority
	opts = getServiceValue(Service, "X-SMB-OPTS")
	if opts != "" {
		exportOpts = opts
	}

	if exportOpts != "" {
		spcRe := regexp.MustCompile(`;`)
		oList := spcRe.Split(exportOpts, -1)
		for _, line := range oList {
			conf += "\t" + line + "\n"
		}
	}

	*export_config += conf

	return nil
}

func LocalExportAdd(Service string, Cluster string, Tenant string, Bucket string, ExportId uint32, dynamic bool) error {
//	var exportPath = Tenant + "/" + Bucket
	var filePath = Cluster + "-" + Tenant + "-" + Bucket
	var exportUri = Cluster + "/" + Tenant + "/" + Bucket

	var PREFIX = os.Getenv("NEDGE_HOME")
	var exportsList = PREFIX + "/etc/samba/exportslist"
	var exportsDir = PREFIX + "/etc/samba/exports/"

	var export_config = "[" + Bucket + "]\n" +
		"\tvfs objects = edgefs\n" +
		"\tpath = /" + Bucket + "\n" +
		"\tedgefs:ccowConfigFile = " + PREFIX + "/etc/ccow/ccow.json\n" +
		"\tedgefs:bucketUri = " + exportUri + "\n" +
		"\twriteable = yes\n"

	err := getOptsBlock(Service, Cluster, Tenant, Bucket, &export_config)

	err = ioutil.WriteFile(exportsDir+filePath, []byte(export_config), 0644)
	if err != nil {
		// handle error
		log.Printf("cannot write to file: %v", err)
		return err
	}

	if dynamic {
		// Add export to online smbd first, to let to decide if it correct
		_, err = smbCommand("EXPORT ADD " + fmt.Sprintf("%d", ExportId) +
			" " + exportsDir + filePath + "\r\n")
		if err != nil { // XXX check for "ERR" response too
			// handle error
			log.Printf("cannot add export to smbd daemon: %v", err)
			return err
		}
	}

	// Then append to list of includes
	explist, err := os.OpenFile(exportsList, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		// handle error
		log.Printf("cannot open export list file: %v", err)
		return err
	}
	defer explist.Close()

	_, err = explist.WriteString("include = " + exportsDir + filePath + "\n")
	if err != nil {
		// handle error
		log.Printf("cannot write to exportlist file: %v", err)
		return err
	}

	return nil
}

func (s *ExportImpl) ExportRemove(ctx context.Context, msg *ExportRequest) (*GenericResponse, error) {
	err := LocalExportRemove(msg.Service, msg.Cluster, msg.Tenant, msg.Bucket, msg.ExportId)

	if err == nil {
		return &GenericResponse{}, nil
	} else {
		return nil, err
	}
}

func LocalExportRemove(Service string, Cluster string, Tenant string, Bucket string, ExportId uint32) error {
	var filePath = Cluster + "-" + Tenant + "-" + Bucket

	var PREFIX = os.Getenv("NEDGE_HOME")
	var exportsList = PREFIX + "/etc/samba/exportslist"
	var exportsDir = PREFIX + "/etc/samba/exports/"

	// Offline export
	_, err := smbCommand("EXPORT REMOVE " + fmt.Sprintf("%d", ExportId) + "\r\n")
	if err != nil {
		// handle error
		log.Printf("cannot remove export from smbd daemon: %v", err)
		return err
	}

	// Remove export from exportslist
	explist, err := os.OpenFile(exportsList, os.O_RDWR, 0644)
	if err != nil {
		// handle error
		log.Printf("cannot open exportslist file: %v", err)
		return err
	}

	matchline := "include = " + exportsDir + filePath
	var lines []string
	scanner := bufio.NewScanner(explist)
	for scanner.Scan() {
		line := scanner.Text()
		if line != matchline {
			lines = append(lines, line)
		}
	}
	explist.Seek(0, 0)

	err = explist.Truncate(0)
	if err != nil {
		// handle error
		log.Printf("cannot truncate exportslist file: %v", err)
		return err
	}
	explist.Sync()

	for _, line := range lines {
		fmt.Fprintf(explist, "%s\n", line)
	}
	explist.Sync()
	explist.Close()

	// Remove export config file
	os.Remove(exportsDir + filePath)

	return nil
}

func (s *ExportImpl) smbdExportsList() ([]string, error) {
	data, err := smbCommand("EXPORT LIST\r\n")
	if err != nil {
		// handle error
		return nil, err
	}
	return data[1:(len(data) - 1)], err
}

func (s *ExportImpl) ExportList(ctx context.Context, msg *ExportListRequest) (*ExportListResponse, error) {

	list, err := s.smbdExportsList()

	if err != nil {
		return nil, err
	}

	info := make(map[string]*ExportInfo)

	for _, export := range list {
		info[export] = &ExportInfo{Name: export}
	}

	return &ExportListResponse{Info: info}, nil
}
