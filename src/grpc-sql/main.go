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
package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"
	"net"

	"../efscli/efsutil"

	"github.com/pborman/getopt"
)

var (
	release string
)

var PREFIX = "/opt/nedge/"
var exportsList = PREFIX + "etc/ganesha/exportslist"

type logWriter struct {
}

func (writer logWriter) Write(bytes []byte) (int, error) {
	return fmt.Print(time.Now().UTC().Format("2006-01-02T15:04:05.999Z") + " " + string(bytes))
}

func InitGrpc(svc string) ([]string, []string, string) {

	var ids []string
	var addrs []string
	var bpath string

	keys, err := efsutil.GetKeys("", "svcs", svc, "", 1000)
	if err != nil {
		log.Fatalf(err.Error())
		return ids, addrs, bpath
	}

	for _, v := range keys {
		s := strings.Split(v, ",")
		x := strings.Split(s[1], "@")
		ids = append(ids, s[0])
		addrs = append(addrs, x[1])
		bpath = x[0]
	}
	return ids, addrs, bpath
}

func main() {
	log.SetFlags(0)
	log.SetOutput(new(logWriter))

	serviceName := getopt.StringLong("service", 's', "", "Name of service")
	optHelp := getopt.BoolLong("help", 0, "Help")
	getopt.Parse()

	if *optHelp {
		getopt.Usage()
		os.Exit(0)
	}

	if *serviceName == "" {
		log.Fatalf("No service name given")
		return
	}

	if !efsutil.CheckService(*serviceName) {
		log.Fatalf("Invalid gRPC service %s", *serviceName)
		return
	}

	ids, addrs, bpath := InitGrpc(*serviceName)

	nodeips, err := net.InterfaceAddrs()
	if err != nil {
		log.Fatalf("Unable to obtain network interfaces")
		return
	}

	var idx, max int
	var start_daemon bool

	var clusterNodes strings.Builder;
	/* Create list of all nodes in the cluster */
	max = len(ids)
	for idx = 0; idx < max; idx++ {
		if idx == max -1 {
			fmt.Fprintf(&clusterNodes, "%s,%s",
			ids[idx], addrs[idx])
		} else {
			fmt.Fprintf(&clusterNodes, "%s,%s;",
			ids[idx], addrs[idx])
		}
	}

	for idx = 0; idx < len(ids); idx++ {
		/* Check if daemon can be started on this node
		 * Check if any of the IP addresses for the cluster IPs
		 * match th IP address of the node.
		 */
		start_daemon = false
		if strings.Contains(addrs[idx], "127.") {
			start_daemon = true
		} else {
			var i int
			for i = 0; i < len(nodeips); i++ {
				ip_str := strings.Split(nodeips[i].String(),
							"/")
				if strings.Contains(addrs[idx], ip_str[0]) {
					start_daemon = true
				}
			}
		}
		if (start_daemon) {
			fmt.Println("Starting service dqlite ...",
			ids[idx], "at", bpath, addrs[idx])
			InitCmd("ccow-sql", []string{"-i", ids[idx],
				"-a", addrs[idx], "-d", bpath,
				"-o", clusterNodes.String()})
		}
	}

	os.Exit(0)
}

// Server wraps the gRPC server
type Server struct {
	bind string
}

// New creates a new rpc server.
func New(bind string) *Server {
	return &Server{bind}
}

func InitCmd(progname string, args []string) {
	var stdoutBuf, stderrBuf bytes.Buffer

	log.Printf("starting %s %+v\n", progname, args)

	cmd := exec.Command(progname, args...)
	cmd.Env = os.Environ()

	stdoutIn, _ := cmd.StdoutPipe()
	stderrIn, _ := cmd.StderrPipe()

	var errStdout, errStderr error
	stdout := io.MultiWriter(os.Stdout, &stdoutBuf)
	stderr := io.MultiWriter(os.Stderr, &stderrBuf)
	err := cmd.Start()

	if err != nil {
		log.Fatalf("ccow-sql: cmd.Start() failed with '%s'\n", err)
	}

	go func() {
		_, errStdout = io.Copy(stdout, stdoutIn)
	}()

	go func() {
		_, errStderr = io.Copy(stderr, stderrIn)
	}()

	err = cmd.Wait()
	if err != nil {
		log.Fatalf("ccow-sql: cmd.Wait() failed with '%s'\n", err)
	}
}
