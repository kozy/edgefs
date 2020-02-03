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
	"strconv"
	"strings"
	"time"

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

func InitGrpc(svc string) (string, string, int){

	keys, err := efsutil.GetKeys("", "svcs", svc, "", 1000)
	if err != nil {
		log.Fatalf(err.Error())
		return "", "", -1
	}

	var x []string
	var s []string

	for _, v := range keys {
		s = strings.Split(v, ",")
		x = strings.Split(s[1], "@")
	}
	instances, _ := strconv.Atoi(s[0])
	return x[0], x[1], instances
}

func main() {
	log.SetFlags(0)
	log.SetOutput(new(logWriter))

	serviceName := getopt.StringLong("service", 's', "", "Name of service")
	srvId := getopt.Int64Long("id", 'i', 1, "Id for server")
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

	bpath, addr, instances := InitGrpc(*serviceName)

	if bpath == "" || addr == "" {
		log.Fatalf("Could not find bucket name or IP address for service")
		return
	}

	fmt.Println("Service configured to start ", instances, " instances...")
	var idx int
	parts := strings.Split(addr, ":");
	port, _ := strconv.Atoi(parts[1])

	var clusterNodes strings.Builder;

	/* Create list of all nodes in the cluster */
	for idx = 0; idx < instances; idx++ {
		if idx == instances -1 {
			fmt.Fprintf(&clusterNodes, "%d,%s:%s",
				int(*srvId) + idx, parts[0],
				strconv.FormatInt(int64(port + idx), 10))
		} else {
			fmt.Fprintf(&clusterNodes, "%d,%s:%s;",
				int(*srvId) + idx, parts[0],
				strconv.FormatInt(int64(port + idx), 10))
		}
	}

	for idx = 0; idx < instances; idx++ {
		dbdir := fmt.Sprintf("%s/%s", bpath,
			strconv.FormatInt(int64(*srvId) + int64(idx), 10))
		newaddr := fmt.Sprintf("%s:%s", parts[0],
				strconv.FormatInt(int64(port + idx), 10))
		fmt.Println("Starting service dqlite ...", *srvId + int64(idx),
				"at", dbdir, newaddr)
		InitCmd("ccow-sql", []string{"-i",
			strconv.FormatInt(int64(*srvId) + int64(idx), 10),
			"-a", newaddr, "-d", dbdir, "-o", clusterNodes.String()})
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
