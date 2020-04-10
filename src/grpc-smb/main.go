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
package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"../efscli/efsutil"
	"./export"

	"github.com/pborman/getopt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	release string
)

type logWriter struct {
}

func (writer logWriter) Write(bytes []byte) (int, error) {
	return fmt.Print(time.Now().UTC().Format("2006-01-02T15:04:05.999Z") + " " + string(bytes))
}

func main() {
	log.SetFlags(0)
	log.SetOutput(new(logWriter))

	serviceName := getopt.StringLong("service", 's', "", "Name of service")
	socket := getopt.StringLong("bind", 'b', "", "Bind to socket")
	optHelp := getopt.BoolLong("help", 0, "Help")
	getopt.Parse()

	if *optHelp {
		getopt.Usage()
		os.Exit(0)
	}

	ipnport := "0.0.0.0:49000"
	if ie := os.Getenv("EFSSMB_BIND"); ie != "" {
		ipnport = ie
	}
	if *socket != "" {
		ipnport = *socket
	}

	if *serviceName == "" {
		log.Fatalf("No service name given")
		return
	}

	if !efsutil.CheckService(*serviceName) {
		log.Fatalf("Invalid gRPC service %s", *serviceName)
		return
	}

	InitGrpc(ipnport, *serviceName)
	InitADS()
	InitSMBD(*serviceName)
}

// Server wraps the gRPC server
type Server struct {
	bind string
}

// New creates a new rpc server.
func New(bind string) *Server {
	return &Server{bind}
}

// Listen binds the server to the indicated interface:port.
func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", s.bind)
	if err != nil {
		return err
	}
	gs := grpc.NewServer()
	export.RegisterExportServer(gs, &export.ExportImpl{})
	reflection.Register(gs)
	if release == "" {
		release = "dev"
	}
	log.Printf("SMB controller in version %v serving on %v is ready for gRPC clients", release, s.bind)
	return gs.Serve(ln)
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
		log.Fatalf("rpc.statd: cmd.Start() failed with '%s'\n", err)
	}

	go func() {
		_, errStdout = io.Copy(stdout, stdoutIn)
	}()

	go func() {
		_, errStderr = io.Copy(stderr, stderrIn)
	}()

	err = cmd.Wait()
	if err != nil {
		log.Fatalf("rpc.statd: cmd.Wait() failed with '%s'\n", err)
	}
}

func InitGrpc(ipnport, svc string) {

	log.Printf("starting gRPC ipnport=%s svc=%s\n", ipnport, svc)

	name, err := efsutil.GetMDKey("", "svcs", svc, "", "X-Service-Name")
	if err != nil {
		log.Fatalf("gRPC service error: %v", err.Error())
		return
	}

	if name != svc {
		log.Fatalf("invalid gRPC service %s", svc)
		return
	}

	immDir := ""
	if envRDU := os.Getenv("EFSSMB_RELAXED_DIR_UPDATES"); envRDU != "" {
		immDir = "0"
	}
	mhImmDir, err := efsutil.GetMDKey("", "svcs", svc, "", "X-MH-ImmDir")
	if err != nil {
		log.Fatalf(err.Error())
	}
	if immDir != "" && mhImmDir != immDir {
		err = efsutil.UpdateMD("", "svcs", svc, "", "X-MH-ImmDir", immDir)
		if err != nil {
			log.Fatalf(err.Error())
		}
		mhImmDir = immDir
	}
	os.Setenv("CCOW_MH_IMMDIR", mhImmDir)

	log.Printf("smb gRPC svc=%s, X-MH-ImmDir=%s\n", svc, mhImmDir)

	keys, err := efsutil.GetKeys("", "svcs", svc, "", 1000)
	if err != nil {
		log.Fatalf(err.Error())
	}

	var PREFIX = os.Getenv("NEDGE_HOME")
	var exportsList = PREFIX + "/etc/samba/exportslist"

	explist, err := os.Create(exportsList)
	if err != nil {
		log.Fatalf(err.Error())
	}
	explist.Close()

	for _, v := range keys {
		x := strings.Split(v, ",")
		exportId := x[0]
		var ExportId uint64
		ExportId, _ = strconv.ParseUint(exportId, 0, 32)
		x = strings.Split(x[1], "@")
		//exportPath := x[0]
		exportUri := x[1]
		u := strings.Split(exportUri, "/")
		err = export.LocalExportAdd(svc, u[0], u[1], u[2], uint32(ExportId), false)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	go func() {
		err = New(ipnport).Listen()
		if err != nil {
			log.Fatalf("Failed to launch the EFSSMB due to %v", err)
		}
	}()
}

func InitADS() {
	if os.Getenv("EFSSMB_DOMAIN_NAME") == "" ||
	   os.Getenv("EFSSMB_AD_USERNAME") == "" ||
	   os.Getenv("EFSSMB_AD_PASSWORD") == "" ||
	   os.Getenv("EFSSMB_WORKGROUP") == "" ||
	   os.Getenv("EFSSMB_NETBIOS_NAME") == "" ||
	   os.Getenv("EFSSMB_DC1") == "" {
		log.Printf("Starting in WORKGROUP mode\n")
		return
	}
	log.Printf("Starting in ADS mode\n")
	InitCmd("ads-join.sh", []string{})
}

func InitSMBD(svc string) {
	go func() {
		efsutil.K8sServiceUp(svc)
	}()
	dbg := "0"
	if envDbg := os.Getenv("EFSSMB_DEBUG"); envDbg != "" {
		dbg = envDbg
	}
	InitCmd("smbd", []string{"-D", "-F", "-S", "-d " + dbg})
	os.Exit(0)
}
