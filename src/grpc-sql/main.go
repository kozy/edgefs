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
	"net"

	"../efscli/efsutil"

	"github.com/pborman/getopt"
)

var (
	release string
)

var PREFIX = "/opt/nedge/"

type logWriter struct {
}

func (writer logWriter) Write(bytes []byte) (int, error) {
	return fmt.Print(time.Now().UTC().Format("2006-01-02T15:04:05.999Z") + " " + string(bytes))
}

func findBpath(svc string) (string, string) {

	var bpath string

	keys, err := efsutil.GetKeys("", "svcs", svc, "", 1000)
	if err != nil {
		log.Fatalf(err.Error())
	}

	for _, v := range keys {
		s := strings.Split(v, ",")
		x := strings.Split(s[1], "@")
		bpath = x[0]
		if bpath != "//" {
			return x[1], bpath
		}
	}

	return "", ""
}

func updateSvc(svc string, bpath string, localId string, ids []string, addrs []string) {

	keys, err := efsutil.GetKeys("", "svcs", svc, "", 1000)
	if err != nil {
		log.Fatalf(err.Error())
	}

	// delete non-existing entries
	var nKeys []string
	for _, v := range keys {
		nKeys = append(nKeys, v)
	}
	for k := 0; k < len(nKeys); k++ {
		s := strings.Split(nKeys[k], ",")
		x := strings.Split(s[1], "@")
		id := s[0]
		addr := x[1]
		found := false
		for i := 0; i < len(addrs); i++ {
			if addrs[i] == addr && ids[i] == id {
				// to trigger delete local record if bpath isn't matching
				if id == localId && bpath != x[0] {
					continue
				}
				found = true
				break
			}
		}
		if !found {
			// delete outdated record from service
			rec := id + "," + x[0] + "@" + addr
			err = efsutil.DeleteByKey("/svcs/" + svc + "/", rec)
			if err != nil {
				log.Fatalf("Cannot delete service record %s: %+v", rec, err)
			}

			// delete element from in-memory array
			for j := 0; j < len(keys); j++ {
				if keys[j] == nKeys[k] {
					keys[j] = keys[len(keys)-1]
					keys[len(keys)-1] = ""
					keys = keys[:len(keys)-1]
					break
				}
			}
		}
	}

	// add new entries
	for i := 0; i < len(addrs); i++ {
		found := false
		for k := 0; k < len(keys); k++ {
			s := strings.Split(keys[k], ",")
			x := strings.Split(s[1], "@")
			id := s[0]
			addr := x[1]
			if addrs[i] == addr && ids[i] == id {
				found = true
				break
			}
		}
		if !found {
			var bp string
			if ids[i] == localId { bp = bpath } else { bp = "//" }
			rec := ids[i] + "," + bp + "@" + addrs[i]
			err = efsutil.InsertKey("/svcs/" + svc + "/", rec)
			if err != nil {
				log.Fatalf("Cannot delete service record %s: %+v", rec, err)
			}
		}
	}
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

	os.Setenv("CCOW_LOG_STDOUT", "1")

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

	var ids []string
	var addrs []string
	var bpath string
	var nodeips []string

	svcIPv4Address := "0.0.0.0"
	defport := ""

	if ns := os.Getenv("K8S_NAMESPACE"); ns != "" {
		K8S_NAMESPACE := ns

		serviceHostEnv := strings.ToUpper(K8S_NAMESPACE + "_dsql_" + *serviceName + "_service_host")
		serviceHostEnv = strings.Replace(serviceHostEnv, "-", "_", -1)

		if envIP := os.Getenv(serviceHostEnv); envIP != "" {
			svcIPv4Address = envIP
		}

		defport = os.Getenv("EFSDSQL_PORT")
		if defport == "" {
			log.Fatalf("Default DSQL port is not defined")
		}

		localId := 0
		ord := 1
		for _, ep := range strings.Split(os.Getenv("EFSDSQL_ENDPOINTS"), ";") {
			ips, err := net.LookupIP(ep)
			if err != nil {
				log.Fatalf("Unreachable endpoint %s", ep)
			} else {
				addr := ""
				for _, ip := range ips {
					if ip.To4() != nil {
						addr = ip.String()
						break
					}
				}
				if addr == "" {
					log.Fatalf("No IPv4 address found for endpoint %s", ep)
				}
				ids = append(ids, strconv.Itoa(ord))
				addrs = append(addrs, addr + ":" + defport)
				if addr == svcIPv4Address {
					localId = ord
				}
				ord++
			}
		}

		if localId == 0 {
			log.Fatalf("Endpoint list missing local dsql service record")
		}

		var laddr string
		laddr, bpath = findBpath(*serviceName)
		if bpath == "" {
			log.Fatalf("Cannot find bucket to hold RAFT append logs. Revisit service %s", *serviceName)
		}

		if laddr != svcIPv4Address + ":" + defport {
			log.Printf("Service %s will be updated to reflect new service IPv4 address %s (old address %s)", *serviceName, svcIPv4Address, laddr)
		}

		updateSvc(*serviceName, bpath, strconv.Itoa(localId), ids, addrs)

	} else {
		ids, addrs, bpath = InitGrpc(*serviceName)
	}

	localAddrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Fatalf("Unable to obtain network interfaces")
		return
	}

	for _, a := range localAddrs {
		nodeips = append(nodeips, a.String())
	}

	var idx, max int

	var clusterNodes strings.Builder;
	/* Create list of all nodes in the cluster */
	max = len(ids)
	for idx = 0; idx < max; idx++ {
		if idx == max -1 {
			fmt.Fprintf(&clusterNodes, "%s,%s", ids[idx], addrs[idx])
		} else {
			fmt.Fprintf(&clusterNodes, "%s,%s;", ids[idx], addrs[idx])
		}
	}

	for idx = 0; idx < len(ids); idx++ {
		/* Check if daemon can be started on this node
		 * Check if any of the IP addresses for the cluster IPs
		 * match th IP address of the node.
		 */
		startAddr := ""
		if strings.Contains(addrs[idx], "127.") {
			startAddr = addrs[idx]
		} else {
			var i int
			for i = 0; i < len(nodeips); i++ {
				ip_str := strings.Split(nodeips[i], "/")
				if strings.Contains(addrs[idx], ip_str[0]) {
					startAddr = addrs[idx]
				}
				if strings.Contains(addrs[idx], svcIPv4Address) {
					startAddr = ip_str[0] + ":" + defport
				}
			}
		}
		if (startAddr != "") {
			fmt.Println("Starting service dqlite ...",
			ids[idx], "at", bpath, startAddr)
			InitCmd("ccow-sql", []string{"-i", ids[idx],
				"-a", startAddr, "-d", bpath + "/.raft-" + *serviceName,
				"-o", clusterNodes.String()})
			break
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
