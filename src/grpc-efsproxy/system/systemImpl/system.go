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
package systemImpl

/*
#include "errno.h"
#include "ccow.h"
#include "auditd.h"
#include "private/trlog.h"
*/
import "C"
import (
	"strings"
	"unsafe"

	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"time"

	proto ".."
	"../../efsutil"
	"golang.org/x/net/context"
)

const (
	dbStatExpiration = 20
)

func readTrlogMarker() (int64, error) {
	conf, err := efsutil.GetLibccowConf()
	if err != nil {
		return -1, err
	}

	c_conf := C.CString(string(conf))
	defer C.free(unsafe.Pointer(c_conf))

	cl := C.CString("")
	defer C.free(unsafe.Pointer(cl))

	var tc C.ccow_t

	ret := C.ccow_admin_init(c_conf, cl, 1, &tc)
	if ret != 0 {
		return -1, fmt.Errorf("%s: ccow_admin_init err=%d", efsutil.GetFUNC(), ret)
	}
	defer C.ccow_tenant_term(tc)

	shard := C.CString(C.SHARD_LEADER_PREFIX)
	defer C.free(unsafe.Pointer(shard))

	var trlog_seq_ts C.uint64_t = 0
	var trlog_seq_prev_ts C.uint64_t = 0
	ret = C.trlog_read_marker_seq_tss(tc, nil, shard, &trlog_seq_ts, &trlog_seq_prev_ts)
	if ret != 0 {
		return -1, fmt.Errorf("%s: cannot read '%s' marker, err=%d", efsutil.GetFUNC(), C.SHARD_LEADER_PREFIX, ret)
	}

	return int64(trlog_seq_prev_ts), nil
}

func SystemStatus(isSummary bool, st *proto.SystemStatusResponse, res *proto.SystemSummaryResponse) error {
	var lock *C.void = nil
	l := unsafe.Pointer(lock)
	rc := C.auditd_stats_sharedlock(&l)
	if rc != 0 {
		return fmt.Errorf("Couldn't acquire a lock on stats.db")
	}
	defer C.auditd_stats_sharedunlock(l)
	f, err := os.OpenFile(os.Getenv("NEDGE_HOME")+"/var/run/stats.db", os.O_RDONLY, os.ModePerm)
	if err != nil {
		fmt.Printf("error opening file: %v\n", err)
		return err
	}
	defer f.Close()

	sdmap := make(map[string]map[string]string)
	vdmap := make(map[string]map[string]string)
	smap := make(map[string]map[string]map[string]string)
	fromCP := false
	// Try to init servermap from checkpoint
	cp_buf, err := ioutil.ReadFile(os.Getenv("NEDGE_HOME") + "/var/run/flexhash-checkpoint.json")
	if err == nil {
		var cp interface{}
		err = json.Unmarshal(cp_buf, &cp)
		if err == nil {
			fromCP = true
			cp_kv := cp.(map[string]interface{})
			// Loading VDEVs
			if vdev_if, ok := cp_kv["vdevlist"]; ok {
				if vdevs, ok := vdev_if.([]interface{}); ok {
					for _, vdev_if := range vdevs {
						if vdev_kv, ok := vdev_if.(map[string]interface{}); ok {
							sid := vdev_kv["serverid"].(string)
							vid := vdev_kv["vdevid"].(string)
							if _, ok := smap[sid]; !ok {
								smap[sid] = map[string]map[string]string{}
							}
							if _, ok := smap[sid][vid]; !ok {
								smap[sid][vid] = map[string]string{}
							}
							if _, ok := sdmap[sid]; !ok {
								sdmap[sid] = map[string]string{}
								sdmap[sid]["hostname"] = "-"
								sdmap[sid]["containerid"] = "-"
							}
							smap[sid][vid]["state"] = "FAULTED"
						}
					}
				}
			}
		}
	}

	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}

			fmt.Printf("read file line error: %v\n", err)
			return err
		}

		pattern := regexp.MustCompile(`^gauges\.ccow\.clengine\.server\.(?P<sid>\w+).(?P<ipaddr>\w+).(?P<vdevid>\w+)\|(?P<state>\d+\.\d+)\|(?P<ts>\d+)`)
		if m := pattern.FindAllStringSubmatch(line, -1); m != nil {
			sid := m[0][1]
			if _, ok := sdmap[sid]; fromCP && !ok {
				// Skip unknown servers (possibly after re-checkpointing)
				continue
			}
			vdevid := m[0][3]
			st := m[0][4]
			ts, err := strconv.Atoi(m[0][5])
			if err != nil {
				return err
			}
			state := "ONLINE"
			now := int(time.Now().Unix())
			if now > ts+dbStatExpiration {
				state = "FAULTED"
			} else {
				v, err := strconv.ParseFloat(st, 64)
				if err != nil {
					return fmt.Errorf("VDEV %v status parse error %v, value %v", vdevid, err, st)
				}
				if v == 0 {
					state = "FAULTED"
				} else if v == 2 {
					state = "READONLY"
				} else if v != 1 {
					return fmt.Errorf("VDEV %v undefined status %v", vdevid, st)
				}
			}

			if _, ok := smap[sid]; !ok {
				smap[sid] = map[string]map[string]string{}
			}
			if _, ok := smap[sid][vdevid]; !ok {
				smap[sid][vdevid] = map[string]string{}
			}
			smap[sid][vdevid]["state"] = state
		}
	}

	f.Seek(0, 0)
	rd = bufio.NewReader(f)
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}

			fmt.Printf("read file line error: %v\n", err)
			return err
		}
		// number values
		pattern := regexp.MustCompile(`^gauges\.ccow\.host.(\w+)\.(\w+)\.(\w+)\|(\d+\.\d+)\|(\d+)`)
		if m := pattern.FindAllStringSubmatch(line, -1); m != nil {
			key := m[0][1]
			sid := m[0][2]
			val := m[0][4]
			if _, ok := sdmap[sid]; fromCP && !ok {
				// Skip unknown servers (possibly after re-checkpointing)
				continue
			}
			ts, err := strconv.Atoi(m[0][5])
			if err != nil {
				return err
			}
			now := int(time.Now().Unix())
			if now > ts+dbStatExpiration {
				continue
			}
			if _, ok := sdmap[sid]; !ok {
				sdmap[sid] = map[string]string{}
			}
			v, err := strconv.ParseFloat(val, 64)
			if err != nil {
				sdmap[sid][key] = strconv.FormatInt(int64(0), 10)
			} else {
				sdmap[sid][key] = strconv.FormatInt(int64(v), 10)
			}
		}
		// string values
		pattern = regexp.MustCompile(`^gauges\.ccow\.host.(\w+)\.(\w+)\.(\w+)\.(.*)\|(\d+\.\d+)\|(\d+)`)
		if m := pattern.FindAllStringSubmatch(line, -1); m != nil {
			key := m[0][1]
			sid := m[0][2]
			strval := m[0][4]
			if _, ok := sdmap[sid]; fromCP && !ok {
				// Skip unknown servers (possibly after re-checkpointing)
				continue
			}
			ts, err := strconv.Atoi(m[0][6])
			if err != nil {
				return err
			}
			now := int(time.Now().Unix())
			if now > ts+dbStatExpiration {
				continue
			}
			if _, ok := sdmap[sid]; !ok {
				sdmap[sid] = map[string]string{}
			}
			sdmap[sid][key] = strval
		}
	}

	f.Seek(0, 0)
	rd = bufio.NewReader(f)
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}

			fmt.Printf("read file line error: %v\n", err)
			return err
		}
		// gauges number values
		pattern := regexp.MustCompile(`^gauges\.ccow\.reptrans\.(\w+)\.(\w+)\.(\w+)\|(\d+\.\d+)\|(\d+)`)
		if m := pattern.FindAllStringSubmatch(line, -1); m != nil {
			key := m[0][1]
			vdevid := m[0][3]
			val := m[0][4]
			ts, err := strconv.Atoi(m[0][5])
			if err != nil {
				return err
			}
			now := int(time.Now().Unix())
			if now > ts+dbStatExpiration {
				continue
			}
			if _, ok := vdmap[vdevid]; !ok {
				vdmap[vdevid] = map[string]string{}
			}
			v, err := strconv.ParseFloat(val, 64)
			if err != nil {
				vdmap[vdevid][key] = strconv.FormatInt(int64(0), 10)
			} else {
				vdmap[vdevid][key] = strconv.FormatInt(int64(v), 10)
			}
		}
		// timer number values
		pattern = regexp.MustCompile(`^timers\.ccow\.reptrans\.(\w+)\.(\w+)\.(\w+).(\w+)\|(\d+\.\d+)\|(\d+)`)
		if m := pattern.FindAllStringSubmatch(line, -1); m != nil {
			key := m[0][1]
			vdevid := m[0][3]
			val := m[0][5]
			ts, err := strconv.Atoi(m[0][6])
			if err != nil {
				return err
			}
			now := int(time.Now().Unix())
			if now > ts+dbStatExpiration {
				continue
			}
			if _, ok := vdmap[vdevid]; !ok {
				vdmap[vdevid] = map[string]string{}
			}
			v, err := strconv.ParseFloat(val, 64)
			if err != nil {
				vdmap[vdevid][key] = strconv.FormatInt(int64(0), 10)
			} else {
				vdmap[vdevid][key] = strconv.FormatInt(int64(v), 10)
			}
		}
		// string values
		pattern = regexp.MustCompile(`^gauges\.ccow\.reptrans.(\w+)\.(\w+)\.(\w+)\.(.*)\|(\d+\.\d+)\|(\d+)`)
		if m := pattern.FindAllStringSubmatch(line, -1); m != nil {
			key := m[0][1]
			vdevid := m[0][3]
			strval := m[0][4]
			ts, err := strconv.Atoi(m[0][6])
			if err != nil {
				return err
			}
			now := int(time.Now().Unix())
			if now > ts+dbStatExpiration {
				continue
			}
			if _, ok := vdmap[vdevid]; !ok {
				vdmap[vdevid] = map[string]string{}
			}
			vdmap[vdevid][key] = strval
		}
	}

	totalCapacity := int64(0)
	totalUsed := int64(0)
	totalMdoffloadCapacity := int64(0)
	totalMdoffloadUsed := int64(0)
	totalS3offloadCapacity := int64(0)
	totalS3offloadUsed := int64(0)
	totalNumObjects := int64(0)
	for sid, vdevs := range smap {
		capacity := int64(0)
		used := int64(0)
		mdoffloadCapacity := int64(0)
		mdoffloadUsed := int64(0)
		s3offloadCapacity := int64(0)
		s3offloadUsed := int64(0)

		numObjects := int64(0)
		for vdevid, vals := range vdevs {
			if vals["state"] != "ONLINE" {
				continue
			}
			if vdevid == "00000000000000000000000000000000" {
				sdmap[sid]["gw"] = "1"
				continue
			}
			for key, val := range vdmap[vdevid] {
				if key == "capacity" {
					v, err := strconv.ParseInt(val, 10, 64)
					if err == nil {
						capacity += v
					}
				} else if key == "used" {
					v, err := strconv.ParseInt(val, 10, 64)
					if err == nil {
						used += v
					}
				} else if key == "num_objects" {
					v, err := strconv.ParseInt(val, 10, 64)
					if err == nil {
						numObjects += v
					}
				} else if key == "mdoffload_total" {
					v, err := strconv.ParseInt(val, 10, 64)
					if err == nil {
						mdoffloadCapacity += v
					}
				} else if key == "mdoffload_size" {
					v, err := strconv.ParseInt(val, 10, 64)
					if err == nil {
						mdoffloadUsed += v
					}
				} else if key == "s3_offload_size" {
					v, err := strconv.ParseInt(val, 10, 64)
					if err == nil {
						s3offloadCapacity += v
					}
				} else if key == "s3_offload_used" {
					v, err := strconv.ParseInt(val, 10, 64)
					if err == nil {
						s3offloadUsed += v
					}
				}
			}
		}
		sdmap[sid]["capacity"] = strconv.FormatInt(int64(capacity), 10)
		sdmap[sid]["used"] = strconv.FormatInt(int64(used), 10)
		sdmap[sid]["mdoffload_capacity"] = strconv.FormatInt(int64(mdoffloadCapacity), 10)
		sdmap[sid]["mdoffload_used"] = strconv.FormatInt(int64(mdoffloadUsed), 10)
		sdmap[sid]["s3_offload_capacity"] = strconv.FormatInt(int64(s3offloadCapacity), 10)
		sdmap[sid]["s3_offload_used"] = strconv.FormatInt(int64(s3offloadUsed), 10)

		sdmap[sid]["numObjects"] = strconv.FormatInt(int64(numObjects), 10)
		totalCapacity += capacity
		totalUsed += used
		totalNumObjects += numObjects
		totalMdoffloadUsed += mdoffloadUsed
		totalMdoffloadCapacity += mdoffloadCapacity
		totalS3offloadCapacity += s3offloadCapacity
		totalS3offloadUsed += s3offloadUsed
	}

	if isSummary {
		res.TotalCapacity = totalCapacity
		res.TotalUsed = totalUsed
		res.TotalAvailable = totalCapacity - totalUsed
		res.TotalUtilization = float32(100*totalUsed) / float32(totalCapacity)
		res.TotalMdoffloadCapacity = totalMdoffloadCapacity
		res.TotalMdoffloadUsed = totalMdoffloadUsed
		res.TotalS3OffloadCapacity = totalS3offloadCapacity
		res.TotalS3OffloadUsed = totalS3offloadUsed
		res.TotalVersions = totalNumObjects
		res.TrlogMarker = 0
		res.TrlogMarkerDelay = 0
		m, err := readTrlogMarker()
		if err == nil && m > 0 && m != -1 {
			cursec := time.Now().UnixNano() / 1000000000
			res.TrlogMarker = m
			res.TrlogMarkerDelay = cursec - (m / int64(1000000))
		}

		conf, err := efsutil.GetLibccowConf()
		if err != nil {
			return err
		}

		c_conf := C.CString(string(conf))
		defer C.free(unsafe.Pointer(c_conf))

		cl := C.CString("")
		defer C.free(unsafe.Pointer(cl))

		var tc C.ccow_t

		ret := C.ccow_admin_init(c_conf, cl, 1, &tc)
		if ret != 0 {
			return fmt.Errorf("%s: ccow_admin_init err=%d", efsutil.GetFUNC(), ret)
		}
		defer C.ccow_tenant_term(tc)

		guid := C.GoString(C.ccow_get_system_guid_formatted(tc))

		res.Guid = guid

		res.Segid = guid[0:16]

		var zone string
		zfile, et := ioutil.ReadFile("/etc/timezone")
		if et != nil {
			t := time.Now()
			zone, _ = t.Zone()
		} else {
			zone = string(zfile)
		}
		res.Timezone = strings.TrimSpace(zone)

		return nil
	}

	st.Status = make(map[string]*proto.SystemInfo)

	for sid, vdevs := range smap {
		_, gw := sdmap[sid]["gw"]
		contID := "N/A"
		if _, ok := sdmap[sid]["containerid"]; ok && len(sdmap[sid]["containerid"]) > 0 {
			contID = sdmap[sid]["containerid"]
		}
		hostName := sdmap[sid]["hostname"]
		state := "ONLINE"
		vdevOfflineCount := 0
		vdevReadOnlyCount := 0
		for _, vdev := range vdevs {
			if vdev["state"] == "FAULTED" {
				vdevOfflineCount++
			} else if vdev["state"] == "READONLY" {
				vdevReadOnlyCount++
			}
		}
		if vdevOfflineCount == len(vdevs) {
			state = "FAULTED"
		} else if vdevOfflineCount > 0 || vdevReadOnlyCount > 0 {
			state = "DEGRADED"
		}

		st.Status[sid] = new(proto.SystemInfo)
		st.Status[sid].HostName = hostName
		st.Status[sid].State = state
		st.Status[sid].Gw = gw
		st.Status[sid].ContId = contID
		st.Status[sid].VdevCount = int32(len(vdevs))
		st.Status[sid].VdevOfflineCount = int32(vdevOfflineCount)
		st.Status[sid].VdevReadOnlyCount = int32(vdevReadOnlyCount)
		st.Status[sid].Vdevs = make(map[string]*proto.VdevInfo)
		if gw {
			continue
		}
		for vdevid, vals := range vdevs {
			util, mdutil, s3util := 0, 0, 0
			used, _ := strconv.Atoi(vdmap[vdevid]["used"])
			capacity, _ := strconv.Atoi(vdmap[vdevid]["capacity"])
			if capacity > 0 {
				util = 10000 * used / capacity
			}
			if _, ok := vdmap[vdevid]["mdoffload_total"]; ok && totalMdoffloadCapacity > 0 {
				c, _ := strconv.Atoi(vdmap[vdevid]["mdoffload_total"])
				u, _ := strconv.Atoi(vdmap[vdevid]["mdoffload_used"])
				if c > 0 {
					mdutil = 10000 * u / c
				}
			}
			if _, ok := vdmap[vdevid]["s3_offload_size"]; ok && totalS3offloadCapacity > 0 {
				c, _ := strconv.Atoi(vdmap[vdevid]["s3_offload_size"])
				u, _ := strconv.Atoi(vdmap[vdevid]["s3_offload_used"])
				if c > 0 {
					s3util = 10000 * u / c
				}
			}
			st.Status[sid].Vdevs[vdevid] = new(proto.VdevInfo)
			st.Status[sid].Vdevs[vdevid].DevName = vdmap[vdevid]["devname"]
			st.Status[sid].Vdevs[vdevid].State = vals["state"]
			st.Status[sid].Vdevs[vdevid].Capacity = int64(capacity)
			st.Status[sid].Vdevs[vdevid].Used = int64(used)
			st.Status[sid].Vdevs[vdevid].Util = int64(util)
			st.Status[sid].Vdevs[vdevid].Mdutil = int64(mdutil)
			st.Status[sid].Vdevs[vdevid].S3Util = int64(s3util)
		}
	}
	return nil
}

type SystemImpl struct {
}

func (s *SystemImpl) SystemStatus(ctx context.Context, msg *proto.GenericRequest) (*proto.SystemStatusResponse, error) {
	var res proto.SystemStatusResponse
	err := SystemStatus(false, &res, nil)
	return &res, err
}

func (s *SystemImpl) SystemSummary(ctx context.Context, msg *proto.GenericRequest) (*proto.SystemSummaryResponse, error) {
	var res proto.SystemSummaryResponse
	err := SystemStatus(true, nil, &res)
	return &res, err
}
