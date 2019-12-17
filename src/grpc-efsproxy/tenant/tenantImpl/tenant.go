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
package tenantImpl

/*
#include "ccow.h"
#include "errno.h"
*/
import "C"
import "unsafe"

import (
	"strings"

	proto ".."
	"../../efsutil"
	"golang.org/x/net/context"
	"google.golang.org/grpc/status"
)

type TenantImpl struct {
}

func (s *TenantImpl) TenantCreate(ctx context.Context, msg *proto.TenantCreateRequest) (*proto.GenericResponse, error) {
	var flagNames = []string{
		"replication-count",
		"number-of-versions",
		"sync-put",
		"ec-data-mode",
		"ec-trigger-policy-timeout",
		"encryption-enabled",
		"select-policy",
		"options",
	}
	var flags []efsutil.FlagValue = make([]efsutil.FlagValue, len(flagNames))
	efsutil.ReadAttributes(msg.Options, flagNames, flags)

	e := efsutil.ValidateFlags(flags)
	if e != nil {
		return nil, status.Errorf(400, "Invalid attributes err=%v", e)
	}

	c_cluster := C.CString(msg.Cluster)
	defer C.free(unsafe.Pointer(c_cluster))

	c_tenant := C.CString(msg.Tenant)
	defer C.free(unsafe.Pointer(c_tenant))

	conf, err := efsutil.GetLibccowConf()
	if err != nil {
		return nil, status.Error(500, "Cannot initialize library")
	}

	c_conf := C.CString(string(conf))
	defer C.free(unsafe.Pointer(c_conf))

	var tc C.ccow_t
	ret := C.ccow_admin_init(c_conf, c_cluster, C.strlen(c_cluster)+1, &tc)
	if ret != 0 {
		return nil, status.Errorf(500, "ccow_admin_init err=%d", ret)
	}
	defer C.ccow_tenant_term(tc)

	var c C.ccow_completion_t
	ret = C.ccow_create_completion(tc, nil, nil, 2, &c)
	if ret != 0 {
		return nil, status.Errorf(500, "ccow_create_completion err=%d", ret)
	}

	err = efsutil.ModifyDefaultAttributes(unsafe.Pointer(c), flags)
	if err != nil {
		return nil, status.Errorf(500, "Modify default attributes err=%v", err)
	}

	ret = C.ccow_tenant_create(tc, c_tenant, C.strlen(c_tenant)+1, c)
	if ret != 0 {
		return nil, status.Errorf(500, "ccow_tenant_create err=%d", ret)
	}

	if efsutil.HasCustomAttributes(flags) {
		err = efsutil.ModifyCustomAttributes(msg.Cluster, msg.Tenant, "", "", flags)
		if err != nil {
			return nil, status.Errorf(500, "Modify custom attributes err=%v", err)
		}
	}

	return &proto.GenericResponse{}, nil
}

func (s *TenantImpl) TenantDelete(ctx context.Context, msg *proto.TenantDeleteRequest) (*proto.GenericResponse, error) {
	c_cluster := C.CString(msg.Cluster)
	defer C.free(unsafe.Pointer(c_cluster))

	c_tenant := C.CString(msg.Tenant)
	defer C.free(unsafe.Pointer(c_tenant))

	conf, err := efsutil.GetLibccowConf()
	if err != nil {
		return nil, status.Error(500, "Cannot initialize library")
	}

	c_conf := C.CString(string(conf))
	defer C.free(unsafe.Pointer(c_conf))

	var tc C.ccow_t

	ret := C.ccow_admin_init(c_conf, c_cluster, C.strlen(c_cluster)+1, &tc)
	if ret != 0 {
		return nil, status.Errorf(500, "ccow_admin_init err=%d", ret)
	}
	defer C.ccow_tenant_term(tc)


	ret = C.ccow_tenant_delete(tc, c_tenant, C.strlen(c_tenant)+1)
	if ret != 0 {
		return nil, status.Errorf(500, "ccow_tenant_delete err=%d", ret)
	}

	return &proto.GenericResponse{}, nil
}

func (s *TenantImpl) TenantList(ctx context.Context, msg *proto.TenantListRequest) (*proto.TenantListResponse, error) {

	c_cluster := C.CString(msg.Cluster)
	defer C.free(unsafe.Pointer(c_cluster))

	conf, err := efsutil.GetLibccowConf()
	if err != nil {
		return nil, status.Error(500, "Cannot initialize library")
	}

	c_conf := C.CString(string(conf))
	defer C.free(unsafe.Pointer(c_conf))

	cl := C.CString("")
	defer C.free(unsafe.Pointer(cl))

	var tc C.ccow_t

	ret := C.ccow_admin_init(c_conf, c_cluster, C.strlen(c_cluster)+1, &tc)
	if ret != 0 {
		return nil, status.Errorf(500, "ccow_admin_init err=%d", ret)
	}
	defer C.ccow_tenant_term(tc)

	c_pat := C.CString(msg.Pattern)
	defer C.free(unsafe.Pointer(c_pat))

	var tnCnt = msg.Count
	if tnCnt == 0 {
		tnCnt = 1000
	}

	var iter C.ccow_lookup_t
	ret = C.ccow_tenant_lookup(tc, nil, 0, c_pat, C.strlen(c_pat)+1, C.ulong(tnCnt), &iter)
	if ret != 0 {
		if iter != nil {
			C.ccow_lookup_release(iter)
		}

		if ret == -C.ENOENT && (msg.Pattern == "" || len(msg.Pattern) == 0) {
			return &proto.TenantListResponse{}, nil
		}

		return nil, status.Errorf(500, "ccow_tenant_lookup err=%d", ret)
	}

	info := make(map[string]*proto.TenantInfo)

	cnt := int32(0)
	found := 0
	var kv *C.struct_ccow_metadata_kv

	for {
		kv = (*C.struct_ccow_metadata_kv)(C.ccow_lookup_iter(iter, C.CCOW_MDTYPE_NAME_INDEX, -1))
		if kv == nil {
			break
		}
		if kv.key_size == 0 {
			continue
		}
		if efsutil.IsSystemName(C.GoString(kv.key)) {
			continue
		}

		if msg.Count > 0 && msg.Count <= cnt {
			break
		}
		cnt++

		if msg.Pattern == "" || len(msg.Pattern) == 0 {
			found = 1
			info[C.GoString(kv.key)] = &proto.TenantInfo{Name: C.GoString(kv.key)}
			continue
		}

		cmpRes := strings.Compare(msg.Pattern, C.GoString(kv.key))
		if cmpRes == 0 {
			found = 1
			info[C.GoString(kv.key)] = &proto.TenantInfo{Name: C.GoString(kv.key)}
		} else if cmpRes < 0 {
			found = 2
			info[C.GoString(kv.key)] = &proto.TenantInfo{Name: C.GoString(kv.key)}
		}
	}

	C.ccow_lookup_release(iter)

	if found == 0 || (found == 2 && msg.Count == 1) {
		return &proto.TenantListResponse{}, nil
	}

	return &proto.TenantListResponse{Info: info}, nil
}
