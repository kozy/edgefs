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
package userImpl

/*
#include "ccow.h"
#include "errno.h"
*/
import "C"
import (
	"strings"

	proto ".."
	"../../efsutil"
	"golang.org/x/net/context"
	"google.golang.org/grpc/status"
)

type UserImpl struct {
}

func (s *UserImpl) UserCreate(ctx context.Context, msg *proto.UserCreateRequest) (*proto.UserCreateResponse, error) {
	if msg.Cluster == "" {
		return nil, status.Error(400, "missing required cluster field ")
	}
	if msg.Tenant == "" {
		return nil, status.Error(400, "missing required tenant field ")
	}
	if msg.Username == "" {
		return nil, status.Error(400, "missing required username field ")
	}
	if msg.Password == "" {
		return nil, status.Error(400, "missing required password field ")
	}

	admin := 0
	if msg.Admin != "" {
		admin = 1
	}

	authkey := strings.ToUpper(efsutil.RandomString(20))
	secret := efsutil.RandomString(40)

	// sent?
	if msg.Authkey != "" {
		authkey = msg.Authkey
	}
	if msg.Secret != "" {
		secret = msg.Secret
	}
	user := efsutil.CreateUser(msg.Cluster, msg.Tenant, msg.Username, msg.Password, "object", "nedge", authkey, secret, admin)
	if user == nil {
		return nil, status.Error(500, "Create user error ")
	}

	err := efsutil.SaveUser(msg.Cluster, msg.Tenant, user)
	if err != nil {
		return nil, status.Errorf(500, "Save user error=%v", err)
	}

	return &proto.UserCreateResponse{Authkey: authkey, Secret: secret}, nil
}

func (s *UserImpl) UserDelete(ctx context.Context, msg *proto.UserDeleteRequest) (*proto.GenericResponse, error) {
	if msg.Cluster == "" {
		return nil, status.Error(400, "missing required cluster field ")
	}
	if msg.Tenant == "" {
		return nil, status.Error(400, "missing required tenant field ")
	}
	if msg.Username == "" {
		return nil, status.Error(400, "missing required username field ")
	}

	user, err := efsutil.LoadUser(msg.Cluster, msg.Tenant, efsutil.UserKey(msg.Username))
	if err != nil {
		return nil, status.Errorf(400, "Load user error=%v", err)
	}

	err = efsutil.DeleteUser(msg.Cluster, msg.Tenant, user)
	if err != nil {
		return nil, status.Errorf(500, "Delete user error=%v", err)
	}

	return &proto.GenericResponse{}, nil
}

func (s *UserImpl) UserList(ctx context.Context, msg *proto.UserListRequest) (*proto.UserListResponse, error) {
	list, err := efsutil.ListUser(msg.Cluster, msg.Tenant, 1000, msg.Marker)
	if err != nil {
		return nil, status.Errorf(500, "User list error=%v", err)
	}

	info := make([]*proto.UserInfo, len(list))
	for i := range list {
		info[i] = &proto.UserInfo{Username: list[i].Username, Usertype: list[i].Type,
			Identity: list[i].Identity, Admin: int32(list[i].Admin)}
	}

	return &proto.UserListResponse{Info: info}, nil
}
