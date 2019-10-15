/*
 * Copyright (c) 2015-2018 Nexenta Systems, inc.
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
 *
 *
 * Put here any temporary code which will be removed in further versions
 * and provide backward-compatibility functions
 *
 */
#include "reptrans.h"
#include "reptrans-data.h"

/* Keep the old verification request here for a while for backward compatibility reason */
struct verification_request_v1 {
	uint512_t chid;
	uint512_t nhid;
	uint128_t target_vdevid;
	uint64_t uvid_timestamp;
	uint64_t generation;
	struct backref vbr;
	uint8_t vtype;
	uint8_t ttag;
	uint8_t htype;
	uint8_t width;
	uint8_t n_parity;
	uint8_t domain;
	uint8_t algorithm;
};

int
vbreq_convert_compat(void* data_ptr,  struct verification_request** out) {
	/* Convert verification/enconding/batch queue entries to new format */
	struct verification_request_v1* vold = data_ptr;
	if (vold->ttag != TT_VERSION_MANIFEST && vold->ttag != TT_CHUNK_MANIFEST)
		return -EINVAL;
	struct verification_request* vbreq = je_calloc(1, sizeof(struct verification_request));
	if (!vbreq)
		return -ENOMEM;
	vbreq->algorithm = vold->algorithm;
	vbreq->chid = vold->chid;
	vbreq->domain = vold->domain;
	vbreq->generation = vold->generation;
	vbreq->htype = vold->htype;
	vbreq->n_parity = vold->n_parity;
	vbreq->nhid = vold->nhid;
	vbreq->target_vdevid = vold->target_vdevid;
	vbreq->ttag = vold->ttag;
	vbreq->uvid_timestamp = vold->uvid_timestamp;
	vbreq->vbr = vold->vbr;
	vbreq->vtype = vold->vtype;
	vbreq->width = vold->width;
	*out = vbreq;
	return 0;
}



