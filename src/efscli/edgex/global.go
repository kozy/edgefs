package edgex

/*
#include <stdio.h>
#include "ccow.h"
#include "ccowutil.h"
#include "errno.h"
*/
import "C"
import "unsafe"

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
	"strings"

	"github.com/Nexenta/edgefs/src/efscli/efsutil"
)

const GVMR_LOCKED int = 1
const GVMR_UNLOCKED int = 0

// GVMR - general global VM structure
type GVMR struct {
	Path      string
	Genid     uint64
	Uvid      uint64
	Deleted   uint
	Nhid      string
	Vmchid    string
	Segid     uint64
	Size      uint64
	Locktime  int64
	Lockstate int
}

// GVMRToCSV - convert GVMR parameters to cvs
func GVMRToCSV(gvm GVMR) string {
	var a bytes.Buffer
	b, _ := json.Marshal(&gvm)
	a.WriteString(gvm.Path)
	a.WriteString(";")
	a.WriteString(string(b))
	return a.String()
}

// GVMRPrint - print GVMR record
func GVMRPrint(head string, g GVMR) {
	fmt.Printf("%s - %s: genid: %v, uvid: %v del: %v, segid: %X, size: %v, lock: [%v, %v], nchid: %s, chid: %s\n",
		head, g.Path, g.Genid, g.Uvid, g.Deleted, g.Segid, g.Size, g.Locktime, g.Lockstate, g.Nhid, g.Vmchid)
}

// GlobalVMGetInt - get global VM record
func GlobalVMGetInt(opath string) (GVMR, int) {
	var res GVMR
	res.Path = opath
	s3x, ed := SetupS3xClient("GVMT")

	r, e := s3x.KeyValueGetInt(ed.Bucket, ed.Object, opath)
	if e != 0 {
		return res, e
	}
	fmt.Printf("VM get by opath: %s, value : %s\n", opath, r)
	err := json.Unmarshal([]byte(r), &res)
	s3x.CloseEdgex()

	if err != nil {
		return res, -22
	}

	return res, 0
}

// GlobalVMAcquire - aquire global VM record lock
func GlobalVMAcquire(opath string) error {
	var gvm GVMR
	gvm.Path = opath

	s3x, ed := SetupS3xClient("GVMT")

	err := s3x.BucketHead(ed.Bucket)
	if err != nil {
		err = s3x.BucketCreate(ed.Bucket)
		if err != nil {
			return err
		}
	}

	err = s3x.ObjectHead(ed.Bucket, ed.Object)
	if err != nil {
		err = s3x.ObjectCreate(ed.Bucket, ed.Object, OBJECT_TYPE_KEY_VALUE,
			"application/json", DEFAULT_CHUNKSIZE, DEFAULT_BTREE_ORDER)
		if err != nil {
			return err
		}
	}


	r, e := s3x.KeyValueGet(ed.Bucket, ed.Object, gvm.Path)
	if e == nil {
		var vmold GVMR
		e = json.Unmarshal([]byte(r), &vmold)
		if e == nil {
			if vmold.Lockstate != GVMR_UNLOCKED {
				return fmt.Errorf("Object %s is locked", gvm.Path)
			}
			gvm = vmold
		}
	}

	gvm.Lockstate = GVMR_LOCKED
	gvm.Locktime = time.Now().UnixNano()

	csv := GVMRToCSV(gvm)
	GVMRPrint("Acquire", gvm)
	err = s3x.KeyValuePostCSV(ed.Bucket, ed.Object, csv, false)
	s3x.CloseEdgex()

	if err != nil {
		return err
	}

	return GlobalGetPrepareInternal(gvm)
}

// GlobalVMRelease - release global VM record lock
func GlobalVMRelease(opath string) error {
	var vmold GVMR
	var gvm GVMR
	vmold.Path = opath
	gvm.Path = opath

	s3x, ed := SetupS3xClient("GVMT")

	r, e := s3x.KeyValueGet(ed.Bucket, ed.Object, opath)
	if e == nil {
		e = json.Unmarshal([]byte(r), &vmold)
		if e == nil {
			if vmold.Lockstate != GVMR_LOCKED {
				return nil
			}
		}
	}

	err := ObjectGetLocalHeaders(opath, &gvm)
	GVMRPrint("Local vm", gvm)
	if err != nil {
		fmt.Printf("Object opath: %s, get local headers error: %v\n", opath, err)
		gvm = vmold
	}

	gvm.Lockstate = GVMR_UNLOCKED
	gvm.Locktime = time.Now().UnixNano()

	csv := GVMRToCSV(gvm)
	GVMRPrint("Release", gvm)
	err = s3x.KeyValuePostCSV(ed.Bucket, ed.Object, csv, false)

	s3x.CloseEdgex()
	return err
}


func ObjectGetLocalHeaders(opath string, gvm *GVMR) error {
	s := strings.SplitN(opath, "/", 4)

	fmt.Printf("ObjectGetLocalHeaders opath: %s, s: %v\n", opath, s)

	c_opath := C.CString(opath)
	defer C.free(unsafe.Pointer(c_opath))

	c_cluster := C.CString(s[0])
	defer C.free(unsafe.Pointer(c_cluster))

	c_tenant := C.CString(s[1])
	defer C.free(unsafe.Pointer(c_tenant))

	c_bucket := C.CString(s[2])
	defer C.free(unsafe.Pointer(c_bucket))

	c_object := C.CString(s[3])
	defer C.free(unsafe.Pointer(c_object))

	conf, err := efsutil.GetLibccowConf()
	if err != nil {
		return err
	}

	c_conf := C.CString(string(conf))
	defer C.free(unsafe.Pointer(c_conf))

	var tc C.ccow_t

	ret := C.ccow_tenant_init(c_conf, c_cluster, C.strlen(c_cluster)+1,
		c_tenant, C.strlen(c_tenant)+1, &tc)
	if ret != 0 {
		return fmt.Errorf("ccow_tenant_init err=%d", ret)
	}
	defer C.ccow_tenant_term(tc)

	var c C.ccow_completion_t
	var cont_flags C.int = 0
	var genid C.uint64_t = 0

	var iter C.ccow_lookup_t

	ret = C.ccow_create_stream_completion(tc, nil, nil, 1, &c,
		c_bucket, C.strlen(c_bucket)+1, c_object, C.strlen(c_object)+1,
		&genid, &cont_flags, &iter)
	if ret != 0 {
		return fmt.Errorf("ccow_create_stream_completion err=%d", ret)
	}

	var uvid C.uint64_t = 0
	var size C.uint64_t = 0
	var deleted uint = 0

	if cont_flags != C.CCOW_CONT_F_EXIST {
		 deleted = 1

		 var vv [C.UINT512_BYTES*2 + 1]C.char

		 vmchid := C.ccow_get_vm_content_hash_id(c)
		 C.uint512_dump(vmchid, (*C.char)(&vv[0]), C.UINT512_BYTES*2+1)
		 gvm.Vmchid = C.GoStringN((*C.char)(&vv[0]), C.UINT512_BYTES)

		 nhid := C.ccow_get_vm_name_hash_id(c)
		 C.uint512_dump(nhid, (*C.char)(&vv[0]), C.UINT512_BYTES*2+1)
		 gvm.Nhid = C.GoStringN((*C.char)(&vv[0]), C.UINT512_BYTES)

	 }


	var kv *C.struct_ccow_metadata_kv
	for {
		kv = (*C.struct_ccow_metadata_kv)(C.ccow_lookup_iter(iter,
			C.CCOW_MDTYPE_METADATA, -1))
		if kv == nil {
			break
		}
		if strings.Compare(C.GoString(kv.key), C.RT_SYSKEY_UVID_TIMESTAMP) == 0 {
			uvid = (*(*C.ulong)(kv.value))
			continue
		}

		if strings.Compare(C.GoString(kv.key), C.RT_SYSKEY_OBJECT_DELETED) == 0 {
			deleted = uint(*(*C.uchar)(kv.value))
			continue
		}

		if strings.Compare(C.GoString(kv.key), C.RT_SYSKEY_LOGICAL_SIZE) == 0 {
			size = (*(*C.ulong)(kv.value))
			continue
		}

		if strings.Compare(C.GoString(kv.key), C.RT_SYSKEY_NAME_HASH_ID) == 0 {
			var vv [C.UINT512_BYTES*2 + 1]C.char
			C.uint512_dump((*C.uint512_t)(kv.value), (*C.char)(&vv[0]), C.UINT512_BYTES*2+1)
			gvm.Nhid = C.GoStringN((*C.char)(&vv[0]), C.UINT512_BYTES)
			continue
		}

		if strings.Compare(C.GoString(kv.key), C.RT_SYSKEY_VM_CONTENT_HASH_ID) == 0 {
			var vv [C.UINT512_BYTES*2 + 1]C.char
			C.uint512_dump((*C.uint512_t)(kv.value), (*C.char)(&vv[0]), C.UINT512_BYTES*2+1)
			gvm.Vmchid = C.GoStringN((*C.char)(&vv[0]), C.UINT512_BYTES)
			continue
		}
	}

	gvm.Genid = uint64(genid)
	gvm.Uvid = uint64(uvid)
	gvm.Size = uint64(size)
	gvm.Deleted = deleted
	gvm.Segid = uint64(C.ccow_get_segment_guid(tc))

	C.ccow_cancel(c)
	return nil
}

func prefetchGlobalVM(gvm GVMR) error {
	c_nhid_str := C.CString(gvm.Nhid + strings.Repeat("0", C.UINT512_BYTES))
	defer C.free(unsafe.Pointer(c_nhid_str))
	var c_nhid C.uint512_t
	C.uint512_fromhex(c_nhid_str, C.UINT512_BYTES * 2 + 1, &c_nhid);

	c_vmchid_str := C.CString(gvm.Vmchid + strings.Repeat("0", C.UINT512_BYTES))
	defer C.free(unsafe.Pointer(c_vmchid_str))
	var c_vmchid C.uint512_t
	C.uint512_fromhex(c_vmchid_str, C.UINT512_BYTES * 2 + 1, &c_vmchid);

	s := strings.SplitN(gvm.Path, "/", 4)

	c_cluster := C.CString(s[0])
	defer C.free(unsafe.Pointer(c_cluster))

	c_tenant := C.CString(s[1])
	defer C.free(unsafe.Pointer(c_tenant))

	c_bucket := C.CString(s[2])
	defer C.free(unsafe.Pointer(c_bucket))

	c_object := C.CString(s[3])
	defer C.free(unsafe.Pointer(c_object))

	conf, err := efsutil.GetLibccowConf()
	if err != nil {
		return err
	}

	c_conf := C.CString(string(conf))
	defer C.free(unsafe.Pointer(c_conf))

	var tc C.ccow_t

	ret := C.ccow_tenant_init(c_conf, c_cluster, C.strlen(c_cluster)+1,
		c_tenant, C.strlen(c_tenant)+1, &tc)
	if ret != 0 {
		return fmt.Errorf("ccow_tenant_init err=%d", ret)
	}
	defer C.ccow_tenant_term(tc)

	var c C.ccow_completion_t
	ret = C.ccow_create_completion(tc, nil, nil, 2, &c)
	if ret != 0 {
		return fmt.Errorf("ccow_create_completion err=%d", ret)
	}

	C.set_isgw_bid(c, c_bucket)

	ret = C.ccow_vm_prefetch(tc, &c_vmchid, &c_nhid,
		c_tenant, C.strlen(c_tenant)+1,
		c_bucket, C.strlen(c_bucket)+1,
		c_object, C.strlen(c_object)+1,
		C.ulong(gvm.Segid),	c)

	if ret != 0 {
		C.ccow_drop(c)
		return fmt.Errorf("ccow_clone_vm_prefetch err=%d", ret)
	}

	ret = C.ccow_wait(c, 0)
	if ret != 0 {
		return fmt.Errorf("%s: ccow_wait err=%d", efsutil.GetFUNC(), ret)
	}

	if gvm.Deleted == 0 {
		ret = C.ccow_ondemand_policy_change(tc, c_bucket, C.strlen(c_bucket)+1,
			c_object, C.strlen(c_object)+1, C.ulong(gvm.Genid), C.ondemand_policy_t(C.ondemandPolicyPersist));
		if ret != 0 {
			return fmt.Errorf("Persist error: %d", ret)
		}
	}
	return nil
}

func GlobalGetPrepareInternal(gvm GVMR) error {
	opath := gvm.Path
	var lvm GVMR
	err := ObjectGetLocalHeaders(opath, &lvm)
	fmt.Printf("Local vm opath: %s, err: %v, genid: %v, deleted: %v, value : %v\n", opath, err, lvm.Genid, lvm.Deleted, lvm)

	if gvm.Genid > 0 {
		if lvm.Genid == 0 || lvm.Genid < gvm.Genid || (lvm.Genid == gvm.Genid && lvm.Uvid < gvm.Uvid)  {
			fmt.Printf("Prefetch global vm: %s, value : %v\n", opath, gvm)
			err = prefetchGlobalVM(gvm)
			fmt.Printf("Prefetch global vm: %s, err : %v\n", opath, err)
			if err != nil {
				return err
			}
		}
	}
	fmt.Printf("Get object opath: %s, value : %v\n", opath, gvm)
	return nil
}

func GlobalGetPrepare(opath string) error {
	var gvm GVMR
	gvm.Path = opath
	var e int
	gvm, e = GlobalVMGetInt(opath)
	if e != 0  {
		fmt.Printf("Global get vm error opath: %s, e: %v\n", opath, e)
	}
	GVMRPrint("Global get", gvm)

	return GlobalGetPrepareInternal(gvm)
}