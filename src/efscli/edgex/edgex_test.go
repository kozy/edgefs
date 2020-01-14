package edgex

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"testing"
)

var ed EdgexTest
var s3x S3xClient

func TestMain(m *testing.M) {
	s3x, ed = SetupS3xClient("")

	// Run tests
	exitVal := m.Run()

	log.Println("Close Edgex connection:")
	s3x.CloseEdgex()

	os.Exit(exitVal)
}

func TestBucketCreate(t *testing.T) {

	res := s3x.BucketCreate(ed.Bucket)

	if res != nil {
		t.Fatal("Bucket create error", res)
	}
}

func TestBucketHead(t *testing.T) {

	res := s3x.BucketHead(ed.Bucket)

	if res != nil {
		t.Fatal("Head error", res)
	}
}

func TestKeyValueCreate(t *testing.T) {

	res := s3x.ObjectCreate(ed.Bucket, ed.Object, OBJECT_TYPE_KEY_VALUE,
		"application/json", DEFAULT_CHUNKSIZE, DEFAULT_BTREE_ORDER)

	if res != nil {
		t.Fatal("K/V object create error", res)
	}
}

func TestObjectHead(t *testing.T) {

	res := s3x.ObjectHead(ed.Bucket, ed.Object)

	if res != nil {
		t.Fatal("Object Head error", res)
	}
}

func TestNotObjectHead(t *testing.T) {

	res := s3x.ObjectHead(ed.Bucket, "notobject")

	if res == nil {
		t.Fatal("Not Object Head error")
	}
}

func TestKeyValuePost(t *testing.T) {

	res := s3x.KeyValuePost(ed.Bucket, ed.Object, "", "key1",
		bytes.NewBufferString("value1"), true)
	if res != nil {
		t.Fatal("K/V object post1 error", res)
	}
	res = s3x.KeyValuePost(ed.Bucket, ed.Object, "", "key2",
		bytes.NewBufferString("value2"), true)
	if res != nil {
		t.Fatal("K/V object post1 error", res)
	}
}

func TestKeyValueCommit(t *testing.T) {
	key := "aaa1"
	res := s3x.KeyValuePost(ed.Bucket, ed.Object, "", key,
		bytes.NewBufferString("value1"), true)
	if res != nil {
		t.Fatal("K/V object post1 error", res)
	}
	res = s3x.KeyValueCommit(ed.Bucket, ed.Object)
	if res != nil {
		t.Fatal("K/V object commit error", res)
	}
	r, err := s3x.KeyValueGet(ed.Bucket, ed.Object, key)
	if err != nil {
		t.Fatal("K/V object get after commit error", err)
	}
	fmt.Printf("K/V get key: %s, value : %s\n", key, r)
}

func TestKeyValueRollback(t *testing.T) {
	key := "aaa2"
	res := s3x.KeyValuePost(ed.Bucket, ed.Object, "", key,
		bytes.NewBufferString("value2"), true)
	if res != nil {
		t.Fatal("K/V object post1 error", res)
	}
	res = s3x.KeyValueRollback(ed.Bucket, ed.Object)
	if res != nil {
		t.Fatal("K/V object rollback error", res)
	}
	_, err := s3x.KeyValueGet(ed.Bucket, ed.Object, key)
	if err == nil {
		t.Fatal("K/V object still exists after rollback")
	}
}

func TestKeyValuePostJSON(t *testing.T) {

	json := ArrToJSON("key3", "value3", "key4", "value4", "key5", "value5")

	res := s3x.KeyValuePostJSON(ed.Bucket, ed.Object, json, false)
	if res != nil {
		t.Fatal("K/V object json post error", res)
	}
}

func TestKeyValuePostCVS(t *testing.T) {

	cvs := ArrToCVS("xx/kk9", "vv9", "xx/kk1", "vv1", "xz", "vv2", "xx/k3", "vv3")

	res := s3x.KeyValuePostCSV(ed.Bucket, ed.Object, cvs, false)
	if res != nil {
		t.Fatal("K/V object cvs post error", res)
	}

	key := "xx"
	prefix := "xx"
	r, err := s3x.KeyValueList(ed.Bucket, ed.Object, key, prefix, "text/csv", 100, true)
	if err != nil {
		t.Fatal("K/V object json list error", err)
	}
	fmt.Printf("K/V list from key: %s:\n%s\n", key, r)

}

func TestKeyValueGet(t *testing.T) {
	key := "key1"
	res, err := s3x.KeyValueGet(ed.Bucket, ed.Object, key)
	if err != nil {
		t.Fatal("K/V object get error", err)
	}
	fmt.Printf("K/V get key: %s, value : %s\n", key, res)
	key = "ke"
	res, err = s3x.KeyValueGet(ed.Bucket, ed.Object, key)
	if err == nil {
		t.Fatal("K/V object get should fail for", key)
	}
	key = "key1x"
	res, err = s3x.KeyValueGet(ed.Bucket, ed.Object, key)
	if err == nil {
		t.Fatal("K/V object get should fail for", key)
	}
}

func TestKeyValueDeleteJSON(t *testing.T) {

	json := ArrToJSON("key1", "", "key2", "")

	res := s3x.KeyValueDeleteJSON(ed.Bucket, ed.Object, json, false)
	if res != nil {
		t.Fatal("K/V object json delete error", res)
	}

	_, err := s3x.KeyValueGet(ed.Bucket, ed.Object, "key1")
	if err == nil {
		t.Fatal("K/V object key1 not deleted")
	}

	_, err = s3x.KeyValueGet(ed.Bucket, ed.Object, "key2")
	if err == nil {
		t.Fatal("K/V object key2 not deleted")
	}
}

func TestKeyValueListCSV(t *testing.T) {
	key := ""
	res, err := s3x.KeyValueList(ed.Bucket, ed.Object, key, "", "text/csv", 100, true)
	if err != nil {
		t.Fatal("K/V object json list error", err)
	}
	fmt.Printf("K/V list from key: %s:\n%s\n", key, res)
}

func TestKeyValueListJSON(t *testing.T) {
	key := "key4"
	res, err := s3x.KeyValueList(ed.Bucket, ed.Object, key, "", "application/json", 100, true)
	if err != nil {
		t.Fatal("K/V object json list error", err)
	}
	fmt.Printf("K/V list from key: %s:\n %s\n", key, res)
}

func TestBucketList(t *testing.T) {
	res, err := s3x.BucketList()
	if err != nil {
		t.Fatal("Bucket list error", err)
	}
	fmt.Printf("Bucket list %v\n", res)
	found := false
	for i := range res {
		if res[i].Name == ed.Bucket {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("Bucket not found: ", ed.Bucket)
	}
}

/*
func TestObjectList(t *testing.T) {
	// Wait for trlog
	time.Sleep(45 * time.Second)
	res, err := s3x.ObjectList(ed.Bucket, "", "", 100)
	if err != nil {
		t.Fatal("Object list error", err)
	}
	fmt.Printf("Object list %v\n", res)
	found := false
	for i := range res {
		if res[i].Key == ed.Object {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("Object not found: ", ed.Object)
	}
}*/

func TestKeyValueDelete(t *testing.T) {
	key := "key5"
	res := s3x.KeyValueDelete(ed.Bucket, ed.Object, key, false)
	if res != nil {
		t.Fatal("K/V object delete error", res)
	}
	_, err := s3x.KeyValueGet(ed.Bucket, ed.Object, key)
	if err == nil {
		t.Fatal("K/V object not deleted")
	}
}

func TestKeyValueObjectDelete(t *testing.T) {

	res := s3x.ObjectDelete(ed.Bucket, ed.Object)

	if res != nil {
		t.Fatal("K/V object delete error", res)
	}
}

func TestBucketDelete(t *testing.T) {

	res := s3x.BucketDelete(ed.Bucket)

	if res != nil {
		t.Fatal("Bucket delete error", res)
	}
}
