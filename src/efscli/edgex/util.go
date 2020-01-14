package edgex

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"time"
)

// EdgexTest - general Edgex client test structure
type EdgexTest struct {
	Mockup  int    `json:"mockup"`
	Url     string `json:"url"`
	Authkey string `json:"authkey"`
	Secret  string `json:"secret"`
	Bucket  string `json:"bucket"`
	Object  string `json:"object"`
	Debug   int    `json:"debug"`
}

// SetupS3xClient - setup S3xClient from configuration file
func SetupS3xClient(obj string) (S3xClient, EdgexTest) {
	var ed EdgexTest
	var s3x S3xClient
	var ex *Edgex
	var em *Mockup

	rand.Seed(time.Now().UnixNano())

	var tp = os.Getenv("NEDGE_HOME")
	tp += "/etc/"
	buf, err := ioutil.ReadFile(tp + "s3x_setup.json")
	if err != nil {
		log.Println("Error reading test_setup.json file", err)
		os.Exit(1)
	}

	log.Println("Read setup", string(buf))

	err = json.Unmarshal(buf, &ed)
	if err != nil {
		log.Println("Error reading setup file", err)
		os.Exit(1)
	}

	if ed.Bucket == "" {
		ed.Bucket = fmt.Sprintf("bktest%d", rand.Intn(1000))
	}
	if ed.Object == "" {
		ed.Object = fmt.Sprintf("obj%d", rand.Intn(10000))
	}
	if obj != "" {
		ed.Object = obj
	}

	if ed.Mockup == 0 {
		log.Println("Create Edgex:", ed)
		ex = CreateEdgex(ed.Url, ed.Authkey, ed.Secret, ed.Debug)
		s3x = ex
	} else {
		log.Println("Create Edgex mockup")
		em = CreateMockup(ed.Debug)
		s3x = em
	}

	return s3x, ed
}

// ArrToJSON - convert k/v pairs to json
func ArrToJSON(arr ...string) string {
	var b bytes.Buffer

	b.WriteString("{")
	n := 0
	for i := 0; i < len(arr); i += 2 {
		if n > 0 {
			b.WriteString(", ")
		}
		b.WriteString(" \"")
		b.WriteString(arr[i])
		b.WriteString("\": \"")
		b.WriteString(arr[i+1])
		b.WriteString("\"")
		n++
	}
	b.WriteString("}")

	return b.String()
}

// ArrToCVS - convert k/v pairs to cvs
func ArrToCVS(arr ...string) string {
	var b bytes.Buffer

	n := 0
	for i := 0; i < len(arr); i += 2 {
		if n > 0 {
			b.WriteString("\n")
		}
		b.WriteString(arr[i])
		b.WriteString(";")
		b.WriteString(arr[i+1])
		n++
	}

	return b.String()
}
