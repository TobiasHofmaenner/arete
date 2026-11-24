/*
Copyright 2026 Tobias Hofmaenner.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"

	aretev1alpha1 "github.com/TobiasHofmaenner/arete/api/v1alpha1"
)

// fakeS3 is a small in-memory S3 server for tests. It speaks just enough of
// the protocol for the calls arete makes (ListObjectsV2, GetObject, and the
// bucket's Object Lock and encryption configuration), so the tests exercise
// the real minio-go client, including its parsing of error responses.
//
// It serves one bucket. Requests are not authenticated.
type fakeS3 struct {
	mu      sync.Mutex
	bucket  string
	objects map[string]fakeObject

	// listErr maps a listing prefix to the S3 error code returned for
	// listings of exactly that prefix.
	listErr map[string]string
	// getErr maps an object key to the S3 error code returned for GETs.
	getErr map[string]string
	// bucketErr, when set, is returned for every request (for example
	// NoSuchBucket).
	bucketErr string

	// objectLock and encryption are "" (not configured), "enabled", or an
	// S3 error code to return instead.
	objectLock string
	encryption string

	srv *httptest.Server
}

type fakeObject struct {
	body     []byte
	modified time.Time
}

const fakeS3Bucket = "backups"

func newFakeS3(t *testing.T) *fakeS3 {
	t.Helper()
	f := &fakeS3{
		bucket:  fakeS3Bucket,
		objects: map[string]fakeObject{},
		listErr: map[string]string{},
		getErr:  map[string]string{},
	}
	f.srv = httptest.NewServer(f)
	t.Cleanup(f.srv.Close)
	return f
}

// put stores an object under the given key.
func (f *fakeS3) put(key, body string, modified time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = fakeObject{body: []byte(body), modified: modified}
}

// spec returns a BackupRepositorySpec that points at this server.
func (f *fakeS3) spec(prefix string, format aretev1alpha1.BackupFormat) aretev1alpha1.BackupRepositorySpec {
	return aretev1alpha1.BackupRepositorySpec{
		Format: format,
		S3: aretev1alpha1.S3Source{
			Endpoint: f.srv.URL,
			Region:   "us-east-1",
			Bucket:   f.bucket,
			Prefix:   prefix,
		},
	}
}

// client returns a minio client talking to this server.
func (f *fakeS3) client(t *testing.T) *minio.Client {
	t.Helper()
	c, err := buildS3Client(f.spec("unused", aretev1alpha1.BackupFormatRestic).S3,
		S3Credentials{AccessKeyID: "test", SecretAccessKey: "test"})
	if err != nil {
		t.Fatalf("build client: %v", err)
	}
	return c
}

var s3StatusByCode = map[string]int{
	"AccessDenied":                http.StatusForbidden,
	"NoSuchBucket":                http.StatusNotFound,
	"NoSuchKey":                   http.StatusNotFound,
	s3CodeObjectLockNotConfigured: http.StatusNotFound,
	s3CodeEncryptionNotConfigured: http.StatusNotFound,
}

func writeS3Error(w http.ResponseWriter, code string) {
	status, ok := s3StatusByCode[code]
	if !ok {
		status = http.StatusBadRequest
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>`+
		`<Error><Code>%s</Code><Message>fake: %s</Message><RequestId>1</RequestId></Error>`, code, code)
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	bucket, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if f.bucketErr != "" {
		writeS3Error(w, f.bucketErr)
		return
	}
	if bucket != f.bucket {
		writeS3Error(w, "NoSuchBucket")
		return
	}
	q := r.URL.Query()
	switch {
	case key == "" && q.Has("object-lock"):
		f.serveSetting(w, f.objectLock, s3CodeObjectLockNotConfigured,
			`<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>`+
				`<Rule><DefaultRetention><Mode>COMPLIANCE</Mode><Days>30</Days></DefaultRetention></Rule>`+
				`</ObjectLockConfiguration>`)
	case key == "" && q.Has("encryption"):
		f.serveSetting(w, f.encryption, s3CodeEncryptionNotConfigured,
			`<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault>`+
				`<SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule>`+
				`</ServerSideEncryptionConfiguration>`)
	case key == "" && q.Get("list-type") == "2":
		f.serveList(w, q.Get("prefix"), q.Get("delimiter"))
	case key != "" && (r.Method == http.MethodGet || r.Method == http.MethodHead):
		f.serveObject(w, r, key)
	default:
		writeS3Error(w, "NotImplemented")
	}
}

func (f *fakeS3) serveSetting(w http.ResponseWriter, state, notConfiguredCode, body string) {
	switch state {
	case "":
		writeS3Error(w, notConfiguredCode)
	case "enabled":
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>`+body)
	default:
		writeS3Error(w, state)
	}
}

type listBucketResult struct {
	XMLName        xml.Name       `xml:"ListBucketResult"`
	Name           string         `xml:"Name"`
	Prefix         string         `xml:"Prefix"`
	Delimiter      string         `xml:"Delimiter,omitempty"`
	KeyCount       int            `xml:"KeyCount"`
	MaxKeys        int            `xml:"MaxKeys"`
	IsTruncated    bool           `xml:"IsTruncated"`
	Contents       []listContent  `xml:"Contents"`
	CommonPrefixes []commonPrefix `xml:"CommonPrefixes"`
}

type listContent struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int    `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type commonPrefix struct {
	Prefix string `xml:"Prefix"`
}

func (f *fakeS3) serveList(w http.ResponseWriter, prefix, delimiter string) {
	if code, ok := f.listErr[prefix]; ok {
		writeS3Error(w, code)
		return
	}
	keys := make([]string, 0, len(f.objects))
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	out := listBucketResult{Name: f.bucket, Prefix: prefix, Delimiter: delimiter, MaxKeys: 1000}
	seen := map[string]bool{}
	for _, k := range keys {
		rest := strings.TrimPrefix(k, prefix)
		if delimiter != "" {
			if i := strings.Index(rest, delimiter); i >= 0 {
				p := prefix + rest[:i+len(delimiter)]
				if !seen[p] {
					seen[p] = true
					out.CommonPrefixes = append(out.CommonPrefixes, commonPrefix{Prefix: p})
				}
				continue
			}
		}
		o := f.objects[k]
		out.Contents = append(out.Contents, listContent{
			Key:          k,
			LastModified: o.modified.UTC().Format("2006-01-02T15:04:05.000Z"),
			ETag:         `"etag"`,
			Size:         len(o.body),
			StorageClass: "STANDARD",
		})
	}
	out.KeyCount = len(out.Contents) + len(out.CommonPrefixes)
	w.Header().Set("Content-Type", "application/xml")
	_, _ = fmt.Fprint(w, xml.Header)
	_ = xml.NewEncoder(w).Encode(out)
}

func (f *fakeS3) serveObject(w http.ResponseWriter, r *http.Request, key string) {
	if code, ok := f.getErr[key]; ok {
		writeS3Error(w, code)
		return
	}
	o, ok := f.objects[key]
	if !ok {
		writeS3Error(w, "NoSuchKey")
		return
	}
	w.Header().Set("ETag", `"etag"`)
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, "", o.modified, strings.NewReader(string(o.body)))
}
