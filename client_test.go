// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package elastic

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"
)

// -- NewClient --

func TestClientDefaults(t *testing.T) {
	client, err := NewClient()
	if err != nil {
		t.Fatal(err)
	}
	if client.basicAuth != false {
		t.Errorf("expected no basic auth; got: %v", client.basicAuth)
	}
	if client.basicAuthUsername != "" {
		t.Errorf("expected no basic auth username; got: %q", client.basicAuthUsername)
	}
	if client.basicAuthPassword != "" {
		t.Errorf("expected no basic auth password; got: %q", client.basicAuthUsername)
	}
	if client.sendGetBodyAs != "GET" {
		t.Errorf("expected sendGetBodyAs to be GET; got: %q", client.sendGetBodyAs)
	}
}

func TestClientWithBasicAuth(t *testing.T) {
	client, err := NewClient(SetBasicAuth("user", "secret"))
	if err != nil {
		t.Fatal(err)
	}
	if client.basicAuth != true {
		t.Errorf("expected basic auth; got: %v", client.basicAuth)
	}
	if got, want := client.basicAuthUsername, "user"; got != want {
		t.Errorf("expected basic auth username %q; got: %q", want, got)
	}
	if got, want := client.basicAuthPassword, "secret"; got != want {
		t.Errorf("expected basic auth password %q; got: %q", want, got)
	}
}

func TestClientWithRequiredPlugins(t *testing.T) {
	_, err := NewClient(SetRequiredPlugins("no-such-plugin"))
	if err == nil {
		t.Fatal("expected error when creating client")
	}
	if got, want := err.Error(), "elastic: plugin no-such-plugin not found"; got != want {
		t.Fatalf("expected error %q; got: %q", want, got)
	}
}

func TestClientExtractHostname(t *testing.T) {
	tests := []struct {
		Scheme  string
		Address string
		Output  string
	}{
		{
			Scheme:  "http",
			Address: "",
			Output:  "",
		},
		{
			Scheme:  "https",
			Address: "abc",
			Output:  "",
		},
		{
			Scheme:  "http",
			Address: "127.0.0.1:19200",
			Output:  "http://127.0.0.1:19200",
		},
		{
			Scheme:  "https",
			Address: "127.0.0.1:9200",
			Output:  "https://127.0.0.1:9200",
		},
		{
			Scheme:  "http",
			Address: "myelk.local/10.1.0.24:9200",
			Output:  "http://10.1.0.24:9200",
		},
	}

	client, err := NewClient()
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		got := client.extractHostname(test.Scheme, test.Address)
		if want := test.Output; want != got {
			t.Errorf("expected %q; got: %q", want, got)
		}
	}
}

// -- ElasticsearchVersion --

func TestElasticsearchVersion(t *testing.T) {
	client, err := NewClient()
	if err != nil {
		t.Fatal(err)
	}
	version, err := client.ElasticsearchVersion(DefaultURL)
	if err != nil {
		t.Fatal(err)
	}
	if version == "" {
		t.Errorf("expected a version number, got: %q", version)
	}
}

// -- IndexNames --

func TestIndexNames(t *testing.T) {
	client := setupTestClientAndCreateIndex(t)
	names, err := client.IndexNames()
	if err != nil {
		t.Fatal(err)
	}
	if len(names) == 0 {
		t.Fatalf("expected some index names, got: %d", len(names))
	}
	var found bool
	for _, name := range names {
		if name == testIndexName {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected to find index %q; got: %v", testIndexName, found)
	}
}

// -- PerformRequest --

func TestPerformRequest(t *testing.T) {
	client, err := NewClient()
	if err != nil {
		t.Fatal(err)
	}
	res, err := client.PerformRequest("GET", "/", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatal("expected response to be != nil")
	}

	ret := new(PingResult)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		t.Fatalf("expected no error on decode; got: %v", err)
	}
	if ret.ClusterName == "" {
		t.Errorf("expected cluster name; got: %q", ret.ClusterName)
	}
}

func TestPerformRequestC(t *testing.T) {
	client, err := NewClient()
	if err != nil {
		t.Fatal(err)
	}
	res, err := client.PerformRequestC(context.Background(), "GET", "/", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatal("expected response to be != nil")
	}

	ret := new(PingResult)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		t.Fatalf("expected no error on decode; got: %v", err)
	}
	if ret.ClusterName == "" {
		t.Errorf("expected cluster name; got: %q", ret.ClusterName)
	}
}

func TestPerformRequestWithLogger(t *testing.T) {
	var w bytes.Buffer
	out := log.New(&w, "LOGGER ", log.LstdFlags)

	client, err := NewClient(SetInfoLog(out))
	if err != nil {
		t.Fatal(err)
	}

	res, err := client.PerformRequest("GET", "/", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatal("expected response to be != nil")
	}

	ret := new(PingResult)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		t.Fatalf("expected no error on decode; got: %v", err)
	}
	if ret.ClusterName == "" {
		t.Errorf("expected cluster name; got: %q", ret.ClusterName)
	}

	got := w.String()
	pattern := `^LOGGER \d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} GET http://.*/ \[status:200, request:\d+\.\d{3}s\]\n`
	matched, err := regexp.MatchString(pattern, got)
	if err != nil {
		t.Fatalf("expected log line to match %q; got: %v", pattern, err)
	}
	if !matched {
		t.Errorf("expected log line to match %q; got: %v", pattern, got)
	}
}

func TestPerformRequestWithLoggerAndTracer(t *testing.T) {
	var lw bytes.Buffer
	lout := log.New(&lw, "LOGGER ", log.LstdFlags)

	var tw bytes.Buffer
	tout := log.New(&tw, "TRACER ", log.LstdFlags)

	client, err := NewClient(SetInfoLog(lout), SetTraceLog(tout))
	if err != nil {
		t.Fatal(err)
	}

	res, err := client.PerformRequest("GET", "/", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatal("expected response to be != nil")
	}

	ret := new(PingResult)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		t.Fatalf("expected no error on decode; got: %v", err)
	}
	if ret.ClusterName == "" {
		t.Errorf("expected cluster name; got: %q", ret.ClusterName)
	}

	lgot := lw.String()
	if lgot == "" {
		t.Errorf("expected logger output; got: %q", lgot)
	}

	tgot := tw.String()
	if tgot == "" {
		t.Errorf("expected tracer output; got: %q", tgot)
	}
}

type customLogger struct {
	out bytes.Buffer
}

func (l *customLogger) Printf(format string, v ...interface{}) {
	l.out.WriteString(fmt.Sprintf(format, v...) + "\n")
}

func TestPerformRequestWithCustomLogger(t *testing.T) {
	logger := &customLogger{}

	client, err := NewClient(SetInfoLog(logger))
	if err != nil {
		t.Fatal(err)
	}

	res, err := client.PerformRequest("GET", "/", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatal("expected response to be != nil")
	}

	ret := new(PingResult)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		t.Fatalf("expected no error on decode; got: %v", err)
	}
	if ret.ClusterName == "" {
		t.Errorf("expected cluster name; got: %q", ret.ClusterName)
	}

	got := logger.out.String()
	pattern := `^GET http://.*/ \[status:200, request:\d+\.\d{3}s\]\n`
	matched, err := regexp.MatchString(pattern, got)
	if err != nil {
		t.Fatalf("expected log line to match %q; got: %v", pattern, err)
	}
	if !matched {
		t.Errorf("expected log line to match %q; got: %v", pattern, got)
	}
}

// failingTransport will run a fail callback if it sees a given URL path prefix.
type failingTransport struct {
	path string                                      // path prefix to look for
	fail func(*http.Request) (*http.Response, error) // call when path prefix is found
	next http.RoundTripper                           // next round-tripper (use http.DefaultTransport if nil)
}

// RoundTrip implements a failing transport.
func (tr *failingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if strings.HasPrefix(r.URL.Path, tr.path) && tr.fail != nil {
		return tr.fail(r)
	}
	if tr.next != nil {
		return tr.next.RoundTrip(r)
	}
	return http.DefaultTransport.RoundTrip(r)
}

func TestPerformRequestRetryOnHttpError(t *testing.T) {
	var numFailedReqs int
	fail := func(r *http.Request) (*http.Response, error) {
		numFailedReqs++
		//return &http.Response{Request: r, StatusCode: 400}, nil
		return nil, errors.New("request failed")
	}

	// Run against a failing endpoint and see if PerformRequest
	// retries correctly.
	tr := &failingTransport{path: "/fail", fail: fail}
	httpClient := &http.Client{Transport: tr}

	client, err := NewClient(SetHttpClient(httpClient), SetMaxRetries(5))
	if err != nil {
		t.Fatal(err)
	}

	res, err := client.PerformRequest("GET", "/fail", nil, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if res != nil {
		t.Fatal("expected no response")
	}
	// Connection should be marked as dead after it failed
	if numFailedReqs != 5 {
		t.Errorf("expected %d failed requests; got: %d", 5, numFailedReqs)
	}
}

func TestPerformRequestNoRetryOnValidButUnsuccessfulHttpStatus(t *testing.T) {
	var numFailedReqs int
	fail := func(r *http.Request) (*http.Response, error) {
		numFailedReqs++
		return &http.Response{Request: r, StatusCode: 500}, nil
	}

	// Run against a failing endpoint and see if PerformRequest
	// retries correctly.
	tr := &failingTransport{path: "/fail", fail: fail}
	httpClient := &http.Client{Transport: tr}

	client, err := NewClient(SetHttpClient(httpClient), SetMaxRetries(5))
	if err != nil {
		t.Fatal(err)
	}

	res, err := client.PerformRequest("GET", "/fail", nil, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if res == nil {
		t.Fatal("expected response, got nil")
	}
	if want, got := 500, res.StatusCode; want != got {
		t.Fatalf("expected status code = %d, got %d", want, got)
	}
	// Retry should not have triggered additional requests because
	if numFailedReqs != 1 {
		t.Errorf("expected %d failed requests; got: %d", 1, numFailedReqs)
	}
}

// failingBody will return an error when json.Marshal is called on it.
type failingBody struct{}

// MarshalJSON implements the json.Marshaler interface and always returns an error.
func (fb failingBody) MarshalJSON() ([]byte, error) {
	return nil, errors.New("failing to marshal")
}

func TestPerformRequestWithSetBodyError(t *testing.T) {
	client, err := NewClient()
	if err != nil {
		t.Fatal(err)
	}
	res, err := client.PerformRequest("GET", "/", nil, failingBody{})
	if err == nil {
		t.Fatal("expected error")
	}
	if res != nil {
		t.Fatal("expected no response")
	}
}

// sleepingTransport will sleep before doing a request.
type sleepingTransport struct {
	timeout time.Duration
}

// RoundTrip implements a "sleepy" transport.
func (tr *sleepingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	time.Sleep(tr.timeout)
	return http.DefaultTransport.RoundTrip(r)
}

func TestPerformRequestCWithCancel(t *testing.T) {
	tr := &sleepingTransport{timeout: 5 * time.Second}
	httpClient := &http.Client{Transport: tr}

	client, err := NewClient(SetHttpClient(httpClient), SetMaxRetries(0))
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		res *Response
		err error
	}
	ctx, cancel := context.WithCancel(context.Background())

	resc := make(chan result, 1)
	go func() {
		res, err := client.PerformRequestC(ctx, "GET", "/", nil, nil)
		resc <- result{res: res, err: err}
	}()
	select {
	case <-time.After(1 * time.Second):
		cancel()
	case res := <-resc:
		t.Fatalf("expected response before cancel, got %v", res)
	case <-ctx.Done():
		t.Fatalf("expected no early termination, got ctx.Done(): %v", ctx.Err())
	}
	err = ctx.Err()
	if err != context.Canceled {
		t.Fatalf("expected error context.Canceled, got: %v", err)
	}
}

func TestPerformRequestCWithTimeout(t *testing.T) {
	tr := &sleepingTransport{timeout: 5 * time.Second}
	httpClient := &http.Client{Transport: tr}

	client, err := NewClient(SetHttpClient(httpClient), SetMaxRetries(0))
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		res *Response
		err error
	}
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)

	resc := make(chan result, 1)
	go func() {
		res, err := client.PerformRequestC(ctx, "GET", "/", nil, nil)
		resc <- result{res: res, err: err}
	}()
	select {
	case res := <-resc:
		t.Fatalf("expected timeout before response, got %v", res)
	case <-ctx.Done():
		err := ctx.Err()
		if err != context.DeadlineExceeded {
			t.Fatalf("expected error context.DeadlineExceeded, got: %v", err)
		}
	}
}

// -- Compression --

// Notice that the trace log does always print "Accept-Encoding: gzip"
// regardless of whether compression is enabled or not. This is because
// of the underlying "httputil.DumpRequestOut".
//
// Use a real HTTP proxy/recorder to convince yourself that
// "Accept-Encoding: gzip" is NOT sent when DisableCompression
// is set to true.
//
// See also:
// https://groups.google.com/forum/#!topic/golang-nuts/ms8QNCzew8Q

func TestPerformRequestWithCompressionEnabled(t *testing.T) {
	testPerformRequestWithCompression(t, &http.Client{
		Transport: &http.Transport{
			DisableCompression: true,
		},
	})
}

func TestPerformRequestWithCompressionDisabled(t *testing.T) {
	testPerformRequestWithCompression(t, &http.Client{
		Transport: &http.Transport{
			DisableCompression: false,
		},
	})
}

func testPerformRequestWithCompression(t *testing.T, hc *http.Client) {
	client, err := NewClient(SetHttpClient(hc))
	if err != nil {
		t.Fatal(err)
	}
	res, err := client.PerformRequestC(context.TODO(), "GET", "/", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatal("expected response to be != nil")
	}

	ret := new(PingResult)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		t.Fatalf("expected no error on decode; got: %v", err)
	}
	if ret.ClusterName == "" {
		t.Errorf("expected cluster name; got: %q", ret.ClusterName)
	}
}
