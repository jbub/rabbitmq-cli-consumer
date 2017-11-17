package handler

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"reflect"
	"strings"
	"testing"
	"time"
)

var (
	mux    *http.ServeMux
	server *httptest.Server
)

const msgStr = `{"request_params":{
"uri":"%v",
"headers":{
"x-user-uuid":"xxxdddsa-bdf7-4382-8cc4-351367c39e48",
"x-user-scope":"cdasf",
"x-php-ob-level":1,
"x-cc-bb-async":1,
"x-cc-bb-ddasd":1.45,
"Content-Type":"application\/json"},
"body":"{\"from\":\"jano\",\"to\":\"palo\"}",
"method":"POST"}}`

func buildMsg(uri string) []byte {
	return []byte(fmt.Sprintf(msgStr, uri))
}

func setup(pattern string) {
	mux = http.NewServeMux()
	mux.HandleFunc(pattern, func(w http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()
		io.Copy(w, req.Body)
	})
	server = httptest.NewServer(mux)
}

func teardown() {
	server.Close()
}

func TestDoHttpRequest(t *testing.T) {
	pattern := "/test"

	setup(pattern)
	defer teardown()

	uri := server.URL + pattern
	data := buildMsg(uri)
	msg := &httpMessage{}
	if err := msg.parse(data); err != nil {
		t.Fatal(err)
	}

	req, err := buildRequest(msg)
	if err != nil {
		t.Fatalf("could not build http request: %v", err)
	}

	client := newHTTPClient(time.Second * 3)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("could not perform http request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("invalid status, got %v, want %v", resp.StatusCode, http.StatusOK)
	}

	var bodyData struct {
		From string `json:"from"`
		To   string `json:"to"`
	}
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("could not read response body: %v", err)
	}
	bodyStr := string(bodyBytes)

	dec := json.NewDecoder(strings.NewReader(bodyStr))
	if err := dec.Decode(&bodyData); err != nil {
		t.Fatalf("could not decode response body to json: %v, data = `%v`", err, bodyStr)
	}

	if expFrom := `jano`; bodyData.From != expFrom {
		t.Fatalf("invalid body from data, got %v, want %v", bodyData.From, expFrom)
	}
	if expTo := `palo`; bodyData.To != expTo {
		t.Fatalf("invalid body to data, got %v, want %v", bodyData.To, expTo)
	}

	headers := http.Header{
		"X-User-Uuid":    {"xxxdddsa-bdf7-4382-8cc4-351367c39e48"},
		"X-User-Scope":   {"cdasf"},
		"X-Php-Ob-Level": {"1"},
		"X-Cc-Bb-Async":  {"1"},
		"X-Cc-Bb-Ddasd":  {"1.45"},
		"Content-Type":   {"application/json"},
	}
	if !reflect.DeepEqual(resp.Request.Header, headers) {
		t.Fatalf("invalid headers, got %v, want %v", resp.Request.Header, headers)
	}

	method := "POST"
	if resp.Request.Method != method {
		t.Fatalf("invalid method, got %v, want %v", resp.Request.Method, method)
	}
}

func TestParseMessage(t *testing.T) {
	uri := `http:\/\/xxx.ccc.de:8080\/ccc\/bbb\/api\/df`
	msg := &httpMessage{}
	if err := msg.parse(buildMsg(uri)); err != nil {
		t.Fatal(err)
	}

	expURI := "http://xxx.ccc.de:8080/ccc/bbb/api/df"
	if msg.RequestParams.URI != expURI {
		t.Fatalf("invalid uri, got %v, want %v", msg.RequestParams.URI, expURI)
	}

	body := `{\"from\":\"jano\",\"to\":\"palo\"}`
	bodyStr := string(msg.RequestParams.Body)
	if bodyStr != body {
		t.Fatalf("invalid body, got %v, want %v", bodyStr, body)
	}

	headers := map[string]interface{}{
		"x-user-uuid":    "xxxdddsa-bdf7-4382-8cc4-351367c39e48",
		"x-user-scope":   "cdasf",
		"x-php-ob-level": json.Number("1"),
		"x-cc-bb-async":  json.Number("1"),
		"x-cc-bb-ddasd":  json.Number("1.45"),
		"Content-Type":   "application/json",
	}
	if !reflect.DeepEqual(msg.RequestParams.Headers, headers) {
		t.Fatalf("invalid headers, got %v, want %v", msg.RequestParams.Headers, headers)
	}

	method := "POST"
	if msg.RequestParams.Method != method {
		t.Fatalf("invalid method, got %v, want %v", msg.RequestParams.Method, method)
	}
}

func TestBuildRequest(t *testing.T) {
	uri := `http:\/\/xxx.ccc.de:8080\/ccc\/bbb\/api\/df`
	msg := &httpMessage{}
	if err := msg.parse(buildMsg(uri)); err != nil {
		t.Fatal(err)
	}

	req, err := buildRequest(msg)
	if err != nil {
		t.Fatal(err)
	}

	reqData, err := httputil.DumpRequest(req, true)
	if err != nil {
		t.Fatal(err)
	}

	want := `POST /ccc/bbb/api/df HTTP/1.1
Host: xxx.ccc.de:8080
Content-Type: application/json
X-Cc-Bb-Async: 1
X-Cc-Bb-Ddasd: 1.45
X-Php-Ob-Level: 1
X-User-Scope: cdasf
X-User-Uuid: xxxdddsa-bdf7-4382-8cc4-351367c39e48

"{\"from\":\"jano\",\"to\":\"palo\"}`
	dataStr := string(reqData)

	wantSplit := strings.Split(want, "\n")
	gotSplit := strings.Split(dataStr, "\r\n")

	if len(wantSplit) != len(gotSplit) {
		t.Fatalf("invalid request split count, got %v, want %v", len(wantSplit), len(gotSplit))
	}

	for i, part := range wantSplit {
		if part != gotSplit[i] {
			t.Fatalf("invalid split part, got %v, want %v", part, gotSplit[i])
		}
	}
}
