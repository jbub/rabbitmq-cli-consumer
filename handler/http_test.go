package handler

import (
	"encoding/json"
	"net/http/httputil"
	"reflect"
	"strings"
	"testing"
)

const msgStr = `{"request_params":{
"uri":"http:\/\/xxx.ccc.de:8080\/ccc\/bbb\/api\/df",
"headers":{
"x-user-uuid":"xxxdddsa-bdf7-4382-8cc4-351367c39e48",
"x-user-scope":"cdasf",
"x-php-ob-level":1,
"x-cc-bb-async":1,
"x-cc-bb-ddasd":1.45,
"Content-Type":"application\/json"},
"body":"{\"from\":\"System notification\",t;:&quot;Bola V\\\\u00e1m prideda.com\\\\\\\/user\\\\\\\/task-list\\\\\\\/108\\\\\\\/edit&quot;}}<\\\/html>\"}",
"method":"POST"}}`

func TestParseMessage(t *testing.T) {
	msg, err := parseMessage([]byte(msgStr))
	if err != nil {
		t.Fatal(err)
	}

	uri := "http://xxx.ccc.de:8080/ccc/bbb/api/df"
	if msg.RequestParams.URI != uri {
		t.Fatalf("invalid uri, got %v, want %v", msg.RequestParams.URI, uri)
	}

	body := `"{\"from\":\"System notification\",t;:&quot;Bola V\\\\u00e1m prideda.com\\\\\\\/user\\\\\\\/task-list\\\\\\\/108\\\\\\\/edit&quot;}}<\\\/html>\"}"`
	bodyStr := string(msg.RequestParams.Body)
	if bodyStr != body {
		t.Fatalf("invalid body, got '%v', want '%v'", bodyStr, body)
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
	msg, err := parseMessage([]byte(msgStr))
	if err != nil {
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

"{\"from\":\"System notification\",t;:&quot;Bola V\\\\u00e1m prideda.com\\\\\\\/user\\\\\\\/task-list\\\\\\\/108\\\\\\\/edit&quot;}}<\\\/html>\"}"`
	dataStr := string(reqData)

	wantSplit := strings.Split(want, "\n")
	gotSplit := strings.Split(dataStr, "\r\n")

	if len(wantSplit) != len(gotSplit) {
		t.Fatalf("invalid request split count, got '%v', want '%v'", len(wantSplit), len(gotSplit))
	}

	for i, part := range wantSplit {
		if part != gotSplit[i] {
			t.Fatalf("invalid split part, got '%v', want '%v'", part, gotSplit[i])
		}
	}
}
