package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/jbub/rabbitmq-cli-consumer/domain"
)

type HTTPJob struct {
	client *http.Client
	req    *http.Request
}

func (hj *HTTPJob) Do(worker int, infLogger *log.Logger, errLogger *log.Logger) {
	resp, err := hj.client.Do(hj.req)
	if err != nil {
		errLogger.Printf("could not perform http request: %v", err)
		return
	}
	defer resp.Body.Close()

	infLogger.Printf("request sent, worker=%v, method=%v, url=%v, status=%v", worker, hj.req.Method, hj.req.URL.String(), resp.Status)
}

func NewHTTPJobBuilder(timeout time.Duration, infLogger *log.Logger) *HTTPJobBuilder {
	return &HTTPJobBuilder{
		client:    newHTTPClient(timeout),
		infLogger: infLogger,
	}
}

func newHTTPClient(timeout time.Duration) *http.Client {
	dialer := &net.Dialer{
		Timeout: timeout,
	}
	trans := &http.Transport{
		Dial:                dialer.Dial,
		TLSHandshakeTimeout: timeout,
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: trans,
	}
}

type HTTPJobBuilder struct {
	client    *http.Client
	infLogger *log.Logger
}

func (h *HTTPJobBuilder) BuildJob(data []byte) (domain.Job, error) {
	msg := httpMessagePool.Get().(*httpMessage)
	defer httpMessagePool.Put(msg)

	msg.reset()
	if err := msg.parse(data); err != nil {
		return nil, fmt.Errorf("could not parse message: %v", err)
	}

	req, err := buildRequest(msg)
	if err != nil {
		return nil, fmt.Errorf("could not build http request: %v", err)
	}

	return &HTTPJob{
		client: h.client,
		req:    req,
	}, nil
}

var httpMessagePool = sync.Pool{
	New: func() interface{} {
		return &httpMessage{}
	},
}

type httpMessage struct {
	RequestParams struct {
		URI     string                 `json:"uri"`
		Headers map[string]interface{} `json:"headers"`
		Body    json.RawMessage        `json:"body"`
		Method  string                 `json:"method"`
	} `json:"request_params"`
}

func (msg *httpMessage) parse(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	if err := dec.Decode(msg); err != nil {
		return err
	}

	if msg.RequestParams.Method == "" {
		return errors.New("empty http method")
	}

	if msg.RequestParams.URI == "" {
		return errors.New("empty http uri")
	}

	return nil
}

func (msg *httpMessage) reset() {
	msg.RequestParams.URI = ""
	msg.RequestParams.Headers = nil
	msg.RequestParams.Body = nil
	msg.RequestParams.Method = ""
}

func buildRequest(msg *httpMessage) (*http.Request, error) {
	var body io.Reader
	if msg.RequestParams.Body != nil {
		body = bytes.NewReader(msg.RequestParams.Body)
	}

	req, err := http.NewRequest(msg.RequestParams.Method, msg.RequestParams.URI, body)
	if err != nil {
		return nil, err
	}

	headers, err := buildHeaders(msg)
	if err != nil {
		return nil, fmt.Errorf("could not build http headers: %v", err)
	}
	if headers != nil {
		req.Header = headers
	}
	return req, nil
}

func buildHeaders(msg *httpMessage) (http.Header, error) {
	if msg.RequestParams.Headers != nil {
		headers := make(http.Header)
		for k, v := range msg.RequestParams.Headers {
			switch val := v.(type) {
			case string:
				headers.Set(k, val)
			case json.Number:
				headers.Set(k, val.String())
			default:
				return nil, fmt.Errorf("invalid header, key=%v, value=%v", k, v)
			}
		}
		return headers, nil
	}
	return nil, nil
}
