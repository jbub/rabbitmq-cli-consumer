package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

func NewHTTPMessagerHandler(timeout time.Duration, infLogger *log.Logger) *HTTPMessagerHandler {
	client := &http.Client{
		Timeout: timeout,
	}
	return &HTTPMessagerHandler{
		client:    client,
		infLogger: infLogger,
	}
}

type HTTPMessagerHandler struct {
	client    *http.Client
	infLogger *log.Logger
}

func (h *HTTPMessagerHandler) HandleMessage(data []byte) error {
	msg, err := parseMessage(data)
	if err != nil {
		return fmt.Errorf("could not parse message: %v", err)
	}

	req, err := buildRequest(msg)
	if err != nil {
		return fmt.Errorf("could not build http request: %v", err)
	}

	if _, err := h.client.Do(req); err != nil {
		return fmt.Errorf("could not perform http request: %v", err)
	}

	h.infLogger.Printf("request sent, method=%v, url=%v, headers=%v", req.Method, req.URL.String(), req.Header)
	return nil
}

type httpMessage struct {
	RequestParams struct {
		URI     string                 `json:"uri"`
		Headers map[string]interface{} `json:"headers"`
		Body    json.RawMessage        `json:"body"`
		Method  string                 `json:"method"`
	} `json:"request_params"`
}

func parseMessage(data []byte) (*httpMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	msg := new(httpMessage)
	if err := dec.Decode(msg); err != nil {
		return nil, err
	}

	if msg.RequestParams.Method == "" {
		return nil, errors.New("empty http method")
	}

	if msg.RequestParams.URI == "" {
		return nil, errors.New("empty http uri")
	}
	return msg, nil
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
