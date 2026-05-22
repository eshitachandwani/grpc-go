/*
 *
 * Copyright 2026 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package extproc

import (
	"testing"

	"google.golang.org/grpc/internal/resolver"
	"google.golang.org/grpc/metadata"
)

var allSupportedAttributes = []string{
	"request.path",
	"request.url_path",
	"request.host",
	"request.scheme",
	"request.method",
	"request.headers",
	"request.referer",
	"request.useragent",
	"request.time",
	"request.id",
	"request.protocol",
	"request.query",
}

func TestConstructAttributes(t *testing.T) {

	ri := resolver.RPCInfo{
		Method:    "/test.Service/TestMethod",
		Authority: "dataplane-host",
	}

	md := metadata.New(map[string]string{
		"referer":      "http://example.com",
		"user-agent":   "grpc-go-test",
		"x-request-id": "12345-abcde",
		"custom-hdr-1": "val1",
		"custom-hdr-2": "val2",
	})

	// Call constructAttributes with all supported attributes
	attr, err := constructAttributes(ri, md, allSupportedAttributes)
	if err != nil {
		t.Fatalf("constructAttributes failed: %v", err)
	}

	// Verify the root key is envoy.filters.http.ext_proc
	rootStruct, ok := attr["envoy.filters.http.ext_proc"]
	if !ok {
		t.Fatalf("expected key 'envoy.filters.http.ext_proc' in attributes map")
	}

	// Verify the nested "request" object
	reqVal, ok := rootStruct.GetFields()["request"]
	if !ok {
		t.Fatalf("expected key 'request' in root struct fields")
	}

	reqStruct := reqVal.GetStructValue()
	if reqStruct == nil {
		t.Fatalf("expected 'request' to be a struct value")
	}

	// Helper to get string field value
	getStringField := func(key string) string {
		val, ok := reqStruct.GetFields()[key]
		if !ok {
			t.Fatalf("expected key %q in request fields", key)
		}
		return val.GetStringValue()
	}

	// Verify path and url_path
	if got := getStringField("path"); got != ri.Method {
		t.Errorf("request.path = %q, want %q", got, ri.Method)
	}
	if got := getStringField("url_path"); got != ri.Method {
		t.Errorf("request.url_path = %q, want %q", got, ri.Method)
	}

	// Verify host (should match ri.Authority)
	if got := getStringField("host"); got != "dataplane-host" {
		t.Errorf("request.host = %q, want 'dataplane-host'", got)
	}

	// Verify scheme, method, query, protocol
	if got := getStringField("scheme"); got != "" {
		t.Errorf("request.scheme = %q, want empty string", got)
	}
	if got := getStringField("method"); got != "POST" {
		t.Errorf("request.method = %q, want 'POST'", got)
	}
	if got := getStringField("query"); got != "" {
		t.Errorf("request.query = %q, want empty string", got)
	}
	if got := getStringField("protocol"); got != "" {
		t.Errorf("request.protocol = %q, want empty string", got)
	}

	// Verify referer, useragent, id
	if got := getStringField("referer"); got != "http://example.com" {
		t.Errorf("request.referer = %q, want 'http://example.com'", got)
	}
	if got := getStringField("useragent"); got != "grpc-go-test" {
		t.Errorf("request.useragent = %q, want 'grpc-go-test'", got)
	}
	if got := getStringField("id"); got != "12345-abcde" {
		t.Errorf("request.id = %q, want '12345-abcde'", got)
	}

	// Verify headers map
	headersVal, ok := reqStruct.GetFields()["headers"]
	if !ok {
		t.Fatalf("expected key 'headers' in request fields")
	}
	headersStruct := headersVal.GetStructValue()
	if headersStruct == nil {
		t.Fatalf("expected 'headers' to be a struct value")
	}

	wantHeaders := map[string]string{
		"referer":      "http://example.com",
		"user-agent":   "grpc-go-test",
		"x-request-id": "12345-abcde",
		"custom-hdr-1": "val1",
		"custom-hdr-2": "val2",
	}

	for k, wantVal := range wantHeaders {
		val, ok := headersStruct.GetFields()[k]
		if !ok {
			t.Errorf("expected header key %q in headers struct", k)
			continue
		}
		if gotVal := val.GetStringValue(); gotVal != wantVal {
			t.Errorf("headers[%q] = %q, want %q", k, gotVal, wantVal)
		}
	}
}

func TestConstructAttributesFiltering(t *testing.T) {

	ri := resolver.RPCInfo{
		Method:    "/test.Service/TestMethod",
		Authority: "dataplane-host",
	}

	md := metadata.New(map[string]string{
		"referer": "http://example.com",
	})

	// Only request request.path and request.host
	requested := []string{"request.path", "request.host"}

	attr, err := constructAttributes(ri, md, requested)
	if err != nil {
		t.Fatalf("constructAttributes failed: %v", err)
	}

	rootStruct := attr["envoy.filters.http.ext_proc"]
	reqStruct := rootStruct.GetFields()["request"].GetStructValue()

	// Verify only path and host are populated
	if _, ok := reqStruct.GetFields()["path"]; !ok {
		t.Errorf("expected 'path' to be present")
	}
	if _, ok := reqStruct.GetFields()["host"]; !ok {
		t.Errorf("expected 'host' to be present")
	}

	// Verify other fields (like headers, referer, method) are absent
	if _, ok := reqStruct.GetFields()["method"]; ok {
		t.Errorf("expected 'method' to be filtered out / absent")
	}
	if _, ok := reqStruct.GetFields()["headers"]; ok {
		t.Errorf("expected 'headers' to be filtered out / absent")
	}
	if _, ok := reqStruct.GetFields()["referer"]; ok {
		t.Errorf("expected 'referer' to be filtered out / absent")
	}
}

func TestConstructAttributesWithEmptyMetadata(t *testing.T) {
	ri := resolver.RPCInfo{Method: "/test/Method"}
	md := metadata.New(nil)

	// Request some headers and referer
	requested := []string{"request.referer", "request.useragent", "request.id", "request.headers"}

	attr, err := constructAttributes(ri, md, requested)
	if err != nil {
		t.Fatalf("constructAttributes failed: %v", err)
	}

	rootStruct := attr["envoy.filters.http.ext_proc"]
	reqStruct := rootStruct.GetFields()["request"].GetStructValue()

	// Verify fields are constructed correctly with default empty values
	if got := reqStruct.GetFields()["referer"].GetStringValue(); got != "" {
		t.Errorf("expected empty referer, got %q", got)
	}
	if got := reqStruct.GetFields()["useragent"].GetStringValue(); got != "" {
		t.Errorf("expected empty useragent, got %q", got)
	}
	if got := reqStruct.GetFields()["id"].GetStringValue(); got != "" {
		t.Errorf("expected empty id, got %q", got)
	}

	headersStruct := reqStruct.GetFields()["headers"].GetStructValue()
	if len(headersStruct.GetFields()) != 0 {
		t.Errorf("expected empty headers struct, got fields: %v", headersStruct.GetFields())
	}
}
