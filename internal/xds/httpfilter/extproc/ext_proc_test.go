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
	"context"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc/internal/buffer"
	"google.golang.org/grpc/internal/grpcsync"
	"google.golang.org/grpc/internal/resolver"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	v3procservicepb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
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
	reqStruct, ok := attr["envoy.filters.http.ext_proc"]
	if !ok {
		t.Fatalf("expected key 'envoy.filters.http.ext_proc' in attributes map")
	}

	// Helper to get string field value from flat structure
	getStringField := func(key string) string {
		val, ok := reqStruct.GetFields()[key]
		if !ok {
			t.Fatalf("expected key %q in request fields", key)
		}
		return val.GetStringValue()
	}

	// Verify path and url_path
	if got := getStringField("request.path"); got != ri.Method {
		t.Errorf("request.path = %q, want %q", got, ri.Method)
	}
	if got := getStringField("request.url_path"); got != ri.Method {
		t.Errorf("request.url_path = %q, want %q", got, ri.Method)
	}

	// Verify host (should match ri.Authority)
	if got := getStringField("request.host"); got != "dataplane-host" {
		t.Errorf("request.host = %q, want 'dataplane-host'", got)
	}

	// Verify scheme, method, query, protocol
	if got := getStringField("request.scheme"); got != "" {
		t.Errorf("request.scheme = %q, want empty string", got)
	}
	if got := getStringField("request.method"); got != "POST" {
		t.Errorf("request.method = %q, want 'POST'", got)
	}
	if got := getStringField("request.query"); got != "" {
		t.Errorf("request.query = %q, want empty string", got)
	}
	if got := getStringField("request.protocol"); got != "" {
		t.Errorf("request.protocol = %q, want empty string", got)
	}

	// Verify referer, useragent, id
	if got := getStringField("request.referer"); got != "http://example.com" {
		t.Errorf("request.referer = %q, want 'http://example.com'", got)
	}
	if got := getStringField("request.useragent"); got != "grpc-go-test" {
		t.Errorf("request.useragent = %q, want 'grpc-go-test'", got)
	}
	if got := getStringField("request.id"); got != "12345-abcde" {
		t.Errorf("request.id = %q, want '12345-abcde'", got)
	}

	// Verify headers map
	headersVal, ok := reqStruct.GetFields()["request.headers"]
	if !ok {
		t.Fatalf("expected key 'request.headers' in request fields")
	}
	headersStruct := headersVal.GetStructValue()
	if headersStruct == nil {
		t.Fatalf("expected 'request.headers' to be a struct value")
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

	reqStruct := attr["envoy.filters.http.ext_proc"]

	// Verify only path and host are populated
	if _, ok := reqStruct.GetFields()["request.path"]; !ok {
		t.Errorf("expected 'request.path' to be present")
	}
	if _, ok := reqStruct.GetFields()["request.host"]; !ok {
		t.Errorf("expected 'request.host' to be present")
	}

	// Verify other fields (like headers, referer, method) are absent
	if _, ok := reqStruct.GetFields()["request.method"]; ok {
		t.Errorf("expected 'request.method' to be filtered out / absent")
	}
	if _, ok := reqStruct.GetFields()["request.headers"]; ok {
		t.Errorf("expected 'request.headers' to be filtered out / absent")
	}
	if _, ok := reqStruct.GetFields()["request.referer"]; ok {
		t.Errorf("expected 'request.referer' to be filtered out / absent")
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

	reqStruct := attr["envoy.filters.http.ext_proc"]

	// Verify fields are constructed correctly with default empty values
	if got := reqStruct.GetFields()["request.referer"].GetStringValue(); got != "" {
		t.Errorf("expected empty referer, got %q", got)
	}
	if got := reqStruct.GetFields()["request.useragent"].GetStringValue(); got != "" {
		t.Errorf("expected empty useragent, got %q", got)
	}
	if got := reqStruct.GetFields()["request.id"].GetStringValue(); got != "" {
		t.Errorf("expected empty id, got %q", got)
	}

	headersStruct := reqStruct.GetFields()["request.headers"].GetStructValue()
	if len(headersStruct.GetFields()) != 0 {
		t.Errorf("expected empty headers struct, got fields: %v", headersStruct.GetFields())
	}
}

type mockDataplaneStream struct {
	resolver.ClientStream
	ctx      context.Context
	recvChan chan proto.Message
	sendChan chan proto.Message
	headerMD metadata.MD
	trailerMD metadata.MD
}

func (m *mockDataplaneStream) Context() context.Context {
	return m.ctx
}

func (m *mockDataplaneStream) RecvMsg(msg any) error {
	pMsg, ok := msg.(proto.Message)
	if !ok {
		return io.EOF
	}
	incoming, ok := <-m.recvChan
	if !ok {
		return io.EOF
	}
	proto.Merge(pMsg, incoming)
	return nil
}

func (m *mockDataplaneStream) SendMsg(msg any) error {
	pMsg := msg.(proto.Message)
	m.sendChan <- pMsg
	return nil
}

func (m *mockDataplaneStream) Header() (metadata.MD, error) {
	if m.headerMD != nil {
		return m.headerMD, nil
	}
	return metadata.MD{}, nil
}

func (m *mockDataplaneStream) Trailer() metadata.MD {
	if m.trailerMD != nil {
		return m.trailerMD
	}
	return metadata.MD{}
}

func (m *mockDataplaneStream) CloseSend() error {
	return nil
}

type mockExtStream struct {
	v3procservicepb.ExternalProcessor_ProcessClient
	ctx      context.Context
	sendChan chan *v3procservicepb.ProcessingRequest
	recvChan chan *v3procservicepb.ProcessingResponse
}

func (m *mockExtStream) Send(req *v3procservicepb.ProcessingRequest) error {
	select {
	case m.sendChan <- req:
		return nil
	case <-m.ctx.Done():
		return m.ctx.Err()
	}
}

func (m *mockExtStream) Recv() (*v3procservicepb.ProcessingResponse, error) {
	select {
	case resp, ok := <-m.recvChan:
		if !ok {
			return nil, io.EOF
		}
		return resp, nil
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	}
}

func (m *mockExtStream) CloseSend() error {
	return nil
}

func TestGracefulDrainZeroDataLossAndFIFO(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dpRecvChan := make(chan proto.Message, 10)
	dpSendChan := make(chan proto.Message, 10)
	extSendChan := make(chan *v3procservicepb.ProcessingRequest, 10)
	extRecvChan := make(chan *v3procservicepb.ProcessingResponse, 10)

	dpStream := &mockDataplaneStream{
		ctx:      ctx,
		recvChan: dpRecvChan,
		sendChan: dpSendChan,
	}

	extStream := &mockExtStream{
		ctx:      ctx,
		sendChan: extSendChan,
		recvChan: extRecvChan,
	}

	cs := &clientStream{
		config: baseConfig{
			processingModes: processingModes{
				responseBodyMode: modeSend,
			},
		},
		dataplaneStream:           dpStream,
		extStream:                 extStream,
		nonOKStreamEnd:            grpcsync.NewEvent(),
		requestBuffer:             buffer.NewUnbounded(),
		responseBodyBuffer:        buffer.NewUnbounded(),
		responseHeaderModified:    grpcsync.NewEvent(),
		responseTrailerModified:   grpcsync.NewEvent(),
		dataplaneStreamCreated:    make(chan struct{}),
		ctx:                       ctx,
		extSendCh:                 make(chan *v3procservicepb.ProcessingRequest),
		drainTriggeredCh:          make(chan struct{}),
		forwardLoopDoneCh:         make(chan struct{}),
		drained:                   grpcsync.NewEvent(),
		responseReceivingLoopDone: make(chan struct{}),
	}
	close(cs.dataplaneStreamCreated)

	go cs.recvFromProcServer(ctx, func() {}, nil)
	go cs.extReqSenderLoop()

	msg1 := &structpb.Struct{Fields: map[string]*structpb.Value{"id": structpb.NewStringValue("msg1")}}
	msg2 := &structpb.Struct{Fields: map[string]*structpb.Value{"id": structpb.NewStringValue("msg2")}}
	msg3 := &structpb.Struct{Fields: map[string]*structpb.Value{"id": structpb.NewStringValue("msg3")}}
	msg4 := &structpb.Struct{Fields: map[string]*structpb.Value{"id": structpb.NewStringValue("msg4")}}

	dpRecvChan <- msg1
	dpRecvChan <- msg2

	mRecv := &structpb.Struct{}

	recvResultChan := make(chan error, 1)
	go func() {
		recvResultChan <- cs.RecvMsg(mRecv)
	}()

	var req1 *v3procservicepb.ProcessingRequest
	select {
	case req1 = <-extSendChan:
	case <-ctx.Done():
		t.Fatal("timeout waiting for msg1 request to ExtProc")
	}

	body1 := req1.GetResponseBody().GetBody()
	pMsg1 := &structpb.Struct{}
	if err := proto.Unmarshal(body1, pMsg1); err != nil {
		t.Fatalf("failed to unmarshal body: %v", err)
	}
	if pMsg1.Fields["id"].GetStringValue() != "msg1" {
		t.Errorf("expected msg1, got %v", pMsg1)
	}

	extRecvChan <- &v3procservicepb.ProcessingResponse{
		Response: &v3procservicepb.ProcessingResponse_ResponseBody{
			ResponseBody: &v3procservicepb.BodyResponse{
				Response: &v3procservicepb.CommonResponse{
					Status: v3procservicepb.CommonResponse_CONTINUE,
					BodyMutation: &v3procservicepb.BodyMutation{
						Mutation: &v3procservicepb.BodyMutation_StreamedResponse{
							StreamedResponse: &v3procservicepb.StreamedBodyResponse{
								Body: body1,
							},
						},
					},
				},
			},
		},
	}

	select {
	case err := <-recvResultChan:
		if err != nil {
			t.Fatalf("RecvMsg(msg1) failed: %v", err)
		}
		if mRecv.Fields["id"].GetStringValue() != "msg1" {
			t.Errorf("expected msg1, got %v", mRecv)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for msg1 consumption")
	}

	go func() {
		recvResultChan <- cs.RecvMsg(mRecv)
	}()

	var req2 *v3procservicepb.ProcessingRequest
	select {
	case req2 = <-extSendChan:
	case <-ctx.Done():
		t.Fatal("timeout waiting for msg2 request to ExtProc")
	}

	body2 := req2.GetResponseBody().GetBody()
	pMsg2 := &structpb.Struct{}
	if err := proto.Unmarshal(body2, pMsg2); err != nil {
		t.Fatalf("failed to unmarshal body: %v", err)
	}
	if pMsg2.Fields["id"].GetStringValue() != "msg2" {
		t.Errorf("expected msg2, got %v", pMsg2)
	}

	extRecvChan <- &v3procservicepb.ProcessingResponse{
		RequestDrain: true,
		Response: &v3procservicepb.ProcessingResponse_ResponseBody{
			ResponseBody: &v3procservicepb.BodyResponse{
				Response: &v3procservicepb.CommonResponse{
					Status: v3procservicepb.CommonResponse_CONTINUE,
					BodyMutation: &v3procservicepb.BodyMutation{
						Mutation: &v3procservicepb.BodyMutation_StreamedResponse{
							StreamedResponse: &v3procservicepb.StreamedBodyResponse{
								Body: body2,
							},
						},
					},
				},
			},
		},
	}

	select {
	case err := <-recvResultChan:
		if err != nil {
			t.Fatalf("RecvMsg(msg2) failed: %v", err)
		}
		if mRecv.Fields["id"].GetStringValue() != "msg2" {
			t.Errorf("expected msg2, got %v", mRecv)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for msg2 consumption")
	}

	close(extRecvChan)

	dpRecvChan <- msg3
	dpRecvChan <- msg4

	go func() {
		recvResultChan <- cs.RecvMsg(mRecv)
	}()

	select {
	case err := <-recvResultChan:
		if err != nil {
			t.Fatalf("RecvMsg(msg3) failed: %v", err)
		}
		if mRecv.Fields["id"].GetStringValue() != "msg3" {
			t.Errorf("expected msg3 (aborted/bypassed), got %v", mRecv)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for msg3 consumption")
	}

	go func() {
		recvResultChan <- cs.RecvMsg(mRecv)
	}()

	select {
	case err := <-recvResultChan:
		if err != nil {
			t.Fatalf("RecvMsg(msg4) failed: %v", err)
		}
		if mRecv.Fields["id"].GetStringValue() != "msg4" {
			t.Errorf("expected msg4 (synchronous direct read), got %v", mRecv)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for msg4 consumption")
	}
}

func TestSendMsgDrainingCancellationUnblocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cs := &clientStream{
		config: baseConfig{
			processingModes: processingModes{
				requestBodyMode: modeSend,
			},
		},
		ctx:               ctx,
		drainTriggeredCh:  make(chan struct{}),
		forwardLoopDoneCh: make(chan struct{}),
		nonOKStreamEnd:    grpcsync.NewEvent(),
	}

	close(cs.drainTriggeredCh)

	errChan := make(chan error, 1)
	msg := &structpb.Struct{Fields: map[string]*structpb.Value{"id": structpb.NewStringValue("msg")}}
	go func() {
		errChan <- cs.SendMsg(msg)
	}()

	select {
	case err := <-errChan:
		t.Fatalf("SendMsg returned prematurely before cancellation: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()

	select {
	case err := <-errChan:
		if err != context.Canceled {
			t.Errorf("SendMsg returned error %v, want %v", err, context.Canceled)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for SendMsg to unblock")
	}
}

func TestCloseSendDrainingCancellationUnblocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cs := &clientStream{
		config: baseConfig{
			processingModes: processingModes{
				requestBodyMode: modeSend,
			},
		},
		ctx:               ctx,
		drainTriggeredCh:  make(chan struct{}),
		forwardLoopDoneCh: make(chan struct{}),
		nonOKStreamEnd:    grpcsync.NewEvent(),
	}

	close(cs.drainTriggeredCh)

	errChan := make(chan error, 1)
	go func() {
		errChan <- cs.CloseSend()
	}()

	select {
	case err := <-errChan:
		t.Fatalf("CloseSend returned prematurely: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()

	select {
	case err := <-errChan:
		if err != context.Canceled {
			t.Errorf("CloseSend returned %v, want %v", err, context.Canceled)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for CloseSend to unblock")
	}
}

func TestRecvMsgDrainingCancellationUnblocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cs := &clientStream{
		config: baseConfig{
			processingModes: processingModes{
				responseBodyMode: modeSend,
			},
		},
		ctx:                       ctx,
		drained:                   grpcsync.NewEvent(),
		responseBodyBuffer:        buffer.NewUnbounded(),
		responseReceivingLoopDone: make(chan struct{}),
		nonOKStreamEnd:            grpcsync.NewEvent(),
	}

	cs.drained.Fire()
	cs.recvfromdataplanestarted = true

	errChan := make(chan error, 1)
	mRecv := &structpb.Struct{}
	go func() {
		errChan <- cs.RecvMsg(mRecv)
	}()

	select {
	case err := <-errChan:
		t.Fatalf("RecvMsg returned prematurely: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()

	select {
	case err := <-errChan:
		if err != context.Canceled {
			t.Errorf("RecvMsg returned %v, want %v", err, context.Canceled)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for RecvMsg to unblock")
	}
}

func TestResponseHeaderModeSkipDoesNotHang(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	dpRecvChan := make(chan proto.Message, 10)
	dpSendChan := make(chan proto.Message, 10)

	dpStream := &mockDataplaneStream{
		ctx:      ctx,
		recvChan: dpRecvChan,
		sendChan: dpSendChan,
	}

	cs := &clientStream{
		config: baseConfig{
			processingModes: processingModes{
				responseHeaderMode: modeSkip,
			},
		},
		dataplaneStream:           dpStream,
		nonOKStreamEnd:            grpcsync.NewEvent(),
		responseHeaderModified:    grpcsync.NewEvent(),
		dataplaneStreamCreated:    make(chan struct{}),
		ctx:                       ctx,
		drainTriggeredCh:          make(chan struct{}),
	}
	close(cs.dataplaneStreamCreated)

	headerChan := make(chan error, 1)
	go func() {
		_, err := cs.Header()
		headerChan <- err
	}()

	select {
	case err := <-headerChan:
		if err != nil {
			t.Errorf("Header() returned error: %v, want nil", err)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for Header() to return (hung)")
	}
}

func TestTrailersOnlyResponse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	dpRecvChan := make(chan proto.Message, 10)
	dpSendChan := make(chan proto.Message, 10)
	extSendChan := make(chan *v3procservicepb.ProcessingRequest, 10)
	extRecvChan := make(chan *v3procservicepb.ProcessingResponse, 10)

	trailersOnlyMD := metadata.New(map[string]string{
		"grpc-status": "0",
	})

	dpStream := &mockDataplaneStream{
		ctx:      ctx,
		recvChan: dpRecvChan,
		sendChan: dpSendChan,
		headerMD: trailersOnlyMD,
	}

	extStream := &mockExtStream{
		ctx:      ctx,
		sendChan: extSendChan,
		recvChan: extRecvChan,
	}

	cs := &clientStream{
		config: baseConfig{
			processingModes: processingModes{
				responseHeaderMode:  modeSend,
				responseTrailerMode: modeSend,
			},
		},
		dataplaneStream:           dpStream,
		extStream:                 extStream,
		nonOKStreamEnd:            grpcsync.NewEvent(),
		responseHeaderModified:    grpcsync.NewEvent(),
		responseTrailerModified:   grpcsync.NewEvent(),
		dataplaneStreamCreated:    make(chan struct{}),
		ctx:                       ctx,
		extSendCh:                 make(chan *v3procservicepb.ProcessingRequest),
		drainTriggeredCh:          make(chan struct{}),
		forwardLoopDoneCh:         make(chan struct{}),
		drained:                   grpcsync.NewEvent(),
		responseReceivingLoopDone: make(chan struct{}),
	}
	close(cs.dataplaneStreamCreated)

	go cs.recvFromProcServer(ctx, func() {}, nil)
	go cs.extReqSenderLoop()

	headerChan := make(chan error, 1)
	go func() {
		_, err := cs.Header()
		headerChan <- err
	}()

	var req *v3procservicepb.ProcessingRequest
	select {
	case req = <-extSendChan:
	case <-ctx.Done():
		t.Fatal("timeout waiting for Msg request to ExtProc")
	}

	headersReq := req.GetResponseHeaders()
	if headersReq == nil {
		t.Fatalf("expected response headers request, got %v", req)
	}
	if !headersReq.GetEndOfStream() {
		t.Errorf("expected EndOfStream to be true, got false")
	}

	extRecvChan <- &v3procservicepb.ProcessingResponse{
		Response: &v3procservicepb.ProcessingResponse_ResponseHeaders{
			ResponseHeaders: &v3procservicepb.HeadersResponse{
				Response: &v3procservicepb.CommonResponse{
					Status: v3procservicepb.CommonResponse_CONTINUE,
				},
			},
		},
	}

	select {
	case err := <-headerChan:
		if err != nil {
			t.Fatalf("Header() returned error: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for Header() to return")
	}

	go func() {
		cs.processResponseTrailers()
	}()

	select {
	case req := <-extSendChan:
		t.Fatalf("unexpected request sent to ExtProc: %v (expected no trailers request for Trailers-Only)", req)
	case <-time.After(100 * time.Millisecond):
	}
}



