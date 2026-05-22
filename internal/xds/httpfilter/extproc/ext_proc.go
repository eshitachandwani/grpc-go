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

// Package extproc implements the Envoy external processing filter.
package extproc

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/internal/buffer"
	"google.golang.org/grpc/internal/grpcsync"
	"google.golang.org/grpc/internal/resolver"
	"google.golang.org/grpc/internal/xds/httpfilter"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	v3procfilterpb "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	v3procservicepb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
)

// interceptor implements the resolver.ClientInterceptor interface.
type interceptor struct {
	config    *baseConfig
	extClient v3procservicepb.ExternalProcessorClient
	cc        *grpc.ClientConn
}

func (i *interceptor) NewStream(ctx context.Context, ri resolver.RPCInfo, done func(), newStream func(ctx context.Context, done func()) (resolver.ClientStream, error)) (resolver.ClientStream, error) {
	// Create a new context for the RPC to the external authorization server.
	// This context has a deadline that is the minimum of the incoming context's
	// deadline and the timeout specified in the config. It also contains the
	// incoming context's metadata, merged with the initial metadata specified
	// in the config.
	var extProcCtx context.Context
	if i.config.server.Timeout != 0 {
		var cancel context.CancelFunc
		extProcCtx, cancel = context.WithTimeout(ctx, i.config.server.Timeout)
		defer cancel()
	} else {
		extProcCtx = ctx
	}
	incomingMD, _ := metadata.FromIncomingContext(ctx)
	extProcCtx = metadata.NewOutgoingContext(extProcCtx, metadata.Join(i.config.server.InitialMetadata, incomingMD))

	// Create nre RPC to the Proc server
	extStream, err := i.extClient.Process(extProcCtx)
	if err != nil {
		return nil, err
	}
	nonOKStreamEnd := grpcsync.NewEvent()
	streamClosed := false
	var streamCloseErr error
	cs := &clientStream{
		config:                  i.config,
		extStream:               extStream,
		nonOKStreamEnd:          nonOKStreamEnd,
		streamClosed:            streamClosed,
		streamCloseErr:          streamCloseErr,
		requestBuffer:           buffer.NewUnbounded(),
		responseBuffer:          buffer.NewUnbounded(),
		responseHeaderModified:  grpcsync.NewEvent(),
		responseTrailerModified: grpcsync.NewEvent(),
	}
	var dataplaneStream resolver.ClientStream
	// if the processing mode for header is send, we need to wait for response before creating dataplane stream.
	if i.config.processingModes.requestHeaderMode == modeSend {
		attr, err := constructAttributes(ri, incomingMD, i.config.requestAttributes)
		if err != nil {
			return nil, err
		}
		cs.attributes = attr
		headerreq := v3procservicepb.ProcessingRequest{
			Request: &v3procservicepb.ProcessingRequest_RequestHeaders{
				RequestHeaders: &v3procservicepb.HttpHeaders{
					Headers: httpfilter.ConstructHeaderMap(incomingMD, i.config.allowedHeaders, i.config.disallowedHeaders),
				},
			},
			ObservabilityMode: i.config.observabilityMode,
			ProtocolConfig: &v3procservicepb.ProtocolConfiguration{
				RequestBodyMode:  convertBodyMode(i.config.processingModes.requestBodyMode),
				ResponseBodyMode: convertBodyMode(i.config.processingModes.responseBodyMode),
			},
			Attributes: attr,
		}
		// send header requet to proc server
		if err = extStream.Send(&headerreq); err != nil {
			return nil, err
		}
		cs.firstMessageSent = true

	} else { // if processing mode is not send, create dataplane stream.
		dataplaneStream, err = newStream(ctx, done)
		if err != nil {
			return nil, err
		}
		cs.dataplaneStream = dataplaneStream
	}

	// failStream is a helper function to fail the stream. If the error is
	// non-io.EOF, then nonOKStreamEnd is fired to make sure the dataplane stream
	// fails with non-ok status. If the failure mode is allow, then the stream is
	// closed and the error is not propagated to the dataplane stream and
	// dataplane stream is allowed to continue.
	failStream := func(err error) {
		streamCloseErr = err
		if err != io.EOF && !i.config.failureModeAllow {
			nonOKStreamEnd.Fire()
			return
		}
		streamClosed = true
	}

	cancelStream := func(err error) {
		streamCloseErr = err
		nonOKStreamEnd.Fire()
		streamClosed = true
	}
	// if the extn proc stream fails with OK error or failureMOdeallow is true
	// before creating the dataplane stream , we should create the dataplane
	// stream with the original headers
	failStreamin1stHeader := func(err error) {
		streamCloseErr = err
		if err != io.EOF && !i.config.failureModeAllow {
			nonOKStreamEnd.Fire()
			return
		}
		streamClosed = true
		dataplaneStream, err = newStream(ctx, done)
		if err != nil {
			return
		}
		cs.dataplaneStream = dataplaneStream
	}
	// start a goroutine to recv from the external proc service and send it to the dataplane stream
	go func() {
		var draining bool
		// if we did send the header, wait for the header and create the dataplane stream after mutating the header.
		if i.config.processingModes.requestHeaderMode == modeSend {
			resp, err := extStream.Recv()
			if err != nil {
				failStreamin1stHeader(err)
				return
			}
			if resp.GetRequestDrain() && !draining {
				draining = true
				extStream.CloseSend()
			}
			if resp.GetImmediateResponse() != nil {
				if i.config.disableImmediateResponse {
					failStreamin1stHeader(status.Error(codes.Unavailable, "ext_proc server sent immediate_response but disable_immediate_response is true"))
				} else {
					imm := resp.GetImmediateResponse()
					statusCode := codes.Internal
					if imm.GetGrpcStatus() != nil {
						statusCode = codes.Code(imm.GetGrpcStatus().GetStatus())
					}
					cancelStream(status.Error(statusCode, imm.GetDetails()))
				}
				return
			}
			if resp.GetRequestHeaders() == nil {
				err := fmt.Errorf("the first response is not header when header was sent to the proc server")
				failStreamin1stHeader(err)
				return
			}
			header := resp.GetRequestHeaders()
			// check if status in header response is continue, if not fail the stream.
			if header.GetResponse().GetStatus() != v3procservicepb.CommonResponse_CONTINUE {
				failStreamin1stHeader(fmt.Errorf("proc server returned non-continue status in request header response"))
				return
			}
			// mutate the headers
			if err = i.config.mutationRules.ApplyAdditions(header.GetResponse().GetHeaderMutation().GetSetHeaders(), incomingMD); err != nil {
				failStreamin1stHeader(err)
				return
			}
			if err = i.config.mutationRules.ApplyRemovals(header.GetResponse().GetHeaderMutation().GetRemoveHeaders(), incomingMD); err != nil {
				failStreamin1stHeader(err)
				return
			}
			dataplaneCtx := metadata.NewOutgoingContext(ctx, incomingMD)
			// create new dataplane stream after mutating headers.
			dataplaneStream, err = newStream(dataplaneCtx, done)
			if err != nil {
				failStream(err)
				return
			}
			cs.dataplaneStream = dataplaneStream
		}

		// if header mode is not send or if we receive the header and create the
		// dataplane stream , start receiving from the proc server.
		for {
			resp, err := extStream.Recv()
			if err != nil {
				failStream(err)
				return
			}
			if resp.GetRequestDrain() && !draining {
				draining = true
				extStream.CloseSend()
			}
			if resp.GetImmediateResponse() != nil {
				if i.config.disableImmediateResponse {
					failStream(status.Error(codes.Unavailable, "ext_proc server sent immediate_response but disable_immediate_response is true"))
				} else {
					imm := resp.GetImmediateResponse()
					statusCode := codes.Internal
					if imm.GetGrpcStatus() != nil {
						statusCode = codes.Code(imm.GetGrpcStatus().GetStatus())
					}
					cancelStream(status.Error(statusCode, imm.GetDetails()))
				}
				return
			}

			switch {
			case resp.GetRequestBody() != nil:
				bodyResp := resp.GetRequestBody()
				if bodyResp.GetResponse().GetStatus() != v3procservicepb.CommonResponse_CONTINUE {
					failStream(fmt.Errorf("proc server returned non-continue status in request body response"))
					return
				}
				streamedResp := bodyResp.GetResponse().GetBodyMutation().GetStreamedResponse()
				if streamedResp == nil {
					failStream(fmt.Errorf("proc server returned invalid body mutation in request body response"))
					return
				}
				if streamedResp.GetGrpcMessageCompressed() {
					failStream(fmt.Errorf("proc server returned grpc_message_compressed in request body response"))
					return
				}
				if streamedResp.GetEndOfStream() || streamedResp.GetEndOfStreamWithoutMessage() {
					cs.discardClientMessages.Store(true)
				}
				cs.requestBuffer.Put(streamedResp)

			case resp.GetResponseBody() != nil:
				bodyResp := resp.GetResponseBody()
				if bodyResp.GetResponse().GetStatus() != v3procservicepb.CommonResponse_CONTINUE {
					failStream(fmt.Errorf("proc server returned non-continue status in response body response"))
					return
				}
				streamedResp := bodyResp.GetResponse().GetBodyMutation().GetStreamedResponse()
				if streamedResp == nil {
					failStream(fmt.Errorf("proc server returned invalid body mutation in response body response"))
					return
				}
				if streamedResp.GetGrpcMessageCompressed() {
					failStream(fmt.Errorf("proc server returned grpc_message_compressed in response body response"))
					return
				}
				cs.responseBuffer.Put(streamedResp)

			case resp.GetResponseHeaders() != nil:
				header := resp.GetResponseHeaders()
				// check if status in header response is continue, if not fail the stream.
				if header.GetResponse().GetStatus() != v3procservicepb.CommonResponse_CONTINUE {
					failStream(fmt.Errorf("proc server returned non-continue status in response header response"))
					return
				}
				if err = i.config.mutationRules.ApplyAdditions(header.GetResponse().GetHeaderMutation().GetSetHeaders(), cs.responseHeader); err != nil {
					failStream(err)
					return
				}
				if err = i.config.mutationRules.ApplyRemovals(header.GetResponse().GetHeaderMutation().GetRemoveHeaders(), cs.responseHeader); err != nil {
					failStream(err)
					return
				}
				// signal that response header is modified and ready to be sent
				// to client, so that if there is any buffered response body, it
				// can be sent after the header.
				cs.responseHeaderModified.Fire()

			case resp.GetResponseTrailers() != nil:
				trailer := resp.GetResponseTrailers()
				if err = i.config.mutationRules.ApplyAdditions(trailer.GetHeaderMutation().GetSetHeaders(), cs.responseTrailers); err != nil {
					failStream(err)
					return
				}
				if err = i.config.mutationRules.ApplyRemovals(trailer.GetHeaderMutation().GetRemoveHeaders(), cs.responseTrailers); err != nil {
					failStream(err)
					return
				}
				// signal that response trailer is modified and ready to be sent
				// to client, so that if there is any buffered response body, it
				// can be sent after the trailer.
				cs.responseTrailerModified.Fire()
			}
		}
	}()

	return cs, nil
}

func convertBodyMode(mode processingMode) v3procfilterpb.ProcessingMode_BodySendMode {
	switch mode {
	case modeSkip:
		return v3procfilterpb.ProcessingMode_NONE
	case modeSend:
		return v3procfilterpb.ProcessingMode_GRPC
	default:
		return v3procfilterpb.ProcessingMode_NONE
	}
}

func getHeader(md metadata.MD, key string) string {
	vs := md.Get(key)
	return strings.Join(vs, ",")
}

func constructAttributes(ri resolver.RPCInfo, md metadata.MD, requestedAttributes []string) (map[string]*structpb.Struct, error) {
	if len(requestedAttributes) == 0 {
		return nil, nil
	}

	reqFields := make(map[string]any)
	for _, attr := range requestedAttributes {
		switch attr {
		case "request.path", "request.url_path":
			reqFields[attr] = ri.Method
		case "request.host":
			reqFields[attr] = ri.Authority
		case "request.method":
			reqFields[attr] = "POST"
		case "request.headers":
			headers := make(map[string]any)
			for k, vs := range md {
				headers[k] = strings.Join(vs, ",")
			}
			reqFields[attr] = headers
		case "request.referer":
			if r := getHeader(md, "referer"); r != "" {
				reqFields[attr] = r
			}
		case "request.useragent":
			if r := getHeader(md, "user-agent"); r != "" {
				reqFields[attr] = r
			}
		case "request.id":
			if r := getHeader(md, "x-request-id"); r != "" {
				reqFields[attr] = r
			}
		case "request.query":
			reqFields[attr] = ""
		default:
			// For attributes that gRPC does not support, don't include them in the
			// map.
		}
	}

	reqStruct, err := structpb.NewStruct(reqFields)
	if err != nil {
		return nil, err
	}

	return map[string]*structpb.Struct{
		"envoy.filters.http.ext_proc": reqStruct,
	}, nil
}

func (i *interceptor) Close() {
	if i.cc != nil {
		i.cc.Close()
	}
}

type clientStream struct {
	resolver.ClientStream
	config                  *baseConfig
	dataplaneStream         resolver.ClientStream
	extStream               v3procservicepb.ExternalProcessor_ProcessClient
	nonOKStreamEnd          *grpcsync.Event
	streamClosed            bool
	streamCloseErr          error
	firstMessageSent        bool
	discardClientMessages   atomic.Bool
	requestBuffer           *buffer.Unbounded
	responseBuffer          *buffer.Unbounded
	responseHeader          metadata.MD
	responseHeaderModified  *grpcsync.Event
	responseTrailers        metadata.MD
	responseTrailerModified *grpcsync.Event
	sendtodataplanestarted  bool
	attributes              map[string]*structpb.Struct
}

// Intercept the messages being sent by the client. The header is already taken care of in the newstream. This takes care of the request body.
func (cs *clientStream) SendMsg(m any) error {
	if cs.nonOKStreamEnd.HasFired() {
		return cs.streamCloseErr
	}
	if cs.streamClosed || cs.config.processingModes.requestBodyMode == modeSkip {
		return cs.dataplaneStream.SendMsg(m)
	}
	// This is a best-effort check to discard client messages once the ExtProc server
	// signals end_of_stream. Due to asynchronous network propagation, a message might
	// still cross the wire before this flag is stored.
	if cs.discardClientMessages.Load() {
		return nil
	}

	msg, ok := m.(proto.Message)
	if !ok {
		return fmt.Errorf("message does not implement proto.Message")
	}
	bodyBytes, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	req := &v3procservicepb.ProcessingRequest{
		Request: &v3procservicepb.ProcessingRequest_RequestBody{
			RequestBody: &v3procservicepb.HttpBody{
				Body: bodyBytes,
			},
		},
		Attributes:        cs.attributes,
		ObservabilityMode: cs.config.observabilityMode,
	}

	if !cs.firstMessageSent {
		req.ProtocolConfig = &v3procservicepb.ProtocolConfiguration{
			RequestBodyMode:  convertBodyMode(cs.config.processingModes.requestBodyMode),
			ResponseBodyMode: convertBodyMode(cs.config.processingModes.responseBodyMode),
		}
		cs.firstMessageSent = true
	}

	// 
	if !cs.sendtodataplanestarted {
		cs.sendtodataplanestarted = true
		go cs.requestForwardingLoop(msg)
	}

	if err := cs.extStream.Send(req); err != nil {
		if cs.config.failureModeAllow {
			cs.streamClosed = true
			return cs.dataplaneStream.SendMsg(m)
		}
		cs.streamCloseErr = err
		cs.nonOKStreamEnd.Fire()
		return err
	}

	return nil
}

// loop to pull data out of request buffer and send to dataplane server
func (cs *clientStream) requestForwardingLoop(reqPrototype proto.Message) {
	for item := range cs.requestBuffer.Get() {
		cs.requestBuffer.Load()
		streamedResp, ok := item.(*v3procservicepb.StreamedBodyResponse)
		if !ok {
			return
		}

		newMsg := reqPrototype.ProtoReflect().New().Interface()
		if err := proto.Unmarshal(streamedResp.GetBody(), newMsg); err != nil {
			cs.streamCloseErr = err
			cs.nonOKStreamEnd.Fire()
			return
		}

		if err := cs.dataplaneStream.SendMsg(newMsg); err != nil {
			cs.streamCloseErr = err
			cs.nonOKStreamEnd.Fire()
			return
		}

		if streamedResp.GetEndOfStream() || streamedResp.GetEndOfStreamWithoutMessage() {
			cs.dataplaneStream.CloseSend()
			return
		}
	}
}
