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

// Package extproc implements the Envoy external processing HTTP filter.
package extproc

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/internal/buffer"
	"google.golang.org/grpc/internal/envconfig"
	"google.golang.org/grpc/internal/grpcsync"
	"google.golang.org/grpc/internal/optional"
	"google.golang.org/grpc/internal/resolver"
	"google.golang.org/grpc/internal/xds/httpfilter"
	"google.golang.org/grpc/internal/xds/matcher"
	"google.golang.org/grpc/internal/xds/xdsclient/xdsresource"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	v3corepb "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	v3procfilterpb "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	v3procservicepb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
)

func init() {
	if envconfig.XDSClientExtProcEnabled {
		httpfilter.Register(builder{})
	}
}

// RegisterForTesting registers the RBAC HTTP Filter for testing purposes, regardless
// of the RBAC environment variable. This is needed because there is no way to set the RBAC
// environment variable to true in a test before init() in this package is run.
func RegisterForTesting() {
	httpfilter.Register(builder{})
}

// UnregisterForTesting unregisters the RBAC HTTP Filter for testing purposes. This is needed because
// there is no way to unregister the HTTP Filter after registering it solely for testing purposes using
// rbac.RegisterForTesting()
func UnregisterForTesting() {
	for _, typeURL := range builder.TypeURLs(builder{}) {
		httpfilter.UnregisterForTesting(typeURL)
	}
}

var (
	parseGRPCServiceConfig = func(gs *v3corepb.GrpcService) (xdsresource.GRPCServiceConfig, error) {
		if gs == nil {
			return xdsresource.GRPCServiceConfig{}, fmt.Errorf("nil GrpcService")
		}
		gg := gs.GetGoogleGrpc()
		if gg == nil {
			return xdsresource.GRPCServiceConfig{}, fmt.Errorf("only GoogleGrpc is supported in GrpcService")
		}
		target := gg.GetTargetUri()
		if target == "" {
			return xdsresource.GRPCServiceConfig{}, fmt.Errorf("empty target_uri in GoogleGrpc")
		}
		return xdsresource.GRPCServiceConfig{
			TargetURI: target,
		}, nil
	}
	createExtProcChannel = func(cfg xdsresource.GRPCServiceConfig) (grpc.ClientConnInterface, func() error, error) {
		cc, err := grpc.NewClient(cfg.TargetURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil, err
		}
		return cc, cc.Close, nil
	}

	logger = grpclog.Component("extproc")
)

const defaultDeferredCloseTimeout = 5 * time.Second

type builder struct{}

func (builder) TypeURLs() []string {
	return []string{
		"type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor",
		"type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExtProcPerRoute",
	}
}

// validateBodyProcessingMode ensures that the body processing mode is either
// NONE or GRPC.
func validateBodyProcessingMode(mode *v3procfilterpb.ProcessingMode) error {
	if m := mode.GetRequestBodyMode(); m != v3procfilterpb.ProcessingMode_NONE && m != v3procfilterpb.ProcessingMode_GRPC {
		return fmt.Errorf("extproc: invalid request body mode %v: want %q or %q", m, "NONE", "GRPC")
	}
	if m := mode.GetResponseBodyMode(); m != v3procfilterpb.ProcessingMode_NONE && m != v3procfilterpb.ProcessingMode_GRPC {
		return fmt.Errorf("extproc: invalid response body mode %v: want %q or %q", m, "NONE", "GRPC")
	}
	return nil
}

func (builder) ParseFilterConfig(cfg proto.Message) (httpfilter.FilterConfig, error) {
	m, ok := cfg.(*anypb.Any)
	if !ok {
		return nil, fmt.Errorf("extproc: error parsing config %v: unknown type %T, want *anypb.Any", cfg, cfg)
	}
	msg := new(v3procfilterpb.ExternalProcessor)
	if err := m.UnmarshalTo(msg); err != nil {
		return nil, fmt.Errorf("extproc: failed to unmarshal config %v: %v", cfg, err)
	}
	if msg.GetProcessingMode() == nil {
		return nil, fmt.Errorf("extproc: missing processing_mode in config %v", cfg)
	}
	if err := validateBodyProcessingMode(msg.GetProcessingMode()); err != nil {
		return nil, err
	}

	if msg.GetGrpcService() == nil {
		return nil, fmt.Errorf("extproc: empty grpc_service provided in config %v", cfg)
	}
	server, err := parseGRPCServiceConfig(msg.GetGrpcService())
	if err != nil {
		return nil, fmt.Errorf("extproc: failed to parse grpc_service %v", err)
	}

	mutationRules, err := httpfilter.HeaderMutationRulesFromProto(msg.GetMutationRules())
	if err != nil {
		return nil, err
	}

	var allowedHeaders, disallowedHeaders []matcher.StringMatcher
	if allowed := msg.GetForwardRules().GetAllowedHeaders(); allowed != nil {
		allowedHeaders, err = httpfilter.ConvertStringMatchers(allowed.GetPatterns())
		if err != nil {
			return nil, err
		}
	}

	if disallowed := msg.GetForwardRules().GetDisallowedHeaders(); disallowed != nil {
		disallowedHeaders, err = httpfilter.ConvertStringMatchers(disallowed.GetPatterns())
		if err != nil {
			return nil, err
		}
	}

	deferredCloseTimeout := defaultDeferredCloseTimeout
	if msg.GetDeferredCloseTimeout() != nil {
		deferredCloseTimeout = msg.GetDeferredCloseTimeout().AsDuration()
	}

	return baseConfig{
		processingModes:          processingModesFromProto(msg.GetProcessingMode()),
		requestAttributes:        msg.GetRequestAttributes(),
		responseAttributes:       msg.GetResponseAttributes(),
		disableImmediateResponse: msg.GetDisableImmediateResponse(),
		observabilityMode:        msg.GetObservabilityMode(),
		failureModeAllow:         msg.GetFailureModeAllow(),
		server:                   server,
		mutationRules:            mutationRules,
		allowedHeaders:           allowedHeaders,
		disallowedHeaders:        disallowedHeaders,
		deferredCloseTimeout:     deferredCloseTimeout,
	}, nil
}

func (builder) ParseFilterConfigOverride(ov proto.Message) (httpfilter.FilterConfig, error) {
	m, ok := ov.(*anypb.Any)
	if !ok {
		return nil, fmt.Errorf("extproc: error parsing override %v: unknown type %T, want *anypb.Any", ov, ov)
	}
	msg := new(v3procfilterpb.ExtProcPerRoute)
	if err := m.UnmarshalTo(msg); err != nil {
		return nil, fmt.Errorf("extproc: failed to unmarshal override %v: %v", ov, err)
	}
	override := msg.GetOverrides()

	var processingModesOpt optional.Optional[processingModes]
	if pm := override.GetProcessingMode(); pm != nil {
		if err := validateBodyProcessingMode(pm); err != nil {
			return nil, err
		}
		processingModesOpt = optional.NewValue(processingModesFromProto(pm))
	}

	var serverOpt optional.Optional[xdsresource.GRPCServiceConfig]
	if override.GetGrpcService() != nil {
		server, err := parseGRPCServiceConfig(override.GetGrpcService())
		if err != nil {
			return nil, fmt.Errorf("extproc: failed to parse grpc_service: %v", err)
		}
		serverOpt = optional.NewValue(server)
	}

	var failureModeAllowOpt optional.Optional[bool]
	if override.GetFailureModeAllow() != nil {
		failureModeAllowOpt = optional.NewValue(override.GetFailureModeAllow().GetValue())
	}

	return overrideConfig{
		server:             serverOpt,
		processingModes:    processingModesOpt,
		failureModeAllow:   failureModeAllowOpt,
		requestAttributes:  override.GetRequestAttributes(),
		responseAttributes: override.GetResponseAttributes(),
	}, nil
}

func (builder) IsTerminal() bool {
	return false
}

func (builder) BuildClientFilter() httpfilter.ClientFilter {
	return clientFilter{}
}

var _ httpfilter.ClientFilterBuilder = builder{}

type clientFilter struct{}

func (clientFilter) Close() {}

func (clientFilter) BuildClientInterceptor(base, override httpfilter.FilterConfig) (resolver.ClientInterceptor, error) {
	b, ok := base.(baseConfig)
	if !ok {
		return nil, fmt.Errorf("extproc: incorrect config type provided (%T): %v", base, base)
	}

	var ov overrideConfig
	if override != nil {
		var ok bool
		ov, ok = override.(overrideConfig)
		if !ok {
			return nil, fmt.Errorf("extproc: incorrect override config type provided (%T): %v", override, override)
		}
	}

	config := newInterceptorConfig(b, ov)

	// Create a channel to the external processing server.
	cc, cancel, err := createExtProcChannel(config.server)
	if err != nil {
		return nil, fmt.Errorf("extproc: failed to create client: %v", err)
	}
	extClient := v3procservicepb.NewExternalProcessorClient(cc)

	return &clientInterceptor{
		config:    config,
		extClient: extClient,
		cancel:    cancel,
	}, nil
}

type clientInterceptor struct {
	resolver.ClientInterceptor
	config    baseConfig
	extClient v3procservicepb.ExternalProcessorClient
	cancel    func() error
}

func (i *clientInterceptor) Close() {
	if i.cancel != nil {
		i.cancel()
	}
}

func (i *clientInterceptor) NewStream(ctx context.Context, ri resolver.RPCInfo, done func(), newStream func(ctx context.Context, done func()) (resolver.ClientStream, error)) (resolver.ClientStream, error) {
	// Create a new context for the RPC to the external processing server. This
	// context has a deadline of the timeout specified in the config, if present.
	// It also contains the incoming context's metadata, merged with the initial
	// metadata specified in the config.
	var extProcCtx context.Context
	if i.config.server.Timeout != 0 {
		extProcCtx, _ = context.WithTimeout(ctx, i.config.server.Timeout)
	} else {
		extProcCtx = ctx
	}

	outgoingMD, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		outgoingMD = metadata.MD{}
	}
	extProcCtx = metadata.NewOutgoingContext(extProcCtx, metadata.Join(i.config.server.InitialMetadata, outgoingMD))

	// Create new RPC to the Proc server
	extStream, err := i.extClient.Process(extProcCtx)
	if err != nil {
		if !i.config.failureModeAllow {
			done()
			return nil, status.Errorf(codes.Internal, "extproc: external processor failed to start: %v", err)
		}
		logger.Warning("extproc: external processor failed to start: %v", err)
		// If failure mode allow is true, bypass extproc and create the dataplane stream directly.
		dataplaneStream, err := newStream(ctx, done)
		if err != nil {
			return nil, err
		}
		cs := &clientStream{
			config:                 i.config,
			nonOKStreamEnd:         grpcsync.NewEvent(),
			ctx:                    ctx,
			streamClosed:           true, // Bypassed
			dataplaneStreamCreated: make(chan struct{}),
		}
		cs.dataplaneStream = dataplaneStream
		close(cs.dataplaneStreamCreated)
		return cs, nil
	}

	cs := &clientStream{
		config:                    i.config,
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

	// If the processing mode for header is send, we need to wait for response
	// before creating dataplane stream.
	if i.config.processingModes.requestHeaderMode == modeSend {
		attr, err := constructAttributes(ri, outgoingMD, i.config.requestAttributes)
		if err != nil {
			return nil, err
		}
		cs.requestAttributes = attr
		headerreq := v3procservicepb.ProcessingRequest{
			Request: &v3procservicepb.ProcessingRequest_RequestHeaders{
				RequestHeaders: &v3procservicepb.HttpHeaders{
					Headers: httpfilter.ConstructHeaderMap(outgoingMD, i.config.allowedHeaders, i.config.disallowedHeaders),
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

	} else { // if processing mode for header is not send, create dataplane stream.
		dataplaneStream, err := newStream(ctx, done)
		if err != nil {
			return nil, err
		}
		cs.dataplaneStream = dataplaneStream
		close(cs.dataplaneStreamCreated)
	}

	// start a goroutine to recv from the external proc service and send it to the dataplane stream
	go cs.recvFromProcServer(ctx, done, newStream)

	// start a goroutine to send to the proc server. We do it in a single
	// goroutine to ensure we do not have concurrent sends to the proc server.
	go cs.extReqSenderLoop()
	return cs, nil
}

func (cs *clientStream) failStream(err error) {
	if err != io.EOF && !cs.config.failureModeAllow {
		cs.streamCloseErr = status.Errorf(codes.Internal, "extproc: external processor RPC failed: %v", err)
		cs.nonOKStreamEnd.Fire()
		select {
		case <-cs.dataplaneStreamCreated:
		default:
			close(cs.dataplaneStreamCreated)
		}
		return
	}
	logger.Warning("extproc: external processor failed: %v", err)
	cs.streamClosed = true
	cs.triggerDrain()
}

func (cs *clientStream) cancelStream(err error) {
	cs.streamCloseErr = err
	cs.nonOKStreamEnd.Fire()
	select {
	case <-cs.dataplaneStreamCreated:
	default:
		close(cs.dataplaneStreamCreated)
	}
	cs.streamClosed = true
}

func (cs *clientStream) failStreamin1stHeader(err error, ctx context.Context, done func(), newStream func(context.Context, func()) (resolver.ClientStream, error)) {
	if err != io.EOF && !cs.config.failureModeAllow {
		cs.streamCloseErr = status.Errorf(codes.Internal, "extproc: external processor failed: %v", err)
		cs.nonOKStreamEnd.Fire()
		select {
		case <-cs.dataplaneStreamCreated:
		default:
			close(cs.dataplaneStreamCreated)
		}
		return
	}
	logger.Warning("extproc: external processor failed: %v", err)
	cs.streamClosed = true
	cs.triggerDrain()
	dataplaneStream, err := newStream(ctx, done)
	if err != nil {
		select {
		case <-cs.dataplaneStreamCreated:
		default:
			close(cs.dataplaneStreamCreated)
		}
		return
	}
	cs.dataplaneStream = dataplaneStream
	close(cs.dataplaneStreamCreated)
}

func (cs *clientStream) recvFromProcServer(ctx context.Context, done func(), newStream func(context.Context, func()) (resolver.ClientStream, error)) {
	defer func() {
		cs.drained.Fire()
		cs.requestBuffer.Put(nil)
		// Push a nil sentinel to the responseBuffer too to announce network termination for responses.
		cs.responseBodyBuffer.Put(nil)
	}()

	outgoingMD, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		outgoingMD = metadata.MD{}
	}

	var draining bool

	// if we did send the header, wait for the header and create the dataplane stream after mutating the header.
	if cs.config.processingModes.requestHeaderMode == modeSend {
		resp, err := cs.extStream.Recv()
		if err != nil {
			cs.failStreamin1stHeader(err, ctx, done, newStream)
			return
		}
		if resp.GetRequestDrain() && !draining {
			draining = true
			cs.triggerDrain()
		}
		if resp.GetImmediateResponse() != nil {
			if cs.config.disableImmediateResponse {
				cs.failStreamin1stHeader(status.Error(codes.Internal, "ext_proc server sent immediate_response but disable_immediate_response is true"), ctx, done, newStream)
			} else {
				imm := resp.GetImmediateResponse()
				statusCode := codes.Internal
				if imm.GetGrpcStatus() != nil {
					statusCode = codes.Code(imm.GetGrpcStatus().GetStatus())
				}
				cs.cancelStream(status.Error(statusCode, imm.GetDetails()))
			}
			return
		}
		if resp.GetRequestHeaders() == nil {
			err := fmt.Errorf("the first response is not header when header was sent to the proc server")
			cs.failStreamin1stHeader(err, ctx, done, newStream)
			return
		}
		header := resp.GetRequestHeaders()
		// check if status in header response is continue, if not fail the stream.
		if header.GetResponse().GetStatus() != v3procservicepb.CommonResponse_CONTINUE {
			cs.failStreamin1stHeader(fmt.Errorf("proc server returned non-continue status in request header response"), ctx, done, newStream)
			return
		}
		// mutate the headers
		if err = cs.config.mutationRules.ApplyAdditions(header.GetResponse().GetHeaderMutation().GetSetHeaders(), outgoingMD); err != nil {
			cs.failStreamin1stHeader(err, ctx, done, newStream)
			return
		}
		if err = cs.config.mutationRules.ApplyRemovals(header.GetResponse().GetHeaderMutation().GetRemoveHeaders(), outgoingMD); err != nil {
			cs.failStreamin1stHeader(err, ctx, done, newStream)
			return
		}
		dataplaneCtx := metadata.NewOutgoingContext(ctx, outgoingMD)
		// create new dataplane stream after mutating headers.
		dataplaneStream, err := newStream(dataplaneCtx, done)
		if err != nil {
			cs.failStream(err)
			return
		}
		cs.dataplaneStream = dataplaneStream
		close(cs.dataplaneStreamCreated)
	}

	// if header mode is not send or if we receive the header and create the
	// dataplane stream , start receiving from the proc server.
	for {
		resp, err := cs.extStream.Recv()
		if err != nil {
			cs.failStream(err)
			return
		}
		if resp.GetRequestDrain() {
			// Trigger the drain but continue receiving the drained messages until
			// we get io.EOF.
			cs.triggerDrain()
		}
		if resp.GetImmediateResponse() != nil {
			if cs.config.disableImmediateResponse {
				cs.failStream(status.Error(codes.Internal, "ext_proc server sent immediate_response but disable_immediate_response is true"))
			} else {
				imm := resp.GetImmediateResponse()
				statusCode := codes.Internal
				if imm.GetGrpcStatus() != nil {
					statusCode = codes.Code(imm.GetGrpcStatus().GetStatus())
				}
				cs.cancelStream(status.Error(statusCode, imm.GetDetails()))
			}
			return
		}

		switch {
		case resp.GetRequestBody() != nil:
			if cs.config.processingModes.requestBodyMode == modeSkip {
				cs.failStream(fmt.Errorf("proc server sent unsolicited request body response when mode is skip"))
				return
			}
			bodyResp := resp.GetRequestBody()
			if bodyResp.GetResponse().GetStatus() != v3procservicepb.CommonResponse_CONTINUE {
				cs.failStream(fmt.Errorf("proc server returned non-continue status in request body response"))
				return
			}
			streamedResp := bodyResp.GetResponse().GetBodyMutation().GetStreamedResponse()
			if streamedResp == nil {
				cs.failStream(fmt.Errorf("proc server returned invalid body mutation in request body response"))
				return
			}
			if streamedResp.GetGrpcMessageCompressed() {
				cs.failStream(fmt.Errorf("proc server returned grpc_message_compressed in request body response"))
				return
			}
			if streamedResp.GetEndOfStream() || streamedResp.GetEndOfStreamWithoutMessage() {
				cs.discardClientMessages.Store(true)
			}
			cs.requestBuffer.Put(streamedResp)

		case resp.GetResponseBody() != nil:
			if cs.config.processingModes.responseBodyMode == modeSkip {
				cs.failStream(fmt.Errorf("proc server sent unsolicited response body response when mode is skip"))
				return
			}
			bodyResp := resp.GetResponseBody()
			if bodyResp.GetResponse().GetStatus() != v3procservicepb.CommonResponse_CONTINUE {
				cs.failStream(fmt.Errorf("proc server returned non-continue status in response body response"))
				return
			}
			streamedResp := bodyResp.GetResponse().GetBodyMutation().GetStreamedResponse()
			if streamedResp == nil {
				cs.failStream(fmt.Errorf("proc server returned invalid body mutation in response body response"))
				return
			}
			if streamedResp.GetGrpcMessageCompressed() {
				cs.failStream(fmt.Errorf("proc server returned grpc_message_compressed in response body response"))
				return
			}
			cs.responseBodyBuffer.Put(streamedResp)

		case resp.GetResponseHeaders() != nil:
			if cs.config.processingModes.responseHeaderMode == modeSkip {
				cs.failStream(fmt.Errorf("proc server sent unsolicited response headers response when mode is skip"))
				return
			}
			header := resp.GetResponseHeaders()
			// check if status in header response is continue, if not fail the stream.
			if header.GetResponse().GetStatus() != v3procservicepb.CommonResponse_CONTINUE {
				cs.failStream(fmt.Errorf("proc server returned non-continue status in response header response"))
				return
			}
			if err = cs.config.mutationRules.ApplyAdditions(header.GetResponse().GetHeaderMutation().GetSetHeaders(), cs.responseHeader); err != nil {
				cs.failStream(err)
				return
			}
			if err = cs.config.mutationRules.ApplyRemovals(header.GetResponse().GetHeaderMutation().GetRemoveHeaders(), cs.responseHeader); err != nil {
				cs.failStream(err)
				return
			}
			// signal that response header is modified and ready to be sent
			// to client, so that if there is any buffered response body, it
			// can be sent after the header.
			cs.responseHeaderModified.Fire()

		case resp.GetResponseTrailers() != nil:
			if cs.config.processingModes.responseTrailerMode == modeSkip {
				cs.failStream(fmt.Errorf("proc server sent unsolicited response trailers response when mode is skip"))
				return
			}
			trailer := resp.GetResponseTrailers()
			if err = cs.config.mutationRules.ApplyAdditions(trailer.GetHeaderMutation().GetSetHeaders(), cs.responseTrailers); err != nil {
				cs.failStream(err)
				return
			}
			if err = cs.config.mutationRules.ApplyRemovals(trailer.GetHeaderMutation().GetRemoveHeaders(), cs.responseTrailers); err != nil {
				cs.failStream(err)
				return
			}
			// signal that response trailer is modified and ready to be sent
			// to client.
			cs.responseTrailerModified.Fire()
		}
	}
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
		case "request.query", "request.scheme", "request.time", "request.protocol":
			reqFields[attr] = ""
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

type clientStream struct {
	config                    baseConfig
	dataplaneStream           resolver.ClientStream
	extStream                 v3procservicepb.ExternalProcessor_ProcessClient
	nonOKStreamEnd            *grpcsync.Event
	streamClosed              bool
	streamCloseErr            error
	firstMessageSent          bool
	discardClientMessages     atomic.Bool
	requestBuffer             *buffer.Unbounded
	responseBodyBuffer        *buffer.Unbounded
	responseHeader            metadata.MD
	responseHeaderOnce        sync.Once
	responseHeaderModified    *grpcsync.Event
	responseTrailers          metadata.MD
	responseTrailerOnce       sync.Once
	responseTrailerModified   *grpcsync.Event
	sendtodataplanestarted    bool
	recvfromdataplanestarted  bool
	requestAttributes         map[string]*structpb.Struct
	dataplaneStreamCreated    chan struct{}
	ctx                       context.Context
	extSendCh                 chan *v3procservicepb.ProcessingRequest
	drainTriggeredCh          chan struct{}
	forwardLoopDoneCh         chan struct{}
	drainTriggered            atomic.Bool
	drained                   *grpcsync.Event
	drainedConsumer           atomic.Bool
	responseReceivingLoopDone chan struct{}
	abortedResponseBody       proto.Message
	trailersOnly              bool
}

func (cs *clientStream) triggerDrain() {
	if cs.drainTriggered.CompareAndSwap(false, true) {
		close(cs.drainTriggeredCh)
	}
}

// extReqSenderLoop is a loop that sends requests to the external processor.
func (cs *clientStream) extReqSenderLoop() {
	for {
		select {
		case req := <-cs.extSendCh:
			if err := cs.extStream.Send(req); err != nil {
				if cs.config.failureModeAllow {
					cs.streamClosed = true
					cs.triggerDrain()
				} else {
					cs.streamCloseErr = status.Errorf(codes.Internal, "extproc: external processor Send failed: %v", err)
					cs.nonOKStreamEnd.Fire()
				}
				return
			}
		case <-cs.drainTriggeredCh:
			cs.extStream.CloseSend()
			return
		case <-cs.nonOKStreamEnd.Done():
			return
		}
	}
}

// waitStream waits for the dataplane stream to be created or for the context to be done.
func (cs *clientStream) waitStream(ctx context.Context) (resolver.ClientStream, error) {
	select {
	case <-cs.dataplaneStreamCreated:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if cs.nonOKStreamEnd.HasFired() {
		return nil, cs.streamCloseErr
	}
	if cs.dataplaneStream == nil {
		return nil, cs.streamCloseErr
	}
	return cs.dataplaneStream, nil
}

func (cs *clientStream) processResponseHeaders() error {
	var err error
	cs.responseHeaderOnce.Do(func() {
		s, waitErr := cs.waitStream(cs.ctx)
		if waitErr != nil {
			err = waitErr
			return
		}
		header, headerErr := s.Header()
		if headerErr != nil {
			err = headerErr
			return
		}
		if header == nil {
			err = io.EOF
			return
		}
		cs.responseHeader = header
		if _, ok := header["grpc-status"]; ok {
			cs.trailersOnly = true
		}
		if cs.config.processingModes.responseHeaderMode == modeSend && !cs.streamClosed {
			req := &v3procservicepb.ProcessingRequest{
				Request: &v3procservicepb.ProcessingRequest_ResponseHeaders{ResponseHeaders: &v3procservicepb.HttpHeaders{
					Headers:     httpfilter.ConstructHeaderMap(header, cs.config.allowedHeaders, cs.config.disallowedHeaders),
					EndOfStream: cs.trailersOnly,
				}},
				Attributes:        cs.requestAttributes,
				ObservabilityMode: cs.config.observabilityMode,
			}
			if !cs.firstMessageSent {
				req.ProtocolConfig = &v3procservicepb.ProtocolConfiguration{
					RequestBodyMode:  convertBodyMode(cs.config.processingModes.requestBodyMode),
					ResponseBodyMode: convertBodyMode(cs.config.processingModes.responseBodyMode),
				}
				cs.firstMessageSent = true
			}
			select {
			case cs.extSendCh <- req:
			case <-cs.nonOKStreamEnd.Done():
				err = cs.streamCloseErr
			case <-cs.drainTriggeredCh:
			}
		} else {
			cs.responseHeaderModified.Fire()
		}
	})
	return err
}

func (cs *clientStream) Header() (metadata.MD, error) {
	if err := cs.processResponseHeaders(); err != nil {
		return nil, err
	}
	select {
	case <-cs.responseHeaderModified.Done():
		if cs.streamClosed || cs.streamCloseErr != nil {
			return nil, cs.streamCloseErr
		}
	case <-cs.nonOKStreamEnd.Done():
		return nil, cs.streamCloseErr
	case <-cs.drainTriggeredCh:
	}
	return cs.responseHeader, nil
}

func (cs *clientStream) processResponseTrailers() {
	cs.responseTrailerOnce.Do(func() {
		s, err := cs.waitStream(cs.ctx)
		if err != nil {
			return
		}
		trailer := s.Trailer()
		if trailer == nil {
			return
		}
		cs.responseTrailers = trailer
		if cs.config.processingModes.responseTrailerMode == modeSend && !cs.streamClosed && !cs.trailersOnly {
			req := &v3procservicepb.ProcessingRequest{
				Request: &v3procservicepb.ProcessingRequest_ResponseTrailers{ResponseTrailers: &v3procservicepb.HttpTrailers{
					Trailers: httpfilter.ConstructHeaderMap(trailer, cs.config.allowedHeaders, cs.config.disallowedHeaders),
				}},
				Attributes:        cs.requestAttributes,
				ObservabilityMode: cs.config.observabilityMode,
			}
			if !cs.firstMessageSent {
				req.ProtocolConfig = &v3procservicepb.ProtocolConfiguration{
					RequestBodyMode:  convertBodyMode(cs.config.processingModes.requestBodyMode),
					ResponseBodyMode: convertBodyMode(cs.config.processingModes.responseBodyMode),
				}
				cs.firstMessageSent = true
			}
			cs.extSendCh <- req
			select {
			case <-cs.responseTrailerModified.Done():
			case <-cs.nonOKStreamEnd.Done():
			case <-cs.drainTriggeredCh:
			}
		}
	})
}

func (cs *clientStream) Trailer() metadata.MD {
	cs.processResponseTrailers()
	return cs.responseTrailers
}

func (cs *clientStream) CloseSend() error {
	s, err := cs.waitStream(cs.ctx)
	if err != nil {
		return err
	}

	if cs.streamClosed || cs.config.processingModes.requestBodyMode == modeSkip {
		return s.CloseSend()
	}

	req := &v3procservicepb.ProcessingRequest{
		Request: &v3procservicepb.ProcessingRequest_RequestBody{
			RequestBody: &v3procservicepb.HttpBody{
				EndOfStreamWithoutMessage: true,
			},
		},
		Attributes:        cs.requestAttributes,
		ObservabilityMode: cs.config.observabilityMode,
	}

	if !cs.firstMessageSent {
		req.ProtocolConfig = &v3procservicepb.ProtocolConfiguration{
			RequestBodyMode:  convertBodyMode(cs.config.processingModes.requestBodyMode),
			ResponseBodyMode: convertBodyMode(cs.config.processingModes.responseBodyMode),
		}
		cs.firstMessageSent = true
	}

	select {
	case cs.extSendCh <- req:
		return nil
	case <-cs.drainTriggeredCh:
		if err := cs.waitChannel(cs.forwardLoopDoneCh); err != nil {
			return err
		}
		return s.CloseSend()
	case <-cs.nonOKStreamEnd.Done():
		return cs.streamCloseErr
	}
}

func (cs *clientStream) Context() context.Context {
	s, err := cs.waitStream(cs.ctx)
	if err != nil {
		return cs.ctx
	}
	return s.Context()
}

func (cs *clientStream) responseReceivingLoop(msgPrototype proto.Message) {
	defer close(cs.responseReceivingLoopDone)
	s, err := cs.waitStream(cs.ctx)
	if err != nil {
		return
	}

	// Ensure response headers are processed and mutated before we receive the first message.
	if err := cs.processResponseHeaders(); err != nil {
		cs.failStream(err)
		return
	}

	for {
		if cs.streamClosed || cs.drained.HasFired() {
			return
		}

		// Allocate a new message using reflection so we do not overwrite in-flight buffered messages.
		newMsg := msgPrototype.ProtoReflect().New().Interface()
		if err := s.RecvMsg(newMsg); err != nil {
			if err == io.EOF {
				cs.processResponseTrailers()
				cs.triggerDrain()
			} else {
				cs.failStream(err)
			}
			return
		}

		bodyBytes, err := proto.Marshal(newMsg)
		if err != nil {
			cs.failStream(err)
			return
		}

		req := &v3procservicepb.ProcessingRequest{
			Request: &v3procservicepb.ProcessingRequest_ResponseBody{
				ResponseBody: &v3procservicepb.HttpBody{
					Body: bodyBytes,
				},
			},
			Attributes:        cs.requestAttributes,
			ObservabilityMode: cs.config.observabilityMode,
		}

		select {
		case cs.extSendCh <- req:
		case <-cs.drainTriggeredCh:
			cs.abortedResponseBody = newMsg.(proto.Message)
			return
		case <-cs.nonOKStreamEnd.Done():
			return
		}
	}
}

func (cs *clientStream) RecvMsg(m any) error {
	// wont this check for cs.StreamClosed result in dataloss? IN a situation
	// where the stream has ened and it has pushed to the response buffer, the
	// streamclosed will be marked as true and will never read from thee response
	// buffer. Or can that never happen? we should check for nil from buffer instead of stream closed
	if cs.streamClosed || cs.config.processingModes.responseBodyMode == modeSkip {
		s, err := cs.waitStream(cs.ctx)
		if err != nil {
			return err
		}
		if err := s.RecvMsg(m); err != nil {
			if err == io.EOF {
				cs.processResponseTrailers()
			} else {
				cs.failStream(err)
			}
			return err
		}
		return nil
	}

	msg, ok := m.(proto.Message)
	if !ok {
		return fmt.Errorf("response message does not implement proto.Message")
	}

	// Start the background receiving loop on the first RecvMsg call.
	if !cs.recvfromdataplanestarted {
		cs.recvfromdataplanestarted = true
		go cs.responseReceivingLoop(msg)
	}

	// Pull from responseBodyBuffer (which strictly receives mutated StreamedBodyResponses or a nil sentinel).
	select {
	case item := <-cs.responseBodyBuffer.Get():
		cs.responseBodyBuffer.Load()
		if item == nil {
			if cs.streamCloseErr != nil {
				return cs.streamCloseErr
			}
			s, err := cs.waitStream(cs.ctx)
			if err != nil {
				return err
			}
			if err := s.RecvMsg(m); err != nil {
				if err == io.EOF {
					cs.processResponseTrailers()
				} else {
					cs.failStream(err)
				}
				return err
			}
			return nil
		}

		streamedResp, ok := item.(*v3procservicepb.StreamedBodyResponse)
		if !ok {
			return fmt.Errorf("unexpected response type in responseBuffer: %T", item)
		}
		if err := proto.Unmarshal(streamedResp.GetBody(), msg); err != nil {
			return err
		}
		return nil

	case <-cs.drained.Done():
		// Bypassed or Drained mid-stream: first flush any remaining mutated messages.
		select {
		case item := <-cs.responseBodyBuffer.Get():
			cs.responseBodyBuffer.Load()
			if item != nil {
				streamedResp, ok := item.(*v3procservicepb.StreamedBodyResponse)
				if !ok {
					return fmt.Errorf("unexpected response type in responseBuffer: %T", item)
				}
				if err := proto.Unmarshal(streamedResp.GetBody(), msg); err != nil {
					return err
				}
				return nil
			}
		default:
		}

		// No mutated messages remaining: wait for background loop to exit safely
		if err := cs.waitChannel(cs.responseReceivingLoopDone); err != nil {
			return err
		}

		// Deliver the aborted message if present
		if cs.abortedResponseBody != nil {
			proto.Reset(msg)
			proto.Merge(msg, cs.abortedResponseBody)
			cs.abortedResponseBody = nil
			return nil
		}

		// Safe direct fallback to synchronous dataplane reads
		s, err := cs.waitStream(cs.ctx)
		if err != nil {
			return err
		}
		if err := s.RecvMsg(m); err != nil {
			if err == io.EOF {
				cs.processResponseTrailers()
			} else {
				cs.failStream(err)
			}
			return err
		}
		return nil

	case <-cs.ctx.Done():
		return cs.ctx.Err()
	case <-cs.nonOKStreamEnd.Done():
		return cs.streamCloseErr
	}
}

// Intercept the messages being sent by the client. The header is already taken
// care of in the newstream. This takes care of the request body.
func (cs *clientStream) SendMsg(m any) error {
	if cs.nonOKStreamEnd.HasFired() {
		return cs.streamCloseErr
	}

	msg, ok := m.(proto.Message)
	if !ok {
		return fmt.Errorf("message does not implement proto.Message")
	}

	// 1. Guaranteed Launch of Forwarding Loop to send the data to the dtaplane server.
	// We start it on the first send
	// because we need the message type to send the data to the dataplane server.
	if !cs.sendtodataplanestarted {
		cs.sendtodataplanestarted = true
		go cs.requestForwardingLoop(msg)
	}

	if cs.streamClosed || cs.config.processingModes.requestBodyMode == modeSkip {
		s, err := cs.waitStream(cs.ctx)
		if err != nil {
			return err
		}
		return s.SendMsg(m)
	}

	// 2. Early Bypass Check, If the forwarding loop is done, it means that the
	// ExtProc server has signaled end_of_stream i.e. all the request messages
	// from the ext proc server have been passed to the dataplaen server. so this
	// message can be safely sent to the dataplane server. This is to maintain the
	// . So we can bypass the ExtProc server and return the raw message.
	select {
	case <-cs.forwardLoopDoneCh:
		s, err := cs.waitStream(cs.ctx)
		if err != nil {
			return err
		}
		return s.SendMsg(m)
	default:
	}

	// This is a best-effort check to discard client messages once the ExtProc server
	// signals end_of_stream. Due to asynchronous network propagation, a message might
	// still cross the wire before this flag is stored.
	if cs.discardClientMessages.Load() {
		return nil
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
		Attributes:        cs.requestAttributes,
		ObservabilityMode: cs.config.observabilityMode,
	}

	if !cs.firstMessageSent {
		req.ProtocolConfig = &v3procservicepb.ProtocolConfiguration{
			RequestBodyMode:  convertBodyMode(cs.config.processingModes.requestBodyMode),
			ResponseBodyMode: convertBodyMode(cs.config.processingModes.responseBodyMode),
		}
		cs.firstMessageSent = true
	}

	// 3. Bounded Sequence-Safe Handoff
	select {
	case cs.extSendCh <- req:
		return nil

	case <-cs.drainTriggeredCh:
		// DRAIN RACE CAUGHT: The sender loop is shut down, preventing new handoffs.
		// Block the application stream thread until all echoes are flushed.
		if err := cs.waitChannel(cs.forwardLoopDoneCh); err != nil {
			return err
		}
		s, err := cs.waitStream(cs.ctx)
		if err != nil {
			return err
		}
		return s.SendMsg(m)

	case <-cs.nonOKStreamEnd.Done():
		return cs.streamCloseErr
	}
}

// loop to pull data out of request buffer and send to dataplane server
func (cs *clientStream) requestForwardingLoop(reqPrototype proto.Message) {
	defer close(cs.forwardLoopDoneCh)
	_, err := cs.waitStream(cs.ctx)
	if err != nil {
		return
	}
	for item := range cs.requestBuffer.Get() {
		cs.requestBuffer.Load()
		if item == nil {
			return
		}
		streamedResp, ok := item.(*v3procservicepb.StreamedBodyResponse)
		if !ok {
			return
		}

		if streamedResp.GetEndOfStreamWithoutMessage() {
			cs.dataplaneStream.CloseSend()
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

		if streamedResp.GetEndOfStream() {
			cs.dataplaneStream.CloseSend()
			return
		}
	}
}

// waitChannel waits for the provided channel to be closed, while also respecting
// context cancellation and stream failures.
func (cs *clientStream) waitChannel(ch <-chan struct{}) error {
	select {
	case <-ch:
		return nil
	case <-cs.ctx.Done():
		return cs.ctx.Err()
	case <-cs.nonOKStreamEnd.Done():
		return cs.streamCloseErr
	}
}
