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
	"fmt"
	"regexp"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/internal/xds/httpfilter"
	"google.golang.org/grpc/internal/xds/matcher"
	"google.golang.org/grpc/metadata"

	mutationpb "github.com/envoyproxy/go-control-plane/envoy/config/common/mutation_rules/v3"
	v3procfilterpb "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	v3matcherpb "github.com/envoyproxy/go-control-plane/envoy/type/matcher/v3"
)

type baseConfig struct {
	httpfilter.FilterConfig
	config interceptorConfig
}

type overrideConfig struct {
	httpfilter.FilterConfig
	config interceptorConfig
}

// interceptorConfig contains the configuration for the external processing
// client interceptor.
type interceptorConfig struct {
	// The following fields can be set either in the filter config or the override
	// config. If both are set, the override config will be used.
	//
	// server is the configuration for the external processing server.
	server *serverConfig
	// failureModeAllow specifies the behavior when the RPC to the external
	// processing server fails. If true, the dataplane RPC will be allowed to
	// continue. If false, the data plane RPC will be failed with a grpc status
	// code of UNAVAILABLE.
	failureModeAllow *bool
	// processingModes specifies the processing mode for each dataplane event.
	processingModes *processingModes
	// Attributes to be sent to the external processing server along with the
	// request and response dataplane events.
	requestAttributes  []string
	responseAttributes []string

	// The following fields can only be set in the base config.
	//
	// mutationRules specifies the rules for what modifications an external
	// processing server may make to headers/trailers sent to it.
	mutationRules headerMutationRules
	// allowedHeaders specifies the headers that are allowed to be sent to the
	// external processing server. If unset, all headers are allowed.
	allowedHeaders []matcher.StringMatcher
	// disallowedHeaders specifies the headers that will not be sent to the
	// external processing server. This overrides the above allowedHeaders if a
	// header matches both.
	disallowedHeaders []matcher.StringMatcher
	// disableImmediateResponse specifies whether to disable immediate response
	// from the external processing server. When true, if the response from
	// external processing server has the `immediate_response` field set, the
	// dataplane RPC will be failed with `UNAVAILABLE` status code. When false,
	// the `immediate_response` field in the response from external processing
	// server will be ignored.
	disableImmediateResponse bool
	// observabilityMode determines if the filter waits for the external
	// processing server. If true, events are sent to the server in
	// "observation-only" mode; the filter does not wait for a response. If false,
	// the filter waits for a response, allowing the server to modify events
	// before they reach the dataplane.
	observabilityMode bool
	// deferredCloseTimeout is the duration the filter waits before closing the
	// external processing stream after the dataplane RPC completes. This is only
	// applicable when observabilityMode is true; otherwise, it is ignored. The
	// default value is 5 seconds.
	deferredCloseTimeout time.Duration
}

// processingMode defines how headers, trailers, and bodies are handled in
// relation to the external processing server.
type processingMode int

const (
	// modeSkip indicates that the header/trailer/body should not be sent.
	modeSkip processingMode = iota
	// modeSend indicates that the header/trailer/body should be sent.
	modeSend
)

type processingModes struct {
	requestHeaderMode   processingMode
	responseHeaderMode  processingMode
	responseTrailerMode processingMode
	requestBodyMode     processingMode
	responseBodyMode    processingMode
}

// headerMutationRules specifies the rules for what modifications an external
// processing server may make to headers sent on the data plane RPC.
type headerMutationRules struct {
	// allowExpr specifies a regular expression that matches the headers that can
	// be mutated.
	allowExpr *regexp.Regexp
	// disallowExpr specifies a regular expression that matches the headers that
	// cannot be mutated. This overrides the above allowExpr if a header matches
	// both.
	disallowExpr *regexp.Regexp
	// disallowAll specifies that no header mutations are allowed. This overrides
	// all other settings.
	disallowAll bool
	// disallowIsError specifies whether to return an error if a header mutation
	// is disallowed. If true, the data plane RPC will be failed with a grpc
	// status code of Unknown.
	disallowIsError bool
}

// serverConfig contains the configuration for an external server.
type serverConfig struct {
	// targetURI is the name of the external server.
	targetURI string
	// channelCredentials specifies the transport credentials to use to connect to
	// the external server. Must not be nil.
	channelCredentials credentials.TransportCredentials
	// callCredentials specifies the per-RPC credentials to use when making calls
	// to the external server.
	callCredentials []credentials.PerRPCCredentials
	// timeout is the RPC timeout for the call to the external server. If unset,
	// the timeout depends on the usage of this external server. For example,
	// cases like ext_authz and ext_proc, where there is a 1:1 mapping between the
	// data plane RPC and the external server call, the timeout will be capped by
	// the timeout on the data plane RPC. For cases like RLQS where there is a
	// side channel to the external server, an unset timeout will result in no
	// timeout being applied to the external server call.
	timeout time.Duration
	// initialMetadata is the additional metadata to include in all RPCs sent to
	// the external server.
	initialMetadata metadata.MD
}

// convertStringMatchers converts a slice of protobuf StringMatcher messages to
// a slice of matcher.StringMatcher.
func convertStringMatchers(patterns []*v3matcherpb.StringMatcher) ([]matcher.StringMatcher, error) {
	matchers := make([]matcher.StringMatcher, 0, len(patterns))
	for _, p := range patterns {
		sm, err := matcher.StringMatcherFromProto(p)
		if err != nil {
			return nil, err
		}
		matchers = append(matchers, sm)
	}
	return matchers, nil
}

// resolveHeaderMode resolves the processing mode for headers based on the
// protobuf enum value. If the mode is not set or set to Default, it returns the
// provided defaultMode.
func resolveHeaderMode(mode v3procfilterpb.ProcessingMode_HeaderSendMode, defaultMode processingMode) processingMode {
	switch mode {
	case v3procfilterpb.ProcessingMode_SEND:
		return modeSend
	case v3procfilterpb.ProcessingMode_SKIP:
		return modeSkip
	default:
		return defaultMode
	}
}

// resolveBodyMode resolves the processing mode for body based on the protobuf
// enum value. If the mode is not set (i.e., default), it returns modeSkip, as
// the default for body is to skip.
func resolveBodyMode(mode v3procfilterpb.ProcessingMode_BodySendMode) processingMode {
	switch mode {
	case v3procfilterpb.ProcessingMode_GRPC:
		return modeSend
	case v3procfilterpb.ProcessingMode_NONE:
		return modeSkip
	default:
		return modeSkip
	}
}

// headerMutationRulesFromProto converts a protobuf HeaderMutationRules message
// to a headerMutationRules struct.
func headerMutationRulesFromProto(mr *mutationpb.HeaderMutationRules) (headerMutationRules, error) {
	var rules headerMutationRules
	if mr == nil {
		return rules, nil
	}
	if allowExpr := mr.GetAllowExpression(); allowExpr != nil {
		re, err := regexp.Compile(allowExpr.GetRegex())
		if err != nil {
			return rules, fmt.Errorf("extproc: %v", err)
		}
		rules.allowExpr = re
	}
	if disallowExpr := mr.GetDisallowExpression(); disallowExpr != nil {
		re, err := regexp.Compile(disallowExpr.GetRegex())
		if err != nil {
			return rules, fmt.Errorf("extproc: %v", err)
		}
		rules.disallowExpr = re
	}
	rules.disallowAll = mr.GetDisallowAll().GetValue()
	rules.disallowIsError = mr.GetDisallowIsError().GetValue()
	return rules, nil
}

// processingModesFromProto converts a protobuf ProcessingMode message
// to a processingModes struct.
func processingModesFromProto(pm *v3procfilterpb.ProcessingMode) *processingModes {
	if pm == nil {
		return nil
	}
	return &processingModes{
		requestHeaderMode:   resolveHeaderMode(pm.GetRequestHeaderMode(), modeSend),
		responseHeaderMode:  resolveHeaderMode(pm.GetResponseHeaderMode(), modeSend),
		responseTrailerMode: resolveHeaderMode(pm.GetResponseTrailerMode(), modeSkip),
		requestBodyMode:     resolveBodyMode(pm.GetRequestBodyMode()),
		responseBodyMode:    resolveBodyMode(pm.GetResponseBodyMode()),
	}
}
