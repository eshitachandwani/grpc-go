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

package xds_test

import (
	"context"
	"io"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/internal/envconfig"
	"google.golang.org/grpc/internal/stubserver"
	"google.golang.org/grpc/internal/testutils"
	"google.golang.org/grpc/internal/testutils/xds/e2e"
	"google.golang.org/grpc/internal/testutils/xds/e2e/setup"
	"google.golang.org/grpc/internal/xds/httpfilter/extproc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	v3corepb "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	v3routepb "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	v3procfilterpb "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	v3httppb "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	v3procservicepb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	testgrpc "google.golang.org/grpc/interop/grpc_testing"
	testpb "google.golang.org/grpc/interop/grpc_testing"
)

type mockExternalProcessorServer struct {
	v3procservicepb.UnimplementedExternalProcessorServer
	requestCh  chan *v3procservicepb.ProcessingRequest
	responseCh chan *v3procservicepb.ProcessingResponse
}

func (s *mockExternalProcessorServer) Process(stream v3procservicepb.ExternalProcessor_ProcessServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		select {
		case s.requestCh <- req:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}

		select {
		case resp := <-s.responseCh:
			if resp == nil {
				return status.Errorf(codes.Aborted, "simulated error")
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

// TestClientSideXDS_ExtProc_HeaderMutation tests that when ExtProc filter is
// configured on the client, it sends request headers to the ExtProc server,
// applies mutation (adding a custom header), and the mutated header is successfully
// received by the backend server.
func TestClientSideXDS_ExtProc_HeaderMutation(t *testing.T) {
	testutils.SetEnvConfig(t, &envconfig.XDSClientExtProcEnabled, true)
	extproc.RegisterForTesting()
	defer extproc.UnregisterForTesting()

	// Start the mock ExtProc server.
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	extprocServer := grpc.NewServer()
	mockProc := &mockExternalProcessorServer{
		requestCh:  make(chan *v3procservicepb.ProcessingRequest, 10),
		responseCh: make(chan *v3procservicepb.ProcessingResponse, 10),
	}
	v3procservicepb.RegisterExternalProcessorServer(extprocServer, mockProc)
	go extprocServer.Serve(lis)
	defer extprocServer.Stop()

	// Start a test backend service that captures incoming headers.
	headerCh := make(chan metadata.MD, 10)
	backend := stubserver.StartTestService(t, &stubserver.StubServer{
		EmptyCallF: func(ctx context.Context, in *testpb.Empty) (*testpb.Empty, error) {
			if md, ok := metadata.FromIncomingContext(ctx); ok {
				headerCh <- md
			}
			return &testpb.Empty{}, nil
		},
	})
	defer backend.Stop()

	// Setup management server and resolver.
	managementServer, nodeID, _, xdsResolver := setup.ManagementServerAndResolver(t)

	const serviceName = "my-service-extproc"
	extprocProto := &v3procfilterpb.ExternalProcessor{
		GrpcService: &v3corepb.GrpcService{
			TargetSpecifier: &v3corepb.GrpcService_GoogleGrpc_{
				GoogleGrpc: &v3corepb.GrpcService_GoogleGrpc{
					TargetUri: lis.Addr().String(),
				},
			},
		},
		ProcessingMode: &v3procfilterpb.ProcessingMode{
			RequestHeaderMode:  v3procfilterpb.ProcessingMode_SEND,
			ResponseHeaderMode: v3procfilterpb.ProcessingMode_SKIP,
		},
	}

	hcm := &v3httppb.HttpConnectionManager{
		RouteSpecifier: &v3httppb.HttpConnectionManager_RouteConfig{
			RouteConfig: &v3routepb.RouteConfiguration{
				Name: "route-config",
				VirtualHosts: []*v3routepb.VirtualHost{{
					Domains: []string{serviceName},
					Routes: []*v3routepb.Route{{
						Match: &v3routepb.RouteMatch{PathSpecifier: &v3routepb.RouteMatch_Prefix{Prefix: ""}},
						Action: &v3routepb.Route_Route{Route: &v3routepb.RouteAction{
							ClusterSpecifier: &v3routepb.RouteAction_Cluster{Cluster: "cluster-" + serviceName},
						}},
					}},
				}},
			},
		},
		HttpFilters: []*v3httppb.HttpFilter{
			{
				Name: "extproc",
				ConfigType: &v3httppb.HttpFilter_TypedConfig{
					TypedConfig: testutils.MarshalAny(t, extprocProto),
				},
			},
			e2e.RouterHTTPFilter,
		},
	}

	resources := e2e.DefaultClientResources(e2e.ResourceParams{
		DialTarget: serviceName,
		NodeID:     nodeID,
		Host:       "localhost",
		Port:       testutils.ParsePort(t, backend.Address),
		SecLevel:   e2e.SecurityLevelNone,
	})
	resources.Listeners[0].ApiListener.ApiListener = testutils.MarshalAny(t, hcm)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if err := managementServer.Update(ctx, resources); err != nil {
		t.Fatal(err)
	}

	// Create ClientConn.
	cc, err := grpc.NewClient("xds:///"+serviceName, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithResolvers(xdsResolver))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()

	client := testgrpc.NewTestServiceClient(cc)

	// Goroutine to make the RPC call synchronously.
	errCh := make(chan error, 1)
	go func() {
		_, err := client.EmptyCall(ctx, &testpb.Empty{})
		errCh <- err
	}()

	// Verify ExtProc server received request headers.
	select {
	case req := <-mockProc.requestCh:
		if req.GetRequestHeaders() == nil {
			t.Fatalf("expected request headers in processing request, got: %v", req)
		}
	case <-ctx.Done():
		t.Fatalf("timeout waiting for extproc processing request")
	}

	// Respond with header mutation adding "x-extproc-header: extproc-value".
	select {
	case mockProc.responseCh <- &v3procservicepb.ProcessingResponse{
		Response: &v3procservicepb.ProcessingResponse_RequestHeaders{
			RequestHeaders: &v3procservicepb.HeadersResponse{
				Response: &v3procservicepb.CommonResponse{
					Status: v3procservicepb.CommonResponse_CONTINUE,
					HeaderMutation: &v3procservicepb.HeaderMutation{
						SetHeaders: []*v3corepb.HeaderValueOption{
							{
								Header: &v3corepb.HeaderValue{
									Key:   "x-extproc-header",
									Value: "extproc-value",
								},
							},
						},
					},
				},
			},
		},
	}:
	case <-ctx.Done():
		t.Fatalf("timeout sending extproc response")
	}

	// Wait for RPC to complete and check error.
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("EmptyCall() RPC failed: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("timeout waiting for RPC completion")
	}

	// Verify that the backend received the mutated header.
	select {
	case md := <-headerCh:
		vals := md.Get("x-extproc-header")
		if len(vals) == 0 || vals[0] != "extproc-value" {
			t.Fatalf("mutated header x-extproc-header mismatch: got %v, want [extproc-value]", vals)
		}
	case <-ctx.Done():
		t.Fatalf("timeout waiting for mutated header on backend")
	}
}

// TestClientSideXDS_ExtProc_FailureModeAllow tests that when ExtProc server fails
// and failure_mode_allow is configured to true, the data plane RPC still succeeds.
func TestClientSideXDS_ExtProc_FailureModeAllow(t *testing.T) {
	testutils.SetEnvConfig(t, &envconfig.XDSClientExtProcEnabled, true)
	extproc.RegisterForTesting()
	defer extproc.UnregisterForTesting()

	// Start mock ExtProc server which returns failure.
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	extprocServer := grpc.NewServer()
	mockProc := &mockExternalProcessorServer{
		requestCh:  make(chan *v3procservicepb.ProcessingRequest, 10),
		responseCh: make(chan *v3procservicepb.ProcessingResponse, 10),
	}
	v3procservicepb.RegisterExternalProcessorServer(extprocServer, mockProc)
	go extprocServer.Serve(lis)
	defer extprocServer.Stop()

	backend := stubserver.StartTestService(t, nil)
	defer backend.Stop()

	managementServer, nodeID, _, xdsResolver := setup.ManagementServerAndResolver(t)

	const serviceName = "my-service-failure-mode-allow"
	extprocProto := &v3procfilterpb.ExternalProcessor{
		GrpcService: &v3corepb.GrpcService{
			TargetSpecifier: &v3corepb.GrpcService_GoogleGrpc_{
				GoogleGrpc: &v3corepb.GrpcService_GoogleGrpc{
					TargetUri: lis.Addr().String(),
				},
			},
		},
		ProcessingMode: &v3procfilterpb.ProcessingMode{
			RequestHeaderMode:  v3procfilterpb.ProcessingMode_SEND,
			ResponseHeaderMode: v3procfilterpb.ProcessingMode_SKIP,
		},
		FailureModeAllow: true, // Allow data plane RPC to proceed even if ExtProc fails.
	}

	hcm := &v3httppb.HttpConnectionManager{
		RouteSpecifier: &v3httppb.HttpConnectionManager_RouteConfig{
			RouteConfig: &v3routepb.RouteConfiguration{
				Name: "route-config",
				VirtualHosts: []*v3routepb.VirtualHost{{
					Domains: []string{serviceName},
					Routes: []*v3routepb.Route{{
						Match: &v3routepb.RouteMatch{PathSpecifier: &v3routepb.RouteMatch_Prefix{Prefix: ""}},
						Action: &v3routepb.Route_Route{Route: &v3routepb.RouteAction{
							ClusterSpecifier: &v3routepb.RouteAction_Cluster{Cluster: "cluster-" + serviceName},
						}},
					}},
				}},
			},
		},
		HttpFilters: []*v3httppb.HttpFilter{
			{
				Name: "extproc",
				ConfigType: &v3httppb.HttpFilter_TypedConfig{
					TypedConfig: testutils.MarshalAny(t, extprocProto),
				},
			},
			e2e.RouterHTTPFilter,
		},
	}

	resources := e2e.DefaultClientResources(e2e.ResourceParams{
		DialTarget: serviceName,
		NodeID:     nodeID,
		Host:       "localhost",
		Port:       testutils.ParsePort(t, backend.Address),
		SecLevel:   e2e.SecurityLevelNone,
	})
	resources.Listeners[0].ApiListener.ApiListener = testutils.MarshalAny(t, hcm)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if err := managementServer.Update(ctx, resources); err != nil {
		t.Fatal(err)
	}

	cc, err := grpc.NewClient("xds:///"+serviceName, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithResolvers(xdsResolver))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()

	client := testgrpc.NewTestServiceClient(cc)

	// Goroutine to make the RPC call.
	errCh := make(chan error, 1)
	go func() {
		_, err := client.EmptyCall(ctx, &testpb.Empty{})
		errCh <- err
	}()

	// Wait for ExtProc server to receive request headers.
	select {
	case req := <-mockProc.requestCh:
		if req.GetRequestHeaders() == nil {
			t.Fatalf("expected request headers, got: %v", req)
		}
	case <-ctx.Done():
		t.Fatalf("timeout waiting for request headers")
	}

	// Simulate an RPC error from ExtProc server.
	select {
	case mockProc.responseCh <- nil: // Send nil to cause mockProc.Process to return error/close stream
	case <-ctx.Done():
		t.Fatalf("timeout sending nil response")
	}

	// Since failure_mode_allow is true, the data plane EmptyCall RPC should still succeed!
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("EmptyCall() failed unexpectedly when failure_mode_allow is true: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("timeout waiting for RPC completion")
	}
}

// TestClientSideXDS_ExtProc_FailureModeDeny tests that when ExtProc server fails
// and failure_mode_allow is false, the data plane RPC fails with Unavailable status.
func (s) TestClientSideXDS_ExtProc_FailureModeDeny(t *testing.T) {
	testutils.SetEnvConfig(t, &envconfig.XDSClientExtProcEnabled, true)
	extproc.RegisterForTesting()
	defer extproc.UnregisterForTesting()

	// Start mock ExtProc server which returns failure.
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	extprocServer := grpc.NewServer()
	mockProc := &mockExternalProcessorServer{
		requestCh:  make(chan *v3procservicepb.ProcessingRequest, 10),
		responseCh: make(chan *v3procservicepb.ProcessingResponse, 10),
	}
	v3procservicepb.RegisterExternalProcessorServer(extprocServer, mockProc)
	go extprocServer.Serve(lis)
	defer extprocServer.Stop()

	backend := stubserver.StartTestService(t, nil)
	defer backend.Stop()

	managementServer, nodeID, _, xdsResolver := setup.ManagementServerAndResolver(t)

	const serviceName = "my-service-failure-mode-deny"
	extprocProto := &v3procfilterpb.ExternalProcessor{
		GrpcService: &v3corepb.GrpcService{
			TargetSpecifier: &v3corepb.GrpcService_GoogleGrpc_{
				GoogleGrpc: &v3corepb.GrpcService_GoogleGrpc{
					TargetUri: lis.Addr().String(),
				},
			},
		},
		ProcessingMode: &v3procfilterpb.ProcessingMode{
			RequestHeaderMode:  v3procfilterpb.ProcessingMode_SEND,
			ResponseHeaderMode: v3procfilterpb.ProcessingMode_SKIP,
		},
		FailureModeAllow: false, // Data plane RPC should fail if ExtProc fails.
	}

	hcm := &v3httppb.HttpConnectionManager{
		RouteSpecifier: &v3httppb.HttpConnectionManager_RouteConfig{
			RouteConfig: &v3routepb.RouteConfiguration{
				Name: "route-config",
				VirtualHosts: []*v3routepb.VirtualHost{{
					Domains: []string{serviceName},
					Routes: []*v3routepb.Route{{
						Match: &v3routepb.RouteMatch{PathSpecifier: &v3routepb.RouteMatch_Prefix{Prefix: ""}},
						Action: &v3routepb.Route_Route{Route: &v3routepb.RouteAction{
							ClusterSpecifier: &v3routepb.RouteAction_Cluster{Cluster: "cluster-" + serviceName},
						}},
					}},
				}},
			},
		},
		HttpFilters: []*v3httppb.HttpFilter{
			{
				Name: "extproc",
				ConfigType: &v3httppb.HttpFilter_TypedConfig{
					TypedConfig: testutils.MarshalAny(t, extprocProto),
				},
			},
			e2e.RouterHTTPFilter,
		},
	}

	resources := e2e.DefaultClientResources(e2e.ResourceParams{
		DialTarget: serviceName,
		NodeID:     nodeID,
		Host:       "localhost",
		Port:       testutils.ParsePort(t, backend.Address),
		SecLevel:   e2e.SecurityLevelNone,
	})
	resources.Listeners[0].ApiListener.ApiListener = testutils.MarshalAny(t, hcm)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if err := managementServer.Update(ctx, resources); err != nil {
		t.Fatal(err)
	}

	cc, err := grpc.NewClient("xds:///"+serviceName, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithResolvers(xdsResolver))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()

	client := testgrpc.NewTestServiceClient(cc)

	// Goroutine to make the RPC call.
	errCh := make(chan error, 1)
	go func() {
		_, err := client.EmptyCall(ctx, &testpb.Empty{})
		errCh <- err
	}()

	// Wait for ExtProc server to receive request headers.
	select {
	case req := <-mockProc.requestCh:
		if req.GetRequestHeaders() == nil {
			t.Fatalf("expected request headers, got: %v", req)
		}
	case <-ctx.Done():
		t.Fatalf("timeout waiting for request headers")
	}

	// Simulate stream close without response.
	select {
	case mockProc.responseCh <- nil:
	case <-ctx.Done():
		t.Fatalf("timeout sending nil response")
	}

	// The data plane EmptyCall RPC should fail since failure_mode_allow is false.
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatalf("EmptyCall() succeeded unexpectedly when failure_mode_allow is false")
		}
		if got, want := status.Code(err), codes.Internal; got != want {
			t.Fatalf("EmptyCall() returned status code: %v, want: %v", got, want)
		}
	case <-ctx.Done():
		t.Fatalf("timeout waiting for RPC completion")
	}
}

// TestClientSideXDS_ExtProc_UnsolicitedResponse tests that if the ExtProc server
// sends an unsolicited response (e.g., request body response when request body
// mode is skip), the client filter detects it as a protocol error, fails the
// stream with Internal status code, and aborts the RPC.
func (s) TestClientSideXDS_ExtProc_UnsolicitedResponse(t *testing.T) {
	testutils.SetEnvConfig(t, &envconfig.XDSClientExtProcEnabled, true)
	extproc.RegisterForTesting()
	defer extproc.UnregisterForTesting()

	// Start custom ExtProc server.
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	extprocServer := grpc.NewServer()

	v3procservicepb.RegisterExternalProcessorServer(extprocServer, &unsolicitedProcessorServer{})
	go extprocServer.Serve(lis)
	defer extprocServer.Stop()

	backend := stubserver.StartTestService(t, nil)
	defer backend.Stop()

	managementServer, nodeID, _, xdsResolver := setup.ManagementServerAndResolver(t)

	const serviceName = "my-service-unsolicited-response"
	extprocProto := &v3procfilterpb.ExternalProcessor{
		GrpcService: &v3corepb.GrpcService{
			TargetSpecifier: &v3corepb.GrpcService_GoogleGrpc_{
				GoogleGrpc: &v3corepb.GrpcService_GoogleGrpc{
					TargetUri: lis.Addr().String(),
				},
			},
		},
		ProcessingMode: &v3procfilterpb.ProcessingMode{
			RequestHeaderMode:  v3procfilterpb.ProcessingMode_SEND,
			ResponseHeaderMode: v3procfilterpb.ProcessingMode_SKIP,
			RequestBodyMode:    v3procfilterpb.ProcessingMode_NONE, // Skip request body events.
		},
		FailureModeAllow: false,
	}

	hcm := &v3httppb.HttpConnectionManager{
		RouteSpecifier: &v3httppb.HttpConnectionManager_RouteConfig{
			RouteConfig: &v3routepb.RouteConfiguration{
				Name: "route-config",
				VirtualHosts: []*v3routepb.VirtualHost{{
					Domains: []string{serviceName},
					Routes: []*v3routepb.Route{{
						Match: &v3routepb.RouteMatch{PathSpecifier: &v3routepb.RouteMatch_Prefix{Prefix: ""}},
						Action: &v3routepb.Route_Route{Route: &v3routepb.RouteAction{
							ClusterSpecifier: &v3routepb.RouteAction_Cluster{Cluster: "cluster-" + serviceName},
						}},
					}},
				}},
			},
		},
		HttpFilters: []*v3httppb.HttpFilter{
			{
				Name: "extproc",
				ConfigType: &v3httppb.HttpFilter_TypedConfig{
					TypedConfig: testutils.MarshalAny(t, extprocProto),
				},
			},
			e2e.RouterHTTPFilter,
		},
	}

	resources := e2e.DefaultClientResources(e2e.ResourceParams{
		DialTarget: serviceName,
		NodeID:     nodeID,
		Host:       "localhost",
		Port:       testutils.ParsePort(t, backend.Address),
		SecLevel:   e2e.SecurityLevelNone,
	})
	resources.Listeners[0].ApiListener.ApiListener = testutils.MarshalAny(t, hcm)

	ctx, cancel := context.WithTimeout(context.Background(), 50*defaultTestTimeout)
	defer cancel()
	if err := managementServer.Update(ctx, resources); err != nil {
		t.Fatal(err)
	}

	cc, err := grpc.NewClient("xds:///"+serviceName, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithResolvers(xdsResolver))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()

	client := testgrpc.NewTestServiceClient(cc)

	// Make the RPC call.
	errCh := make(chan error, 1)
	go func() {
		_, err := client.EmptyCall(ctx, &testpb.Empty{})
		errCh <- err
	}()

	// Verify that the data plane EmptyCall RPC fails with Internal status code.
	select {
	case err := <-errCh:
		t.Logf("ESHITA : Error received : %v", err)
		if err == nil {
			t.Fatalf("EmptyCall() succeeded unexpectedly when unsolicited response was sent")
		}
		if got, want := status.Code(err), codes.Internal; got != want {
			t.Fatalf("EmptyCall() returned status code: %v, want: %v", got, want)
		}
	case <-ctx.Done():
		t.Fatalf("timeout waiting for RPC completion")
	}
}

type unsolicitedProcessorServer struct {
	v3procservicepb.UnimplementedExternalProcessorServer
}

func (s *unsolicitedProcessorServer) Process(stream v3procservicepb.ExternalProcessor_ProcessServer) error {
	// 1. Receive request headers.
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	if req.GetRequestHeaders() == nil {
		return status.Errorf(codes.InvalidArgument, "expected request headers, got: %v", req)
	}

	// 2. Send the valid RequestHeaders response to initialize the stream.
	if err := stream.Send(&v3procservicepb.ProcessingResponse{
		Response: &v3procservicepb.ProcessingResponse_RequestHeaders{
			RequestHeaders: &v3procservicepb.HeadersResponse{
				Response: &v3procservicepb.CommonResponse{
					Status: v3procservicepb.CommonResponse_CONTINUE,
				},
			},
		},
	}); err != nil {
		return err
	}

	// 3. Unsolicitedly send a RequestBody response from the mock server.
	if err := stream.Send(&v3procservicepb.ProcessingResponse{
		Response: &v3procservicepb.ProcessingResponse_RequestBody{
			RequestBody: &v3procservicepb.BodyResponse{
				Response: &v3procservicepb.CommonResponse{
					Status: v3procservicepb.CommonResponse_CONTINUE,
					BodyMutation: &v3procservicepb.BodyMutation{
						Mutation: &v3procservicepb.BodyMutation_StreamedResponse{
							StreamedResponse: &v3procservicepb.StreamedBodyResponse{
								Body: []byte("unsolicited body"),
							},
						},
					},
				},
			},
		},
	}); err != nil {
		return err
	}

	return nil
}

type closeSendProcessorServer struct {
	v3procservicepb.UnimplementedExternalProcessorServer
	halfClosedCh chan struct{}
}

func (s *closeSendProcessorServer) Process(stream v3procservicepb.ExternalProcessor_ProcessServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if req.GetRequestHeaders() != nil {
			resp := &v3procservicepb.ProcessingResponse{
				Response: &v3procservicepb.ProcessingResponse_RequestHeaders{
					RequestHeaders: &v3procservicepb.HeadersResponse{
						Response: &v3procservicepb.CommonResponse{
							Status: v3procservicepb.CommonResponse_CONTINUE,
						},
					},
				},
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		} else if req.GetRequestBody() != nil {
			bodyReq := req.GetRequestBody()
			if bodyReq.GetEndOfStreamWithoutMessage() {
				close(s.halfClosedCh)
				resp := &v3procservicepb.ProcessingResponse{
					Response: &v3procservicepb.ProcessingResponse_RequestBody{
						RequestBody: &v3procservicepb.BodyResponse{
							Response: &v3procservicepb.CommonResponse{
								Status: v3procservicepb.CommonResponse_CONTINUE,
								BodyMutation: &v3procservicepb.BodyMutation{
									Mutation: &v3procservicepb.BodyMutation_StreamedResponse{
										StreamedResponse: &v3procservicepb.StreamedBodyResponse{
											EndOfStreamWithoutMessage: true,
										},
									},
								},
							},
						},
					},
				}
				if err := stream.Send(resp); err != nil {
					return err
				}
			} else {
				resp := &v3procservicepb.ProcessingResponse{
					Response: &v3procservicepb.ProcessingResponse_RequestBody{
						RequestBody: &v3procservicepb.BodyResponse{
							Response: &v3procservicepb.CommonResponse{
								Status: v3procservicepb.CommonResponse_CONTINUE,
								BodyMutation: &v3procservicepb.BodyMutation{
									Mutation: &v3procservicepb.BodyMutation_StreamedResponse{
										StreamedResponse: &v3procservicepb.StreamedBodyResponse{
											Body: bodyReq.GetBody(),
										},
									},
								},
							},
						},
					},
				}
				if err := stream.Send(resp); err != nil {
					return err
				}
			}
		}
	}
}

// TestClientSideXDS_ExtProc_ClientCloseSend verifies that when the client application
// calls CloseSend(), the filter sends an EndOfStreamWithoutMessage request to the
// ExtProc server, and after the server responds, the data plane stream is cleanly
// half-closed.
func (s) TestClientSideXDS_ExtProc_ClientCloseSend(t *testing.T) {
	testutils.SetEnvConfig(t, &envconfig.XDSClientExtProcEnabled, true)
	extproc.RegisterForTesting()
	defer extproc.UnregisterForTesting()

	// Start mock ExtProc server.
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	extprocServer := grpc.NewServer()
	mockProc := &closeSendProcessorServer{
		halfClosedCh: make(chan struct{}),
	}
	v3procservicepb.RegisterExternalProcessorServer(extprocServer, mockProc)
	go extprocServer.Serve(lis)
	defer extprocServer.Stop()

	// Capture streamed messages and half-close on the backend server.
	receivedCh := make(chan string, 10)
	halfClosedOnServerCh := make(chan struct{})
	backend := stubserver.StartTestService(t, &stubserver.StubServer{
		FullDuplexCallF: func(stream testpb.TestService_FullDuplexCallServer) error {
			for {
				req, err := stream.Recv()
				if err == io.EOF {
					close(halfClosedOnServerCh)
					return nil
				}
				if err != nil {
					return err
				}
				receivedCh <- string(req.GetPayload().GetBody())
			}
		},
	})
	defer backend.Stop()

	managementServer, nodeID, _, xdsResolver := setup.ManagementServerAndResolver(t)

	const serviceName = "my-service-close-send"
	extprocProto := &v3procfilterpb.ExternalProcessor{
		GrpcService: &v3corepb.GrpcService{
			TargetSpecifier: &v3corepb.GrpcService_GoogleGrpc_{
				GoogleGrpc: &v3corepb.GrpcService_GoogleGrpc{
					TargetUri: lis.Addr().String(),
				},
			},
		},
		ProcessingMode: &v3procfilterpb.ProcessingMode{
			RequestHeaderMode:  v3procfilterpb.ProcessingMode_SEND,
			RequestBodyMode:    v3procfilterpb.ProcessingMode_GRPC,
			ResponseHeaderMode: v3procfilterpb.ProcessingMode_SKIP,
		},
		FailureModeAllow: false,
	}

	hcm := &v3httppb.HttpConnectionManager{
		RouteSpecifier: &v3httppb.HttpConnectionManager_RouteConfig{
			RouteConfig: &v3routepb.RouteConfiguration{
				Name: "route-config",
				VirtualHosts: []*v3routepb.VirtualHost{{
					Domains: []string{serviceName},
					Routes: []*v3routepb.Route{{
						Match: &v3routepb.RouteMatch{PathSpecifier: &v3routepb.RouteMatch_Prefix{Prefix: ""}},
						Action: &v3routepb.Route_Route{Route: &v3routepb.RouteAction{
							ClusterSpecifier: &v3routepb.RouteAction_Cluster{Cluster: "cluster-" + serviceName},
						}},
					}},
				}},
			},
		},
		HttpFilters: []*v3httppb.HttpFilter{
			{
				Name: "extproc",
				ConfigType: &v3httppb.HttpFilter_TypedConfig{
					TypedConfig: testutils.MarshalAny(t, extprocProto),
				},
			},
			e2e.RouterHTTPFilter,
		},
	}

	resources := e2e.DefaultClientResources(e2e.ResourceParams{
		DialTarget: serviceName,
		NodeID:     nodeID,
		Host:       "localhost",
		Port:       testutils.ParsePort(t, backend.Address),
		SecLevel:   e2e.SecurityLevelNone,
	})
	resources.Listeners[0].ApiListener.ApiListener = testutils.MarshalAny(t, hcm)

	ctx, cancel := context.WithTimeout(context.Background(), 50*defaultTestTimeout)
	defer cancel()
	if err := managementServer.Update(ctx, resources); err != nil {
		t.Fatal(err)
	}

	cc, err := grpc.NewClient("xds:///"+serviceName, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithResolvers(xdsResolver))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()

	client := testgrpc.NewTestServiceClient(cc)
	stream, err := client.FullDuplexCall(ctx)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	// 1. Send one request message.
	req := &testpb.StreamingOutputCallRequest{
		Payload: &testpb.Payload{
			Body: []byte("message1"),
		},
	}
	if err := stream.Send(req); err != nil {
		t.Fatalf("failed to send message: %v", err)
	}

	// 2. Verify backend server received message 1.
	select {
	case msg := <-receivedCh:
		if msg != "message1" {
			t.Errorf("received message = %q, want \"message1\"", msg)
		}
	case <-ctx.Done():
		t.Fatalf("timeout waiting for message 1 on backend")
	}

	// 3. Call CloseSend() to trigger client half-close.
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("failed to CloseSend: %v", err)
	}

	// 4. Verify ExtProc server received EndOfStreamWithoutMessage.
	select {
	case <-mockProc.halfClosedCh:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for EndOfStreamWithoutMessage on mock processor")
	}

	// 5. Verify data plane server received half-close (stream.Recv returned EOF).
	select {
	case <-halfClosedOnServerCh:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for half-close on data plane server")
	}
}
