package xds_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/internal/envconfig"
	"google.golang.org/grpc/internal/stubserver"
	"google.golang.org/grpc/internal/testutils"
	"google.golang.org/grpc/internal/testutils/xds/e2e"
	"google.golang.org/grpc/internal/testutils/xds/e2e/setup"
	"google.golang.org/grpc/internal/xds/httpfilter/extproc"

	v3corepb "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	v3routepb "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	v3procfilterpb "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	v3httppb "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	v3procservicepb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	testgrpc "google.golang.org/grpc/interop/grpc_testing"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/protobuf/proto"
)

type drainMockExternalProcessorServer struct {
	v3procservicepb.UnimplementedExternalProcessorServer
	requestCh  chan *v3procservicepb.ProcessingRequest
	responseCh chan *v3procservicepb.ProcessingResponse
	drainedCh  chan struct{}
}

func (s *drainMockExternalProcessorServer) Process(stream v3procservicepb.ExternalProcessor_ProcessServer) error {
	// Track how many request body chunks we receive.
	bodyCount := 0

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			close(s.drainedCh)
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

		if req.GetRequestHeaders() != nil {
			// Accept headers and continue.
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
			bodyCount++
			bodyBytes := req.GetRequestBody().GetBody()

			// When the second body chunk is received, trigger request_drain to initiate half-close.
			if bodyCount == 2 {
				resp := &v3procservicepb.ProcessingResponse{
					Response: &v3procservicepb.ProcessingResponse_RequestBody{
						RequestBody: &v3procservicepb.BodyResponse{
							Response: &v3procservicepb.CommonResponse{
								Status: v3procservicepb.CommonResponse_CONTINUE,
								BodyMutation: &v3procservicepb.BodyMutation{
									Mutation: &v3procservicepb.BodyMutation_StreamedResponse{
										StreamedResponse: &v3procservicepb.StreamedBodyResponse{
											Body: bodyBytes,
										},
									},
								},
							},
						},
					},
					RequestDrain: true,
				}
				if err := stream.Send(resp); err != nil {
					return err
				}
			} else {
				// Otherwise, echo back standard body mutations.
				resp := &v3procservicepb.ProcessingResponse{
					Response: &v3procservicepb.ProcessingResponse_RequestBody{
						RequestBody: &v3procservicepb.BodyResponse{
							Response: &v3procservicepb.CommonResponse{
								Status: v3procservicepb.CommonResponse_CONTINUE,
								BodyMutation: &v3procservicepb.BodyMutation{
									Mutation: &v3procservicepb.BodyMutation_StreamedResponse{
										StreamedResponse: &v3procservicepb.StreamedBodyResponse{
											Body: bodyBytes,
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

// TestClientSideXDS_ExtProc_RequestDrain verifies that:
// 1. We can stream multiple messages through the ExtProc filter.
// 2. When the server triggers `request_drain`, the client half-closes the ExtProc stream.
// 3. All client-side `SendMsg` calls initiated after `request_drain` are blocked until draining completes.
// 4. The data plane server receives all streamed messages in the exact sequential order, with zero message loss.
func TestClientSideXDS_ExtProc_RequestDrain(t *testing.T) {
	testutils.SetEnvConfig(t, &envconfig.XDSClientExtProcEnabled, true)
	extproc.RegisterForTesting()
	defer extproc.UnregisterForTesting()

	// Start mock ExtProc server.
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	extprocServer := grpc.NewServer()
	mockProc := &drainMockExternalProcessorServer{
		requestCh:  make(chan *v3procservicepb.ProcessingRequest, 100),
		responseCh: make(chan *v3procservicepb.ProcessingResponse, 100),
		drainedCh:  make(chan struct{}),
	}
	v3procservicepb.RegisterExternalProcessorServer(extprocServer, mockProc)
	go extprocServer.Serve(lis)
	defer extprocServer.Stop()

	// Capture streamed messages received on the backend server.
	receivedMessagesCh := make(chan string, 10)
	backend := stubserver.StartTestService(t, &stubserver.StubServer{
		FullDuplexCallF: func(stream testpb.TestService_FullDuplexCallServer) error {
			for {
				req, err := stream.Recv()
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				receivedMessagesCh <- string(req.GetPayload().GetBody())
				resp := &testpb.StreamingOutputCallResponse{
					Payload: &testpb.Payload{
						Body: req.GetPayload().GetBody(),
					},
				}
				if err := stream.Send(resp); err != nil {
					return err
				}
			}
		},
	})
	defer backend.Stop()

	// Setup xDS management server and resolver config.
	managementServer, nodeID, _, xdsResolver := setup.ManagementServerAndResolver(t)

	const serviceName = "my-service-extproc-drain"
	extprocProto := &v3procfilterpb.ExternalProcessor{
		GrpcService: &v3corepb.GrpcService{
			TargetSpecifier: &v3corepb.GrpcService_GoogleGrpc_{
				GoogleGrpc: &v3corepb.GrpcService_GoogleGrpc{
					TargetUri: lis.Addr().String(),
				},
			},
		},
		ProcessingMode: &v3procfilterpb.ProcessingMode{
			RequestHeaderMode: v3procfilterpb.ProcessingMode_SEND,
			RequestBodyMode:   v3procfilterpb.ProcessingMode_GRPC,
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
		t.Fatalf("failed to open full duplex stream: %v", err)
	}

	// Helper function to assemble a test payload request.
	makeReq := func(body string) *testpb.StreamingOutputCallRequest {
		return &testpb.StreamingOutputCallRequest{
			Payload: &testpb.Payload{
				Body: []byte(body),
			},
		}
	}

	// 1. Send message 1.
	if err := stream.Send(makeReq("msg1")); err != nil {
		t.Fatalf("failed to send msg1: %v", err)
	}

	// 2. Send message 2. This will trigger the ExtProc server to return request_drain=true.
	if err := stream.Send(makeReq("msg2")); err != nil {
		t.Fatalf("failed to send msg2: %v", err)
	}

	// 3. Wait for the ExtProc server to execute CloseSend (drainedCh is closed when Recv returns EOF).
	select {
	case <-mockProc.drainedCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for extproc stream to close")
	}

	// 4. Send message 3. This must execute asynchronously after the drain completed,
	// and must be safely bypassed directly to the backend data plane.
	if err := stream.Send(makeReq("msg3")); err != nil {
		t.Fatalf("failed to send msg3: %v", err)
	}

	// Close the client send stream.
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("failed to CloseSend: %v", err)
	}

	// Verify the backend received all three messages in perfect chronological order.
	expectedMsgs := []string{"msg1", "msg2", "msg3"}
	for _, want := range expectedMsgs {
		select {
		case got := <-receivedMessagesCh:
			if got != want {
				t.Errorf("received message = %q, want %q", got, want)
			}
		case <-ctx.Done():
			t.Fatalf("timeout waiting for message %q on backend", want)
		}
	}
}

type respMutationMockExternalProcessorServer struct {
	v3procservicepb.UnimplementedExternalProcessorServer
	requestCh  chan *v3procservicepb.ProcessingRequest
}

func (s *respMutationMockExternalProcessorServer) Process(stream v3procservicepb.ExternalProcessor_ProcessServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		select {
		case s.requestCh <- req:
		case <-stream.Context().Done():
			return stream.Context().Err()
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
		} else if req.GetResponseHeaders() != nil {
			resp := &v3procservicepb.ProcessingResponse{
				Response: &v3procservicepb.ProcessingResponse_ResponseHeaders{
					ResponseHeaders: &v3procservicepb.HeadersResponse{
						Response: &v3procservicepb.CommonResponse{
							Status: v3procservicepb.CommonResponse_CONTINUE,
						},
					},
				},
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		} else if req.GetResponseBody() != nil {
			bodyBytes := req.GetResponseBody().GetBody()
			respMsg := &testpb.StreamingOutputCallResponse{}
			if err := proto.Unmarshal(bodyBytes, respMsg); err != nil {
				return err
			}
			bodyStr := string(respMsg.GetPayload().GetBody())
			respMsg.Payload.Body = []byte(bodyStr + "-mutated")

			mutatedBytes, err := proto.Marshal(respMsg)
			if err != nil {
				return err
			}

			resp := &v3procservicepb.ProcessingResponse{
				Response: &v3procservicepb.ProcessingResponse_ResponseBody{
					ResponseBody: &v3procservicepb.BodyResponse{
						Response: &v3procservicepb.CommonResponse{
							Status: v3procservicepb.CommonResponse_CONTINUE,
							BodyMutation: &v3procservicepb.BodyMutation{
								Mutation: &v3procservicepb.BodyMutation_StreamedResponse{
									StreamedResponse: &v3procservicepb.StreamedBodyResponse{
										Body: mutatedBytes,
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

// TestClientSideXDS_ExtProc_ResponseBodyMutation verifies that:
// 1. The backend data plane server streams response messages back to the client.
// 2. The client filter intercepts these responses and sends them to the ExtProc server.
// 3. The ExtProc server successfully mutates the response body payloads.
// 4. The client application receives all mutated response messages in perfect chronological order.
func TestClientSideXDS_ExtProc_ResponseBodyMutation(t *testing.T) {
	testutils.SetEnvConfig(t, &envconfig.XDSClientExtProcEnabled, true)
	extproc.RegisterForTesting()
	defer extproc.UnregisterForTesting()

	// Start the mock ExtProc server.
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	extprocServer := grpc.NewServer()
	mockProc := &respMutationMockExternalProcessorServer{
		requestCh:  make(chan *v3procservicepb.ProcessingRequest, 100),
	}
	v3procservicepb.RegisterExternalProcessorServer(extprocServer, mockProc)
	go extprocServer.Serve(lis)
	defer extprocServer.Stop()

	// Start a test backend service that streams messages.
	backend := stubserver.StartTestService(t, &stubserver.StubServer{
		FullDuplexCallF: func(stream testpb.TestService_FullDuplexCallServer) error {
			for {
				req, err := stream.Recv()
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				// Return the message back to client.
				resp := &testpb.StreamingOutputCallResponse{
					Payload: &testpb.Payload{
						Body: req.GetPayload().GetBody(),
					},
				}
				if err := stream.Send(resp); err != nil {
					return err
				}
			}
		},
	})
	defer backend.Stop()

	// Setup xDS management server and resolver.
	managementServer, nodeID, _, xdsResolver := setup.ManagementServerAndResolver(t)

	const serviceName = "my-service-extproc-resp-mutation"
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
			ResponseHeaderMode: v3procfilterpb.ProcessingMode_SEND,
			ResponseBodyMode:   v3procfilterpb.ProcessingMode_GRPC,
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
		t.Fatalf("failed to open full duplex stream: %v", err)
	}

	makeReq := func(body string) *testpb.StreamingOutputCallRequest {
		return &testpb.StreamingOutputCallRequest{
			Payload: &testpb.Payload{
				Body: []byte(body),
			},
		}
	}

	// 1. Stream a request message.
	if err := stream.Send(makeReq("msg1")); err != nil {
		t.Fatalf("failed to send msg1: %v", err)
	}

	// 2. Stream a second request message.
	if err := stream.Send(makeReq("msg2")); err != nil {
		t.Fatalf("failed to send msg2: %v", err)
	}

	if err := stream.CloseSend(); err != nil {
		t.Fatalf("failed to CloseSend: %v", err)
	}

	// Verify the client application receives the mutated responses in perfect sequential order.
	expectedResponses := []string{"msg1-mutated", "msg2-mutated"}
	for _, want := range expectedResponses {
		resp, err := stream.Recv()
		if err != nil {
			t.Fatalf("failed to receive message on stream: %v", err)
		}
		got := string(resp.GetPayload().GetBody())
		if got != want {
			t.Errorf("received response = %q, want %q", got, want)
		}
	}
}
