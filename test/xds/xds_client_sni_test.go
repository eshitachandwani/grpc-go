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
	"crypto/tls"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	xdscreds "google.golang.org/grpc/credentials/xds"
	"google.golang.org/grpc/internal/envconfig"
	"google.golang.org/grpc/internal/stubserver"
	"google.golang.org/grpc/internal/testutils"
	"google.golang.org/grpc/internal/testutils/xds/e2e"
	"google.golang.org/grpc/internal/testutils/xds/e2e/setup"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	v3clusterpb "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	v3corepb "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	v3endpointpb "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	v3listenerpb "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	v3routepb "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	v3tlspb "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	v3matcherpb "github.com/envoyproxy/go-control-plane/envoy/type/matcher/v3"
	testgrpc "google.golang.org/grpc/interop/grpc_testing"
	testpb "google.golang.org/grpc/interop/grpc_testing"
)

const defaultTestCertSAN = "x.test.example.com"

func TestClientSideXDS_SNISANValidation(t *testing.T) {
	testutils.SetEnvConfig(t, &envconfig.XDSSNIEnabled, true)

	// Spin up an xDS management server.
	mgmtServer, nodeID, _, xdsResolver := setup.ManagementServerAndResolver(t)

	// Create test backends for two clusters
	// backend1 configured with TLS creds, represents cluster1 (valid SNI)
	// backend2 configured with TLS creds, represents cluster2 (invalid SNI)
	serverCreds := testutils.CreateServerTLSCredentials(t, tls.RequireAndVerifyClientCert)
	server1 := stubserver.StartTestService(t, nil, grpc.Creds(serverCreds))
	defer server1.Stop()

	server2 := stubserver.StartTestService(t, nil, grpc.Creds(serverCreds))
	defer server2.Stop()

	// Configure client side xDS resources on the management server.
	const serviceName = "my-service-client-side-xds"
	const routeConfigName = "route-" + serviceName
	const clusterName1 = "cluster1-" + serviceName
	const clusterName2 = "cluster2-" + serviceName
	const endpointsName1 = "endpoints1-" + serviceName
	const endpointsName2 = "endpoints2-" + serviceName

	listeners := []*v3listenerpb.Listener{e2e.DefaultClientListener(serviceName, routeConfigName)}

	// Route configuration:
	// - "/grpc.testing.TestService/EmptyCall" --> cluster1 (valid SNI)
	// - "/grpc.testing.TestService/UnaryCall" --> cluster2 (invalid SNI)
	routes := []*v3routepb.RouteConfiguration{{
		Name: routeConfigName,
		VirtualHosts: []*v3routepb.VirtualHost{{
			Domains: []string{serviceName},
			Routes: []*v3routepb.Route{
				{
					Match: &v3routepb.RouteMatch{PathSpecifier: &v3routepb.RouteMatch_Prefix{Prefix: "/grpc.testing.TestService/EmptyCall"}},
					Action: &v3routepb.Route_Route{Route: &v3routepb.RouteAction{
						ClusterSpecifier: &v3routepb.RouteAction_Cluster{Cluster: clusterName1},
					}},
				},
				{
					Match: &v3routepb.RouteMatch{PathSpecifier: &v3routepb.RouteMatch_Prefix{Prefix: "/grpc.testing.TestService/UnaryCall"}},
					Action: &v3routepb.Route_Route{Route: &v3routepb.RouteAction{
						ClusterSpecifier: &v3routepb.RouteAction_Cluster{Cluster: clusterName2},
					}},
				},
			},
		}},
	}}

	tlsContext := &v3tlspb.UpstreamTlsContext{
		Sni:                  defaultTestCertSAN,
		AutoSniSanValidation: true,
		CommonTlsContext: &v3tlspb.CommonTlsContext{
			ValidationContextType: &v3tlspb.CommonTlsContext_ValidationContextCertificateProviderInstance{
				ValidationContextCertificateProviderInstance: &v3tlspb.CommonTlsContext_CertificateProviderInstance{
					InstanceName: e2e.ClientSideCertProviderInstance,
				},
			},
			TlsCertificateCertificateProviderInstance: &v3tlspb.CommonTlsContext_CertificateProviderInstance{
				InstanceName: e2e.ClientSideCertProviderInstance,
			},
		},
	}
	cluster1 := e2e.DefaultCluster(clusterName1, endpointsName1, e2e.SecurityLevelMTLS)
	cluster1.TransportSocket = &v3corepb.TransportSocket{
		Name: "envoy.transport_sockets.tls",
		ConfigType: &v3corepb.TransportSocket_TypedConfig{
			TypedConfig: marshalAny(tlsContext),
		},
	}
	cluster2 := e2e.DefaultCluster(clusterName2, endpointsName2, e2e.SecurityLevelMTLS)
	cluster2.TransportSocket = &v3corepb.TransportSocket{
		Name: "envoy.transport_sockets.tls",
		ConfigType: &v3corepb.TransportSocket_TypedConfig{
			TypedConfig: testutils.MarshalAny(t, &v3tlspb.UpstreamTlsContext{
				Sni:                  "wrong.sni.domain",
				AutoSniSanValidation: true,
				CommonTlsContext: &v3tlspb.CommonTlsContext{
					ValidationContextType: &v3tlspb.CommonTlsContext_ValidationContextCertificateProviderInstance{
						ValidationContextCertificateProviderInstance: &v3tlspb.CommonTlsContext_CertificateProviderInstance{
							InstanceName:    e2e.ClientSideCertProviderInstance,
							CertificateName: "root",
						},
					},
					TlsCertificateCertificateProviderInstance: &v3tlspb.CommonTlsContext_CertificateProviderInstance{
						InstanceName:    e2e.ClientSideCertProviderInstance,
						CertificateName: "identity",
					},
				},
			}),
		},
	}

	clusters := []*v3clusterpb.Cluster{cluster1, cluster2}

	// Endpoints for each of the above clusters with backends created earlier.
	endpoints := []*v3endpointpb.ClusterLoadAssignment{
		e2e.DefaultEndpoint(endpointsName1, "localhost", []uint32{testutils.ParsePort(t, server1.Address)}),
		e2e.DefaultEndpoint(endpointsName2, "localhost", []uint32{testutils.ParsePort(t, server2.Address)}),
	}

	resources := e2e.UpdateOptions{
		NodeID:    nodeID,
		Listeners: listeners,
		Routes:    routes,
		Clusters:  clusters,
		Endpoints: endpoints,
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if err := mgmtServer.Update(ctx, resources); err != nil {
		t.Fatal(err)
	}

	// Create client-side xDS credentials.
	clientCreds, err := xdscreds.NewClientCredentials(xdscreds.ClientOptions{FallbackCreds: insecure.NewCredentials()})
	if err != nil {
		t.Fatal(err)
	}

	// Create a ClientConn.
	cc, err := grpc.NewClient(fmt.Sprintf("xds:///%s", serviceName), grpc.WithTransportCredentials(clientCreds), grpc.WithResolvers(xdsResolver))
	if err != nil {
		t.Fatalf("failed to dial local test server: %v", err)
	}
	defer cc.Close()

	client := testgrpc.NewTestServiceClient(cc)

	// RPC to cluster1 should succeed because auto_sni_san_validation is true
	// and sni matches server cert SAN.
	peerInfo := &peer.Peer{}
	if _, err := client.EmptyCall(ctx, &testpb.Empty{}, grpc.WaitForReady(true), grpc.Peer(peerInfo)); err != nil {
		t.Fatalf("EmptyCall() failed: %v", err)
	}
	if got, want := peerInfo.Addr.String(), server1.Address; got != want {
		t.Errorf("EmptyCall() routed to %q, want to be routed to: %q", got, want)
	}

	// RPC to cluster2 should fail because even though auto_sni_san_validation
	// is true, sni doesn't match server cert SAN.
	const wantErr = "do not match the SNI"
	if _, err := client.UnaryCall(ctx, &testpb.SimpleRequest{}); status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("UnaryCall() failed: %v, wantCode: %s, wantErr: %s", err, codes.Unavailable, wantErr)
	}
}

func marshalAny(m proto.Message) *anypb.Any {
	a, err := anypb.New(m)
	if err != nil {
		panic(fmt.Sprintf("anypb.New(%+v) failed: %v", m, err))
	}
	return a
}

func TestClientSideXDS_AutoHostSNIValidation(t *testing.T) {
	testutils.SetEnvConfig(t, &envconfig.XDSSNIEnabled, true)

	// Spin up an xDS management server.
	mgmtServer, nodeID, _, xdsResolver := setup.ManagementServerAndResolver(t)

	// Create test backend
	serverCreds := testutils.CreateServerTLSCredentials(t, tls.RequireAndVerifyClientCert)
	server := stubserver.StartTestService(t, nil, grpc.Creds(serverCreds))
	defer server.Stop()

	// Configure client side xDS resources on the management server.
	const serviceName = "my-service-client-side-xds"
	const routeConfigName = "route-" + serviceName
	const clusterName = "cluster-" + serviceName
	const endpointsName = "endpoints-" + serviceName

	listeners := []*v3listenerpb.Listener{e2e.DefaultClientListener(serviceName, routeConfigName)}

	routes := []*v3routepb.RouteConfiguration{{
		Name: routeConfigName,
		VirtualHosts: []*v3routepb.VirtualHost{{
			Domains: []string{serviceName},
			Routes: []*v3routepb.Route{
				{
					Match: &v3routepb.RouteMatch{PathSpecifier: &v3routepb.RouteMatch_Prefix{Prefix: "/"}},
					Action: &v3routepb.Route_Route{Route: &v3routepb.RouteAction{
						ClusterSpecifier: &v3routepb.RouteAction_Cluster{Cluster: clusterName},
					}},
				},
			},
		}},
	}}

	cluster := e2e.DefaultCluster(clusterName, endpointsName, e2e.SecurityLevelMTLS)
	cluster.TransportSocket = &v3corepb.TransportSocket{
		Name: "envoy.transport_sockets.tls",
		ConfigType: &v3corepb.TransportSocket_TypedConfig{
			TypedConfig: testutils.MarshalAny(t, &v3tlspb.UpstreamTlsContext{
				AutoHostSni:          true,
				AutoSniSanValidation: true,
				CommonTlsContext: &v3tlspb.CommonTlsContext{
					ValidationContextType: &v3tlspb.CommonTlsContext_ValidationContextCertificateProviderInstance{
						ValidationContextCertificateProviderInstance: &v3tlspb.CommonTlsContext_CertificateProviderInstance{
							InstanceName:    e2e.ClientSideCertProviderInstance,
							CertificateName: "root",
						},
					},
					TlsCertificateCertificateProviderInstance: &v3tlspb.CommonTlsContext_CertificateProviderInstance{
						InstanceName:    e2e.ClientSideCertProviderInstance,
						CertificateName: "identity",
					},
				},
			}),
		},
	}

	// Endpoints configuring Hostname to the defaultTestCertSAN to verify AutoHostSni usage
	endpoints := []*v3endpointpb.ClusterLoadAssignment{
		e2e.EndpointResourceWithOptions(e2e.EndpointOptions{
			ClusterName: endpointsName,
			Host:        "localhost",
			Localities: []e2e.LocalityOptions{{
				Weight: 1,
				Backends: []e2e.BackendOptions{{
					Ports:    []uint32{testutils.ParsePort(t, server.Address)},
					Hostname: defaultTestCertSAN,
				}},
			}},
		}),
	}

	resources := e2e.UpdateOptions{
		NodeID:    nodeID,
		Listeners: listeners,
		Routes:    routes,
		Clusters:  []*v3clusterpb.Cluster{cluster},
		Endpoints: endpoints,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*defaultTestTimeout)
	defer cancel()
	if err := mgmtServer.Update(ctx, resources); err != nil {
		t.Fatal(err)
	}

	// Create client-side xDS credentials.
	clientCreds, err := xdscreds.NewClientCredentials(xdscreds.ClientOptions{FallbackCreds: insecure.NewCredentials()})
	if err != nil {
		t.Fatal(err)
	}

	// Create a ClientConn.
	cc, err := grpc.NewClient(fmt.Sprintf("xds:///%s", serviceName), grpc.WithTransportCredentials(clientCreds), grpc.WithResolvers(xdsResolver))
	if err != nil {
		t.Fatalf("failed to dial local test server: %v", err)
	}
	defer cc.Close()

	client := testgrpc.NewTestServiceClient(cc)

	// RPC should succeed because auto_host_sni sets SNI from the endpoint hostname
	// and auto_sni_san_validation validates that the SNI matches server cert SAN.
	peerInfo := &peer.Peer{}
	if _, err := client.EmptyCall(ctx, &testpb.Empty{}, grpc.WaitForReady(true), grpc.Peer(peerInfo)); err != nil {
		t.Fatalf("EmptyCall() failed: %v", err)
	}
	if got, want := peerInfo.Addr.String(), server.Address; got != want {
		t.Errorf("EmptyCall() routed to %q, want to be routed to: %q", got, want)
	}
}

func (s) TestClientSideXDS_FallbackSANMatchers(t *testing.T) {
	testutils.SetEnvConfig(t, &envconfig.XDSSNIEnabled, true)

	// Spin up an xDS management server.
	mgmtServer, nodeID, _, xdsResolver := setup.ManagementServerAndResolver(t)

	// Create test backends
	serverCreds := testutils.CreateServerTLSCredentials(t, tls.RequireAndVerifyClientCert)
	server1 := stubserver.StartTestService(t, nil, grpc.Creds(serverCreds))
	defer server1.Stop()

	server2 := stubserver.StartTestService(t, nil, grpc.Creds(serverCreds))
	defer server2.Stop()

	// Configure client side xDS resources on the management server.
	const serviceName = "my-service-client-side-xds"
	const routeConfigName = "route-" + serviceName
	const clusterName1 = "cluster1-" + serviceName
	const clusterName2 = "cluster2-" + serviceName
	const endpointsName1 = "endpoints1-" + serviceName
	const endpointsName2 = "endpoints2-" + serviceName

	listeners := []*v3listenerpb.Listener{e2e.DefaultClientListener(serviceName, routeConfigName)}

	routes := []*v3routepb.RouteConfiguration{{
		Name: routeConfigName,
		VirtualHosts: []*v3routepb.VirtualHost{{
			Domains: []string{serviceName},
			Routes: []*v3routepb.Route{
				{
					Match: &v3routepb.RouteMatch{PathSpecifier: &v3routepb.RouteMatch_Prefix{Prefix: "/grpc.testing.TestService/EmptyCall"}},
					Action: &v3routepb.Route_Route{Route: &v3routepb.RouteAction{
						ClusterSpecifier: &v3routepb.RouteAction_Cluster{Cluster: clusterName1},
					}},
				},
				{
					Match: &v3routepb.RouteMatch{PathSpecifier: &v3routepb.RouteMatch_Prefix{Prefix: "/grpc.testing.TestService/UnaryCall"}},
					Action: &v3routepb.Route_Route{Route: &v3routepb.RouteAction{
						ClusterSpecifier: &v3routepb.RouteAction_Cluster{Cluster: clusterName2},
					}},
				},
			},
		}},
	}}

	tlsContext := &v3tlspb.UpstreamTlsContext{
		AutoSniSanValidation: true,
		CommonTlsContext: &v3tlspb.CommonTlsContext{
			ValidationContextType: &v3tlspb.CommonTlsContext_CombinedValidationContext{
				CombinedValidationContext: &v3tlspb.CommonTlsContext_CombinedCertificateValidationContext{
					DefaultValidationContext: &v3tlspb.CertificateValidationContext{
						MatchSubjectAltNames: []*v3matcherpb.StringMatcher{
							{MatchPattern: &v3matcherpb.StringMatcher_Exact{Exact: "*.test.example.com"}},
						},
						CaCertificateProviderInstance: &v3tlspb.CertificateProviderPluginInstance{
							InstanceName:    e2e.ClientSideCertProviderInstance,
							CertificateName: "root",
						},
					},
				},
			},
			TlsCertificateProviderInstance: &v3tlspb.CertificateProviderPluginInstance{
				InstanceName:    e2e.ClientSideCertProviderInstance,
				CertificateName: "identity",
			},
		},
	}
	cluster1 := e2e.DefaultCluster(clusterName1, endpointsName1, e2e.SecurityLevelMTLS)
	cluster1.TransportSocket = &v3corepb.TransportSocket{
		Name: "envoy.transport_sockets.tls",
		ConfigType: &v3corepb.TransportSocket_TypedConfig{
			TypedConfig: marshalAny(tlsContext),
		},
	}
	cluster2 := e2e.DefaultCluster(clusterName2, endpointsName2, e2e.SecurityLevelMTLS)
	cluster2.TransportSocket = &v3corepb.TransportSocket{
		Name: "envoy.transport_sockets.tls",
		ConfigType: &v3corepb.TransportSocket_TypedConfig{
			TypedConfig: marshalAny(&v3tlspb.UpstreamTlsContext{
				AutoSniSanValidation: true,
				CommonTlsContext: &v3tlspb.CommonTlsContext{
					ValidationContextType: &v3tlspb.CommonTlsContext_CombinedValidationContext{
						CombinedValidationContext: &v3tlspb.CommonTlsContext_CombinedCertificateValidationContext{
							DefaultValidationContext: &v3tlspb.CertificateValidationContext{
								MatchSubjectAltNames: []*v3matcherpb.StringMatcher{
									{MatchPattern: &v3matcherpb.StringMatcher_Exact{Exact: "wrong.san.domain"}},
								},
								CaCertificateProviderInstance: &v3tlspb.CertificateProviderPluginInstance{
									InstanceName:    e2e.ClientSideCertProviderInstance,
									CertificateName: "root",
								},
							},
						},
					},
					TlsCertificateProviderInstance: &v3tlspb.CertificateProviderPluginInstance{
						InstanceName:    e2e.ClientSideCertProviderInstance,
						CertificateName: "identity",
					},
				},
			}),
		},
	}

	endpoints := []*v3endpointpb.ClusterLoadAssignment{
		e2e.DefaultEndpoint(endpointsName1, "localhost", []uint32{testutils.ParsePort(t, server1.Address)}),
		e2e.DefaultEndpoint(endpointsName2, "localhost", []uint32{testutils.ParsePort(t, server2.Address)}),
	}

	resources := e2e.UpdateOptions{
		NodeID:    nodeID,
		Listeners: listeners,
		Routes:    routes,
		Clusters:  []*v3clusterpb.Cluster{cluster1, cluster2},
		Endpoints: endpoints,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*defaultTestTimeout)
	defer cancel()
	if err := mgmtServer.Update(ctx, resources); err != nil {
		t.Fatal(err)
	}

	// Create client-side xDS credentials.
	clientCreds, err := xdscreds.NewClientCredentials(xdscreds.ClientOptions{FallbackCreds: insecure.NewCredentials()})
	if err != nil {
		t.Fatal(err)
	}

	// Create a ClientConn.
	cc, err := grpc.NewClient(fmt.Sprintf("xds:///%s", serviceName), grpc.WithTransportCredentials(clientCreds), grpc.WithResolvers(xdsResolver))
	if err != nil {
		t.Fatalf("failed to dial local test server: %v", err)
	}
	defer cc.Close()

	client := testgrpc.NewTestServiceClient(cc)

	// RPC to cluster1 should succeed because fallback SAN mathchers are used
	// and they match server cert SAN.
	peerInfo := &peer.Peer{}
	if _, err := client.EmptyCall(ctx, &testpb.Empty{}, grpc.WaitForReady(true), grpc.Peer(peerInfo)); err != nil {
		t.Fatalf("EmptyCall() failed: %v", err)
	}
	if got, want := peerInfo.Addr.String(), server1.Address; got != want {
		t.Errorf("EmptyCall() routed to %q, want to be routed to: %q", got, want)
	}

	// RPC to cluster2 should fail because fallback SAN matchers are used
	// but they don't match server cert SAN.
	const wantErr = "do not match any of the accepted SANs"
	if _, err := client.UnaryCall(ctx, &testpb.SimpleRequest{}); status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("UnaryCall() failed: %v, wantCode: %s, wantErr: %s", err, codes.Unavailable, wantErr)
	}
}

func (s) TestClientSideXDS_SNIDisabledSANMatchers(t *testing.T) {
	testutils.SetEnvConfig(t, &envconfig.XDSSNIEnabled, false)

	// Spin up an xDS management server.
	mgmtServer, nodeID, _, xdsResolver := setup.ManagementServerAndResolver(t)

	// Create test backends
	serverCreds := testutils.CreateServerTLSCredentials(t, tls.RequireAndVerifyClientCert)
	server1 := stubserver.StartTestService(t, nil, grpc.Creds(serverCreds))
	defer server1.Stop()

	server2 := stubserver.StartTestService(t, nil, grpc.Creds(serverCreds))
	defer server2.Stop()

	// Configure client side xDS resources on the management server.
	const serviceName = "my-service-client-side-xds"
	const routeConfigName = "route-" + serviceName
	const clusterName1 = "cluster1-" + serviceName
	const clusterName2 = "cluster2-" + serviceName
	const endpointsName1 = "endpoints1-" + serviceName
	const endpointsName2 = "endpoints2-" + serviceName

	listeners := []*v3listenerpb.Listener{e2e.DefaultClientListener(serviceName, routeConfigName)}

	routes := []*v3routepb.RouteConfiguration{{
		Name: routeConfigName,
		VirtualHosts: []*v3routepb.VirtualHost{{
			Domains: []string{serviceName},
			Routes: []*v3routepb.Route{
				{
					Match: &v3routepb.RouteMatch{PathSpecifier: &v3routepb.RouteMatch_Prefix{Prefix: "/grpc.testing.TestService/EmptyCall"}},
					Action: &v3routepb.Route_Route{Route: &v3routepb.RouteAction{
						ClusterSpecifier: &v3routepb.RouteAction_Cluster{Cluster: clusterName1},
					}},
				},
				{
					Match: &v3routepb.RouteMatch{PathSpecifier: &v3routepb.RouteMatch_Prefix{Prefix: "/grpc.testing.TestService/UnaryCall"}},
					Action: &v3routepb.Route_Route{Route: &v3routepb.RouteAction{
						ClusterSpecifier: &v3routepb.RouteAction_Cluster{Cluster: clusterName2},
					}},
				},
			},
		}},
	}}

	tlsContext := &v3tlspb.UpstreamTlsContext{
		Sni:                  "incorrect.sni",
		AutoSniSanValidation: true,
		CommonTlsContext: &v3tlspb.CommonTlsContext{
			ValidationContextType: &v3tlspb.CommonTlsContext_CombinedValidationContext{
				CombinedValidationContext: &v3tlspb.CommonTlsContext_CombinedCertificateValidationContext{
					DefaultValidationContext: &v3tlspb.CertificateValidationContext{
						MatchSubjectAltNames: []*v3matcherpb.StringMatcher{
							{MatchPattern: &v3matcherpb.StringMatcher_Exact{Exact: "*.test.example.com"}},
						},
						CaCertificateProviderInstance: &v3tlspb.CertificateProviderPluginInstance{
							InstanceName:    e2e.ClientSideCertProviderInstance,
							CertificateName: "root",
						},
					},
				},
			},
			TlsCertificateProviderInstance: &v3tlspb.CertificateProviderPluginInstance{
				InstanceName:    e2e.ClientSideCertProviderInstance,
				CertificateName: "identity",
			},
		},
	}
	cluster1 := e2e.DefaultCluster(clusterName1, endpointsName1, e2e.SecurityLevelMTLS)
	cluster1.TransportSocket = &v3corepb.TransportSocket{
		Name: "envoy.transport_sockets.tls",
		ConfigType: &v3corepb.TransportSocket_TypedConfig{
			TypedConfig: marshalAny(tlsContext),
		},
	}
	cluster2 := e2e.DefaultCluster(clusterName2, endpointsName2, e2e.SecurityLevelMTLS)
	cluster2.TransportSocket = &v3corepb.TransportSocket{
		Name: "envoy.transport_sockets.tls",
		ConfigType: &v3corepb.TransportSocket_TypedConfig{
			TypedConfig: marshalAny(&v3tlspb.UpstreamTlsContext{
				Sni:                  defaultTestCertSAN,
				AutoSniSanValidation: true,
				CommonTlsContext: &v3tlspb.CommonTlsContext{
					ValidationContextType: &v3tlspb.CommonTlsContext_CombinedValidationContext{
						CombinedValidationContext: &v3tlspb.CommonTlsContext_CombinedCertificateValidationContext{
							DefaultValidationContext: &v3tlspb.CertificateValidationContext{
								MatchSubjectAltNames: []*v3matcherpb.StringMatcher{
									{MatchPattern: &v3matcherpb.StringMatcher_Exact{Exact: "wrong.san.domain"}},
								},
								CaCertificateProviderInstance: &v3tlspb.CertificateProviderPluginInstance{
									InstanceName:    e2e.ClientSideCertProviderInstance,
									CertificateName: "root",
								},
							},
						},
					},
					TlsCertificateProviderInstance: &v3tlspb.CertificateProviderPluginInstance{
						InstanceName:    e2e.ClientSideCertProviderInstance,
						CertificateName: "identity",
					},
				},
			}),
		},
	}

	endpoints := []*v3endpointpb.ClusterLoadAssignment{
		e2e.DefaultEndpoint(endpointsName1, "localhost", []uint32{testutils.ParsePort(t, server1.Address)}),
		e2e.DefaultEndpoint(endpointsName2, "localhost", []uint32{testutils.ParsePort(t, server2.Address)}),
	}

	resources := e2e.UpdateOptions{
		NodeID:    nodeID,
		Listeners: listeners,
		Routes:    routes,
		Clusters:  []*v3clusterpb.Cluster{cluster1, cluster2},
		Endpoints: endpoints,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*defaultTestTimeout)
	defer cancel()
	if err := mgmtServer.Update(ctx, resources); err != nil {
		t.Fatal(err)
	}

	// Create client-side xDS credentials.
	clientCreds, err := xdscreds.NewClientCredentials(xdscreds.ClientOptions{FallbackCreds: insecure.NewCredentials()})
	if err != nil {
		t.Fatal(err)
	}

	// Create a ClientConn.
	cc, err := grpc.NewClient(fmt.Sprintf("xds:///%s", serviceName), grpc.WithTransportCredentials(clientCreds), grpc.WithResolvers(xdsResolver))
	if err != nil {
		t.Fatalf("failed to dial local test server: %v", err)
	}
	defer cc.Close()

	client := testgrpc.NewTestServiceClient(cc)

	// RPC to cluster1 should succeed because fallback SAN mathchers are used
	// and they match server cert SAN.
	peerInfo := &peer.Peer{}
	if _, err := client.EmptyCall(ctx, &testpb.Empty{}, grpc.WaitForReady(true), grpc.Peer(peerInfo)); err != nil {
		t.Fatalf("EmptyCall() failed: %v", err)
	}
	if got, want := peerInfo.Addr.String(), server1.Address; got != want {
		t.Errorf("EmptyCall() routed to %q, want to be routed to: %q", got, want)
	}

	// RPC to cluster2 should fail because fallback SAN matchers are used
	// but they don't match server cert SAN.
	const wantErr = "do not match any of the accepted SANs"
	if _, err := client.UnaryCall(ctx, &testpb.SimpleRequest{}); status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("UnaryCall() failed: %v, wantCode: %s, wantErr: %s", err, codes.Unavailable, wantErr)
	}
}
