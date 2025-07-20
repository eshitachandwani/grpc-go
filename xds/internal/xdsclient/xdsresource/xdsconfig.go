/*
 * Copyright 2025 gRPC authors.
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

package xdsresource

import (
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
)

type XdsConfig struct {
	// listener resource
	Listener ListenerUpdate
	// RouteConfig resource.  Will be populated even if RouteConfig is
	// inlined into the Listener resource.
	Route_config RouteConfigUpdate
	// Virtual host.  Points into route_config.  Will always be non-null.
	Virtual_host *VirtualHost
	// Cluster map.  A cluster will have a non-OK status if either
	// (a) there was an error and we did not already have a valid
	// resource or (b) the resource does not exist.
	Clusters map[string]ClusterConfigOrError
}

type ClusterConfigOrError struct {
	Err error
	// pointer to make it nil if error is present - check if necessary
	Cluster_config ClusterConfig
}

type ClusterConfig struct {
	// Cluster resource.
	Cluster  ClusterUpdate
	Children ClusterChild
}

type EndpointConfig struct {
	// Endpoint info for EDS and LOGICAL_DNS clusters.  If there was an
	// error, endpoints will be null and resolution_note will be set.
	Endpoints       EndpointsResource
	Resolution_note string
}

// emchandwani this is new or update the EndpointsUpdate struct
type EndpointsResource struct {
	EDSUpdate    EndpointsUpdate
	DNSEndpoints []resolver.Endpoint
}

type AggregateConfig struct {
	// The list of leaf clusters for an aggregate cluster.
	Leaf_clusters []string
}

type ClusterChild struct {
	// pointers are used so that one of them can be set to nil
	// will probably need a check which is present
	// doubt - do we need a type field, it is already there in ClusterUpdate
	Child_type       string
	Endpoint_config  EndpointConfig
	Aggregate_config AggregateConfig
}

// handshakeClusterNameKey is the type used as the key to store cluster name in
// the Attributes field of resolver.stateess.
type xdsconfigkey struct{}

// SetXDSHandshakeClusterName returns a copy of state in which the Attributes field
// is updated with the cluster name.
func SetXDSConfig(state resolver.State, config XdsConfig) resolver.State {
	state.Attributes = state.Attributes.WithValue(xdsconfigkey{}, config)
	return state
}

// GetXDSHandshakeClusterName returns cluster name stored in attr.
func GetXDSConfig(attr *attributes.Attributes) (XdsConfig, bool) {
	v := attr.Value(xdsconfigkey{})
	config, ok := v.(XdsConfig)
	return config, ok
}
