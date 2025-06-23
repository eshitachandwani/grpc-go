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

package resolver

import "google.golang.org/grpc/xds/internal/xdsclient/xdsresource"

type XdsConfig struct {
	// listener resource
	listener xdsresource.ListenerUpdate
	// RouteConfig resource.  Will be populated even if RouteConfig is
	// inlined into the Listener resource.
	route_config xdsresource.RouteConfigUpdate
	// Virtual host.  Points into route_config.  Will always be non-null.
	virtual_host xdsresource.VirtualHost
	// Cluster map.  A cluster will have a non-OK status if either
	// (a) there was an error and we did not already have a valid
	// resource or (b) the resource does not exist.
	clusters map[string]ClusterConfigOrError
}

type ClusterConfigOrError struct {
	err error
	// pointer to make it nil if error is present - check if necessary
	cluster_config *ClusterConfig
}

type ClusterConfig struct {
	// Cluster resource.
	cluster  xdsresource.ClusterUpdate
	children ClusterChild
}

type EndpointConfig struct {
	// Endpoint info for EDS and LOGICAL_DNS clusters.  If there was an
	// error, endpoints will be null and resolution_note will be set.
	endpoints       xdsresource.EndpointsUpdate
	resolution_note string
}

type AggregateConfig struct {
	// The list of leaf clusters for an aggregate cluster.
	leaf_clusters []string
}

type ClusterChild struct {
	// pointers are used so that one of them can be set to nil
	// will probably need a check which is present
	// doubt - do we need a type field, it is already there in ClusterUpdate
	endpoint_config  *EndpointConfig
	aggregate_config *AggregateConfig
}
