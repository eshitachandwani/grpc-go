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

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/internal/grpcsync"
	"google.golang.org/grpc/internal/pretty"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/xds/internal/xdsclient"
	"google.golang.org/grpc/xds/internal/xdsclient/xdsresource"
)

const (
	aggregateClusterMaxDepth = 16
)

var errExceedsMaxDepth = status.Errorf(codes.Unavailable, "aggregate cluster graph exceeds max depth (%d)", aggregateClusterMaxDepth)

type xdsDependencyManager struct {
	// All methods on the dependency manager type except, are guaranteed to execute in the context of
	// this serializer's callback. And since the serializer guarantees mutual
	// exclusion among these callbacks, we can get by without any mutexes to
	// access all of the below defined state. The only exception is Close(),
	// which does access some of this shared state, but it does so after
	// cancelling the context passed to the serializer.
	serializer         *grpcsync.CallbackSerializer
	serializerCancel   context.CancelFunc
	listenerName       string
	watcher            *xdsResolver
	dataplaneAuthority string
	// The underlying xdsClient which performs all xDS requests and responses.
	xdsClient xdsclient.XDSClient

	// listener state
	listenerWatcher     *listenerWatcher
	listenerUpdateRecvd bool
	currentListener     xdsresource.ListenerUpdate

	rdsResourceName        string
	routeConfigWatcher     *routeConfigWatcher
	routeConfigUpdateRecvd bool
	currentRouteConfig     xdsresource.RouteConfigUpdate
	currentVirtualHost     *xdsresource.VirtualHost // Matched virtual host for quick access.

	// activeClusters is a map from cluster name to information about the
	// cluster that includes a ref count and load balancing configuration.
	activeClusters    map[string]*clusterInfo
	clustersFromRoute map[string]struct{} // clusters from route config
	clustersFromSubs  map[string]struct{} // clusters from subscription

	clusterWatchers map[string]*watcherState // Set of watchers and associated state, keyed by cluster name.
	edsWatchers     map[string]*edsWatcherState
	dnsWatcher      map[string]*dnsWatcher

	Clusters map[string]xdsresource.ClusterConfigOrError

	//xdsClient
}

type XdsConfigOrError struct {
	xdsresource.XdsConfig
	error
}

func (xdm *xdsDependencyManager) build() {
	//listener watcher

	// Initialize the serializer used to synchronize the following:
	// - updates from the xDS client. This could lead to generation of new
	//   service config if resolution is complete.
	// - completion of an RPC to a removed cluster causing the associated ref
	//   count to become zero, resulting in generation of new service config.
	// - stopping of a config selector that results in generation of new service
	//   config.
	// ctx, cancel := context.WithCancel(context.Background())
	// xdm.serializer = grpcsync.NewCallbackSerializer(ctx)
	// xdm.serializerCancel = cancel
	xdm.clusterWatchers = make(map[string]*watcherState)
	xdm.dnsWatcher = make(map[string]*dnsWatcher)
	xdm.edsWatchers = make(map[string]*edsWatcherState)
	xdm.listenerWatcher = newListenerWatcher(xdm.listenerName, xdm) //pass xds dependency manager as parent)
	xdm.Clusters = make(map[string]xdsresource.ClusterConfigOrError)

	//route watcher
	//cds watcher
	// eds watcher
	// xdm.watcher.OnUpdate(XdsConfigOrError{error: nil})

}

func (xdm *xdsDependencyManager) Close() {
	// xdm.serializerCancel()
	// <-xdm.serializer.Done()
	if xdm.listenerWatcher != nil {
		xdm.listenerWatcher.stop()
	}
	if xdm.routeConfigWatcher != nil {
		xdm.routeConfigWatcher.stop()
	}
	// emchandwani : close all watchers
	for _, state := range xdm.clusterWatchers {
		state.cancelWatch()
	}
	for _, state := range xdm.edsWatchers {
		state.cancelWatch()
	}
	for _, state := range xdm.dnsWatcher {
		state.dnsR.Close()
	}

	// Cancel the context passed to the serializer and wait for any scheduled
	// callbacks to complete. Canceling the context ensures that no new
	// callbacks will be scheduled.
}

func (xdm *xdsDependencyManager) sendError(err error) {
	xdm.watcher.OnUpdate(XdsConfigOrError{error: err})
}

func (xdm *xdsDependencyManager) sendUpdate() {
	configUpdate := xdsresource.XdsConfig{}
	configUpdate.Listener = xdm.currentListener
	configUpdate.Route_config = xdm.currentRouteConfig
	configUpdate.Virtual_host = xdm.currentVirtualHost
	configUpdate.Clusters = make(map[string]xdsresource.ClusterConfigOrError)
	clustersTowatch := make(map[string]struct{})
	for cluster := range xdm.clustersFromRoute {
		clustersTowatch[cluster] = struct{}{}
	}
	// add clusters for subscription
	edsResourcesSeen := make(map[string]struct{})
	dnsResourcesSeen := make(map[string]struct{})
	haveAllResources := true
	// var err error
	// clustermap := make(map[string]xdsresource.ClusterConfigOrError)
	leaf_cluster := &[]string{}
	for cluster := range clustersTowatch {
		haveAllClusterResources, err := xdm.populateCLutserConfig(cluster, 0, xdm.Clusters, edsResourcesSeen, dnsResourcesSeen, leaf_cluster)
		if err != nil {
			configUpdate.Clusters[cluster] = xdsresource.ClusterConfigOrError{Err: err}
		}
		if !haveAllClusterResources {
			haveAllResources = false
		}
	}
	if haveAllResources {
		configUpdate.Clusters = xdm.Clusters
		xdm.watcher.OnUpdate(XdsConfigOrError{XdsConfig: configUpdate, error: nil})
	}
	//cancel watchers for clusters not in cluster map
	// for clustername, state := range xdm.clusterWatchers {
	// 	if _, ok := clustermap[clustername]; !ok {
	// 		state.cancelWatch()
	// 		delete(xdm.clusterWatchers, clustername)
	// 	}
	// }
	// for edsname, state := range xdm.edsWatchers {
	// 	if _, ok := edsResourcesSeen[edsname]; !ok {
	// 		state.cancelWatch()
	// 		delete(xdm.edsWatchers, edsname)
	// 	}
	// }
	// close dns resolver
	// clear cluster subs
}

func (xdm xdsDependencyManager) createAndAddWatcherForCluster(name string) {
	w := &clusterWatcher{
		name:   name,
		parent: &xdm,
	}
	ws := &watcherState{
		watcher:     w,
		cancelWatch: xdsresource.WatchCluster(xdm.xdsClient, name, w),
	}
	xdm.clusterWatchers[name] = ws
}

func (xdm *xdsDependencyManager) populateCLutserConfig(clusterName string, depth int, clusterConfigMap map[string]xdsresource.ClusterConfigOrError, edsResourcesSeen map[string]struct{}, dnsNamesSeen map[string]struct{}, leafClusters *[]string) (bool, error) {
	if depth >= aggregateClusterMaxDepth {
		return true, errExceedsMaxDepth
	}
	_, ok := clusterConfigMap[clusterName]
	if ok {
		return true, nil
	}
	// clusterConfigMap[clusterName] = xdsresource.ClusterConfigOrError{Err: fmt.Errorf("no CLuster data yet")}

	state, ok := xdm.clusterWatchers[clusterName]
	if !ok {
		// If we have not seen this cluster so far, create a watcher for it, add
		// it to the map, start the watch and return.
		xdm.createAndAddWatcherForCluster(clusterName)

		// And since we just created the watcher, we know that we haven't
		// resolved the cluster graph yet.
		return false, nil
	}

	// A watcher exists, but no update has been received yet.
	if state.lastUpdate == nil {
		return false, nil
	}

	cluster := state.lastUpdate
	switch cluster.ClusterType {
	case xdsresource.ClusterTypeAggregate: // emchandwani : do later
		// This boolean is used to track if any of the clusters in the graph is
		// not yet completely resolved or returns errors, thereby allowing us to
		// traverse as much of the graph as possible (and start the associated
		// watches where required) to ensure that clustersSeen contains all
		// clusters in the graph that we can traverse to.
		missingCluster := false
		clusterUpdateRef := *state.lastUpdate
		var err error
		var haveAllResources bool
		child_leaf_clusters := &[]string{}
		for _, child := range cluster.PrioritizedClusterNames {
			haveAllResources, err = xdm.populateCLutserConfig(child, depth+1, clusterConfigMap, edsResourcesSeen, dnsNamesSeen, child_leaf_clusters)
			// error will only be non-nil in case of exceeding depth
			if err != nil {
				break
			}
			if !haveAllResources {
				missingCluster = true
			}
		}
		//when get depth exceeded error, put that in config map and propogate up the tree
		if err != nil {
			clusterConfigMap[clusterName] = xdsresource.ClusterConfigOrError{Err: err}
			return true, err
		}
		// if reached all ther children and no leaf clusters , this is an error, put that in cluster config map
		if !missingCluster && len(*child_leaf_clusters) == 0 {
			clusterConfigMap[clusterName] = xdsresource.ClusterConfigOrError{Err: status.Errorf(codes.Unavailable, "No leaf clusters found for aggregate cluster %s", clusterName)}
			return true, nil
		}
		// if reached all clusters and have leaf clusters, update the cluster config with the cluster update and leaf clusters for even the middle aggregate cluster
		clusterConfigMap[clusterName] = xdsresource.ClusterConfigOrError{
			Cluster_config: xdsresource.ClusterConfig{
				Cluster: clusterUpdateRef,
				Children: xdsresource.ClusterChild{
					Child_type:       "aggregate",
					Aggregate_config: xdsresource.AggregateConfig{Leaf_clusters: *child_leaf_clusters},
				},
			},
			Err: nil,
		}
		// append the leaf clusters for the upper clusters in tree.
		*leafClusters = append(*leafClusters, *child_leaf_clusters...)
		return !missingCluster, nil
	case xdsresource.ClusterTypeEDS:
		var name string
		if cluster.EDSServiceName != "" {
			name = cluster.EDSServiceName
		} else {
			name = cluster.ClusterName
		}
		edsResourcesSeen[name] = struct{}{}

		// start edsWatch if not already
		edsstate, ok := xdm.edsWatchers[name]
		if !ok {
			xdm.edsWatchers[name] = newEDSResolver(name, xdm.xdsClient, xdm)
			return false, nil
		}
		if edsstate == nil {
			return false, nil
		}
		if depth > 0 {
			*leafClusters = append(*leafClusters, name)
		}
		// if edsstate.update == nil {
		// 	// Handle the case where the update hasn’t been received yet
		// 	clusterConfigMap[name] = xdsresource.ClusterConfigOrError{
		// 		Err: fmt.Errorf("EDS update for cluster %q not yet available", name),
		// 	}
		// 	return true, nil
		// }
		clusterConfigMap[clusterName] = xdsresource.ClusterConfigOrError{
			Cluster_config: xdsresource.ClusterConfig{
				Cluster: *cluster,
				Children: xdsresource.ClusterChild{
					Child_type: "endpoint",
					Endpoint_config: xdsresource.EndpointConfig{
						Endpoints: xdsresource.EndpointsResource{
							EDSUpdate: *edsstate.update}}}},
			Err: nil}

	case xdsresource.ClusterTypeLogicalDNS:
		host := cluster.DNSHostName
		dnsNamesSeen[host] = struct{}{}
		dnsstate, ok := xdm.dnsWatcher[host]
		if !ok {
			newDNSResolver(host, xdm)
			return false, nil
		}
		if dnsstate == nil {
			return false, nil
		}
		if depth > 0 {
			*leafClusters = append(*leafClusters, host)
		}
		clusterConfigMap[host] = xdsresource.ClusterConfigOrError{
			Cluster_config: xdsresource.ClusterConfig{
				Cluster: *cluster,
				Children: xdsresource.ClusterChild{
					Child_type: "logicaldns",
					Endpoint_config: xdsresource.EndpointConfig{
						Endpoints: xdsresource.EndpointsResource{
							DNSEndpoints: dnsstate.endpoints}}}},
			Err: nil}
	}
	return true, nil

	// odJSON := cluster.OutlierDetection
	// // "In the cds LB policy, if the outlier_detection field is not set in
	// // the Cluster resource, a "no-op" outlier_detection config will be
	// // generated in the corresponding DiscoveryMechanism config, with all
	// // fields unset." - A50
	// if odJSON == nil {
	// 	// This will pick up top level defaults in Cluster Resolver
	// 	// ParseConfig, but sre and fpe will be nil still so still a
	// 	// "no-op" config.
	// 	odJSON = json.RawMessage(`{}`)
	// }
	// dm.OutlierDetection = odJSON

	// dm.TelemetryLabels = cluster.TelemetryLabels
}

type Watcher interface {
	// update from xdsDepsManager to resolver , invoked by xdsdepsmanager ,
	// implemented by resolver
	OnUpdate(XdsConfigOrError)
}

// Listener and route functions
// Only executed in the context of a serializer callback.
func (xdm *xdsDependencyManager) onListenerResourceUpdate(update xdsresource.ListenerUpdate) {
	if logger.V(2) {
		logger.Infof("Received update for Listener resource %q: %v", xdm.listenerName, pretty.ToJSON(update))
	}

	xdm.currentListener = update
	//emchandwani - check where this is needed and if it should go in xdm or resolver
	xdm.listenerUpdateRecvd = true

	if update.InlineRouteConfig != nil {
		// If there was a previous route config watcher because of a non-inline
		// route configuration, cancel it.
		xdm.rdsResourceName = ""
		if xdm.routeConfigWatcher != nil {
			xdm.routeConfigWatcher.stop()
			xdm.routeConfigWatcher = nil
		}

		xdm.applyRouteConfigUpdate(*update.InlineRouteConfig)
		return
	}

	// We get here only if there was no inline route configuration.

	// If the route config name has not changed, send an update with existing
	// route configuration and the newly received listener configuration.
	if xdm.rdsResourceName == update.RouteConfigName {
		// emchandwani : need to call OnUpdate with no changes to the xdsConfig
		// call maybSendeUpdate
		//to update the listener resources
		xdm.sendUpdate()
		// r.onResolutionComplete()
		return
	}

	// If the route config name has changed, cancel the old watcher and start a
	// new one. At this point, since we have not yet resolved the new route
	// config name, we don't send an update to the channel, and therefore
	// continue using the old route configuration (if received) until the new
	// one is received.
	xdm.rdsResourceName = update.RouteConfigName
	if xdm.routeConfigWatcher != nil {
		xdm.routeConfigWatcher.stop()
		xdm.currentVirtualHost = nil
		xdm.routeConfigUpdateRecvd = false
	}
	xdm.routeConfigWatcher = newRouteConfigWatcher(xdm.rdsResourceName, xdm)
}

func (xdm *xdsDependencyManager) applyRouteConfigUpdate(update xdsresource.RouteConfigUpdate) {
	matchVh := xdsresource.FindBestMatchingVirtualHost(xdm.dataplaneAuthority, update.VirtualHosts)
	if matchVh == nil {
		// TODO(purnesh42h): Should this be a resource or ambient error? Note
		// that its being called only from resource update methods when we have
		// finished removing the previous update.
		// emchandwani : call maybeapplyUpdate
		// change to resource error
		xdm.watcher.cc.ReportError(fmt.Errorf("no matching virtual host found for %q", xdm.dataplaneAuthority))
		// xdm.watcher.OnUpdate(XdsConfigOrError{error: fmt.Errorf("no matching virtual host found for %q", xdm.dataplaneAuthority)})
		return
	}
	xdm.currentRouteConfig = update
	xdm.currentVirtualHost = matchVh
	xdm.routeConfigUpdateRecvd = true

	xdm.clustersFromRoute = getClustersFromVirtualHost(xdm.currentVirtualHost)

	// create set of clusters to watch , from these and from cluster subscriptions

	// called in OnUpdate in resolver
	xdm.sendUpdate()
	// r.onResolutionComplete()
}

func (xdm *xdsDependencyManager) onListenerResourceAmbientError(err error) {
	if logger.V(2) {
		logger.Infof("Received ambient error for Listener resource %q: %v", xdm.listenerName, err)
	}
	//emchandwani : need to probably ignore or set in notes, current behavior - send Reporterror to clientconn
	// xdm.watcher.OnUpdate(XdsConfigOrError{error: err})
	// r.onAmbientError(err)
	xdm.watcher.cc.ReportError(err)
}

// Only executed in the context of a serializer callback.
func (xdm *xdsDependencyManager) onListenerResourceError(err error) {
	if logger.V(2) {
		logger.Infof("Received resource error for Listener resource %q: %v", xdm.listenerName, err)
	}

	xdm.listenerUpdateRecvd = false
	if xdm.routeConfigWatcher != nil {
		xdm.routeConfigWatcher.stop()
	}
	xdm.rdsResourceName = ""
	xdm.currentVirtualHost = nil
	xdm.routeConfigUpdateRecvd = false
	xdm.routeConfigWatcher = nil
	xdm.sendError(err)
	// r.onResourceError(err)
	// xdm.watcher.OnUpdate(XdsConfigOrError{error: err})
}

// Only executed in the context of a serializer callback.
func (xdm *xdsDependencyManager) onRouteConfigResourceUpdate(name string, update xdsresource.RouteConfigUpdate) {
	if logger.V(2) {
		logger.Infof("Received update for RouteConfiguration resource %q: %v", name, pretty.ToJSON(update))
	}

	if xdm.rdsResourceName != name {
		// Drop updates from canceled watchers.
		return
	}

	xdm.applyRouteConfigUpdate(update)
}

// Only executed in the context of a serializer callback.
func (xdm *xdsDependencyManager) onRouteConfigResourceAmbientError(name string, err error) {
	if logger.V(2) {
		logger.Infof("Received ambient error for RouteConfiguration resource %q: %v", name, err)
	}
	xdm.watcher.cc.ReportError(err)
	// r.onAmbientError(err)
	//emchandwani : need to probably ignore or set in notes, current behavior - send Reporterror to clientconn
	// xdm.watcher.OnUpdate(XdsConfigOrError{error: err})

}

// Only executed in the context of a serializer callback.
func (xdm *xdsDependencyManager) onRouteConfigResourceError(name string, err error) {
	if logger.V(2) {
		logger.Infof("Received resource error for RouteConfiguration resource %q: %v", name, err)
	}

	// if error received is not for current route in xdm
	if xdm.rdsResourceName != name {
		return
	}
	xdm.sendError(err)
	// xdm.watcher.OnUpdate(XdsConfigOrError{error: err})
	// r.onResourceError(err)
}

func getClustersFromVirtualHost(vh *xdsresource.VirtualHost) map[string]struct{} {
	clusters := make(map[string]struct{})
	for _, rt := range vh.Routes {
		if rt.ClusterSpecifierPlugin != "" {
			clusters[clusterSpecifierPluginPrefix+rt.ClusterSpecifierPlugin] = struct{}{}
		}
		for cluster := range rt.WeightedClusters {
			clusters[cluster] = struct{}{}
		}
	}
	return clusters
}

// Handles a good Cluster update from the xDS client. Kicks off the discovery
// mechanism generation process from the top-level cluster and if the cluster
// graph is resolved, generates child policy config and pushes it down.
//
// Only executed in the context of a serializer callback.
func (xdm *xdsDependencyManager) onClusterUpdate(name string, update xdsresource.ClusterUpdate) {
	state := xdm.clusterWatchers[name]
	if state == nil {
		// We are currently not watching this cluster anymore. Return early.
		return
	}

	logger.Infof("Received Cluster resource: %s", pretty.ToJSON(update))

	// Update the watchers map with the update for the cluster.
	state.lastUpdate = &update
	xdm.sendUpdate()
}

// Handles an ambient error Cluster update from the xDS client to not stop
// using the previously seen resource.
//
// Only executed in the context of a serializer callback.
func (xdm *xdsDependencyManager) onClusterAmbientError(name string, err error) {
	// logger.Warningf("Cluster resource %q received ambient error update: %v", name, err)

	// if xdsresource.ErrType(err) != xdsresource.ErrorTypeConnection && b.childLB != nil {
	// 	// Connection errors will be sent to the child balancers directly.
	// 	// There's no need to forward them.
	// 	b.childLB.ResolverError(err)
	// }
	// emchandwani : add in note
}

// Handles an error Cluster update from the xDS client to stop using the
// previously seen resource. Propagates the error down to the child policy
// if one exists, and puts the channel in TRANSIENT_FAILURE.
//
// Only executed in the context of a serializer callback.
func (xdm *xdsDependencyManager) onClusterResourceError(name string, err error) {
	logger.Warningf("CDS watch for resource %q reported resource error", name)
	// emchandwani , will need a field to set error for a particular cluster.
	// b.closeChildPolicyAndReportTF(err)

}

func (xdm *xdsDependencyManager) onEndpointResourceUpdate(name string, update xdsresource.EndpointsUpdate) {
	// write what to do on endpoint update
	state := xdm.edsWatchers[name]
	// if state.update.Localities == nil {
	// 	//write note in resolution note
	// }
	state.update = &update
	xdm.sendUpdate()
}

func (xdm *xdsDependencyManager) onEndpointAmbientError(name string, err error) {
	//check what to do
}

func (xdm *xdsDependencyManager) onEndpointResourceError(name string, err error) {
	// xdm.sendError(err)
	// emchandwani : find a way to get error each endpoint resource
}

func (xdm *xdsDependencyManager) onDnsResourceUpdate(name string, update []resolver.Endpoint) {
	// write what to do on endpoint update
	state, ok := xdm.dnsWatcher[name]
	if ok {
		state.endpoints = update
	}
}

func (xdm *xdsDependencyManager) onDnsAmbientError(name string, err error) {
	//check what to do
}

func (xdm *xdsDependencyManager) onDnsResourceError(name string, err error) {
	// nil the endpoints
}
