// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v3beta4

import (
	"context"
	"fmt"
	"github.com/atomix/atomix/api/errors"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	atomixv3beta4 "github.com/atomix/atomix/controller/pkg/apis/atomix/v3beta4"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/atomix/runtime/pkg/utils/grpc/interceptors"
	gogotypes "github.com/gogo/protobuf/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"time"
)

const runtimeReadyCondition = "runtime.atomix.io/ready"

func addRuntimeController(mgr manager.Manager) error {
	// Create a new controller
	c, err := controller.New("runtime-controller", mgr, controller.Options{
		Reconciler: &RuntimeReconciler{
			client: mgr.GetClient(),
			scheme: mgr.GetScheme(),
			config: mgr.GetConfig(),
			events: mgr.GetEventRecorderFor("atomix"),
		},
		RateLimiter: workqueue.NewItemExponentialFailureRateLimiter(time.Millisecond*10, time.Second*5),
	})
	if err != nil {
		return err
	}

	// Watch for changes to Pods
	err = c.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for changes to StorageProfiles and reconcile all Pods in the same namespace
	err = c.Watch(&source.Kind{Type: &atomixv3beta4.StorageProfile{}}, handler.EnqueueRequestsFromMapFunc(func(object client.Object) []reconcile.Request {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		requirement, err := labels.NewRequirement(runtimeProfileLabel, selection.Exists, nil)
		if err != nil {
			log.Error(err)
			return nil
		}
		selector := labels.NewSelector().Add(*requirement)

		pods := &corev1.PodList{}
		options := &client.ListOptions{
			Namespace:     object.GetNamespace(),
			LabelSelector: selector,
		}
		if err := mgr.GetClient().List(ctx, pods, options); err != nil {
			log.Error(err)
			return nil
		}

		var requests []reconcile.Request
		for _, pod := range pods.Items {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: pod.Namespace,
					Name:      pod.Name,
				},
			})
		}
		return requests
	}))
	if err != nil {
		return err
	}

	// Watch for changes to StorageProfiles and reconcile all Pods in the same namespace
	err = c.Watch(&source.Kind{Type: &atomixv3beta4.StorageProfile{}}, handler.EnqueueRequestsFromMapFunc(func(object client.Object) []reconcile.Request {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		requirement, err := labels.NewRequirement(proxyProfileLabel, selection.Exists, nil)
		if err != nil {
			log.Error(err)
			return nil
		}
		selector := labels.NewSelector().Add(*requirement)

		pods := &corev1.PodList{}
		options := &client.ListOptions{
			Namespace:     object.GetNamespace(),
			LabelSelector: selector,
		}
		if err := mgr.GetClient().List(ctx, pods, options); err != nil {
			log.Error(err)
			return nil
		}

		var requests []reconcile.Request
		for _, pod := range pods.Items {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: pod.Namespace,
					Name:      pod.Name,
				},
			})
		}
		return requests
	}))
	if err != nil {
		return err
	}

	// Watch for changes to DataStores and reconcile all Pods
	err = c.Watch(&source.Kind{Type: &atomixv3beta4.DataStore{}}, handler.EnqueueRequestsFromMapFunc(func(object client.Object) []reconcile.Request {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		requirement, err := labels.NewRequirement(runtimeProfileLabel, selection.Exists, nil)
		if err != nil {
			log.Error(err)
			return nil
		}
		selector := labels.NewSelector().Add(*requirement)

		pods := &corev1.PodList{}
		options := &client.ListOptions{
			Namespace:     object.GetNamespace(),
			LabelSelector: selector,
		}
		if err := mgr.GetClient().List(ctx, pods, options); err != nil {
			log.Error(err)
			return nil
		}

		var requests []reconcile.Request
		for _, pod := range pods.Items {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: pod.Namespace,
					Name:      pod.Name,
				},
			})
		}
		return requests
	}))
	if err != nil {
		return err
	}

	// Watch for changes to DataStores and reconcile all Pods
	err = c.Watch(&source.Kind{Type: &atomixv3beta4.DataStore{}}, handler.EnqueueRequestsFromMapFunc(func(object client.Object) []reconcile.Request {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		requirement, err := labels.NewRequirement(proxyProfileLabel, selection.Exists, nil)
		if err != nil {
			log.Error(err)
			return nil
		}
		selector := labels.NewSelector().Add(*requirement)

		pods := &corev1.PodList{}
		options := &client.ListOptions{
			Namespace:     object.GetNamespace(),
			LabelSelector: selector,
		}
		if err := mgr.GetClient().List(ctx, pods, options); err != nil {
			log.Error(err)
			return nil
		}

		var requests []reconcile.Request
		for _, pod := range pods.Items {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: pod.Namespace,
					Name:      pod.Name,
				},
			})
		}
		return requests
	}))
	if err != nil {
		return err
	}
	return nil
}

// RuntimeReconciler is a Reconciler for Profiles
type RuntimeReconciler struct {
	client client.Client
	scheme *runtime.Scheme
	config *rest.Config
	events record.EventRecorder
}

// Reconcile reconciles StorageProfile resources
func (r *RuntimeReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log := log.WithFields(logging.String("Pod", request.NamespacedName.String()))
	log.Debug("Reconciling Pod")
	pod := &corev1.Pod{}
	err := r.client.Get(context.TODO(), request.NamespacedName, pod)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		log.Error(err)
		return reconcile.Result{}, err
	}

	if pod.Labels == nil {
		return reconcile.Result{}, nil
	}

	profileName, ok := pod.Labels[runtimeProfileLabel]
	if !ok {
		profileName, ok = pod.Labels[proxyProfileLabel]
		if !ok {
			return reconcile.Result{}, nil
		}
	}

	profileNamespacedName := types.NamespacedName{
		Namespace: pod.Namespace,
		Name:      profileName,
	}
	log = log.WithFields(logging.String("StorageProfile", profileNamespacedName.String()))
	profile := &atomixv3beta4.StorageProfile{}
	if err := r.client.Get(ctx, profileNamespacedName, profile); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	if ok, err := r.reconcileProfile(ctx, log, pod, profile); err != nil {
		return reconcile.Result{}, err
	} else if ok {
		return reconcile.Result{Requeue: true}, nil
	}
	return reconcile.Result{}, nil
}

func (r *RuntimeReconciler) reconcileProfile(ctx context.Context, log logging.Logger, pod *corev1.Pod, profile *atomixv3beta4.StorageProfile) (bool, error) {
	ready := true

	// Get the profile's pod status
	podStatus := r.getPodStatus(profile, pod)

	// If the pod resource version has changed, reset all the Connected routes to the Configuring state
	if podStatus.ResourceVersion != pod.ResourceVersion {
		log.Debug("Pod status changed; verifying proxy connections to all routes...")
		for i, routeStatus := range podStatus.Runtime.Routes {
			switch routeStatus.State {
			case atomixv3beta4.RouteConnected:
				routeStatus.State = atomixv3beta4.RouteConfiguring
			}
			podStatus.Runtime.Routes[i] = routeStatus
		}
		podStatus.ResourceVersion = pod.ResourceVersion
		if err := r.setPodStatus(ctx, log, profile, podStatus); err != nil {
			log.Error(err)
			return false, err
		}
		return true, nil
	}

	// Iterate through the profile's bindings and inject the routes into the pod
	for _, route := range profile.Spec.Routes {
		routeStatus := atomixv3beta4.RouteStatus{
			Store: route.Store,
		}
		var routeIndex *int
		for i, rs := range podStatus.Runtime.Routes {
			if rs.Store.Namespace == route.Store.Namespace && rs.Store.Name == route.Store.Name {
				routeStatus = rs
				routeIndex = &i
				break
			}
		}

		if updated, err := r.reconcileRoute(ctx, log, pod, &route, &routeStatus); err != nil {
			return false, err
		} else if updated {
			if routeIndex == nil {
				podStatus.Runtime.Routes = append(podStatus.Runtime.Routes, routeStatus)
			} else {
				podStatus.Runtime.Routes[*routeIndex] = routeStatus
			}
			if err := r.setPodStatus(ctx, log, profile, podStatus); err != nil {
				log.Error(err)
				return false, err
			}
			switch routeStatus.State {
			case atomixv3beta4.RouteConnecting, atomixv3beta4.RouteDisconnected:
				if ok, err := r.setReadyCondition(ctx, log, pod, corev1.ConditionFalse, "RouteNotConnected", fmt.Sprintf("Route to '%s' is not connected", route.Store.Name)); err != nil {
					log.Error(err)
					return ok, err
				}
			}
			return true, nil
		} else {
			switch routeStatus.State {
			case atomixv3beta4.RouteConnecting, atomixv3beta4.RouteDisconnected:
				ready = false
			}
		}
	}

	if ready {
		return r.setReadyCondition(ctx, log, pod, corev1.ConditionTrue, "RoutesConnected", "")
	}
	return false, nil
}

func (r *RuntimeReconciler) reconcileRoute(ctx context.Context, log logging.Logger, pod *corev1.Pod, route *atomixv3beta4.Route, status *atomixv3beta4.RouteStatus) (bool, error) {
	storeNamespace := route.Store.Namespace
	if storeNamespace == "" {
		storeNamespace = pod.Namespace
	}
	storeNamespacedName := types.NamespacedName{
		Namespace: storeNamespace,
		Name:      route.Store.Name,
	}

	log = log.WithFields(logging.String("DataStore", storeNamespacedName.String()))

	log.Debug("Reconciling profile route")

	// If the route is ready but the store has been removed, disconnect the route
	store := &atomixv3beta4.DataStore{}
	if err := r.client.Get(ctx, storeNamespacedName, store); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return false, err
		}

		switch status.State {
		case atomixv3beta4.RoutePending:
			return false, nil
		case atomixv3beta4.RouteConnecting, atomixv3beta4.RouteConnected, atomixv3beta4.RouteConfiguring:
			log.Info("DataStore not found; disconnecting route...")
			status.State = atomixv3beta4.RouteDisconnecting
			return true, nil
		case atomixv3beta4.RouteDisconnecting:
			conn, err := connect(ctx, pod)
			if err != nil {
				log.Error(err)
				return false, err
			}

			client := runtimev1.NewRuntimeClient(conn)
			request := &runtimev1.DisconnectRouteRequest{
				RouteID: runtimev1.RouteID{
					Namespace: storeNamespacedName.Namespace,
					Name:      storeNamespacedName.Name,
				},
			}
			_, err = client.DisconnectRoute(ctx, request)
			if err != nil {
				if !errors.IsNotFound(err) {
					log.Warn(err)
					r.events.Eventf(pod, "Warning", "DisconnectRouteFailed", "Failed disconnecting route to '%s': %s", storeNamespacedName, err)
					return false, err
				}
			}

			r.events.Eventf(pod, "Normal", "DisconnectedRoute", "Successfully disconnected route to '%s'", storeNamespacedName)
			log.Info("Disconnected route")
			status.State = atomixv3beta4.RouteDisconnected
			status.Version = ""
			return true, nil
		default:
			status.State = atomixv3beta4.RoutePending
			return true, nil
		}
	}

	switch status.State {
	case atomixv3beta4.RoutePending:
		log.Info("Connecting route to store")
		status.State = atomixv3beta4.RouteConnecting
		status.Version = store.ResourceVersion
		return true, nil
	case atomixv3beta4.RouteConnecting:
		if status.Version != store.ResourceVersion {
			log.Debug("DataStore configuration changed; reconfiguring route...")
			status.Version = store.ResourceVersion
			return true, nil
		}

		conn, err := connect(ctx, pod)
		if err != nil {
			log.Error(err)
			return false, err
		}

		driverVersion := store.Spec.Driver.Version
		if driverVersion == "" {
			driverVersion = store.Spec.Driver.APIVersion
		}

		client := runtimev1.NewRuntimeClient(conn)
		request := &runtimev1.ConnectRouteRequest{
			DriverID: runtimev1.DriverID{
				Name:       store.Spec.Driver.Name,
				APIVersion: driverVersion,
			},
			Route: toRuntimeRoute(store, route),
		}
		_, err = client.ConnectRoute(ctx, request)
		if err != nil {
			if !errors.IsAlreadyExists(err) {
				log.Warn(err)
				r.events.Eventf(pod, "Warning", "ConnectRouteFailed", "Failed connecting route to '%s': %s", storeNamespacedName, err)
				return false, err
			}
		}

		r.events.Eventf(pod, "Normal", "ConnectedRoute", "Successfully connected route to '%s'", storeNamespacedName)
		log.Info("Proxy connected")
		status.State = atomixv3beta4.RouteConnected
		return true, nil
	case atomixv3beta4.RouteConnected:
		if status.Version != store.ResourceVersion {
			log.Debug("DataStore configuration changed; reconfiguring route...")
			status.State = atomixv3beta4.RouteConfiguring
			status.Version = store.ResourceVersion
			return true, nil
		}
	case atomixv3beta4.RouteConfiguring:
		if status.Version != store.ResourceVersion {
			log.Debug("DataStore configuration changed; reconfiguring route...")
			status.Version = store.ResourceVersion
			return true, nil
		}

		conn, err := connect(ctx, pod)
		if err != nil {
			log.Error(err)
			return false, err
		}

		client := runtimev1.NewRuntimeClient(conn)
		request := &runtimev1.ConfigureRouteRequest{
			Route: toRuntimeRoute(store, route),
		}
		_, err = client.ConfigureRoute(ctx, request)
		if err != nil {
			if !errors.IsNotFound(err) {
				r.events.Eventf(pod, "Warning", "ConfigureRouteFailed", "Failed reconfiguring route to '%s': %s", storeNamespacedName, err)
				log.Warn(err)
				return false, err
			}
			// If the runtime returned a NotFound error, set the route to Connecting to establish the connection.
			log.Warn("Proxy not connected; connecting...")
			status.State = atomixv3beta4.RouteConnecting
			return true, nil
		}

		r.events.Eventf(pod, "Normal", "ConfiguredRoute", "Configured route to '%s'", storeNamespacedName)
		log.Info("Proxy configured")
		status.State = atomixv3beta4.RouteConnected
		return true, nil
	default:
		status.State = atomixv3beta4.RoutePending
		return true, nil
	}
	return false, nil
}

func toRuntimeRoute(store *atomixv3beta4.DataStore, route *atomixv3beta4.Route) runtimev1.Route {
	var rules []runtimev1.RoutingRule
	for _, rule := range route.Rules {
		var primitives []runtimev1.PrimitiveID
		for _, name := range rule.Names {
			primitives = append(primitives, runtimev1.PrimitiveID{
				Name: name,
			})
		}

		var config *gogotypes.Any
		if rule.Config.Raw != nil {
			config = &gogotypes.Any{
				Value: rule.Config.Raw,
			}
		}

		rules = append(rules, runtimev1.RoutingRule{
			Type: runtimev1.PrimitiveType{
				Name:       rule.Kind,
				APIVersion: rule.APIVersion,
			},
			Primitives: primitives,
			Tags:       rule.Tags,
			Config:     config,
		})
	}

	return runtimev1.Route{
		RouteID: runtimev1.RouteID{
			Namespace: store.Namespace,
			Name:      store.Name,
		},
		Config: &gogotypes.Any{
			Value: store.Spec.Config.Raw,
		},
		Rules: rules,
	}
}

func (r *RuntimeReconciler) getPodStatus(profile *atomixv3beta4.StorageProfile, pod *corev1.Pod) atomixv3beta4.PodStatus {
	for _, podStatus := range profile.Status.PodStatuses {
		if podStatus.Name == pod.Name && podStatus.UID == pod.UID {
			return podStatus
		}
	}
	return atomixv3beta4.PodStatus{
		ObjectReference: corev1.ObjectReference{
			Namespace:       pod.Namespace,
			Name:            pod.Name,
			UID:             pod.UID,
			ResourceVersion: pod.ResourceVersion,
		},
	}
}

func (r *RuntimeReconciler) setPodStatus(ctx context.Context, log logging.Logger, profile *atomixv3beta4.StorageProfile, status atomixv3beta4.PodStatus) error {
	for i, podStatus := range profile.Status.PodStatuses {
		if podStatus.Name == status.Name {
			log.Infof("Updating StorageProfile %s PodStatus %s", status)
			profile.Status.PodStatuses[i] = status
			return r.client.Status().Update(ctx, profile)
		}
	}

	log.Infow("Initializing Pod status in StorageProfile")
	profile.Status.PodStatuses = append(profile.Status.PodStatuses, status)
	return r.client.Status().Update(ctx, profile)
}

func (r *RuntimeReconciler) setReadyCondition(ctx context.Context, log logging.Logger, pod *corev1.Pod, status corev1.ConditionStatus, reason string, message string) (bool, error) {
	for i, condition := range pod.Status.Conditions {
		if condition.Type == runtimeReadyCondition {
			if condition.Status == status && condition.Reason == reason {
				return false, nil
			}
			log.Infow("Updating Pod condition",
				logging.String("Status", string(status)),
				logging.String("Reason", reason),
				logging.String("Message", message))
			if condition.Status != status {
				condition.LastTransitionTime = metav1.Now()
			}
			condition.Status = status
			condition.Reason = reason
			condition.Message = message
			pod.Status.Conditions[i] = condition
			return true, r.client.Status().Update(ctx, pod)
		}
	}

	log.Infow("Initializing Pod condition",
		logging.String("Status", string(status)),
		logging.String("Reason", reason),
		logging.String("Message", message))
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type:               runtimeReadyCondition,
		Status:             status,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	})
	return true, r.client.Status().Update(ctx, pod)
}

func connect(ctx context.Context, pod *corev1.Pod) (*grpc.ClientConn, error) {
	target := fmt.Sprintf("%s:%d", pod.Status.PodIP, defaultRuntimePort)
	return grpc.DialContext(ctx, target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(interceptors.ErrorHandlingUnaryClientInterceptor()),
		grpc.WithStreamInterceptor(interceptors.RetryingStreamClientInterceptor()))
}
