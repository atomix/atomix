// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v3beta3

import (
	"context"
	"fmt"
	"github.com/atomix/atomix/api/errors"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	atomixv3beta3 "github.com/atomix/atomix/controller/pkg/apis/atomix/v3beta3"
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

const proxyReadyCondition = "proxy.atomix.io/ready"

func addPodController(mgr manager.Manager) error {
	// Create a new controller
	c, err := controller.New("pod-controller", mgr, controller.Options{
		Reconciler: &PodReconciler{
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
	err = c.Watch(&source.Kind{Type: &atomixv3beta3.StorageProfile{}}, handler.EnqueueRequestsFromMapFunc(func(object client.Object) []reconcile.Request {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		pods := &corev1.PodList{}
		options := &client.ListOptions{
			Namespace: object.GetNamespace(),
			LabelSelector: labels.SelectorFromSet(map[string]string{
				proxyInjectLabel: "true",
			}),
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
	err = c.Watch(&source.Kind{Type: &atomixv3beta3.DataStore{}}, handler.EnqueueRequestsFromMapFunc(func(object client.Object) []reconcile.Request {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		pods := &corev1.PodList{}
		options := &client.ListOptions{
			LabelSelector: labels.SelectorFromSet(map[string]string{
				proxyInjectLabel: "true",
			}),
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

// PodReconciler is a Reconciler for Profiles
type PodReconciler struct {
	client client.Client
	scheme *runtime.Scheme
	config *rest.Config
	events record.EventRecorder
}

// Reconcile reconciles StorageProfile resources
func (r *PodReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
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

	if pod.Annotations == nil {
		return reconcile.Result{}, nil
	}

	profileName, ok := pod.Annotations[proxyProfileAnnotation]
	if !ok {
		return reconcile.Result{}, nil
	}

	profileNamespacedName := types.NamespacedName{
		Namespace: pod.Namespace,
		Name:      profileName,
	}
	log = log.WithFields(logging.String("StorageProfile", profileNamespacedName.String()))
	profile := &atomixv3beta3.StorageProfile{}
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

func (r *PodReconciler) reconcileProfile(ctx context.Context, log logging.Logger, pod *corev1.Pod, profile *atomixv3beta3.StorageProfile) (bool, error) {
	ready := true

	// Get the profile's pod status
	podStatus := r.getPodStatus(profile, pod)

	// If the pod resource version has changed, reset all the Connected routes to the Configuring state
	if podStatus.ResourceVersion != pod.ResourceVersion {
		log.Debug("Pod status changed; verifying proxy connections to all stores...")
		for i, routeStatus := range podStatus.Proxy.Routes {
			switch routeStatus.State {
			case atomixv3beta3.RouteConnected:
				routeStatus.State = atomixv3beta3.RouteConfiguring
			}
			podStatus.Proxy.Routes[i] = routeStatus
		}
		podStatus.ResourceVersion = pod.ResourceVersion
		if err := r.setPodStatus(ctx, log, profile, podStatus); err != nil {
			log.Error(err)
			return false, err
		}
		return true, nil
	}

	// Iterate through the profile's bindings and inject the routes into the pod
	for _, binding := range profile.Spec.Bindings {
		routeStatus := atomixv3beta3.RouteStatus{
			Store: binding.Store,
		}
		var routeIndex *int
		for i, rs := range podStatus.Proxy.Routes {
			if rs.Store.Namespace == binding.Store.Namespace && rs.Store.Name == binding.Store.Name {
				routeStatus = rs
				routeIndex = &i
				break
			}
		}

		if updated, err := r.reconcileRoute(ctx, log, pod, &binding, &routeStatus); err != nil {
			return false, err
		} else if updated {
			if routeIndex == nil {
				podStatus.Proxy.Routes = append(podStatus.Proxy.Routes, routeStatus)
			} else {
				podStatus.Proxy.Routes[*routeIndex] = routeStatus
			}
			if err := r.setPodStatus(ctx, log, profile, podStatus); err != nil {
				log.Error(err)
				return false, err
			}
			switch routeStatus.State {
			case atomixv3beta3.RouteConnecting, atomixv3beta3.RouteDisconnected:
				if ok, err := r.setReadyCondition(ctx, log, pod, corev1.ConditionFalse, "RouteNotConnected", fmt.Sprintf("Route to '%s' is not connected", binding.Store.Name)); err != nil {
					log.Error(err)
					return ok, err
				}
			}
			return true, nil
		} else {
			switch routeStatus.State {
			case atomixv3beta3.RouteConnecting, atomixv3beta3.RouteDisconnected:
				ready = false
			}
		}
	}

	if ready {
		return r.setReadyCondition(ctx, log, pod, corev1.ConditionTrue, "RoutesConnected", "")
	}
	return false, nil
}

func (r *PodReconciler) reconcileRoute(ctx context.Context, log logging.Logger, pod *corev1.Pod, binding *atomixv3beta3.Binding, status *atomixv3beta3.RouteStatus) (bool, error) {
	storeNamespace := binding.Store.Namespace
	if storeNamespace == "" {
		storeNamespace = pod.Namespace
	}
	storeNamespacedName := types.NamespacedName{
		Namespace: storeNamespace,
		Name:      binding.Store.Name,
	}

	log = log.WithFields(logging.String("DataStore", storeNamespacedName.String()))

	log.Debug("Reconciling profile binding")

	// If the route is ready but the store has been removed, disconnect the route
	store := &atomixv3beta3.DataStore{}
	if err := r.client.Get(ctx, storeNamespacedName, store); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return false, err
		}

		switch status.State {
		case atomixv3beta3.RoutePending:
			return false, nil
		case atomixv3beta3.RouteConnecting, atomixv3beta3.RouteConnected, atomixv3beta3.RouteConfiguring:
			log.Info("DataStore not found; disconnecting proxy...")
			status.State = atomixv3beta3.RouteDisconnecting
			return true, nil
		case atomixv3beta3.RouteDisconnecting:
			conn, err := connect(ctx, pod)
			if err != nil {
				log.Error(err)
				return false, err
			}

			client := runtimev1.NewRuntimeClient(conn)
			request := &runtimev1.DisconnectRequest{
				StoreID: runtimev1.StoreID{
					Namespace: storeNamespacedName.Namespace,
					Name:      storeNamespacedName.Name,
				},
			}
			_, err = client.Disconnect(ctx, request)
			if err != nil {
				if !errors.IsNotFound(err) {
					log.Error(err)
					r.events.Eventf(pod, "Warning", "DisconnectStoreFailed", "Failed disconnecting from store '%s': %s", storeNamespacedName, err)
					return false, err
				}
			}

			r.events.Eventf(pod, "Normal", "DisconnectedStore", "Successfully disconnected from store '%s'", storeNamespacedName)
			log.Info("Disconnected proxy")
			status.State = atomixv3beta3.RouteDisconnected
			status.Version = ""
			return true, nil
		default:
			status.State = atomixv3beta3.RoutePending
			return true, nil
		}
	}

	switch status.State {
	case atomixv3beta3.RoutePending:
		log.Info("Connecting proxy to store")
		status.State = atomixv3beta3.RouteConnecting
		status.Version = store.ResourceVersion
		return true, nil
	case atomixv3beta3.RouteConnecting:
		if status.Version != store.ResourceVersion {
			log.Debug("DataStore configuration changed; reconfiguring proxy...")
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
		request := &runtimev1.ConnectRequest{
			DriverID: runtimev1.DriverID{
				Name:       store.Spec.Driver.Name,
				APIVersion: driverVersion,
			},
			Store: runtimev1.Store{
				StoreID: runtimev1.StoreID{
					Namespace: storeNamespacedName.Namespace,
					Name:      storeNamespacedName.Name,
				},
				Spec: &gogotypes.Any{
					Value: store.Spec.Config.Raw,
				},
			},
		}
		_, err = client.Connect(ctx, request)
		if err != nil {
			if !errors.IsAlreadyExists(err) {
				log.Error(err)
				r.events.Eventf(pod, "Warning", "ConnectStoreFailed", "Failed connecting to store '%s': %s", storeNamespacedName, err)
				return false, err
			}
		}

		r.events.Eventf(pod, "Normal", "ConnectedStore", "Successfully connected to store '%s'", storeNamespacedName)
		log.Info("Proxy connected")
		status.State = atomixv3beta3.RouteConnected
		return true, nil
	case atomixv3beta3.RouteConnected:
		if status.Version != store.ResourceVersion {
			log.Debug("DataStore configuration changed; reconfiguring proxy...")
			status.State = atomixv3beta3.RouteConfiguring
			status.Version = store.ResourceVersion
			return true, nil
		}
	case atomixv3beta3.RouteConfiguring:
		if status.Version != store.ResourceVersion {
			log.Debug("DataStore configuration changed; reconfiguring proxy...")
			status.Version = store.ResourceVersion
			return true, nil
		}

		conn, err := connect(ctx, pod)
		if err != nil {
			log.Error(err)
			return false, err
		}

		client := runtimev1.NewRuntimeClient(conn)
		request := &runtimev1.ConfigureRequest{
			Store: runtimev1.Store{
				StoreID: runtimev1.StoreID{
					Namespace: storeNamespacedName.Namespace,
					Name:      storeNamespacedName.Name,
				},
				Spec: &gogotypes.Any{
					Value: store.Spec.Config.Raw,
				},
			},
		}
		_, err = client.Configure(ctx, request)
		if err != nil {
			if !errors.IsNotFound(err) {
				r.events.Eventf(pod, "Warning", "ConfigureStoreFailed", "Failed reconfiguring store '%s': %s", storeNamespacedName, err)
				log.Error(err)
				return false, err
			}
			// If the proxy returned a NotFound error, set the route to Connecting to establish the connection.
			log.Warn("Proxy not connected; connecting...")
			status.State = atomixv3beta3.RouteConnecting
			return true, nil
		}

		r.events.Eventf(pod, "Normal", "ConfiguredStore", "Configured store '%s'", storeNamespacedName)
		log.Info("Proxy configured")
		status.State = atomixv3beta3.RouteConnected
		return true, nil
	default:
		status.State = atomixv3beta3.RoutePending
		return true, nil
	}
	return false, nil
}

func (r *PodReconciler) getPodStatus(profile *atomixv3beta3.StorageProfile, pod *corev1.Pod) atomixv3beta3.PodStatus {
	for _, podStatus := range profile.Status.PodStatuses {
		if podStatus.Name == pod.Name && podStatus.UID == pod.UID {
			return podStatus
		}
	}
	return atomixv3beta3.PodStatus{
		ObjectReference: corev1.ObjectReference{
			Namespace:       pod.Namespace,
			Name:            pod.Name,
			UID:             pod.UID,
			ResourceVersion: pod.ResourceVersion,
		},
	}
}

func (r *PodReconciler) setPodStatus(ctx context.Context, log logging.Logger, profile *atomixv3beta3.StorageProfile, status atomixv3beta3.PodStatus) error {
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

func (r *PodReconciler) setReadyCondition(ctx context.Context, log logging.Logger, pod *corev1.Pod, status corev1.ConditionStatus, reason string, message string) (bool, error) {
	for i, condition := range pod.Status.Conditions {
		if condition.Type == proxyReadyCondition {
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
		Type:               proxyReadyCondition,
		Status:             status,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	})
	return true, r.client.Status().Update(ctx, pod)
}

func connect(ctx context.Context, pod *corev1.Pod) (*grpc.ClientConn, error) {
	target := fmt.Sprintf("%s:%d", pod.Status.PodIP, defaultProxyPort)
	return grpc.DialContext(ctx, target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(interceptors.ErrorHandlingUnaryClientInterceptor()),
		grpc.WithStreamInterceptor(interceptors.RetryingStreamClientInterceptor()))
}
