// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v3beta2

import (
	"context"
	"fmt"
	proxyv1 "github.com/atomix/runtime/api/atomix/proxy/v1"
	atomixv3beta2 "github.com/atomix/runtime/controller/pkg/apis/storage/v3beta2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

const proxyReadyCondition = "proxy.storage.atomix.io/ready"

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

	// Watch for changes to Profiles
	err = c.Watch(&source.Kind{Type: &atomixv3beta2.Profile{}}, handler.EnqueueRequestsFromMapFunc(func(object client.Object) []reconcile.Request {
		podList := &corev1.PodList{}
		if err := mgr.GetClient().List(context.Background(), podList, &client.ListOptions{Namespace: object.GetNamespace()}); err != nil {
			log.Error(err)
			return nil
		}

		var requests []reconcile.Request
		for _, pod := range podList.Items {
			if pod.Annotations[proxyInjectStatusAnnotation] == injectedStatus && pod.Annotations[proxyProfileAnnotation] == object.GetName() {
				requests = append(requests, reconcile.Request{
					NamespacedName: types.NamespacedName{
						Namespace: pod.Namespace,
						Name:      pod.Name,
					},
				})
			}
		}
		return requests
	}))
	if err != nil {
		return err
	}

	// Watch for changes to Stores
	err = c.Watch(&source.Kind{Type: &atomixv3beta2.Store{}}, handler.EnqueueRequestsFromMapFunc(func(object client.Object) []reconcile.Request {
		podList := &corev1.PodList{}
		if err := mgr.GetClient().List(context.Background(), podList, &client.ListOptions{Namespace: object.GetNamespace()}); err != nil {
			log.Error(err)
			return nil
		}

		var requests []reconcile.Request
		for _, pod := range podList.Items {
			if pod.Annotations[proxyInjectStatusAnnotation] == injectedStatus {
				requests = append(requests, reconcile.Request{
					NamespacedName: types.NamespacedName{
						Namespace: pod.Namespace,
						Name:      pod.Name,
					},
				})
			}
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

func getPodStatus(profile *atomixv3beta2.Profile, pod *corev1.Pod) atomixv3beta2.PodStatus {
	for _, podStatus := range profile.Status.PodStatuses {
		if podStatus.Name == pod.Name {
			return podStatus
		}
	}
	return atomixv3beta2.PodStatus{
		Name: pod.Name,
	}
}

func setPodStatus(profile *atomixv3beta2.Profile, podStatus atomixv3beta2.PodStatus) {
	for i, status := range profile.Status.PodStatuses {
		if status.Name == podStatus.Name {
			profile.Status.PodStatuses[i] = podStatus
			return
		}
	}
	profile.Status.PodStatuses = append(profile.Status.PodStatuses, podStatus)
}

func getRouteStatus(podStatus *atomixv3beta2.PodStatus, route atomixv3beta2.Binding) atomixv3beta2.RouteStatus {
	for _, routeStatus := range podStatus.Proxy.Routes {
		if routeStatus.Store.Namespace == route.Store.Namespace && routeStatus.Store.Name == route.Store.Name {
			return routeStatus
		}
	}
	return atomixv3beta2.RouteStatus{
		Store: route.Store,
	}
}

func setRouteStatus(podStatus *atomixv3beta2.PodStatus, routeStatus atomixv3beta2.RouteStatus) {
	for i, status := range podStatus.Proxy.Routes {
		if status.Store.Namespace == routeStatus.Store.Namespace && status.Store.Name == routeStatus.Store.Name {
			podStatus.Proxy.Routes[i] = routeStatus
			return
		}
	}
	podStatus.Proxy.Routes = append(podStatus.Proxy.Routes, routeStatus)
}

// Reconcile reconciles Profile resources
func (r *PodReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log.Infof("Reconciling Pod '%s'", request.NamespacedName)
	pod := &corev1.Pod{}
	err := r.client.Get(context.TODO(), request.NamespacedName, pod)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		log.Error(err)
		return reconcile.Result{}, err
	}

	profileName, ok := pod.Annotations[proxyProfileAnnotation]
	if !ok {
		return reconcile.Result{}, nil
	}

	profileNamespacedName := types.NamespacedName{
		Namespace: pod.Namespace,
		Name:      profileName,
	}
	profile := &atomixv3beta2.Profile{}
	if err := r.client.Get(ctx, profileNamespacedName, profile); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	if ok, err := r.reconcileProfile(ctx, pod, profile); err != nil {
		return reconcile.Result{}, err
	} else if ok {
		return reconcile.Result{}, nil
	}
	return reconcile.Result{}, nil
}

func (r *PodReconciler) reconcileProfile(ctx context.Context, pod *corev1.Pod, profile *atomixv3beta2.Profile) (bool, error) {
	status := getPodStatus(profile, pod)
	if ok, err := r.reconcileRoutes(ctx, pod, profile, &status); err != nil {
		return false, err
	} else if ok {
		setPodStatus(profile, status)
		if err := r.client.Status().Update(ctx, profile); err != nil {
			log.Error(err)
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (r *PodReconciler) reconcileRoutes(ctx context.Context, pod *corev1.Pod, profile *atomixv3beta2.Profile, status *atomixv3beta2.PodStatus) (bool, error) {
	if len(status.Proxy.Routes) == 0 {
		if ok, err := r.setReadyCondition(pod, corev1.ConditionFalse, "ConfiguringRoutes", "Configuring routes"); err != nil {
			return false, err
		} else if ok {
			return false, nil
		}
	}

	ready := true
	for _, binding := range profile.Spec.Bindings {
		routeStatus := getRouteStatus(status, binding)
		if ok, err := r.reconcileRoute(ctx, pod, binding, &routeStatus); err != nil {
			return false, err
		} else if ok {
			setRouteStatus(status, routeStatus)
			return true, nil
		} else if ready && !routeStatus.Ready {
			ready = false
			if ok, err := r.setReadyCondition(pod, corev1.ConditionFalse, "ConfiguringRoutes", fmt.Sprintf("Configuring route to '%s'", binding.Store.Name)); err != nil {
				return false, err
			} else if ok {
				return false, nil
			}
		}
	}

	if status.Proxy.Ready != ready {
		status.Proxy.Ready = ready
		return true, nil
	}

	if status.Proxy.Ready {
		if ok, err := r.setReadyCondition(pod, corev1.ConditionTrue, "", ""); err != nil {
			return false, err
		} else if ok {
			return false, nil
		}
	}
	return false, nil
}

func (r *PodReconciler) reconcileRoute(ctx context.Context, pod *corev1.Pod, route atomixv3beta2.Binding, status *atomixv3beta2.RouteStatus) (bool, error) {
	storeNamespace := route.Store.Namespace
	if storeNamespace == "" {
		storeNamespace = pod.Namespace
	}
	storeNamespacedName := types.NamespacedName{
		Namespace: storeNamespace,
		Name:      route.Store.Name,
	}

	// If the route is ready but the store has been removed, disconnect the route
	store := &atomixv3beta2.Store{}
	if err := r.client.Get(ctx, storeNamespacedName, store); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return false, err
		}

		if status.Ready {
			// Disconnect the binding in the pod
			conn, err := connect(ctx, pod)
			if err != nil {
				log.Error(err)
				return false, err
			}

			r.events.Eventf(pod, "Normal", "DisconnectStore", "Disconnecting store '%s'", storeNamespacedName)
			client := proxyv1.NewProxyClient(conn)
			request := &proxyv1.DisconnectRequest{
				StoreID: proxyv1.StoreId{
					Namespace: storeNamespacedName.Namespace,
					Name:      storeNamespacedName.Name,
				},
			}
			_, err = client.Disconnect(ctx, request)
			if err != nil {
				log.Error(err)
				r.events.Eventf(pod, "Warning", "DisconnectStoreFailed", "Failed disconnecting from store '%s': %s", storeNamespacedName, err)
				return false, err
			}
			r.events.Eventf(pod, "Normal", "DisconnectStoreSucceeded", "Successfully disconnected from store '%s'", storeNamespacedName)

			// Update the binding status
			status.Ready = false
			status.Version = ""
			return true, nil
		}
		return false, nil
	}

	// If the route is ready but the store version changed, update the route configuration
	if status.Ready && status.Version != store.ResourceVersion {
		// Configure the binding in the pod
		conn, err := connect(ctx, pod)
		if err != nil {
			log.Error(err)
			return false, err
		}

		r.events.Eventf(pod, "Normal", "ConfigureStore", "Configuring store '%s'", storeNamespacedName)
		client := proxyv1.NewProxyClient(conn)
		request := &proxyv1.ConfigureRequest{
			StoreID: proxyv1.StoreId{
				Namespace: storeNamespacedName.Namespace,
				Name:      storeNamespacedName.Name,
			},
			Config: store.Spec.Config.Raw,
		}
		_, err = client.Configure(ctx, request)
		if err != nil {
			r.events.Eventf(pod, "Warning", "ConfigureStoreFailed", "Failed reconfiguring store '%s': %s", storeNamespacedName, err)
			log.Error(err)
			return false, err
		}
		r.events.Eventf(pod, "Normal", "ConfigureStoreSucceeded", "Successfully configured store '%s'", storeNamespacedName)

		// Update the binding status
		status.Version = store.ResourceVersion
		return true, nil
	}

	// If the route is not ready, connect the route
	if !status.Ready {
		// Connect the binding in the pod
		conn, err := connect(ctx, pod)
		if err != nil {
			log.Error(err)
			return false, err
		}

		r.events.Eventf(pod, "Normal", "ConnectStore", "Connecting store '%s'", storeNamespacedName)
		client := proxyv1.NewProxyClient(conn)
		request := &proxyv1.ConnectRequest{
			StoreID: proxyv1.StoreId{
				Namespace: storeNamespacedName.Namespace,
				Name:      storeNamespacedName.Name,
			},
			DriverID: proxyv1.DriverId{
				Name:    store.Spec.Driver.Name,
				Version: store.Spec.Driver.Version,
			},
			Config: store.Spec.Config.Raw,
		}
		_, err = client.Connect(ctx, request)
		if err != nil {
			log.Error(err)
			r.events.Eventf(pod, "Warning", "ConnectStoreFailed", "Failed connecting to store '%s': %s", storeNamespacedName, err)
			return false, err
		}
		r.events.Eventf(pod, "Normal", "ConnectStoreSucceeded", "Successfully connected to store '%s'", storeNamespacedName)

		// Update the binding status
		status.Ready = true
		status.Version = store.ResourceVersion
		return true, nil
	}
	return false, nil
}

func (r *PodReconciler) setReadyCondition(pod *corev1.Pod, status corev1.ConditionStatus, reason string, message string) (bool, error) {
	for i, condition := range pod.Status.Conditions {
		if condition.Type == proxyReadyCondition {
			if condition.Status == status && condition.Reason == reason {
				return false, nil
			}
			log.Infof("Updating Pod %s condition: status=%s, reason=%s, message=%s",
				types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, status, reason, message)
			if condition.Status != status {
				condition.LastTransitionTime = metav1.Now()
			}
			condition.Status = status
			condition.Reason = reason
			condition.Message = message
			pod.Status.Conditions[i] = condition
			if err := r.client.Status().Update(context.TODO(), pod); err != nil {
				log.Error(err)
				return false, err
			}
			return true, nil
		}
	}

	log.Infof("Initializing Pod %s condition: status=%s, reason=%s, message=%s",
		types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, status, reason, message)
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type:               proxyReadyCondition,
		Status:             status,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	})
	if err := r.client.Status().Update(context.TODO(), pod); err != nil {
		log.Error(err)
		return false, err
	}
	return true, nil
}

func connect(ctx context.Context, pod *corev1.Pod) (*grpc.ClientConn, error) {
	target := fmt.Sprintf("%s:%d", pod.Status.PodIP, defaultProxyPort)
	return grpc.DialContext(ctx, target, grpc.WithTransportCredentials(insecure.NewCredentials()))
}
