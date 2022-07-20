// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta1

import (
	"context"
	"fmt"
	proxyv1 "github.com/atomix/proxy/api/atomix/proxy/v1"
	atomixv1beta1 "github.com/atomix/runtime/controller/pkg/apis/atomix/v1beta1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"net/http"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	"strconv"
	"time"
)

const (
	podIDEnv           = "POD_ID"
	podNamespaceEnv    = "POD_NAMESPACE"
	podNameEnv         = "POD_NAME"
	nodeIDEnv          = "NODE_ID"
	atomixNamespaceEnv = "ATOMIX_NAMESPACE"
)

const (
	proxyInjectPath             = "/inject-proxy"
	proxyInjectAnnotation       = "proxy.atomix.io/inject"
	proxyInjectStatusAnnotation = "proxy.atomix.io/status"
	proxyProfileAnnotation      = "proxy.atomix.io/profile"
	injectedStatus              = "injected"
	proxyContainerName          = "atomix-proxy"
)

const (
	proxyImageEnv     = "PROXY_IMAGE"
	defaultProxyImage = "atomix/proxy:latest"
)

const (
	defaultProxyPort = 5679
)

func getProxyImage() string {
	image := os.Getenv(proxyImageEnv)
	if image != "" {
		return image
	}
	return defaultProxyImage
}

func addProxyController(mgr manager.Manager) error {
	mgr.GetWebhookServer().Register(proxyInjectPath, &webhook.Admission{
		Handler: &ProxyInjector{
			client: mgr.GetClient(),
			scheme: mgr.GetScheme(),
		},
	})

	// Create a new controller
	c, err := controller.New("proxy-controller", mgr, controller.Options{
		Reconciler: &ProxyReconciler{
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

	// Watch for changes to Proxies
	err = c.Watch(&source.Kind{Type: &atomixv1beta1.Proxy{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for changes to Profiles
	err = c.Watch(&source.Kind{Type: &atomixv1beta1.Profile{}}, handler.EnqueueRequestsFromMapFunc(func(object client.Object) []reconcile.Request {
		proxyList := &atomixv1beta1.ProxyList{}
		if err := mgr.GetClient().List(context.Background(), proxyList, &client.ListOptions{Namespace: object.GetNamespace()}); err != nil {
			log.Error(err)
			return nil
		}

		var requests []reconcile.Request
		for _, proxy := range proxyList.Items {
			if proxy.Profile.Name == object.GetName() {
				requests = append(requests, reconcile.Request{
					NamespacedName: types.NamespacedName{
						Namespace: proxy.Namespace,
						Name:      proxy.Name,
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
	err = c.Watch(&source.Kind{Type: &atomixv1beta1.Store{}}, handler.EnqueueRequestsFromMapFunc(func(object client.Object) []reconcile.Request {
		proxyList := &atomixv1beta1.ProxyList{}
		if err := mgr.GetClient().List(context.Background(), proxyList); err != nil {
			log.Error(err)
			return nil
		}

		var requests []reconcile.Request
		for _, proxy := range proxyList.Items {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: proxy.Namespace,
					Name:      proxy.Name,
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

// ProxyReconciler is a Reconciler for Proxies
type ProxyReconciler struct {
	client client.Client
	scheme *runtime.Scheme
	config *rest.Config
	events record.EventRecorder
}

// Reconcile reconciles Proxy resources
func (r *ProxyReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log.Infof("Reconciling Proxy '%s'", request.NamespacedName)
	proxy := &atomixv1beta1.Proxy{}
	err := r.client.Get(context.TODO(), request.NamespacedName, proxy)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		log.Error(err)
		return reconcile.Result{}, err
	}

	podNamespacedName := types.NamespacedName{
		Namespace: proxy.Namespace,
		Name:      proxy.Name,
	}
	pod := &corev1.Pod{}
	if err := r.client.Get(ctx, podNamespacedName, pod); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	profileNamespacedName := types.NamespacedName{
		Namespace: proxy.Namespace,
		Name:      proxy.Profile.Name,
	}
	profile := &atomixv1beta1.Profile{}
	if err := r.client.Get(ctx, profileNamespacedName, profile); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	for _, binding := range profile.Spec.Bindings {
		if ok, err := r.reconcileBinding(ctx, pod, proxy, binding); err != nil {
			return reconcile.Result{}, err
		} else if ok {
			return reconcile.Result{}, nil
		}
	}
	return reconcile.Result{}, nil
}

func (r *ProxyReconciler) setStatus(ctx context.Context, proxy *atomixv1beta1.Proxy) error {
	ready := true
	for _, status := range proxy.Status.Bindings {
		if status.State != atomixv1beta1.BindingBound {
			ready = false
			break
		}
	}
	proxy.Status.Ready = ready
	return r.client.Status().Update(ctx, proxy)
}

func (r *ProxyReconciler) reconcileBinding(ctx context.Context, pod *corev1.Pod, proxy *atomixv1beta1.Proxy, binding atomixv1beta1.ProfileBinding) (bool, error) {
	storeNamespace := binding.Store.Namespace
	if storeNamespace == "" {
		storeNamespace = proxy.Namespace
	}
	storeNamespacedName := types.NamespacedName{
		Namespace: storeNamespace,
		Name:      binding.Store.Name,
	}
	store := &atomixv1beta1.Store{}
	if err := r.client.Get(ctx, storeNamespacedName, store); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return false, err
		}

		for i, status := range proxy.Status.Bindings {
			if status.Name == binding.Name {
				switch status.State {
				case atomixv1beta1.BindingBound:
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
					status.State = atomixv1beta1.BindingUnbound
					status.Version = ""
					proxy.Status.Bindings[i] = status
					if err := r.setStatus(ctx, proxy); err != nil {
						log.Error(err)
						return false, err
					}
					return true, nil
				}
				return false, nil
			}
		}
		return false, nil
	}

	for i, status := range proxy.Status.Bindings {
		if status.Name == binding.Name {
			switch status.State {
			case atomixv1beta1.BindingUnbound:
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
				status.State = atomixv1beta1.BindingBound
				status.Version = store.ResourceVersion
				proxy.Status.Bindings[i] = status
				if err := r.setStatus(ctx, proxy); err != nil {
					log.Error(err)
					return false, err
				}
				return true, nil
			case atomixv1beta1.BindingBound:
				if status.Version != store.ResourceVersion {
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
					proxy.Status.Bindings[i] = status
					if err := r.setStatus(ctx, proxy); err != nil {
						log.Error(err)
						return false, err
					}
					return true, nil
				}
			}
			return false, nil
		}
	}

	status := atomixv1beta1.BindingStatus{
		Name:  binding.Name,
		State: atomixv1beta1.BindingUnbound,
	}
	proxy.Status.Bindings = append(proxy.Status.Bindings, status)
	if err := r.setStatus(ctx, proxy); err != nil {
		log.Error(err)
		return false, err
	}
	return true, nil
}

func connect(ctx context.Context, pod *corev1.Pod) (*grpc.ClientConn, error) {
	target := fmt.Sprintf("%s:%d", pod.Status.PodIP, defaultProxyPort)
	return grpc.DialContext(ctx, target, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// ProxyInjector is a mutating webhook that injects the proxy container into pods
type ProxyInjector struct {
	client  client.Client
	scheme  *runtime.Scheme
	decoder *admission.Decoder
}

// InjectDecoder :
func (i *ProxyInjector) InjectDecoder(decoder *admission.Decoder) error {
	i.decoder = decoder
	return nil
}

// Handle :
func (i *ProxyInjector) Handle(ctx context.Context, request admission.Request) admission.Response {
	log.Infof("Received admission request for Pod '%s'", request.UID)

	// Decode the pod
	pod := &corev1.Pod{}
	if err := i.decoder.Decode(request, pod); err != nil {
		log.Errorf("Could not decode Pod '%s'", request.UID, err)
		return admission.Errored(http.StatusBadRequest, err)
	}

	injectRuntime, ok := pod.Annotations[proxyInjectAnnotation]
	if !ok {
		log.Infof("Skipping proxy injection for Pod '%s'", request.UID)
		return admission.Allowed(fmt.Sprintf("'%s' annotation not found", proxyInjectAnnotation))
	}
	if inject, err := strconv.ParseBool(injectRuntime); err != nil {
		log.Errorf("Runtime injection failed for Pod '%s'", request.UID, err)
		return admission.Allowed(fmt.Sprintf("'%s' annotation could not be parsed", proxyInjectAnnotation))
	} else if !inject {
		log.Infof("Skipping proxy injection for Pod '%s'", request.UID)
		return admission.Allowed(fmt.Sprintf("'%s' annotation is false", proxyInjectAnnotation))
	}

	injectedRuntime, ok := pod.Annotations[proxyInjectStatusAnnotation]
	if ok && injectedRuntime == injectedStatus {
		log.Infof("Skipping proxy injection for Pod '%s'", request.UID)
		return admission.Allowed(fmt.Sprintf("'%s' annotation is '%s'", proxyInjectStatusAnnotation, injectedRuntime))
	}

	profileName, ok := pod.Annotations[proxyProfileAnnotation]
	if !ok {
		log.Warnf("No profile specified for Pod '%s'", request.UID)
		return admission.Denied(fmt.Sprintf("'%s' annotation not found", proxyProfileAnnotation))
	}

	pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{
		Name:            proxyContainerName,
		Image:           getProxyImage(),
		ImagePullPolicy: corev1.PullIfNotPresent,
		Args: []string{
			"--config",
			fmt.Sprintf("/etc/atomix/%s", configFile),
		},
		Env: []corev1.EnvVar{
			{
				Name: podIDEnv,
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.uid",
					},
				},
			},
			{
				Name: podNamespaceEnv,
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.namespace",
					},
				},
			},
			{
				Name: podNameEnv,
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.name",
					},
				},
			},
			{
				Name: nodeIDEnv,
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "spec.nodeName",
					},
				},
			},
			{
				Name: atomixNamespaceEnv,
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.namespace",
					},
				},
			},
		},
		Ports: []corev1.ContainerPort{
			{
				Name:          "runtime",
				ContainerPort: 5678,
			},
			{
				Name:          "control",
				ContainerPort: 5679,
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "config",
				ReadOnly:  true,
				MountPath: "/etc/atomix",
			},
		},
	})
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: "config",
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: profileName,
				},
			},
		},
	})
	pod.Annotations[proxyInjectStatusAnnotation] = injectedStatus

	// Marshal the pod and return a patch response
	marshaledPod, err := json.Marshal(pod)
	if err != nil {
		log.Errorf("Runtime injection failed for Pod '%s'", request.UID, err)
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(request.Object.Raw, marshaledPod)
}

var _ admission.Handler = &ProxyInjector{}
