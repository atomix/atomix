// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v3beta3

import (
	"context"
	"encoding/json"
	atomixv3beta3 "github.com/atomix/atomix/controller/pkg/apis/atomix/v3beta3"
	"github.com/atomix/atomix/proxy/pkg/proxy"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"sort"
	"time"
)

const configFile = "config.yaml"
const loggingFile = "logging.yaml"

func addProfileController(mgr manager.Manager) error {
	// Create a new controller
	c, err := controller.New("profile-controller", mgr, controller.Options{
		Reconciler: &ProfileReconciler{
			client: mgr.GetClient(),
			scheme: mgr.GetScheme(),
			config: mgr.GetConfig(),
		},
		RateLimiter: workqueue.NewItemExponentialFailureRateLimiter(time.Millisecond*10, time.Second*5),
	})
	if err != nil {
		return err
	}

	// Watch for changes to Profiles
	err = c.Watch(&source.Kind{Type: &atomixv3beta3.StorageProfile{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for changes to ConfigMap
	err = c.Watch(&source.Kind{Type: &corev1.ConfigMap{}}, &handler.EnqueueRequestForOwner{
		OwnerType: &atomixv3beta3.StorageProfile{},
	})
	if err != nil {
		return err
	}
	return nil
}

// ProfileReconciler is a Reconciler for Profiles
type ProfileReconciler struct {
	client client.Client
	scheme *runtime.Scheme
	config *rest.Config
}

// Reconcile reconciles StorageProfile resources
func (r *ProfileReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log.Infof("Reconciling StorageProfile '%s'", request.NamespacedName)
	profile := &atomixv3beta3.StorageProfile{}
	err := r.client.Get(context.TODO(), request.NamespacedName, profile)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		log.Error(err)
		return reconcile.Result{}, err
	}

	configMap := &corev1.ConfigMap{}
	if err := r.client.Get(ctx, getNamespacedName(profile), configMap); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return reconcile.Result{}, err
		}

		// Sort the bindings in priority order
		bindings := profile.Spec.Bindings
		sort.Slice(bindings, func(i, j int) bool {
			var p1, p2 uint32
			if bindings[i].Priority != nil {
				p1 = *bindings[i].Priority
			}
			if bindings[j].Priority != nil {
				p2 = *bindings[j].Priority
			}
			return p1 < p2
		})

		routes := make([]proxy.RouteConfig, 0, len(bindings))
		for _, binding := range bindings {
			storeID := proxy.StoreID{
				Namespace: binding.Store.Namespace,
				Name:      binding.Store.Name,
			}
			if storeID.Namespace == "" {
				storeID.Namespace = profile.Namespace
			}

			routeConfig := proxy.RouteConfig{
				Store: storeID,
				Tags:  binding.Tags,
			}

			for _, service := range binding.Services {
				serviceConfig := proxy.ServiceConfig{
					Name: service.Name,
				}
				if service.Config.Raw != nil {
					config := make(map[string]interface{})
					if err := json.Unmarshal(service.Config.Raw, &config); err != nil {
						return reconcile.Result{}, err
					}
					serviceConfig.Config = config
				}
				routeConfig.Services = append(routeConfig.Services, serviceConfig)
			}

			routes = append(routes, routeConfig)
		}

		var config proxy.Config
		config.Router = proxy.RouterConfig{
			Routes: routes,
		}
		config.Server = proxy.ServerConfig{
			ReadBufferSize:       profile.Spec.Proxy.Config.Server.ReadBufferSize,
			WriteBufferSize:      profile.Spec.Proxy.Config.Server.WriteBufferSize,
			NumStreamWorkers:     profile.Spec.Proxy.Config.Server.NumStreamWorkers,
			MaxConcurrentStreams: profile.Spec.Proxy.Config.Server.MaxConcurrentStreams,
		}
		if profile.Spec.Proxy.Config.Server.MaxRecvMsgSize != nil {
			maxRecvMsgSize := int(profile.Spec.Proxy.Config.Server.MaxRecvMsgSize.Value())
			config.Server.MaxRecvMsgSize = &maxRecvMsgSize
		}
		if profile.Spec.Proxy.Config.Server.MaxSendMsgSize != nil {
			maxSendMsgSize := int(profile.Spec.Proxy.Config.Server.MaxSendMsgSize.Value())
			config.Server.MaxSendMsgSize = &maxSendMsgSize
		}

		configBytes, err := yaml.Marshal(config)
		if err != nil {
			log.Error(err)
			return reconcile.Result{}, err
		}

		loggingBytes, err := yaml.Marshal(&profile.Spec.Proxy.Config.Logging)
		if err != nil {
			log.Error(err)
			return reconcile.Result{}, err
		}

		configMap = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: profile.Namespace,
				Name:      profile.Name,
			},
			Data: map[string]string{
				configFile:  string(configBytes),
				loggingFile: string(loggingBytes),
			},
		}

		if err := controllerutil.SetOwnerReference(profile, configMap, r.scheme); err != nil {
			log.Error(err)
			return reconcile.Result{}, err
		}

		if err := r.client.Create(ctx, configMap); err != nil {
			log.Error(err)
			return reconcile.Result{}, err
		}
	}
	return reconcile.Result{}, nil
}
