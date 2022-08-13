// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v3beta1

import (
	"context"
	"encoding/json"
	atomixv3beta1 "github.com/atomix/runtime/controller/pkg/apis/storage/v3beta1"
	"github.com/atomix/runtime/proxy/pkg/proxy"
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
	"time"
)

const configFile = "config.yaml"

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
	err = c.Watch(&source.Kind{Type: &atomixv3beta1.Profile{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for changes to ConfigMap
	err = c.Watch(&source.Kind{Type: &corev1.ConfigMap{}}, &handler.EnqueueRequestForOwner{
		OwnerType: &atomixv3beta1.Profile{},
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

// Reconcile reconciles Profile resources
func (r *ProfileReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log.Infof("Reconciling Profile '%s'", request.NamespacedName)
	profile := &atomixv3beta1.Profile{}
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

		var routerConfig proxy.RouterConfig
		for _, route := range profile.Spec.Routes {
			var routeConfig proxy.RouteConfig
			storeNamespace := route.Store.Namespace
			if storeNamespace == "" {
				storeNamespace = profile.Namespace
			}
			routeConfig.Store = proxy.StoreID{
				Namespace: storeNamespace,
				Name:      route.Store.Name,
			}
			for _, binding := range route.Bindings {
				var bindingConfig proxy.BindingConfig
				for _, service := range binding.Services {
					config := make(map[string]interface{})
					if service.Config.Raw != nil {
						if err := json.Unmarshal(service.Config.Raw, &config); err != nil {
							return reconcile.Result{}, err
						}
					}
					serviceConfig := proxy.ServiceConfig{
						Name:   service.Name,
						Config: config,
					}
					bindingConfig.Services = append(bindingConfig.Services, serviceConfig)
				}
				for _, rule := range binding.MatchRules {
					ruleConfig := proxy.MatchRuleConfig{
						Names: rule.Names,
						Tags:  rule.Tags,
					}
					bindingConfig.MatchRules = append(bindingConfig.MatchRules, ruleConfig)
				}
				routeConfig.Bindings = append(routeConfig.Bindings, bindingConfig)
			}
			routerConfig.Routes = append(routerConfig.Routes, routeConfig)
		}

		configBytes, err := yaml.Marshal(routerConfig)
		if err != nil {
			log.Error(err)
			return reconcile.Result{}, err
		}

		configMap = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: profile.Namespace,
				Name:      profile.Name,
			},
			BinaryData: map[string][]byte{
				configFile: configBytes,
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
