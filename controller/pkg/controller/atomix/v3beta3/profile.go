// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v3beta3

import (
	"context"
	"fmt"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	atomixv3beta3 "github.com/atomix/atomix/controller/pkg/apis/atomix/v3beta3"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/gogo/protobuf/jsonpb"
	gogotypes "github.com/gogo/protobuf/types"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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

const (
	configFile     = "config.yaml"
	loggingFile    = "logging.yaml"
	rootLoggerName = "root"
	stdoutSinkName = "stdout"
)

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
	log := log.WithFields(logging.String("StorageProfile", request.NamespacedName.String()))
	log.Debug("Reconciling StorageProfile")

	profile := &atomixv3beta3.StorageProfile{}
	err := r.client.Get(context.TODO(), request.NamespacedName, profile)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		log.Warn(err)
		return reconcile.Result{}, err
	}

	configMapName := types.NamespacedName{
		Namespace: profile.Namespace,
		Name:      fmt.Sprintf("%s-proxy-config", profile.Name),
	}
	configMap := &corev1.ConfigMap{}
	if err := r.client.Get(ctx, configMapName, configMap); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return reconcile.Result{}, err
		}

		log.Infof("Creating ConfigMap", logging.String("ConfigMap", request.NamespacedName.String()))

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

		routes := make([]*runtimev1.Route, 0, len(bindings))
		for _, binding := range bindings {
			storeID := runtimev1.StoreID{
				Namespace: binding.Store.Namespace,
				Name:      binding.Store.Name,
			}
			if storeID.Namespace == "" {
				storeID.Namespace = profile.Namespace
			}

			route := &runtimev1.Route{
				StoreID: storeID,
				Tags:    binding.MatchTags,
			}
			if binding.Tags != nil {
				route.Tags = binding.Tags
			}

			for _, primitive := range binding.Primitives {
				meta := runtimev1.Primitive{
					PrimitiveMeta: runtimev1.PrimitiveMeta{
						Type: runtimev1.PrimitiveType{
							Name:       primitive.Kind,
							APIVersion: primitive.APIVersion,
						},
						PrimitiveID: runtimev1.PrimitiveID{
							Name: primitive.Name,
						},
						Tags: primitive.MatchTags,
					},
					Spec: &gogotypes.Any{
						Value: primitive.Config.Raw,
					},
				}
				if primitive.Tags != nil {
					meta.Tags = primitive.Tags
				}
				route.Primitives = append(route.Primitives, meta)
			}

			routes = append(routes, route)
		}

		config := runtimev1.RuntimeConfig{
			Routes: routes,
		}

		marshaler := &jsonpb.Marshaler{}
		configString, err := marshaler.MarshalToString(&config)
		if err != nil {
			log.Error(err)
			return reconcile.Result{}, err
		}

		sinkName := stdoutSinkName
		loggingOutputs := map[string]logging.OutputConfig{
			stdoutSinkName: {
				Name: stdoutSinkName,
				Sink: &sinkName,
			},
		}

		sinkEncoding := logging.SinkEncoding(profile.Spec.Proxy.Logging.Encoding)
		loggingConfig := logging.Config{
			Loggers: map[string]logging.LoggerConfig{
				rootLoggerName: {
					Level:  &profile.Spec.Proxy.Logging.RootLevel,
					Output: loggingOutputs,
				},
			},
			Sinks: map[string]logging.SinkConfig{
				stdoutSinkName: {
					Name:     stdoutSinkName,
					Encoding: &sinkEncoding,
					Stdout:   &logging.StdoutSinkConfig{},
				},
			},
		}

		for _, loggerConfig := range profile.Spec.Proxy.Logging.Loggers {
			loggingConfig.Loggers[loggerConfig.Name] = logging.LoggerConfig{
				Level:  loggerConfig.Level,
				Output: loggingOutputs,
			}
		}

		loggingBytes, err := yaml.Marshal(&loggingConfig)
		if err != nil {
			log.Error(err)
			return reconcile.Result{}, err
		}

		configMap = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: configMapName.Namespace,
				Name:      configMapName.Name,
			},
			Data: map[string]string{
				configFile:  configString,
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
