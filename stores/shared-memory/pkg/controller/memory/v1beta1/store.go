// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta1

import (
	"context"
	"fmt"
	atomixv3beta3 "github.com/atomix/atomix/controller/pkg/apis/atomix/v3beta3"
	rsmv1 "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/atomix/stores/shared-memory/pkg/node"
	"github.com/gogo/protobuf/jsonpb"
	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/pointer"
	"net"
	"os"
	"path/filepath"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"strings"
	"time"

	sharedmemoryv1beta1 "github.com/atomix/atomix/stores/shared-memory/pkg/apis/sharedmemory/v1beta1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	driverName    = "SharedMemory"
	driverVersion = "v2beta1"
)

const (
	apiPort                     = 5678
	probePort                   = 5678
	defaultImageEnv             = "DEFAULT_NODE_IMAGE"
	defaultImagePullPolicyEnv   = "DEFAULT_NODE_IMAGE_PULL_POLICY"
	defaultImage                = "atomix/shared-memory-node:latest"
	appLabel                    = "app"
	storeLabel                  = "store"
	appAtomix                   = "atomix"
	nodeContainerName           = "atomix-shared-memory-node"
	sharedMemoryStoreAnnotation = "sharedmemory.atomix.io/store"
)

const (
	configPath        = "/etc/atomix"
	configFile        = "config.yaml"
	loggingConfigFile = "logging.yaml"
)

const (
	configVolume = "config"
)

const clusterDomainEnv = "CLUSTER_DOMAIN"

const (
	rootLoggerName = "root"
	stdoutSinkName = "stdout"
)

func addSharedMemoryStoreController(mgr manager.Manager) error {
	options := controller.Options{
		Reconciler: &SharedMemoryStoreReconciler{
			client: mgr.GetClient(),
			scheme: mgr.GetScheme(),
			events: mgr.GetEventRecorderFor("atomix-shared-memory-storage"),
		},
		RateLimiter: workqueue.NewItemExponentialFailureRateLimiter(time.Millisecond*10, time.Second*5),
	}

	// Create a new controller
	controller, err := controller.New("atomix-shared-memory-store", mgr, options)
	if err != nil {
		return err
	}

	// Watch for changes to the storage resource and enqueue Stores that reference it
	err = controller.Watch(&source.Kind{Type: &sharedmemoryv1beta1.SharedMemoryStore{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource Deployment
	err = controller.Watch(&source.Kind{Type: &appsv1.Deployment{}}, &handler.EnqueueRequestForOwner{
		OwnerType:    &sharedmemoryv1beta1.SharedMemoryStore{},
		IsController: true,
	})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource DataStore
	err = controller.Watch(&source.Kind{Type: &atomixv3beta3.DataStore{}}, &handler.EnqueueRequestForOwner{
		OwnerType:    &sharedmemoryv1beta1.SharedMemoryStore{},
		IsController: true,
	})
	if err != nil {
		return err
	}
	return nil
}

// SharedMemoryStoreReconciler reconciles a SharedMemoryStore object
type SharedMemoryStoreReconciler struct {
	client client.Client
	scheme *runtime.Scheme
	events record.EventRecorder
}

// Reconcile reads that state of the cluster for a Store object and makes changes based on the state read
// and what is in the Store.Spec
func (r *SharedMemoryStoreReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log := log.WithFields(logging.String("SharedMemoryStore", request.NamespacedName.String()))
	log.Debug("Reconciling SharedMemoryStore")

	store := &sharedmemoryv1beta1.SharedMemoryStore{}
	err := r.client.Get(ctx, request.NamespacedName, store)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		log.Error(err)
		return reconcile.Result{}, err
	}

	if err := r.reconcileConfigMap(ctx, log, store); err != nil {
		return reconcile.Result{}, err
	}

	if err := r.reconcileDeployment(ctx, log, store); err != nil {
		return reconcile.Result{}, err
	}

	if err := r.reconcileService(ctx, log, store); err != nil {
		return reconcile.Result{}, err
	}

	if err := r.reconcileDataStore(ctx, log, store); err != nil {
		return reconcile.Result{}, err
	}

	if ok, err := r.reconcileStatus(ctx, log, store); err != nil {
		return reconcile.Result{}, err
	} else if ok {
		return reconcile.Result{}, nil
	}
	return reconcile.Result{}, nil
}

func (r *SharedMemoryStoreReconciler) reconcileConfigMap(ctx context.Context, log logging.Logger, store *sharedmemoryv1beta1.SharedMemoryStore) error {
	cm := &corev1.ConfigMap{}
	name := types.NamespacedName{
		Namespace: store.Namespace,
		Name:      store.Name,
	}
	if err := r.client.Get(ctx, name, cm); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return err
		}
		log.Infow("Creating ConfigMap", logging.Stringer("ConfigMap", name))
		return r.addConfigMap(ctx, log, store)
	}
	return nil
}

func (r *SharedMemoryStoreReconciler) addConfigMap(ctx context.Context, log logging.Logger, store *sharedmemoryv1beta1.SharedMemoryStore) error {
	sinkName := stdoutSinkName
	loggingOutputs := map[string]logging.OutputConfig{
		stdoutSinkName: {
			Name: stdoutSinkName,
			Sink: &sinkName,
		},
	}

	sinkEncoding := logging.SinkEncoding(store.Spec.Logging.Encoding)
	loggingConfig := logging.Config{
		Loggers: map[string]logging.LoggerConfig{
			rootLoggerName: {
				Level:  &store.Spec.Logging.RootLevel,
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

	for _, loggerConfig := range store.Spec.Logging.Loggers {
		loggingConfig.Loggers[loggerConfig.Name] = logging.LoggerConfig{
			Level:  loggerConfig.Level,
			Output: loggingOutputs,
		}
	}

	loggingConfigBytes, err := yaml.Marshal(&loggingConfig)
	if err != nil {
		log.Error(err)
		return err
	}

	nodeConfigBytes, err := newNodeConfig(store)
	if err != nil {
		return err
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:        store.Name,
			Namespace:   store.Namespace,
			Labels:      newStoreLabels(store),
			Annotations: newStoreAnnotations(store),
		},
		Data: map[string]string{
			configFile:        string(nodeConfigBytes),
			loggingConfigFile: string(loggingConfigBytes),
		},
	}

	if err := controllerutil.SetControllerReference(store, cm, r.scheme); err != nil {
		return err
	}
	return r.client.Create(ctx, cm)
}

// newNodeConfig creates a protocol configuration string for the given store and protocol
func newNodeConfig(store *sharedmemoryv1beta1.SharedMemoryStore) ([]byte, error) {
	config := node.Config{}
	return yaml.Marshal(&config)
}

func (r *SharedMemoryStoreReconciler) reconcileDeployment(ctx context.Context, log logging.Logger, store *sharedmemoryv1beta1.SharedMemoryStore) error {
	deployment := &appsv1.Deployment{}
	name := types.NamespacedName{
		Namespace: store.Namespace,
		Name:      store.Name,
	}
	if err := r.client.Get(ctx, name, deployment); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return err
		}
		log.Infow("Creating Deployment", logging.Stringer("Deployment", name))
		return r.addDeployment(ctx, log, store)
	}
	return nil
}

func (r *SharedMemoryStoreReconciler) addDeployment(ctx context.Context, log logging.Logger, store *sharedmemoryv1beta1.SharedMemoryStore) error {
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        store.Name,
			Namespace:   store.Namespace,
			Labels:      newStoreLabels(store),
			Annotations: newStoreAnnotations(store),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: pointer.Int32Ptr(int32(1)),
			Selector: &metav1.LabelSelector{
				MatchLabels: newStoreLabels(store),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      newStoreLabels(store),
					Annotations: newStoreAnnotations(store),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            nodeContainerName,
							Image:           getImage(store),
							ImagePullPolicy: getPullPolicy(store),
							Ports: []corev1.ContainerPort{
								{
									Name:          "api",
									ContainerPort: apiPort,
								},
							},
							Args: []string{
								"--config",
								filepath.Join(configPath, configFile),
								"--port",
								fmt.Sprint(apiPort),
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									TCPSocket: &corev1.TCPSocketAction{
										Port: intstr.IntOrString{Type: intstr.Int, IntVal: probePort},
									},
								},
								InitialDelaySeconds: 5,
								TimeoutSeconds:      10,
								FailureThreshold:    12,
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									TCPSocket: &corev1.TCPSocketAction{
										Port: intstr.IntOrString{Type: intstr.Int, IntVal: probePort},
									},
								},
								InitialDelaySeconds: 60,
								TimeoutSeconds:      10,
							},
							SecurityContext: store.Spec.SecurityContext,
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      configVolume,
									MountPath: configPath,
								},
							},
						},
					},
					ImagePullSecrets: store.Spec.ImagePullSecrets,
					Volumes: []corev1.Volume{
						{
							Name: configVolume,
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: store.Name,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(store, deployment, r.scheme); err != nil {
		return err
	}
	return r.client.Create(ctx, deployment)
}

func (r *SharedMemoryStoreReconciler) reconcileService(ctx context.Context, log logging.Logger, store *sharedmemoryv1beta1.SharedMemoryStore) error {
	service := &corev1.Service{}
	name := types.NamespacedName{
		Namespace: store.Namespace,
		Name:      store.Name,
	}
	if err := r.client.Get(ctx, name, service); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return err
		}
		log.Infow("Creating Service", logging.Stringer("Service", name))
		return r.addService(ctx, log, store)
	}
	return nil
}

func (r *SharedMemoryStoreReconciler) addService(ctx context.Context, log logging.Logger, store *sharedmemoryv1beta1.SharedMemoryStore) error {
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        store.Name,
			Namespace:   store.Namespace,
			Labels:      newStoreLabels(store),
			Annotations: newStoreAnnotations(store),
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name: "api",
					Port: apiPort,
				},
			},
			Selector: newStoreLabels(store),
		},
	}

	if err := controllerutil.SetControllerReference(store, service, r.scheme); err != nil {
		return err
	}
	return r.client.Create(ctx, service)
}

func (r *SharedMemoryStoreReconciler) reconcileDataStore(ctx context.Context, log logging.Logger, store *sharedmemoryv1beta1.SharedMemoryStore) error {
	dataStore := &atomixv3beta3.DataStore{}
	name := types.NamespacedName{
		Namespace: store.Namespace,
		Name:      store.Name,
	}
	if err := r.client.Get(ctx, name, dataStore); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return err
		}
		log.Infow("Creating DataStore", logging.Stringer("DataStore", name))
		return r.addDataStore(ctx, log, store)
	}
	return nil
}

func (r *SharedMemoryStoreReconciler) addDataStore(ctx context.Context, log logging.Logger, store *sharedmemoryv1beta1.SharedMemoryStore) error {
	config := rsmv1.ProtocolConfig{
		Partitions: []rsmv1.PartitionConfig{
			{
				PartitionID: 1,
				Leader:      fmt.Sprintf("%s.%s.svc.%s:%d", store.Name, store.Namespace, getClusterDomain(), apiPort),
			},
		},
	}
	marshaler := &jsonpb.Marshaler{}
	configString, err := marshaler.MarshalToString(&config)
	if err != nil {
		log.Error(err)
		return err
	}

	dataStore := &atomixv3beta3.DataStore{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: store.Namespace,
			Name:      store.Name,
			Labels:    store.Labels,
		},
		Spec: atomixv3beta3.DataStoreSpec{
			Driver: atomixv3beta3.Driver{
				Name:       driverName,
				APIVersion: driverVersion,
			},
			Config: runtime.RawExtension{
				Raw: []byte(configString),
			},
		},
	}
	if err := controllerutil.SetControllerReference(store, dataStore, r.scheme); err != nil {
		log.Error(err)
		return err
	}
	if err := r.client.Create(ctx, dataStore); err != nil {
		log.Error(err)
		return err
	}
	return nil
}

func (r *SharedMemoryStoreReconciler) reconcileStatus(ctx context.Context, log logging.Logger, store *sharedmemoryv1beta1.SharedMemoryStore) (bool, error) {
	deployment := &appsv1.Deployment{}
	name := types.NamespacedName{
		Namespace: store.Namespace,
		Name:      store.Name,
	}
	if err := r.client.Get(ctx, name, deployment); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
		}
		return false, err
	}

	if deployment.Status.ReadyReplicas == 0 &&
		store.Status.State != sharedmemoryv1beta1.SharedMemoryStoreNotReady {
		store.Status.State = sharedmemoryv1beta1.SharedMemoryStoreNotReady
		log.Info("SharedMemoryStore status changed",
			logging.String("Status", string(store.Status.State)))
		if err := r.client.Status().Update(ctx, store); err != nil {
			if !k8serrors.IsNotFound(err) && !k8serrors.IsConflict(err) {
				log.Error(err)
			}
			return false, err
		}
		return true, nil
	}

	if deployment.Status.ReadyReplicas == 1 &&
		store.Status.State != sharedmemoryv1beta1.SharedMemoryStoreReady {
		store.Status.State = sharedmemoryv1beta1.SharedMemoryStoreReady
		log.Info("SharedMemoryStore status changed",
			logging.String("Status", string(store.Status.State)))
		if err := r.client.Status().Update(ctx, store); err != nil {
			if !k8serrors.IsNotFound(err) && !k8serrors.IsConflict(err) {
				log.Error(err)
			}
			return false, err
		}
		return true, nil
	}
	return false, nil
}

var _ reconcile.Reconciler = (*SharedMemoryStoreReconciler)(nil)

// getClusterDomain returns Kubernetes cluster domain, default to "cluster.local"
func getClusterDomain() string {
	clusterDomain := os.Getenv(clusterDomainEnv)
	if clusterDomain == "" {
		apiSvc := "kubernetes.default.svc"
		cname, err := net.LookupCNAME(apiSvc)
		if err != nil {
			return "cluster.local"
		}
		clusterDomain = strings.TrimSuffix(strings.TrimPrefix(cname, apiSvc+"."), ".")
	}
	return clusterDomain
}

// newStoreLabels returns the labels for the given cluster
func newStoreLabels(store *sharedmemoryv1beta1.SharedMemoryStore) map[string]string {
	labels := make(map[string]string)
	for key, value := range store.Labels {
		labels[key] = value
	}
	labels[appLabel] = appAtomix
	labels[storeLabel] = store.Name
	return labels
}

func newStoreAnnotations(store *sharedmemoryv1beta1.SharedMemoryStore) map[string]string {
	annotations := make(map[string]string)
	for key, value := range store.Annotations {
		annotations[key] = value
	}
	annotations[sharedMemoryStoreAnnotation] = store.Name
	return annotations
}

func getImage(store *sharedmemoryv1beta1.SharedMemoryStore) string {
	if store.Spec.Image != "" {
		return store.Spec.Image
	}
	return getDefaultImage()
}

func getDefaultImage() string {
	image := os.Getenv(defaultImageEnv)
	if image == "" {
		image = defaultImage
	}
	return image
}

func getPullPolicy(store *sharedmemoryv1beta1.SharedMemoryStore) corev1.PullPolicy {
	if store.Spec.ImagePullPolicy != "" {
		return store.Spec.ImagePullPolicy
	}
	return corev1.PullPolicy(os.Getenv(defaultImagePullPolicyEnv))
}
