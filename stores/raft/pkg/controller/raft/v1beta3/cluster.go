// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta3

import (
	"context"
	"fmt"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/atomix/stores/raft/pkg/raft"
	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"time"

	raftv1beta3 "github.com/atomix/atomix/stores/raft/pkg/apis/raft/v1beta3"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	apiPort               = 5678
	raftPort              = 5679
	probePort             = 5679
	defaultImageEnv       = "DEFAULT_NODE_IMAGE"
	defaultImage          = "atomix/raft-node:latest"
	headlessServiceSuffix = "hs"
	nodeContainerName     = "atomix-raft-node"
	storeKey              = "atomix.io/store"
	podKey                = "raft.atomix.io/pod"
	raftNamespaceKey      = "raft.atomix.io/namespace"
	raftStoreKey          = "raft.atomix.io/store"
	raftClusterKey        = "raft.atomix.io/cluster"
	raftPartitionIDKey    = "raft.atomix.io/partition-id"
	raftGroupIDKey        = "raft.atomix.io/group-id"
	raftMemberIDKey       = "raft.atomix.io/member-id"
	raftReplicaIDKey      = "raft.atomix.io/replica-id"
)

const (
	configPath        = "/etc/atomix"
	raftConfigFile    = "raft.yaml"
	loggingConfigFile = "logging.yaml"
	dataPath          = "/var/lib/atomix"
)

const (
	configVolume = "atomix-config"
	dataVolume   = "atomix-data"
)

const clusterDomainEnv = "CLUSTER_DOMAIN"

const (
	rootLoggerName = "root"
	stdoutSinkName = "stdout"
)

func addRaftClusterController(mgr manager.Manager) error {
	options := controller.Options{
		Reconciler: &RaftClusterReconciler{
			client: mgr.GetClient(),
			scheme: mgr.GetScheme(),
			events: mgr.GetEventRecorderFor("atomix-raft"),
		},
		RateLimiter: workqueue.NewItemExponentialFailureRateLimiter(time.Millisecond*10, time.Second*5),
	}

	// Create a new controller
	controller, err := controller.New("atomix-multi-raft-cluster", mgr, options)
	if err != nil {
		return err
	}

	// Watch for changes to the storage resource and enqueue Stores that reference it
	err = controller.Watch(&source.Kind{Type: &raftv1beta3.RaftCluster{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource StatefulSet
	err = controller.Watch(&source.Kind{Type: &appsv1.StatefulSet{}}, &handler.EnqueueRequestForOwner{
		OwnerType:    &raftv1beta3.RaftCluster{},
		IsController: true,
	})
	if err != nil {
		return err
	}
	return nil
}

// RaftClusterReconciler reconciles a RaftCluster object
type RaftClusterReconciler struct {
	client client.Client
	scheme *runtime.Scheme
	events record.EventRecorder
}

// Reconcile reads that state of the cluster for a Store object and makes changes based on the state read
// and what is in the Store.Spec
func (r *RaftClusterReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log := log.WithFields(logging.String("RaftCluster", request.NamespacedName.String()))
	log.Debug("Reconciling RaftCluster")

	cluster := &raftv1beta3.RaftCluster{}
	if ok, err := get(r.client, ctx, request.NamespacedName, cluster, log); err != nil {
		return reconcile.Result{}, err
	} else if !ok {
		return reconcile.Result{}, nil
	}

	if err := r.reconcileConfigMap(ctx, log, cluster); err != nil {
		return reconcile.Result{}, err
	}

	if err := r.reconcileStatefulSet(ctx, log, cluster); err != nil {
		return reconcile.Result{}, err
	}

	if err := r.reconcileService(ctx, log, cluster); err != nil {
		return reconcile.Result{}, err
	}

	if err := r.reconcileHeadlessService(ctx, log, cluster); err != nil {
		return reconcile.Result{}, err
	}

	if err := r.reconcileStatus(ctx, log, cluster); err != nil {
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, nil
}

func (r *RaftClusterReconciler) reconcileConfigMap(ctx context.Context, log logging.Logger, cluster *raftv1beta3.RaftCluster) error {
	cm := &corev1.ConfigMap{}
	name := types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      fmt.Sprintf("%s-raft-config", cluster.Name),
	}
	if ok, err := get(r.client, ctx, name, cm, log); err != nil {
		return err
	} else if !ok {
		return r.addConfigMap(ctx, log, cluster)
	}
	return nil
}

func (r *RaftClusterReconciler) addConfigMap(ctx context.Context, log logging.Logger, cluster *raftv1beta3.RaftCluster) error {
	log.Infow("Creating ConfigMap")
	sinkName := stdoutSinkName
	loggingOutputs := map[string]logging.OutputConfig{
		stdoutSinkName: {
			Name: stdoutSinkName,
			Sink: &sinkName,
		},
	}

	sinkEncoding := logging.SinkEncoding(cluster.Spec.Logging.Encoding)
	loggingConfig := logging.Config{
		Loggers: map[string]logging.LoggerConfig{
			rootLoggerName: {
				Level:  &cluster.Spec.Logging.RootLevel,
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

	for _, loggerConfig := range cluster.Spec.Logging.Loggers {
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

	raftConfigBytes, err := newNodeConfig(cluster)
	if err != nil {
		log.Error(err)
		return err
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:        fmt.Sprintf("%s-raft-config", cluster.Name),
			Namespace:   cluster.Namespace,
			Labels:      newClusterLabels(cluster),
			Annotations: newClusterAnnotations(cluster),
		},
		Data: map[string]string{
			raftConfigFile:    string(raftConfigBytes),
			loggingConfigFile: string(loggingConfigBytes),
		},
	}

	if err := controllerutil.SetControllerReference(cluster, cm, r.scheme); err != nil {
		log.Error(err)
		return err
	}
	return create(r.client, ctx, cm, log)
}

func newNodeConfig(cluster *raftv1beta3.RaftCluster) ([]byte, error) {
	config := raft.Config{}
	config.Node.RTT = &cluster.Spec.AverageRTT.Duration
	return yaml.Marshal(&config)
}

func (r *RaftClusterReconciler) reconcileStatefulSet(ctx context.Context, log logging.Logger, cluster *raftv1beta3.RaftCluster) error {
	statefulSet := &appsv1.StatefulSet{}
	name := types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      cluster.Name,
	}
	if ok, err := get(r.client, ctx, name, statefulSet, log); err != nil {
		return err
	} else if !ok {
		return r.addStatefulSet(ctx, log, cluster)
	}
	return nil
}

func (r *RaftClusterReconciler) addStatefulSet(ctx context.Context, log logging.Logger, cluster *raftv1beta3.RaftCluster) error {
	log.Infow("Creating StatefulSet")
	image := getImage(cluster)
	volumes := []corev1.Volume{
		{
			Name: configVolume,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: fmt.Sprintf("%s-raft-config", cluster.Name),
					},
				},
			},
		},
	}

	var volumeClaimTemplates []corev1.PersistentVolumeClaim
	if cluster.Spec.Persistence.Enabled {
		var resources corev1.ResourceRequirements
		if cluster.Spec.Persistence.Size != nil {
			resources.Requests = corev1.ResourceList{
				corev1.ResourceStorage: *cluster.Spec.Persistence.Size,
			}
		}
		volumeClaimTemplates = append(volumeClaimTemplates, corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: dataVolume,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: cluster.Spec.Persistence.StorageClass,
				AccessModes:      cluster.Spec.Persistence.AccessModes,
				Selector:         cluster.Spec.Persistence.Selector,
				Resources:        resources,
			},
		})
	} else {
		volumes = append(volumes, corev1.Volume{
			Name: dataVolume,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		})
	}

	set := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        cluster.Name,
			Namespace:   cluster.Namespace,
			Labels:      newClusterLabels(cluster),
			Annotations: newClusterAnnotations(cluster),
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: getHeadlessServiceName(cluster),
			Replicas:    pointer.Int32Ptr(int32(cluster.Spec.Replicas)),
			Selector: &metav1.LabelSelector{
				MatchLabels: newClusterSelector(cluster),
			},
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
			},
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      newClusterLabels(cluster),
					Annotations: newClusterAnnotations(cluster),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            nodeContainerName,
							Image:           image,
							ImagePullPolicy: cluster.Spec.ImagePullPolicy,
							Ports: []corev1.ContainerPort{
								{
									Name:          "api",
									ContainerPort: apiPort,
								},
								{
									Name:          "raft",
									ContainerPort: raftPort,
								},
							},
							Command: []string{
								"bash",
								"-c",
								fmt.Sprintf(`set -ex
[[ `+"`hostname`"+` =~ -([0-9]+)$ ]] || exit 1
ordinal=${BASH_REMATCH[1]}
atomix-raft-node --config %s/%s --api-port %d --raft-host %s-$ordinal.%s.%s.svc.%s --raft-port %d`,
									configPath, raftConfigFile, apiPort, cluster.Name, getHeadlessServiceName(cluster), cluster.Namespace, getClusterDomain(), raftPort),
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
							SecurityContext: cluster.Spec.SecurityContext,
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      dataVolume,
									MountPath: dataPath,
								},
								{
									Name:      configVolume,
									MountPath: configPath,
								},
							},
						},
					},
					Affinity: &corev1.Affinity{
						PodAntiAffinity: &corev1.PodAntiAffinity{
							PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
								{
									Weight: 1,
									PodAffinityTerm: corev1.PodAffinityTerm{
										LabelSelector: &metav1.LabelSelector{
											MatchLabels: newClusterSelector(cluster),
										},
										Namespaces:  []string{cluster.Namespace},
										TopologyKey: "kubernetes.io/hostname",
									},
								},
							},
						},
					},
					ImagePullSecrets: cluster.Spec.ImagePullSecrets,
					Volumes:          volumes,
				},
			},
			VolumeClaimTemplates: volumeClaimTemplates,
		},
	}

	if err := controllerutil.SetControllerReference(cluster, set, r.scheme); err != nil {
		log.Error(err)
		return err
	}
	return create(r.client, ctx, set, log)
}

func (r *RaftClusterReconciler) reconcileService(ctx context.Context, log logging.Logger, cluster *raftv1beta3.RaftCluster) error {
	service := &corev1.Service{}
	name := types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      cluster.Name,
	}
	if ok, err := get(r.client, ctx, name, service, log); err != nil {
		return err
	} else if !ok {
		return r.addService(ctx, log, cluster)
	}
	return nil
}

func (r *RaftClusterReconciler) addService(ctx context.Context, log logging.Logger, cluster *raftv1beta3.RaftCluster) error {
	log.Infow("Creating Service")
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        cluster.Name,
			Namespace:   cluster.Namespace,
			Labels:      newClusterLabels(cluster),
			Annotations: newClusterAnnotations(cluster),
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name: "api",
					Port: apiPort,
				},
				{
					Name: "protocol",
					Port: raftPort,
				},
			},
			Selector: newClusterSelector(cluster),
		},
	}

	if err := controllerutil.SetControllerReference(cluster, service, r.scheme); err != nil {
		log.Error(err)
		return err
	}
	return create(r.client, ctx, service, log)
}

func (r *RaftClusterReconciler) reconcileHeadlessService(ctx context.Context, log logging.Logger, cluster *raftv1beta3.RaftCluster) error {
	service := &corev1.Service{}
	name := types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      getHeadlessServiceName(cluster),
	}
	if ok, err := get(r.client, ctx, name, service, log); err != nil {
		return err
	} else if !ok {
		return r.addHeadlessService(ctx, log, cluster)
	}
	return nil
}

func (r *RaftClusterReconciler) addHeadlessService(ctx context.Context, log logging.Logger, cluster *raftv1beta3.RaftCluster) error {
	log.Infow("Creating headless Service")
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        getHeadlessServiceName(cluster),
			Namespace:   cluster.Namespace,
			Labels:      newClusterLabels(cluster),
			Annotations: newClusterAnnotations(cluster),
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name: "api",
					Port: apiPort,
				},
				{
					Name: "protocol",
					Port: raftPort,
				},
			},
			PublishNotReadyAddresses: true,
			ClusterIP:                "None",
			Selector:                 newClusterSelector(cluster),
		},
	}

	if err := controllerutil.SetControllerReference(cluster, service, r.scheme); err != nil {
		log.Error(err)
		return err
	}
	return create(r.client, ctx, service, log)
}

func (r *RaftClusterReconciler) reconcileStatus(ctx context.Context, log logging.Logger, cluster *raftv1beta3.RaftCluster) error {
	statefulSet := &appsv1.StatefulSet{}
	name := types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      cluster.Name,
	}
	if err := r.client.Get(ctx, name, statefulSet); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return err
		}
		log.Warn(err)
		return nil
	}

	switch cluster.Status.State {
	case raftv1beta3.RaftClusterNotReady:
		if statefulSet.Status.ReadyReplicas == statefulSet.Status.Replicas {
			cluster.Status.State = raftv1beta3.RaftClusterReady
			log.Infow("RaftCluster status changed",
				logging.String("Status", string(cluster.Status.State)))
			if err := r.client.Status().Update(ctx, cluster); err != nil {
				if !k8serrors.IsNotFound(err) && !k8serrors.IsConflict(err) {
					log.Error(err)
					return err
				}
				log.Debug(err)
				return nil
			}
		}
	case raftv1beta3.RaftClusterReady:
		if statefulSet.Status.ReadyReplicas != statefulSet.Status.Replicas {
			cluster.Status.State = raftv1beta3.RaftClusterNotReady
			log.Infow("RaftCluster status changed",
				logging.String("Status", string(cluster.Status.State)))
			if err := r.client.Status().Update(ctx, cluster); err != nil {
				if !k8serrors.IsNotFound(err) && !k8serrors.IsConflict(err) {
					log.Error(err)
					return err
				}
				log.Debug(err)
				return nil
			}
		}
	}
	return nil
}

var _ reconcile.Reconciler = (*RaftClusterReconciler)(nil)
