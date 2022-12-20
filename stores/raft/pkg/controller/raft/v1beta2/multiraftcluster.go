// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta2

import (
	"context"
	"fmt"
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

	raftv1beta2 "github.com/atomix/atomix/stores/raft/pkg/apis/raft/v1beta2"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	apiPort               = 5678
	protocolPort          = 5679
	probePort             = 5679
	defaultImageEnv       = "DEFAULT_NODE_IMAGE"
	defaultImage          = "atomix/raft-node:latest"
	headlessServiceSuffix = "hs"
	nodeContainerName     = "atomix-raft-node"
	storeKey              = "atomix.io/store"
	podKey                = "raft.atomix.io/pod"
	raftStoreKey          = "raft.atomix.io/store"
	raftClusterKey        = "raft.atomix.io/cluster"
	raftPartitionKey      = "raft.atomix.io/partition"
	raftShardKey          = "raft.atomix.io/shard"
	raftReplicaKey        = "raft.atomix.io/replica"
	raftMemberKey         = "raft.atomix.io/member"
)

const (
	configPath        = "/etc/atomix"
	raftConfigFile    = "raft.yaml"
	loggingConfigFile = "logging.yaml"
	dataPath          = "/var/lib/atomix"
)

const (
	configVolume = "config"
	dataVolume   = "data"
)

const clusterDomainEnv = "CLUSTER_DOMAIN"

func addMultiRaftClusterController(mgr manager.Manager) error {
	options := controller.Options{
		Reconciler: &MultiRaftClusterReconciler{
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
	err = controller.Watch(&source.Kind{Type: &raftv1beta2.MultiRaftCluster{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource StatefulSet
	err = controller.Watch(&source.Kind{Type: &appsv1.StatefulSet{}}, &handler.EnqueueRequestForOwner{
		OwnerType:    &raftv1beta2.MultiRaftCluster{},
		IsController: true,
	})
	if err != nil {
		return err
	}
	return nil
}

// MultiRaftClusterReconciler reconciles a MultiRaftCluster object
type MultiRaftClusterReconciler struct {
	client client.Client
	scheme *runtime.Scheme
	events record.EventRecorder
}

// Reconcile reads that state of the cluster for a Store object and makes changes based on the state read
// and what is in the Store.Spec
func (r *MultiRaftClusterReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log.Info("Reconcile MultiRaftCluster")
	cluster := &raftv1beta2.MultiRaftCluster{}
	err := r.client.Get(ctx, request.NamespacedName, cluster)
	if err != nil {
		log.Error(err, "Reconcile MultiRaftCluster")
		if k8serrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	if err := r.reconcileConfigMap(ctx, cluster); err != nil {
		log.Error(err, "Reconcile MultiRaftCluster")
		return reconcile.Result{}, err
	}

	if err := r.reconcileStatefulSet(ctx, cluster); err != nil {
		log.Error(err, "Reconcile MultiRaftCluster")
		return reconcile.Result{}, err
	}

	if err := r.reconcileService(ctx, cluster); err != nil {
		log.Error(err, "Reconcile MultiRaftCluster")
		return reconcile.Result{}, err
	}

	if err := r.reconcileHeadlessService(ctx, cluster); err != nil {
		log.Error(err, "Reconcile MultiRaftCluster")
		return reconcile.Result{}, err
	}

	if err := r.reconcileStatus(ctx, cluster); err != nil {
		log.Error(err, "Reconcile MultiRaftCluster")
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, nil
}

func (r *MultiRaftClusterReconciler) reconcileConfigMap(ctx context.Context, cluster *raftv1beta2.MultiRaftCluster) error {
	log.Info("Reconcile raft protocol config map")
	cm := &corev1.ConfigMap{}
	name := types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      cluster.Name,
	}
	err := r.client.Get(ctx, name, cm)
	if err != nil && k8serrors.IsNotFound(err) {
		err = r.addConfigMap(ctx, cluster)
	}
	return err
}

func (r *MultiRaftClusterReconciler) addConfigMap(ctx context.Context, cluster *raftv1beta2.MultiRaftCluster) error {
	log.Info("Creating raft ConfigMap", "Name", cluster.Name, "Namespace", cluster.Namespace)
	loggingConfig, err := yaml.Marshal(&cluster.Spec.Config.Logging)
	if err != nil {
		log.Error(err, "Reconcile MultiRaftCluster")
		return err
	}

	raftConfig, err := newNodeConfig(cluster)
	if err != nil {
		log.Error(err, "Reconcile MultiRaftCluster")
		return err
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:        cluster.Name,
			Namespace:   cluster.Namespace,
			Labels:      newClusterLabels(cluster),
			Annotations: newClusterAnnotations(cluster),
		},
		Data: map[string]string{
			raftConfigFile:    string(raftConfig),
			loggingConfigFile: string(loggingConfig),
		},
	}

	if err := controllerutil.SetControllerReference(cluster, cm, r.scheme); err != nil {
		log.Error(err, "Reconcile MultiRaftCluster")
		return err
	}
	if err := r.client.Create(ctx, cm); err != nil {
		log.Error(err, "Reconcile MultiRaftCluster")
		return err
	}
	return nil
}

func newNodeConfig(cluster *raftv1beta2.MultiRaftCluster) ([]byte, error) {
	config := raft.Config{}
	config.Server = raft.ServerConfig{
		ReadBufferSize:       cluster.Spec.Config.Server.ReadBufferSize,
		WriteBufferSize:      cluster.Spec.Config.Server.WriteBufferSize,
		NumStreamWorkers:     cluster.Spec.Config.Server.NumStreamWorkers,
		MaxConcurrentStreams: cluster.Spec.Config.Server.MaxConcurrentStreams,
	}
	if cluster.Spec.Config.Server.MaxRecvMsgSize != nil {
		maxRecvMsgSize := int(cluster.Spec.Config.Server.MaxRecvMsgSize.Value())
		config.Server.MaxRecvMsgSize = &maxRecvMsgSize
	}
	if cluster.Spec.Config.Server.MaxSendMsgSize != nil {
		maxSendMsgSize := int(cluster.Spec.Config.Server.MaxSendMsgSize.Value())
		config.Server.MaxSendMsgSize = &maxSendMsgSize
	}
	config.Node.RTT = &cluster.Spec.Config.RTT.Duration
	return yaml.Marshal(&config)
}

func (r *MultiRaftClusterReconciler) reconcileStatefulSet(ctx context.Context, cluster *raftv1beta2.MultiRaftCluster) error {
	log.Info("Reconcile raft protocol stateful set")
	statefulSet := &appsv1.StatefulSet{}
	name := types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      cluster.Name,
	}
	err := r.client.Get(ctx, name, statefulSet)
	if err != nil && k8serrors.IsNotFound(err) {
		err = r.addStatefulSet(ctx, cluster)
	}
	return err
}

func (r *MultiRaftClusterReconciler) addStatefulSet(ctx context.Context, cluster *raftv1beta2.MultiRaftCluster) error {
	log.Info("Creating raft replicas", "Name", cluster.Name, "Namespace", cluster.Namespace)

	image := getImage(cluster)
	volumes := []corev1.Volume{
		{
			Name: configVolume,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: cluster.Name,
					},
				},
			},
		},
	}

	var volumeClaimTemplates []corev1.PersistentVolumeClaim

	dataVolumeName := dataVolume
	if cluster.Spec.VolumeClaimTemplate != nil {
		pvc := cluster.Spec.VolumeClaimTemplate
		if pvc.Name == "" {
			pvc.Name = dataVolume
		} else {
			dataVolumeName = pvc.Name
		}
		volumeClaimTemplates = append(volumeClaimTemplates, *pvc)
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
									Name:          "protocol",
									ContainerPort: protocolPort,
								},
							},
							Command: []string{
								"bash",
								"-c",
								fmt.Sprintf(`set -ex
[[ `+"`hostname`"+` =~ -([0-9]+)$ ]] || exit 1
ordinal=${BASH_REMATCH[1]}
atomix-raft-node --config %s/%s --api-port %d --raft-host %s-$ordinal.%s.%s.svc.%s --raft-port %d`,
									configPath, raftConfigFile, apiPort, cluster.Name, getHeadlessServiceName(cluster), cluster.Namespace, getClusterDomain(), protocolPort),
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
									Name:      dataVolumeName,
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
		log.Error(err, "Reconcile MultiRaftCluster")
		return err
	}
	if err := r.client.Create(ctx, set); err != nil {
		log.Error(err, "Reconcile MultiRaftCluster")
		return err
	}
	return nil
}

func (r *MultiRaftClusterReconciler) reconcileService(ctx context.Context, cluster *raftv1beta2.MultiRaftCluster) error {
	log.Info("Reconcile raft protocol service")
	service := &corev1.Service{}
	name := types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      cluster.Name,
	}
	err := r.client.Get(ctx, name, service)
	if err != nil && k8serrors.IsNotFound(err) {
		err = r.addService(ctx, cluster)
	}
	return err
}

func (r *MultiRaftClusterReconciler) addService(ctx context.Context, cluster *raftv1beta2.MultiRaftCluster) error {
	log.Info("Creating raft service", "Name", cluster.Name, "Namespace", cluster.Namespace)

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
					Port: protocolPort,
				},
			},
			Selector: newClusterSelector(cluster),
		},
	}

	if err := controllerutil.SetControllerReference(cluster, service, r.scheme); err != nil {
		log.Error(err, "Reconcile MultiRaftCluster")
		return err
	}
	if err := r.client.Create(ctx, service); err != nil {
		log.Error(err, "Reconcile RaftPMultiRaftClusterartition")
		return err
	}
	return nil
}

func (r *MultiRaftClusterReconciler) reconcileHeadlessService(ctx context.Context, cluster *raftv1beta2.MultiRaftCluster) error {
	log.Info("Reconcile raft protocol headless service")
	service := &corev1.Service{}
	name := types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      getHeadlessServiceName(cluster),
	}
	err := r.client.Get(ctx, name, service)
	if err != nil && k8serrors.IsNotFound(err) {
		err = r.addHeadlessService(ctx, cluster)
	}
	return err
}

func (r *MultiRaftClusterReconciler) addHeadlessService(ctx context.Context, cluster *raftv1beta2.MultiRaftCluster) error {
	log.Info("Creating headless raft service", "Name", cluster.Name, "Namespace", cluster.Namespace)

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
					Port: protocolPort,
				},
			},
			PublishNotReadyAddresses: true,
			ClusterIP:                "None",
			Selector:                 newClusterSelector(cluster),
		},
	}

	if err := controllerutil.SetControllerReference(cluster, service, r.scheme); err != nil {
		log.Error(err, "Reconcile MultiRaftCluster")
		return err
	}
	if err := r.client.Create(ctx, service); err != nil {
		log.Error(err, "Reconcile MultiRaftCluster")
		return err
	}
	return nil
}

func (r *MultiRaftClusterReconciler) reconcileStatus(ctx context.Context, cluster *raftv1beta2.MultiRaftCluster) error {
	statefulSet := &appsv1.StatefulSet{}
	name := types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      cluster.Name,
	}
	if err := r.client.Get(ctx, name, statefulSet); err != nil {
		log.Error(err, "Reconcile MultiRaftCluster")
		return err
	}

	switch cluster.Status.State {
	case raftv1beta2.MultiRaftClusterNotReady:
		if statefulSet.Status.ReadyReplicas == statefulSet.Status.Replicas {
			cluster.Status.State = raftv1beta2.MultiRaftClusterReady
			if err := r.client.Status().Update(ctx, cluster); err != nil {
				log.Error(err, "Reconcile MultiRaftCluster")
				return err
			}
		}
	case raftv1beta2.MultiRaftClusterReady:
		if statefulSet.Status.ReadyReplicas != statefulSet.Status.Replicas {
			cluster.Status.State = raftv1beta2.MultiRaftClusterNotReady
			if err := r.client.Status().Update(ctx, cluster); err != nil {
				log.Error(err, "Reconcile MultiRaftCluster")
				return err
			}
		}
	}
	return nil
}

var _ reconcile.Reconciler = (*MultiRaftClusterReconciler)(nil)
