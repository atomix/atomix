// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta1

import (
	"context"
	"fmt"
	multiraftv1 "github.com/atomix/atomix/stores/multi-raft/pkg/api/v1"
	multiraft "github.com/atomix/atomix/stores/multi-raft/pkg/node"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/pointer"
	"k8s.io/utils/strings/slices"
	"net"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"strconv"
	"strings"
	"time"

	multiraftv1beta1 "github.com/atomix/atomix/stores/multi-raft/pkg/apis/multiraft/v1beta1"
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
	defaultImage          = "atomix/multiraft-node:latest"
	headlessServiceSuffix = "hs"
	nodeContainerName     = "atomix-multiraft-node"
	multiRaftPodFinalizer = "multiraft.atomix.io/pod"
	storeKey              = "atomix.io/store"
	multiRaftStoreKey     = "multiraft.atomix.io/store"
	multiRaftClusterKey   = "multiraft.atomix.io/cluster"
	raftGroupKey          = "multiraft.atomix.io/group"
	raftPartitionKey      = "multiraft.atomix.io/partition"
	raftMemberKey         = "multiraft.atomix.io/member"
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
			events: mgr.GetEventRecorderFor("atomix-multiraft-storage"),
		},
		RateLimiter: workqueue.NewItemExponentialFailureRateLimiter(time.Millisecond*10, time.Second*5),
	}

	// Create a new controller
	controller, err := controller.New("atomix-multiraft-cluster-v3beta1", mgr, options)
	if err != nil {
		return err
	}

	// Watch for changes to the storage resource and enqueue Stores that reference it
	err = controller.Watch(&source.Kind{Type: &multiraftv1beta1.MultiRaftCluster{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource StatefulSet
	err = controller.Watch(&source.Kind{Type: &appsv1.StatefulSet{}}, &handler.EnqueueRequestForOwner{
		OwnerType:    &multiraftv1beta1.MultiRaftCluster{},
		IsController: true,
	})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource RaftGroup
	err = controller.Watch(&source.Kind{Type: &multiraftv1beta1.RaftGroup{}}, &handler.EnqueueRequestForOwner{
		OwnerType:    &multiraftv1beta1.MultiRaftCluster{},
		IsController: true,
	})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource RaftMember
	err = controller.Watch(&source.Kind{Type: &multiraftv1beta1.RaftMember{}}, &handler.EnqueueRequestForOwner{
		OwnerType:    &multiraftv1beta1.MultiRaftCluster{},
		IsController: true,
	})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource Pod
	err = controller.Watch(&source.Kind{Type: &corev1.Pod{}}, handler.EnqueueRequestsFromMapFunc(func(object client.Object) []reconcile.Request {
		clusterName, ok := object.GetAnnotations()[multiRaftClusterKey]
		if !ok {
			return nil
		}
		return []reconcile.Request{
			{
				NamespacedName: types.NamespacedName{
					Namespace: object.GetNamespace(),
					Name:      clusterName,
				},
			},
		}
	}))
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
	cluster := &multiraftv1beta1.MultiRaftCluster{}
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

	if ok, err := r.reconcileGroups(ctx, cluster); err != nil {
		log.Error(err, "Reconcile MultiRaftCluster")
		return reconcile.Result{}, err
	} else if ok {
		return reconcile.Result{}, nil
	}

	if ok, err := r.reconcileStatus(ctx, cluster); err != nil {
		log.Error(err, "Reconcile MultiRaftCluster")
		return reconcile.Result{}, err
	} else if ok {
		return reconcile.Result{}, nil
	}
	return reconcile.Result{}, nil
}

func (r *MultiRaftClusterReconciler) reconcileConfigMap(ctx context.Context, cluster *multiraftv1beta1.MultiRaftCluster) error {
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

func (r *MultiRaftClusterReconciler) addConfigMap(ctx context.Context, cluster *multiraftv1beta1.MultiRaftCluster) error {
	log.Info("Creating raft ConfigMap", "Name", cluster.Name, "Namespace", cluster.Namespace)
	loggingConfig, err := yaml.Marshal(&cluster.Spec.Config.Logging)
	if err != nil {
		return err
	}

	raftConfig, err := newNodeConfig(cluster)
	if err != nil {
		return err
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:        cluster.Name,
			Namespace:   cluster.Namespace,
			Labels:      cluster.Labels,
			Annotations: cluster.Annotations,
		},
		Data: map[string]string{
			raftConfigFile:    string(raftConfig),
			loggingConfigFile: string(loggingConfig),
		},
	}

	if err := controllerutil.SetControllerReference(cluster, cm, r.scheme); err != nil {
		return err
	}
	return r.client.Create(ctx, cm)
}

func newNodeConfig(cluster *multiraftv1beta1.MultiRaftCluster) ([]byte, error) {
	config := multiraft.Config{}
	config.Server = multiraft.ServerConfig{
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
	electionTimeout := cluster.Spec.Config.Raft.ElectionTimeout
	if electionTimeout != nil {
		config.Raft.ElectionTimeout = &electionTimeout.Duration
	}
	heartbeatPeriod := cluster.Spec.Config.Raft.HeartbeatPeriod
	if heartbeatPeriod != nil {
		config.Raft.HeartbeatPeriod = &heartbeatPeriod.Duration
	}
	if cluster.Spec.Config.Raft.SnapshotEntryThreshold != nil {
		entryThreshold := uint64(*cluster.Spec.Config.Raft.SnapshotEntryThreshold)
		config.Raft.SnapshotEntryThreshold = &entryThreshold
	}
	if cluster.Spec.Config.Raft.CompactionRetainEntries != nil {
		compactionRetainEntries := uint64(*cluster.Spec.Config.Raft.CompactionRetainEntries)
		config.Raft.CompactionRetainEntries = &compactionRetainEntries
	}
	return yaml.Marshal(&config)
}

func (r *MultiRaftClusterReconciler) reconcileStatefulSet(ctx context.Context, cluster *multiraftv1beta1.MultiRaftCluster) error {
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

func (r *MultiRaftClusterReconciler) addStatefulSet(ctx context.Context, cluster *multiraftv1beta1.MultiRaftCluster) error {
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
			Labels:      cluster.Labels,
			Annotations: cluster.Annotations,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: getHeadlessServiceName(cluster.Name),
			Replicas:    pointer.Int32Ptr(int32(getNumReplicas(cluster))),
			Selector: &metav1.LabelSelector{
				MatchLabels: cluster.Labels,
			},
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
			},
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      cluster.Labels,
					Annotations: cluster.Annotations,
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
atomix-multiraft-node --config %s/%s --api-port %d --raft-host %s-$ordinal.%s.%s.svc.%s --raft-port %d`,
									configPath, raftConfigFile, apiPort, cluster.Name, getHeadlessServiceName(cluster.Name), cluster.Namespace, getClusterDomain(), protocolPort),
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
											MatchLabels: cluster.Labels,
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
		return err
	}
	return r.client.Create(ctx, set)
}

func (r *MultiRaftClusterReconciler) reconcileService(ctx context.Context, cluster *multiraftv1beta1.MultiRaftCluster) error {
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

func (r *MultiRaftClusterReconciler) addService(ctx context.Context, cluster *multiraftv1beta1.MultiRaftCluster) error {
	log.Info("Creating raft service", "Name", cluster.Name, "Namespace", cluster.Namespace)

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        cluster.Name,
			Namespace:   cluster.Namespace,
			Labels:      cluster.Labels,
			Annotations: cluster.Annotations,
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
			Selector: cluster.Labels,
		},
	}

	if err := controllerutil.SetControllerReference(cluster, service, r.scheme); err != nil {
		return err
	}
	return r.client.Create(ctx, service)
}

func (r *MultiRaftClusterReconciler) reconcileHeadlessService(ctx context.Context, cluster *multiraftv1beta1.MultiRaftCluster) error {
	log.Info("Reconcile raft protocol headless service")
	service := &corev1.Service{}
	name := types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      getHeadlessServiceName(cluster.Name),
	}
	err := r.client.Get(ctx, name, service)
	if err != nil && k8serrors.IsNotFound(err) {
		err = r.addHeadlessService(ctx, cluster)
	}
	return err
}

func (r *MultiRaftClusterReconciler) addHeadlessService(ctx context.Context, cluster *multiraftv1beta1.MultiRaftCluster) error {
	log.Info("Creating headless raft service", "Name", cluster.Name, "Namespace", cluster.Namespace)

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        getHeadlessServiceName(cluster.Name),
			Namespace:   cluster.Namespace,
			Labels:      cluster.Labels,
			Annotations: cluster.Annotations,
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
			Selector:                 cluster.Labels,
		},
	}

	if err := controllerutil.SetControllerReference(cluster, service, r.scheme); err != nil {
		return err
	}
	return r.client.Create(ctx, service)
}

func (r *MultiRaftClusterReconciler) reconcileGroups(ctx context.Context, cluster *multiraftv1beta1.MultiRaftCluster) (bool, error) {
	numGroups := getNumGroups(cluster)
	state := multiraftv1beta1.MultiRaftClusterReady
	for groupID := 1; groupID <= numGroups; groupID++ {
		if group, updated, err := r.reconcileGroup(ctx, cluster, groupID); err != nil {
			return false, err
		} else if updated {
			return true, nil
		} else if group.Status.State == multiraftv1beta1.RaftGroupNotReady {
			state = multiraftv1beta1.MultiRaftClusterNotReady
		}
	}

	if cluster.Status.State != state {
		cluster.Status.State = state
		if err := r.client.Status().Update(ctx, cluster); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (r *MultiRaftClusterReconciler) reconcileGroup(ctx context.Context, cluster *multiraftv1beta1.MultiRaftCluster, groupID int) (*multiraftv1beta1.RaftGroup, bool, error) {
	groupName := types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      fmt.Sprintf("%s-%d", cluster.Name, groupID),
	}
	group := &multiraftv1beta1.RaftGroup{}
	if err := r.client.Get(ctx, groupName, group); err != nil {
		if !k8serrors.IsNotFound(err) {
			return nil, false, err
		}

		group = &multiraftv1beta1.RaftGroup{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   groupName.Namespace,
				Name:        groupName.Name,
				Labels:      newGroupLabels(cluster, groupID),
				Annotations: newGroupAnnotations(cluster, groupID),
			},
			Spec: multiraftv1beta1.RaftGroupSpec{
				RaftConfig: cluster.Spec.Config.Raft,
			},
		}
		if err := controllerutil.SetControllerReference(cluster, group, r.scheme); err != nil {
			return nil, false, err
		}
		if err := r.client.Create(ctx, group); err != nil {
			return nil, false, err
		}
		return group, true, nil
	}

	if ok, err := r.reconcileMembers(ctx, cluster, group, groupID); err != nil {
		return group, false, err
	} else if ok {
		return group, true, nil
	}
	return group, false, nil
}

func (r *MultiRaftClusterReconciler) reconcileMembers(ctx context.Context, cluster *multiraftv1beta1.MultiRaftCluster, group *multiraftv1beta1.RaftGroup, groupID int) (bool, error) {
	state := multiraftv1beta1.RaftGroupReady
	for memberID := 1; memberID <= getNumMembers(cluster); memberID++ {
		if member, ok, err := r.reconcileMember(ctx, cluster, group, groupID, memberID); err != nil {
			return false, err
		} else if ok {
			return true, nil
		} else if member.Status.State == multiraftv1beta1.RaftMemberNotReady {
			state = multiraftv1beta1.RaftGroupNotReady
		}
	}

	if group.Status.State != state {
		group.Status.State = state
		if err := r.client.Status().Update(ctx, group); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (r *MultiRaftClusterReconciler) reconcileMember(ctx context.Context, cluster *multiraftv1beta1.MultiRaftCluster, group *multiraftv1beta1.RaftGroup, groupID int, memberID int) (*multiraftv1beta1.RaftMember, bool, error) {
	memberName := types.NamespacedName{
		Namespace: group.Namespace,
		Name:      fmt.Sprintf("%s-%d", group.Name, memberID),
	}
	member := &multiraftv1beta1.RaftMember{}
	if err := r.client.Get(ctx, memberName, member); err != nil {
		if !k8serrors.IsNotFound(err) {
			return nil, false, err
		}

		var memberType multiraftv1beta1.RaftMemberType
		if memberID <= getNumVotingMembers(cluster) {
			memberType = multiraftv1beta1.RaftVotingMember
		} else if memberID <= getNumMembers(cluster) {
			memberType = multiraftv1beta1.RaftObserver
		} else {
			memberType = multiraftv1beta1.RaftWitness
		}

		member = &multiraftv1beta1.RaftMember{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   memberName.Namespace,
				Name:        memberName.Name,
				Labels:      newMemberLabels(group, memberID),
				Annotations: newMemberAnnotations(group, memberID),
			},
			Spec: multiraftv1beta1.RaftMemberSpec{
				Pod: corev1.LocalObjectReference{
					Name: getPodName(cluster, groupID, memberID),
				},
				Type: memberType,
			},
		}
		if err := controllerutil.SetControllerReference(cluster, member, r.scheme); err != nil {
			return nil, false, err
		}
		if err := controllerutil.SetOwnerReference(group, member, r.scheme); err != nil {
			return nil, false, err
		}
		if err := r.client.Create(ctx, member); err != nil {
			return nil, false, err
		}
		return member, true, nil
	}

	podName := types.NamespacedName{
		Namespace: member.Namespace,
		Name:      member.Spec.Pod.Name,
	}
	pod := &corev1.Pod{}
	if err := r.client.Get(ctx, podName, pod); err != nil {
		return nil, false, err
	}

	if member.Status.PodRef == nil || member.Status.PodRef.UID != pod.UID {
		member.Status.PodRef = &corev1.ObjectReference{
			APIVersion: pod.APIVersion,
			Kind:       pod.Kind,
			Namespace:  pod.Namespace,
			Name:       pod.Name,
			UID:        pod.UID,
		}
		member.Status.Version = nil
		if err := r.client.Status().Update(ctx, member); err != nil {
			return nil, false, err
		}
		return member, true, nil
	}

	var containerVersion int32
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.Name == nodeContainerName {
			containerVersion = containerStatus.RestartCount + 1
			break
		}
	}

	if member.Status.Version == nil || containerVersion > *member.Status.Version {
		if member.Status.State != multiraftv1beta1.RaftMemberNotReady {
			member.Status.State = multiraftv1beta1.RaftMemberNotReady
			if err := r.client.Status().Update(ctx, member); err != nil {
				return nil, false, err
			}
			r.events.Eventf(member, "Normal", "StateChanged", "State changed to %s", member.Status.State)
			return member, true, nil
		}

		address := fmt.Sprintf("%s:%d", pod.Status.PodIP, apiPort)
		conn, err := grpc.DialContext(ctx, address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, false, err
		}
		defer conn.Close()

		members := make([]multiraftv1.MemberConfig, getNumMembers(cluster))
		for i := 0; i < getNumMembers(cluster); i++ {
			members[i] = multiraftv1.MemberConfig{
				MemberID: multiraftv1.MemberID(i + 1),
				Host:     getPodDNSName(cluster.Namespace, cluster.Name, getPodName(cluster, groupID, i+1)),
				Port:     protocolPort,
			}
		}

		var role multiraftv1.MemberRole
		switch member.Spec.Type {
		case multiraftv1beta1.RaftVotingMember:
			role = multiraftv1.MemberRole_MEMBER
		case multiraftv1beta1.RaftObserver:
			role = multiraftv1.MemberRole_OBSERVER
		case multiraftv1beta1.RaftWitness:
			role = multiraftv1.MemberRole_WITNESS
		}

		client := multiraftv1.NewNodeClient(conn)
		request := &multiraftv1.BootstrapRequest{
			Group: multiraftv1.GroupConfig{
				GroupID:  multiraftv1.GroupID(groupID),
				MemberID: multiraftv1.MemberID(memberID),
				Role:     role,
				Members:  members,
			},
		}
		if _, err := client.Bootstrap(ctx, request); err != nil {
			return nil, false, err
		}

		member.Status.Version = &containerVersion
		if err := r.client.Status().Update(ctx, member); err != nil {
			return nil, false, err
		}
		return member, true, nil
	}
	return member, false, nil
}

func (r *MultiRaftClusterReconciler) reconcileStatus(ctx context.Context, cluster *multiraftv1beta1.MultiRaftCluster) (bool, error) {
	partitions, err := r.getPartitionStatuses(ctx, cluster)
	if err != nil {
		return false, err
	}

	if !isPartitionStatusesEqual(cluster.Status.Partitions, partitions) {
		cluster.Status.Partitions = partitions
		if err := r.client.Status().Update(ctx, cluster); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func isPartitionStatusesEqual(partitions1, partitions2 []multiraftv1beta1.RaftPartitionStatus) bool {
	if len(partitions1) != len(partitions2) {
		return false
	}
	for _, partition1 := range partitions1 {
		for _, partition2 := range partitions2 {
			if partition1.PartitionID == partition2.PartitionID && !isPartitionStatusEqual(partition1, partition2) {
				return false
			}
		}
	}
	return true
}

func isPartitionStatusEqual(partition1, partition2 multiraftv1beta1.RaftPartitionStatus) bool {
	if partition1.PartitionID != partition2.PartitionID {
		return false
	}
	if partition1.Leader == nil && partition2.Leader != nil {
		return false
	}
	if partition1.Leader != nil && partition2.Leader == nil {
		return false
	}
	if partition1.Leader != nil && partition2.Leader != nil && *partition1.Leader != *partition2.Leader {
		return false
	}
	if len(partition1.Followers) != len(partition2.Followers) {
		return false
	}
	for _, follower := range partition1.Followers {
		if !slices.Contains(partition2.Followers, follower) {
			return false
		}
	}
	return true
}

func (r *MultiRaftClusterReconciler) getPartitionStatuses(ctx context.Context, cluster *multiraftv1beta1.MultiRaftCluster) ([]multiraftv1beta1.RaftPartitionStatus, error) {
	numGroups := getNumGroups(cluster)
	partitions := make([]multiraftv1beta1.RaftPartitionStatus, 0, numGroups)
	for groupID := 1; groupID <= numGroups; groupID++ {
		groupName := types.NamespacedName{
			Namespace: cluster.Namespace,
			Name:      fmt.Sprintf("%s-%d", cluster.Name, groupID),
		}
		group := &multiraftv1beta1.RaftGroup{}
		if err := r.client.Get(ctx, groupName, group); err != nil {
			return nil, err
		}

		partition := multiraftv1beta1.RaftPartitionStatus{
			PartitionID: int32(groupID),
		}
		if group.Status.Leader != nil {
			memberName := types.NamespacedName{
				Namespace: group.Namespace,
				Name:      group.Status.Leader.Name,
			}
			member := &multiraftv1beta1.RaftMember{}
			if err := r.client.Get(ctx, memberName, member); err != nil {
				return nil, err
			}
			address := fmt.Sprintf("%s:%d", getPodDNSName(cluster.Namespace, cluster.Name, member.Spec.Pod.Name), apiPort)
			partition.Leader = &address
		}

		for _, follower := range group.Status.Followers {
			memberName := types.NamespacedName{
				Namespace: group.Namespace,
				Name:      follower.Name,
			}
			member := &multiraftv1beta1.RaftMember{}
			if err := r.client.Get(ctx, memberName, member); err != nil {
				return nil, err
			}
			address := fmt.Sprintf("%s:%d", getPodDNSName(cluster.Namespace, cluster.Name, member.Spec.Pod.Name), apiPort)
			partition.Followers = append(partition.Followers, address)
		}
		partitions = append(partitions, partition)
	}
	return partitions, nil
}

var _ reconcile.Reconciler = (*MultiRaftClusterReconciler)(nil)

func getNumGroups(cluster *multiraftv1beta1.MultiRaftCluster) int {
	if cluster.Spec.Groups == 0 {
		return 1
	}
	return int(cluster.Spec.Groups)
}

func getNumReplicas(cluster *multiraftv1beta1.MultiRaftCluster) int {
	if cluster.Spec.Replicas == 0 {
		return 1
	}
	return int(cluster.Spec.Replicas)
}

func getNumMembers(cluster *multiraftv1beta1.MultiRaftCluster) int {
	return getNumVotingMembers(cluster) + getNumNonVotingMembers(cluster)
}

func getNumVotingMembers(cluster *multiraftv1beta1.MultiRaftCluster) int {
	if cluster.Spec.Config.Raft.QuorumSize == nil {
		return getNumReplicas(cluster)
	}
	return int(*cluster.Spec.Config.Raft.QuorumSize)
}

func getNumNonVotingMembers(cluster *multiraftv1beta1.MultiRaftCluster) int {
	if cluster.Spec.Config.Raft.ReadReplicas == nil {
		return 0
	}
	return int(*cluster.Spec.Config.Raft.ReadReplicas)
}

// getResourceName returns the given resource name for the given object name
func getResourceName(name string, resource string) string {
	return fmt.Sprintf("%s-%s", name, resource)
}

// getHeadlessServiceName returns the headless service name for the given cluster
func getHeadlessServiceName(cluster string) string {
	return getResourceName(cluster, headlessServiceSuffix)
}

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

// getPodDNSName returns the fully qualified DNS name for the given pod ID
func getPodDNSName(namespace string, cluster string, name string) string {
	return fmt.Sprintf("%s.%s.%s.svc.%s", name, getHeadlessServiceName(cluster), namespace, getClusterDomain())
}

func getPodName(cluster *multiraftv1beta1.MultiRaftCluster, groupID int, memberID int) string {
	podOrdinal := ((getNumMembers(cluster) * groupID) + (memberID - 1)) % getNumReplicas(cluster)
	return fmt.Sprintf("%s-%d", cluster.Name, podOrdinal)
}

// newClusterLabels returns the labels for the given cluster
func newClusterLabels(store *multiraftv1beta1.MultiRaftStore) map[string]string {
	labels := make(map[string]string)
	for key, value := range store.Labels {
		labels[key] = value
	}
	labels[storeKey] = store.Name
	labels[multiRaftStoreKey] = store.Name
	return labels
}

// newGroupLabels returns the labels for the given cluster
func newGroupLabels(cluster *multiraftv1beta1.MultiRaftCluster, partitionID int) map[string]string {
	labels := make(map[string]string)
	for key, value := range cluster.Labels {
		labels[key] = value
	}
	labels[multiRaftClusterKey] = cluster.Name
	labels[raftPartitionKey] = strconv.Itoa(partitionID)
	return labels
}

// newMemberLabels returns the labels for the given cluster
func newMemberLabels(group *multiraftv1beta1.RaftGroup, memberID int) map[string]string {
	labels := make(map[string]string)
	for key, value := range group.Labels {
		labels[key] = value
	}
	labels[raftGroupKey] = group.Name
	labels[raftMemberKey] = strconv.Itoa(memberID)
	return labels
}

func newClusterAnnotations(store *multiraftv1beta1.MultiRaftStore) map[string]string {
	annotations := make(map[string]string)
	for key, value := range store.Annotations {
		annotations[key] = value
	}
	annotations[storeKey] = store.Name
	annotations[multiRaftStoreKey] = store.Name
	return annotations
}

func newGroupAnnotations(cluster *multiraftv1beta1.MultiRaftCluster, partitionID int) map[string]string {
	annotations := make(map[string]string)
	for key, value := range cluster.Labels {
		annotations[key] = value
	}
	annotations[multiRaftClusterKey] = cluster.Name
	annotations[raftPartitionKey] = strconv.Itoa(partitionID)
	return annotations
}

func newMemberAnnotations(group *multiraftv1beta1.RaftGroup, memberID int) map[string]string {
	annotations := make(map[string]string)
	for key, value := range group.Labels {
		annotations[key] = value
	}
	annotations[raftGroupKey] = group.Name
	annotations[raftMemberKey] = strconv.Itoa(memberID)
	return annotations
}

func getImage(cluster *multiraftv1beta1.MultiRaftCluster) string {
	if cluster.Spec.Image != "" {
		return cluster.Spec.Image
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
