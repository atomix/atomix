// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"context"
	"fmt"
	rsmv1 "github.com/atomix/atomix/protocols/rsm/pkg/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/node"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	raftv1 "github.com/atomix/atomix/stores/raft/pkg/api/v1"
	"github.com/lni/dragonboat/v3"
	raftconfig "github.com/lni/dragonboat/v3/config"
	dbstatemachine "github.com/lni/dragonboat/v3/statemachine"
	"strconv"
	"strings"
	"sync"
	"time"
)

var log = logging.GetLogger()

func NewProtocol(config NodeConfig, registry *statemachine.PrimitiveTypeRegistry, opts ...Option) *Protocol {
	var options Options
	options.apply(opts...)

	protocol := &Protocol{
		config:     config,
		registry:   registry,
		partitions: make(map[rsmv1.PartitionID]*Partition),
		watchers:   make(map[int]chan<- raftv1.Event),
	}

	listener := newEventListener(protocol)
	address := fmt.Sprintf("%s:%d", options.Host, options.Port)
	nodeConfig := raftconfig.NodeHostConfig{
		WALDir:              config.GetDataDir(),
		NodeHostDir:         config.GetDataDir(),
		RTTMillisecond:      uint64(config.GetRTT().Milliseconds()),
		RaftAddress:         address,
		RaftEventListener:   listener,
		SystemEventListener: listener,
	}

	host, err := dragonboat.NewNodeHost(nodeConfig)
	if err != nil {
		panic(err)
	}

	protocol.host = host
	return protocol
}

type Protocol struct {
	host       *dragonboat.NodeHost
	config     NodeConfig
	registry   *statemachine.PrimitiveTypeRegistry
	partitions map[rsmv1.PartitionID]*Partition
	watchers   map[int]chan<- raftv1.Event
	watcherID  int
	mu         sync.RWMutex
}

func (n *Protocol) publish(event *raftv1.Event) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	switch e := event.Event.(type) {
	case *raftv1.Event_ReplicaReady:
		if partition, ok := n.partitions[rsmv1.PartitionID(e.ReplicaReady.ShardID)]; ok {
			partition.setReady()
		}
	case *raftv1.Event_LeaderUpdated:
		if partition, ok := n.partitions[rsmv1.PartitionID(e.LeaderUpdated.ShardID)]; ok {
			partition.setLeader(e.LeaderUpdated.Term, e.LeaderUpdated.Leader)
		}
	}
	log.Infow("Publish Event",
		logging.Stringer("Event", event))
	for _, listener := range n.watchers {
		listener <- *event
	}
}

func (n *Protocol) Partition(partitionID rsmv1.PartitionID) (node.Partition, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	partition, ok := n.partitions[partitionID]
	return partition, ok
}

func (n *Protocol) Partitions() []node.Partition {
	n.mu.RLock()
	defer n.mu.RUnlock()
	partitions := make([]node.Partition, 0, len(n.partitions))
	for _, partition := range n.partitions {
		partitions = append(partitions, partition)
	}
	return partitions
}

func (n *Protocol) Watch(ctx context.Context, watcher chan<- raftv1.Event) {
	n.watcherID++
	id := n.watcherID
	n.mu.Lock()
	n.watchers[id] = watcher
	for _, partition := range n.partitions {
		term, leader := partition.getLeader()
		if term > 0 {
			watcher <- raftv1.Event{
				Event: &raftv1.Event_LeaderUpdated{
					LeaderUpdated: &raftv1.LeaderUpdatedEvent{
						ReplicaEvent: raftv1.ReplicaEvent{
							ShardID:   raftv1.ShardID(partition.ID()),
							ReplicaID: partition.replicaID,
						},
						Term:   term,
						Leader: leader,
					},
				},
			}
		}
		ready := partition.getReady()
		if ready {
			watcher <- raftv1.Event{
				Event: &raftv1.Event_ReplicaReady{
					ReplicaReady: &raftv1.ReplicaReadyEvent{
						ReplicaEvent: raftv1.ReplicaEvent{
							ShardID:   raftv1.ShardID(partition.ID()),
							ReplicaID: partition.replicaID,
						},
					},
				},
			}
		}
	}
	n.mu.Unlock()

	go func() {
		<-ctx.Done()
		n.mu.Lock()
		close(watcher)
		delete(n.watchers, id)
		n.mu.Unlock()
	}()
}

func (n *Protocol) GetConfig(shardID raftv1.ShardID) (raftv1.ShardConfig, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	membership, err := n.host.SyncGetClusterMembership(ctx, uint64(shardID))
	if err != nil {
		if err == dragonboat.ErrClusterNotFound {
			return raftv1.ShardConfig{}, errors.NewNotFound(err.Error())
		}
		return raftv1.ShardConfig{}, wrapError(err)
	}

	var replicas []raftv1.ReplicaConfig
	for replicaID, address := range membership.Nodes {
		member, err := getReplica(replicaID, address, raftv1.ReplicaRole_MEMBER)
		if err != nil {
			return raftv1.ShardConfig{}, err
		}
		replicas = append(replicas, member)
	}
	for replicaID, address := range membership.Observers {
		member, err := getReplica(replicaID, address, raftv1.ReplicaRole_OBSERVER)
		if err != nil {
			return raftv1.ShardConfig{}, err
		}
		replicas = append(replicas, member)
	}
	for replicaID, address := range membership.Witnesses {
		member, err := getReplica(replicaID, address, raftv1.ReplicaRole_WITNESS)
		if err != nil {
			return raftv1.ShardConfig{}, err
		}
		replicas = append(replicas, member)
	}
	for replicaID := range membership.Removed {
		replicas = append(replicas, raftv1.ReplicaConfig{
			ReplicaID: raftv1.ReplicaID(replicaID),
			Role:      raftv1.ReplicaRole_REMOVED,
		})
	}
	return raftv1.ShardConfig{
		ShardID:  shardID,
		Replicas: replicas,
		Version:  membership.ConfigChangeID,
	}, nil
}

func getReplica(replicaID uint64, address string, role raftv1.ReplicaRole) (raftv1.ReplicaConfig, error) {
	parts := strings.Split(address, ":")
	host, portS := parts[0], parts[1]
	port, err := strconv.Atoi(portS)
	if err != nil {
		return raftv1.ReplicaConfig{}, err
	}
	return raftv1.ReplicaConfig{
		ReplicaID: raftv1.ReplicaID(replicaID),
		Host:      host,
		Port:      int32(port),
		Role:      role,
	}, nil
}

func (n *Protocol) BootstrapShard(ctx context.Context, shardID raftv1.ShardID, replicaID raftv1.ReplicaID, config raftv1.RaftConfig, replicas ...raftv1.ReplicaConfig) error {
	var replica *raftv1.ReplicaConfig
	for _, r := range replicas {
		if r.ReplicaID == replicaID {
			replica = &r
			break
		}
	}

	if replica == nil {
		return errors.NewInvalid("unknown replica %d", replicaID)
	}

	raftConfig := n.getRaftConfig(shardID, replicaID, replica.Role, config)
	targets := make(map[uint64]dragonboat.Target)
	for _, member := range replicas {
		targets[uint64(member.ReplicaID)] = fmt.Sprintf("%s:%d", member.Host, member.Port)
	}
	if err := n.host.StartCluster(targets, false, n.newStateMachine, raftConfig); err != nil {
		if err == dragonboat.ErrClusterAlreadyExist {
			return nil
		}
		return wrapError(err)
	}
	return nil
}

func (n *Protocol) AddReplica(ctx context.Context, shardID raftv1.ShardID, member raftv1.ReplicaConfig, version uint64) error {
	address := fmt.Sprintf("%s:%d", member.Host, member.Port)
	if err := n.host.SyncRequestAddNode(ctx, uint64(shardID), uint64(member.ReplicaID), address, version); err != nil {
		if err == dragonboat.ErrClusterNotFound {
			return errors.NewNotFound(err.Error())
		}
		return wrapError(err)
	}
	return nil
}

func (n *Protocol) RemoveReplica(ctx context.Context, shardID raftv1.ShardID, replicaID raftv1.ReplicaID, version uint64) error {
	if err := n.host.SyncRequestDeleteNode(ctx, uint64(shardID), uint64(replicaID), version); err != nil {
		if err == dragonboat.ErrClusterNotFound {
			return errors.NewNotFound(err.Error())
		}
		return wrapError(err)
	}
	return nil
}

func (n *Protocol) JoinShard(ctx context.Context, shardID raftv1.ShardID, replicaID raftv1.ReplicaID, config raftv1.RaftConfig) error {
	raftConfig := n.getRaftConfig(shardID, replicaID, raftv1.ReplicaRole_MEMBER, config)
	if err := n.host.StartCluster(map[uint64]dragonboat.Target{}, true, n.newStateMachine, raftConfig); err != nil {
		if err == dragonboat.ErrClusterAlreadyExist {
			return errors.NewAlreadyExists(err.Error())
		}
		return wrapError(err)
	}
	return nil
}

func (n *Protocol) LeaveShard(ctx context.Context, shardID raftv1.ShardID) error {
	if err := n.host.StopCluster(uint64(shardID)); err != nil {
		if err == dragonboat.ErrClusterNotFound {
			return errors.NewNotFound(err.Error())
		}
		return wrapError(err)
	}
	return nil
}

func (n *Protocol) DeleteData(ctx context.Context, shardID raftv1.ShardID, replicaID raftv1.ReplicaID) error {
	if err := n.host.SyncRemoveData(ctx, uint64(shardID), uint64(replicaID)); err != nil {
		return wrapError(err)
	}
	return nil
}

func (n *Protocol) newStateMachine(clusterID, replicaID uint64) dbstatemachine.IStateMachine {
	streams := newContext()
	partition := newPartition(rsmv1.PartitionID(clusterID), raftv1.ReplicaID(replicaID), n.host, streams)
	n.mu.Lock()
	n.partitions[partition.ID()] = partition
	n.mu.Unlock()
	return newStateMachine(streams, n.registry)
}

func (n *Protocol) getRaftConfig(shardID raftv1.ShardID, replicaID raftv1.ReplicaID, role raftv1.ReplicaRole, config raftv1.RaftConfig) raftconfig.Config {
	electionRTT := config.ElectionRTT
	if electionRTT == 0 {
		electionRTT = defaultElectionRTT
	}
	heartbeatRTT := config.HeartbeatRTT
	if heartbeatRTT == 0 {
		heartbeatRTT = defaultHeartbeatRTT
	}
	snapshotEntries := config.SnapshotEntries
	if snapshotEntries == 0 {
		snapshotEntries = defaultSnapshotEntries
	}
	compactionOverhead := config.CompactionOverhead
	if compactionOverhead == 0 {
		compactionOverhead = defaultCompactionOverhead
	}
	return raftconfig.Config{
		NodeID:                 uint64(replicaID),
		ClusterID:              uint64(shardID),
		ElectionRTT:            electionRTT,
		HeartbeatRTT:           heartbeatRTT,
		CheckQuorum:            true,
		SnapshotEntries:        snapshotEntries,
		CompactionOverhead:     compactionOverhead,
		MaxInMemLogSize:        config.MaxInMemLogSize,
		OrderedConfigChange:    config.OrderedConfigChange,
		DisableAutoCompactions: config.DisableAutoCompactions,
		IsObserver:             role == raftv1.ReplicaRole_OBSERVER,
		IsWitness:              role == raftv1.ReplicaRole_WITNESS,
	}
}

func wrapError(err error) error {
	switch err {
	case dragonboat.ErrClusterNotFound,
		dragonboat.ErrClusterNotBootstrapped,
		dragonboat.ErrClusterNotInitialized,
		dragonboat.ErrClusterNotReady,
		dragonboat.ErrClusterClosed:
		return errors.NewUnavailable(err.Error())
	case dragonboat.ErrSystemBusy,
		dragonboat.ErrBadKey:
		return errors.NewUnavailable(err.Error())
	case dragonboat.ErrClosed,
		dragonboat.ErrNodeRemoved:
		return errors.NewUnavailable(err.Error())
	case dragonboat.ErrInvalidSession,
		dragonboat.ErrInvalidTarget,
		dragonboat.ErrInvalidAddress,
		dragonboat.ErrInvalidOperation:
		return errors.NewInvalid(err.Error())
	case dragonboat.ErrPayloadTooBig,
		dragonboat.ErrTimeoutTooSmall:
		return errors.NewForbidden(err.Error())
	case dragonboat.ErrDeadlineNotSet,
		dragonboat.ErrInvalidDeadline:
		return errors.NewInternal(err.Error())
	case dragonboat.ErrDirNotExist:
		return errors.NewInternal(err.Error())
	case dragonboat.ErrTimeout:
		return errors.NewTimeout(err.Error())
	case dragonboat.ErrCanceled:
		return errors.NewCanceled(err.Error())
	case dragonboat.ErrRejected:
		return errors.NewForbidden(err.Error())
	default:
		return errors.NewUnknown(err.Error())
	}
}
