// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"context"
	"fmt"
	"github.com/atomix/atomix/api/errors"
	rsmv1 "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/node"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
	"github.com/atomix/atomix/runtime/pkg/logging"
	raftv1 "github.com/atomix/atomix/stores/raft/api/v1"
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
	case *raftv1.Event_MemberReady:
		if partition, ok := n.partitions[rsmv1.PartitionID(e.MemberReady.GroupID)]; ok {
			partition.setReady()
		}
	case *raftv1.Event_LeaderUpdated:
		if partition, ok := n.partitions[rsmv1.PartitionID(e.LeaderUpdated.GroupID)]; ok {
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
						MemberEvent: raftv1.MemberEvent{
							GroupID:  raftv1.GroupID(partition.ID()),
							MemberID: partition.memberID,
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
				Event: &raftv1.Event_MemberReady{
					MemberReady: &raftv1.MemberReadyEvent{
						MemberEvent: raftv1.MemberEvent{
							GroupID:  raftv1.GroupID(partition.ID()),
							MemberID: partition.memberID,
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

func (n *Protocol) GetConfig(groupID raftv1.GroupID) (raftv1.GroupConfig, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	membership, err := n.host.SyncGetClusterMembership(ctx, uint64(groupID))
	if err != nil {
		if err == dragonboat.ErrClusterNotFound {
			return raftv1.GroupConfig{}, errors.NewNotFound(err.Error())
		}
		return raftv1.GroupConfig{}, wrapError(err)
	}

	var members []raftv1.MemberConfig
	for memberID, address := range membership.Nodes {
		member, err := getMember(memberID, address, raftv1.MemberRole_MEMBER)
		if err != nil {
			return raftv1.GroupConfig{}, err
		}
		members = append(members, member)
	}
	for memberID, address := range membership.Observers {
		member, err := getMember(memberID, address, raftv1.MemberRole_OBSERVER)
		if err != nil {
			return raftv1.GroupConfig{}, err
		}
		members = append(members, member)
	}
	for memberID, address := range membership.Witnesses {
		member, err := getMember(memberID, address, raftv1.MemberRole_WITNESS)
		if err != nil {
			return raftv1.GroupConfig{}, err
		}
		members = append(members, member)
	}
	for memberID := range membership.Removed {
		members = append(members, raftv1.MemberConfig{
			MemberID: raftv1.MemberID(memberID),
			Role:     raftv1.MemberRole_REMOVED,
		})
	}
	return raftv1.GroupConfig{
		GroupID: groupID,
		Members: members,
		Version: membership.ConfigChangeID,
	}, nil
}

func getMember(memberID uint64, address string, role raftv1.MemberRole) (raftv1.MemberConfig, error) {
	parts := strings.Split(address, ":")
	host, portS := parts[0], parts[1]
	port, err := strconv.Atoi(portS)
	if err != nil {
		return raftv1.MemberConfig{}, err
	}
	return raftv1.MemberConfig{
		MemberID: raftv1.MemberID(memberID),
		Host:     host,
		Port:     int32(port),
		Role:     role,
	}, nil
}

func (n *Protocol) BootstrapGroup(ctx context.Context, groupID raftv1.GroupID, memberID raftv1.MemberID, config raftv1.RaftConfig, members ...raftv1.MemberConfig) error {
	var member *raftv1.MemberConfig
	for _, r := range members {
		if r.MemberID == memberID {
			member = &r
			break
		}
	}

	if member == nil {
		return errors.NewInvalid("unknown member %d", memberID)
	}

	raftConfig := n.getRaftConfig(groupID, memberID, member.Role, config)
	targets := make(map[uint64]dragonboat.Target)
	for _, member := range members {
		targets[uint64(member.MemberID)] = fmt.Sprintf("%s:%d", member.Host, member.Port)
	}
	if err := n.host.StartCluster(targets, false, n.newStateMachine, raftConfig); err != nil {
		if err == dragonboat.ErrClusterAlreadyExist {
			return nil
		}
		return wrapError(err)
	}
	return nil
}

func (n *Protocol) AddMember(ctx context.Context, groupID raftv1.GroupID, member raftv1.MemberConfig, version uint64) error {
	address := fmt.Sprintf("%s:%d", member.Host, member.Port)
	if err := n.host.SyncRequestAddNode(ctx, uint64(groupID), uint64(member.MemberID), address, version); err != nil {
		if err == dragonboat.ErrClusterNotFound {
			return errors.NewNotFound(err.Error())
		}
		return wrapError(err)
	}
	return nil
}

func (n *Protocol) RemoveMember(ctx context.Context, groupID raftv1.GroupID, memberID raftv1.MemberID, version uint64) error {
	if err := n.host.SyncRequestDeleteNode(ctx, uint64(groupID), uint64(memberID), version); err != nil {
		if err == dragonboat.ErrClusterNotFound {
			return errors.NewNotFound(err.Error())
		}
		return wrapError(err)
	}
	return nil
}

func (n *Protocol) JoinGroup(ctx context.Context, groupID raftv1.GroupID, memberID raftv1.MemberID, config raftv1.RaftConfig) error {
	raftConfig := n.getRaftConfig(groupID, memberID, raftv1.MemberRole_MEMBER, config)
	if err := n.host.StartCluster(map[uint64]dragonboat.Target{}, true, n.newStateMachine, raftConfig); err != nil {
		if err == dragonboat.ErrClusterAlreadyExist {
			return errors.NewAlreadyExists(err.Error())
		}
		return wrapError(err)
	}
	return nil
}

func (n *Protocol) LeaveGroup(ctx context.Context, groupID raftv1.GroupID) error {
	if err := n.host.StopCluster(uint64(groupID)); err != nil {
		if err == dragonboat.ErrClusterNotFound {
			return errors.NewNotFound(err.Error())
		}
		return wrapError(err)
	}
	return nil
}

func (n *Protocol) DeleteData(ctx context.Context, groupID raftv1.GroupID, memberID raftv1.MemberID) error {
	if err := n.host.SyncRemoveData(ctx, uint64(groupID), uint64(memberID)); err != nil {
		return wrapError(err)
	}
	return nil
}

func (n *Protocol) newStateMachine(clusterID, memberID uint64) dbstatemachine.IStateMachine {
	streams := newContext()
	partition := newPartition(rsmv1.PartitionID(clusterID), raftv1.MemberID(memberID), n.host, streams)
	n.mu.Lock()
	n.partitions[partition.ID()] = partition
	n.mu.Unlock()
	return newStateMachine(streams, n.registry)
}

func (n *Protocol) getRaftConfig(groupID raftv1.GroupID, memberID raftv1.MemberID, role raftv1.MemberRole, config raftv1.RaftConfig) raftconfig.Config {
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
		NodeID:                 uint64(memberID),
		ClusterID:              uint64(groupID),
		ElectionRTT:            electionRTT,
		HeartbeatRTT:           heartbeatRTT,
		CheckQuorum:            true,
		SnapshotEntries:        snapshotEntries,
		CompactionOverhead:     compactionOverhead,
		MaxInMemLogSize:        config.MaxInMemLogSize,
		OrderedConfigChange:    config.OrderedConfigChange,
		DisableAutoCompactions: config.DisableAutoCompactions,
		IsObserver:             role == raftv1.MemberRole_OBSERVER,
		IsWitness:              role == raftv1.MemberRole_WITNESS,
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
