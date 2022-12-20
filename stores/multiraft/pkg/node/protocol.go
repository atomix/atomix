// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package multiraft

import (
	"context"
	"fmt"
	rsmv1 "github.com/atomix/atomix/protocols/rsm/pkg/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/node"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	multiraftv1 "github.com/atomix/atomix/stores/multiraft/pkg/api/v1"
	"github.com/lni/dragonboat/v3"
	raftconfig "github.com/lni/dragonboat/v3/config"
	dbstatemachine "github.com/lni/dragonboat/v3/statemachine"
	"sync"
)

var log = logging.GetLogger()

func NewProtocol(config RaftConfig, registry *statemachine.PrimitiveTypeRegistry, opts ...Option) *Protocol {
	var options Options
	options.apply(opts...)

	protocol := &Protocol{
		config:     config,
		registry:   registry,
		partitions: make(map[rsmv1.PartitionID]*Partition),
		watchers:   make(map[int]chan<- multiraftv1.Event),
	}

	listener := newEventListener(protocol)
	address := fmt.Sprintf("%s:%d", options.Host, options.Port)
	nodeConfig := raftconfig.NodeHostConfig{
		WALDir:              config.GetDataDir(),
		NodeHostDir:         config.GetDataDir(),
		RTTMillisecond:      uint64(config.GetHeartbeatPeriod().Milliseconds()),
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
	config     RaftConfig
	registry   *statemachine.PrimitiveTypeRegistry
	partitions map[rsmv1.PartitionID]*Partition
	watchers   map[int]chan<- multiraftv1.Event
	watcherID  int
	mu         sync.RWMutex
}

func (n *Protocol) publish(event *multiraftv1.Event) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	switch e := event.Event.(type) {
	case *multiraftv1.Event_MemberReady:
		if partition, ok := n.partitions[rsmv1.PartitionID(e.MemberReady.GroupID)]; ok {
			partition.setReady()
		}
	case *multiraftv1.Event_LeaderUpdated:
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

func (n *Protocol) Watch(ctx context.Context, watcher chan<- multiraftv1.Event) {
	n.watcherID++
	id := n.watcherID
	n.mu.Lock()
	n.watchers[id] = watcher
	for _, partition := range n.partitions {
		term, leader := partition.getLeader()
		if term > 0 {
			watcher <- multiraftv1.Event{
				Event: &multiraftv1.Event_LeaderUpdated{
					LeaderUpdated: &multiraftv1.LeaderUpdatedEvent{
						MemberEvent: multiraftv1.MemberEvent{
							GroupID:  multiraftv1.GroupID(partition.ID()),
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
			watcher <- multiraftv1.Event{
				Event: &multiraftv1.Event_MemberReady{
					MemberReady: &multiraftv1.MemberReadyEvent{
						MemberEvent: multiraftv1.MemberEvent{
							GroupID:  multiraftv1.GroupID(partition.ID()),
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

func (n *Protocol) Bootstrap(config multiraftv1.GroupConfig) error {
	raftConfig := n.getRaftConfig(config)
	members := make(map[uint64]dragonboat.Target)
	for _, member := range config.Members {
		members[uint64(member.MemberID)] = fmt.Sprintf("%s:%d", member.Host, member.Port)
	}
	if err := n.host.StartCluster(members, false, n.newStateMachine, raftConfig); err != nil {
		if err == dragonboat.ErrClusterAlreadyExist {
			return nil
		}
		return wrapError(err)
	}
	return nil
}

func (n *Protocol) Join(config multiraftv1.GroupConfig) error {
	raftConfig := n.getRaftConfig(config)
	members := make(map[uint64]dragonboat.Target)
	for _, member := range config.Members {
		members[uint64(member.MemberID)] = fmt.Sprintf("%s:%d", member.Host, member.Port)
	}
	if err := n.host.StartCluster(members, true, n.newStateMachine, raftConfig); err != nil {
		if err == dragonboat.ErrClusterAlreadyExist {
			return nil
		}
		return wrapError(err)
	}
	return nil
}

func (n *Protocol) Leave(groupID multiraftv1.GroupID) error {
	return n.host.StopCluster(uint64(groupID))
}

func (n *Protocol) newStateMachine(clusterID, nodeID uint64) dbstatemachine.IStateMachine {
	streams := newContext()
	partition := newPartition(rsmv1.PartitionID(clusterID), multiraftv1.MemberID(nodeID), n.host, streams)
	n.mu.Lock()
	n.partitions[partition.ID()] = partition
	n.mu.Unlock()
	return newStateMachine(streams, n.registry)
}

func (n *Protocol) Shutdown() error {
	n.host.Stop()
	return nil
}

func (n *Protocol) getRaftConfig(config multiraftv1.GroupConfig) raftconfig.Config {
	electionRTT := uint64(10)
	if n.config.ElectionTimeout != nil {
		electionRTT = uint64(n.config.ElectionTimeout.Milliseconds() / n.config.GetHeartbeatPeriod().Milliseconds())
	}
	return raftconfig.Config{
		NodeID:             uint64(config.MemberID),
		ClusterID:          uint64(config.GroupID),
		ElectionRTT:        electionRTT,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    n.config.GetSnapshotEntryThreshold(),
		CompactionOverhead: n.config.GetCompactionRetainEntries(),
		IsObserver:         config.Role == multiraftv1.MemberRole_OBSERVER,
		IsWitness:          config.Role == multiraftv1.MemberRole_WITNESS,
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
