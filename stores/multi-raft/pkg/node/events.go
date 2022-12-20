// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package multiraft

import (
	multiraftv1 "github.com/atomix/atomix/stores/multi-raft/pkg/api/v1"
	"github.com/lni/dragonboat/v3/raftio"
	"time"
)

func newEventListener(protocol *Protocol) *eventListener {
	return &eventListener{
		protocol: protocol,
	}
}

type eventListener struct {
	protocol *Protocol
}

func (e *eventListener) publish(event *multiraftv1.Event) {
	e.protocol.publish(event)
}

func (e *eventListener) LeaderUpdated(info raftio.LeaderInfo) {
	e.publish(&multiraftv1.Event{
		Timestamp: time.Now(),
		Event: &multiraftv1.Event_LeaderUpdated{
			LeaderUpdated: &multiraftv1.LeaderUpdatedEvent{
				MemberEvent: multiraftv1.MemberEvent{
					GroupID:  multiraftv1.GroupID(info.ClusterID),
					MemberID: multiraftv1.MemberID(info.NodeID),
				},
				Term:   multiraftv1.Term(info.Term),
				Leader: multiraftv1.MemberID(info.LeaderID),
			},
		},
	})
}

func (e *eventListener) NodeHostShuttingDown() {

}

func (e *eventListener) NodeUnloaded(info raftio.NodeInfo) {

}

func (e *eventListener) NodeReady(info raftio.NodeInfo) {
	e.publish(&multiraftv1.Event{
		Timestamp: time.Now(),
		Event: &multiraftv1.Event_MemberReady{
			MemberReady: &multiraftv1.MemberReadyEvent{
				MemberEvent: multiraftv1.MemberEvent{
					GroupID:  multiraftv1.GroupID(info.ClusterID),
					MemberID: multiraftv1.MemberID(info.NodeID),
				},
			},
		},
	})
}

func (e *eventListener) MembershipChanged(info raftio.NodeInfo) {
	e.publish(&multiraftv1.Event{
		Timestamp: time.Now(),
		Event: &multiraftv1.Event_MembershipChanged{
			MembershipChanged: &multiraftv1.MembershipChangedEvent{
				MemberEvent: multiraftv1.MemberEvent{
					GroupID:  multiraftv1.GroupID(info.ClusterID),
					MemberID: multiraftv1.MemberID(info.NodeID),
				},
			},
		},
	})
}

func (e *eventListener) ConnectionEstablished(info raftio.ConnectionInfo) {
	e.publish(&multiraftv1.Event{
		Timestamp: time.Now(),
		Event: &multiraftv1.Event_ConnectionEstablished{
			ConnectionEstablished: &multiraftv1.ConnectionEstablishedEvent{
				ConnectionInfo: multiraftv1.ConnectionInfo{
					Address:  info.Address,
					Snapshot: info.SnapshotConnection,
				},
			},
		},
	})
}

func (e *eventListener) ConnectionFailed(info raftio.ConnectionInfo) {
	e.publish(&multiraftv1.Event{
		Timestamp: time.Now(),
		Event: &multiraftv1.Event_ConnectionFailed{
			ConnectionFailed: &multiraftv1.ConnectionFailedEvent{
				ConnectionInfo: multiraftv1.ConnectionInfo{
					Address:  info.Address,
					Snapshot: info.SnapshotConnection,
				},
			},
		},
	})
}

func (e *eventListener) SendSnapshotStarted(info raftio.SnapshotInfo) {
	e.publish(&multiraftv1.Event{
		Timestamp: time.Now(),
		Event: &multiraftv1.Event_SendSnapshotStarted{
			SendSnapshotStarted: &multiraftv1.SendSnapshotStartedEvent{
				MemberEvent: multiraftv1.MemberEvent{
					GroupID:  multiraftv1.GroupID(info.ClusterID),
					MemberID: multiraftv1.MemberID(info.NodeID),
				},
				Index: multiraftv1.Index(info.Index),
				To:    multiraftv1.MemberID(info.NodeID),
			},
		},
	})
}

func (e *eventListener) SendSnapshotCompleted(info raftio.SnapshotInfo) {
	e.publish(&multiraftv1.Event{
		Timestamp: time.Now(),
		Event: &multiraftv1.Event_SendSnapshotCompleted{
			SendSnapshotCompleted: &multiraftv1.SendSnapshotCompletedEvent{
				MemberEvent: multiraftv1.MemberEvent{
					GroupID:  multiraftv1.GroupID(info.ClusterID),
					MemberID: multiraftv1.MemberID(info.NodeID),
				},
				Index: multiraftv1.Index(info.Index),
				To:    multiraftv1.MemberID(info.NodeID),
			},
		},
	})
}

func (e *eventListener) SendSnapshotAborted(info raftio.SnapshotInfo) {
	e.publish(&multiraftv1.Event{
		Timestamp: time.Now(),
		Event: &multiraftv1.Event_SendSnapshotAborted{
			SendSnapshotAborted: &multiraftv1.SendSnapshotAbortedEvent{
				MemberEvent: multiraftv1.MemberEvent{
					GroupID:  multiraftv1.GroupID(info.ClusterID),
					MemberID: multiraftv1.MemberID(info.NodeID),
				},
				Index: multiraftv1.Index(info.Index),
				To:    multiraftv1.MemberID(info.NodeID),
			},
		},
	})
}

func (e *eventListener) SnapshotReceived(info raftio.SnapshotInfo) {
	e.publish(&multiraftv1.Event{
		Timestamp: time.Now(),
		Event: &multiraftv1.Event_SnapshotReceived{
			SnapshotReceived: &multiraftv1.SnapshotReceivedEvent{
				MemberEvent: multiraftv1.MemberEvent{
					GroupID:  multiraftv1.GroupID(info.ClusterID),
					MemberID: multiraftv1.MemberID(info.NodeID),
				},
				Index: multiraftv1.Index(info.Index),
				From:  multiraftv1.MemberID(info.From),
			},
		},
	})
}

func (e *eventListener) SnapshotRecovered(info raftio.SnapshotInfo) {
	e.publish(&multiraftv1.Event{
		Timestamp: time.Now(),
		Event: &multiraftv1.Event_SnapshotRecovered{
			SnapshotRecovered: &multiraftv1.SnapshotRecoveredEvent{
				MemberEvent: multiraftv1.MemberEvent{
					GroupID:  multiraftv1.GroupID(info.ClusterID),
					MemberID: multiraftv1.MemberID(info.NodeID),
				},
				Index: multiraftv1.Index(info.Index),
			},
		},
	})
}

func (e *eventListener) SnapshotCreated(info raftio.SnapshotInfo) {
	e.publish(&multiraftv1.Event{
		Timestamp: time.Now(),
		Event: &multiraftv1.Event_SnapshotCreated{
			SnapshotCreated: &multiraftv1.SnapshotCreatedEvent{
				MemberEvent: multiraftv1.MemberEvent{
					GroupID:  multiraftv1.GroupID(info.ClusterID),
					MemberID: multiraftv1.MemberID(info.NodeID),
				},
				Index: multiraftv1.Index(info.Index),
			},
		},
	})
}

func (e *eventListener) SnapshotCompacted(info raftio.SnapshotInfo) {
	e.publish(&multiraftv1.Event{
		Timestamp: time.Now(),
		Event: &multiraftv1.Event_SnapshotCompacted{
			SnapshotCompacted: &multiraftv1.SnapshotCompactedEvent{
				MemberEvent: multiraftv1.MemberEvent{
					GroupID:  multiraftv1.GroupID(info.ClusterID),
					MemberID: multiraftv1.MemberID(info.NodeID),
				},
				Index: multiraftv1.Index(info.Index),
			},
		},
	})
}

func (e *eventListener) LogCompacted(info raftio.EntryInfo) {
	e.publish(&multiraftv1.Event{
		Timestamp: time.Now(),
		Event: &multiraftv1.Event_LogCompacted{
			LogCompacted: &multiraftv1.LogCompactedEvent{
				MemberEvent: multiraftv1.MemberEvent{
					GroupID:  multiraftv1.GroupID(info.ClusterID),
					MemberID: multiraftv1.MemberID(info.NodeID),
				},
				Index: multiraftv1.Index(info.Index),
			},
		},
	})
}

func (e *eventListener) LogDBCompacted(info raftio.EntryInfo) {
	e.publish(&multiraftv1.Event{
		Timestamp: time.Now(),
		Event: &multiraftv1.Event_LogdbCompacted{
			LogdbCompacted: &multiraftv1.LogDBCompactedEvent{
				MemberEvent: multiraftv1.MemberEvent{
					GroupID:  multiraftv1.GroupID(info.ClusterID),
					MemberID: multiraftv1.MemberID(info.NodeID),
				},
				Index: multiraftv1.Index(info.Index),
			},
		},
	})
}
