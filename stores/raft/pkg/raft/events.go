// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	raftv1 "github.com/atomix/atomix/stores/raft/api/v1"
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

func (e *eventListener) publish(event *raftv1.Event) {
	e.protocol.publish(event)
}

func (e *eventListener) LeaderUpdated(info raftio.LeaderInfo) {
	e.publish(&raftv1.Event{
		Timestamp: time.Now(),
		Event: &raftv1.Event_LeaderUpdated{
			LeaderUpdated: &raftv1.LeaderUpdatedEvent{
				MemberEvent: raftv1.MemberEvent{
					GroupID:  raftv1.GroupID(info.ClusterID),
					MemberID: raftv1.MemberID(info.NodeID),
				},
				Term:   raftv1.Term(info.Term),
				Leader: raftv1.MemberID(info.LeaderID),
			},
		},
	})
}

func (e *eventListener) NodeHostShuttingDown() {

}

func (e *eventListener) NodeUnloaded(info raftio.NodeInfo) {

}

func (e *eventListener) NodeReady(info raftio.NodeInfo) {
	e.publish(&raftv1.Event{
		Timestamp: time.Now(),
		Event: &raftv1.Event_MemberReady{
			MemberReady: &raftv1.MemberReadyEvent{
				MemberEvent: raftv1.MemberEvent{
					GroupID:  raftv1.GroupID(info.ClusterID),
					MemberID: raftv1.MemberID(info.NodeID),
				},
			},
		},
	})
}

func (e *eventListener) MembershipChanged(info raftio.NodeInfo) {
	e.publish(&raftv1.Event{
		Timestamp: time.Now(),
		Event: &raftv1.Event_ConfigurationChanged{
			ConfigurationChanged: &raftv1.ConfigurationChangedEvent{
				MemberEvent: raftv1.MemberEvent{
					GroupID:  raftv1.GroupID(info.ClusterID),
					MemberID: raftv1.MemberID(info.NodeID),
				},
			},
		},
	})
}

func (e *eventListener) ConnectionEstablished(info raftio.ConnectionInfo) {
	e.publish(&raftv1.Event{
		Timestamp: time.Now(),
		Event: &raftv1.Event_ConnectionEstablished{
			ConnectionEstablished: &raftv1.ConnectionEstablishedEvent{
				ConnectionInfo: raftv1.ConnectionInfo{
					Address:  info.Address,
					Snapshot: info.SnapshotConnection,
				},
			},
		},
	})
}

func (e *eventListener) ConnectionFailed(info raftio.ConnectionInfo) {
	e.publish(&raftv1.Event{
		Timestamp: time.Now(),
		Event: &raftv1.Event_ConnectionFailed{
			ConnectionFailed: &raftv1.ConnectionFailedEvent{
				ConnectionInfo: raftv1.ConnectionInfo{
					Address:  info.Address,
					Snapshot: info.SnapshotConnection,
				},
			},
		},
	})
}

func (e *eventListener) SendSnapshotStarted(info raftio.SnapshotInfo) {
	e.publish(&raftv1.Event{
		Timestamp: time.Now(),
		Event: &raftv1.Event_SendSnapshotStarted{
			SendSnapshotStarted: &raftv1.SendSnapshotStartedEvent{
				MemberEvent: raftv1.MemberEvent{
					GroupID:  raftv1.GroupID(info.ClusterID),
					MemberID: raftv1.MemberID(info.NodeID),
				},
				Index: raftv1.Index(info.Index),
				To:    raftv1.MemberID(info.NodeID),
			},
		},
	})
}

func (e *eventListener) SendSnapshotCompleted(info raftio.SnapshotInfo) {
	e.publish(&raftv1.Event{
		Timestamp: time.Now(),
		Event: &raftv1.Event_SendSnapshotCompleted{
			SendSnapshotCompleted: &raftv1.SendSnapshotCompletedEvent{
				MemberEvent: raftv1.MemberEvent{
					GroupID:  raftv1.GroupID(info.ClusterID),
					MemberID: raftv1.MemberID(info.NodeID),
				},
				Index: raftv1.Index(info.Index),
				To:    raftv1.MemberID(info.NodeID),
			},
		},
	})
}

func (e *eventListener) SendSnapshotAborted(info raftio.SnapshotInfo) {
	e.publish(&raftv1.Event{
		Timestamp: time.Now(),
		Event: &raftv1.Event_SendSnapshotAborted{
			SendSnapshotAborted: &raftv1.SendSnapshotAbortedEvent{
				MemberEvent: raftv1.MemberEvent{
					GroupID:  raftv1.GroupID(info.ClusterID),
					MemberID: raftv1.MemberID(info.NodeID),
				},
				Index: raftv1.Index(info.Index),
				To:    raftv1.MemberID(info.NodeID),
			},
		},
	})
}

func (e *eventListener) SnapshotReceived(info raftio.SnapshotInfo) {
	e.publish(&raftv1.Event{
		Timestamp: time.Now(),
		Event: &raftv1.Event_SnapshotReceived{
			SnapshotReceived: &raftv1.SnapshotReceivedEvent{
				MemberEvent: raftv1.MemberEvent{
					GroupID:  raftv1.GroupID(info.ClusterID),
					MemberID: raftv1.MemberID(info.NodeID),
				},
				Index: raftv1.Index(info.Index),
				From:  raftv1.MemberID(info.From),
			},
		},
	})
}

func (e *eventListener) SnapshotRecovered(info raftio.SnapshotInfo) {
	e.publish(&raftv1.Event{
		Timestamp: time.Now(),
		Event: &raftv1.Event_SnapshotRecovered{
			SnapshotRecovered: &raftv1.SnapshotRecoveredEvent{
				MemberEvent: raftv1.MemberEvent{
					GroupID:  raftv1.GroupID(info.ClusterID),
					MemberID: raftv1.MemberID(info.NodeID),
				},
				Index: raftv1.Index(info.Index),
			},
		},
	})
}

func (e *eventListener) SnapshotCreated(info raftio.SnapshotInfo) {
	e.publish(&raftv1.Event{
		Timestamp: time.Now(),
		Event: &raftv1.Event_SnapshotCreated{
			SnapshotCreated: &raftv1.SnapshotCreatedEvent{
				MemberEvent: raftv1.MemberEvent{
					GroupID:  raftv1.GroupID(info.ClusterID),
					MemberID: raftv1.MemberID(info.NodeID),
				},
				Index: raftv1.Index(info.Index),
			},
		},
	})
}

func (e *eventListener) SnapshotCompacted(info raftio.SnapshotInfo) {
	e.publish(&raftv1.Event{
		Timestamp: time.Now(),
		Event: &raftv1.Event_SnapshotCompacted{
			SnapshotCompacted: &raftv1.SnapshotCompactedEvent{
				MemberEvent: raftv1.MemberEvent{
					GroupID:  raftv1.GroupID(info.ClusterID),
					MemberID: raftv1.MemberID(info.NodeID),
				},
				Index: raftv1.Index(info.Index),
			},
		},
	})
}

func (e *eventListener) LogCompacted(info raftio.EntryInfo) {
	e.publish(&raftv1.Event{
		Timestamp: time.Now(),
		Event: &raftv1.Event_LogCompacted{
			LogCompacted: &raftv1.LogCompactedEvent{
				MemberEvent: raftv1.MemberEvent{
					GroupID:  raftv1.GroupID(info.ClusterID),
					MemberID: raftv1.MemberID(info.NodeID),
				},
				Index: raftv1.Index(info.Index),
			},
		},
	})
}

func (e *eventListener) LogDBCompacted(info raftio.EntryInfo) {
	e.publish(&raftv1.Event{
		Timestamp: time.Now(),
		Event: &raftv1.Event_LogdbCompacted{
			LogdbCompacted: &raftv1.LogDBCompactedEvent{
				MemberEvent: raftv1.MemberEvent{
					GroupID:  raftv1.GroupID(info.ClusterID),
					MemberID: raftv1.MemberID(info.NodeID),
				},
				Index: raftv1.Index(info.Index),
			},
		},
	})
}
