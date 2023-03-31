// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta3

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RaftReplicaState is a state constant for RaftReplica
type RaftReplicaState string

const (
	// RaftReplicaPending indicates a RaftReplica is waiting to join the cluster
	RaftReplicaPending RaftReplicaState = "Pending"
	// RaftReplicaBootstrapping indicates a RaftReplica is bootstrapping the cluster
	RaftReplicaBootstrapping RaftReplicaState = "Bootstrapping"
	// RaftReplicaJoining indicates a RaftReplica is joining the cluster
	RaftReplicaJoining RaftReplicaState = "Joining"
	// RaftReplicaLeaving indicates a RaftReplica is leaving the cluster
	RaftReplicaLeaving RaftReplicaState = "Leaving"
	// RaftReplicaRunning indicates a RaftReplica is running
	RaftReplicaRunning RaftReplicaState = "Running"
	// RaftReplicaReady indicates a RaftReplica is ready
	RaftReplicaReady RaftReplicaState = "Ready"
)

type RaftReplicaType string

const (
	RaftVoter    RaftReplicaType = "Voter"
	RaftWitness  RaftReplicaType = "Witness"
	RaftObserver RaftReplicaType = "Observer"
)

// RaftReplicaRole is a constant for RaftReplica representing the current role of the replica
type RaftReplicaRole string

const (
	// RaftLeader is a RaftReplicaRole indicating the RaftReplica is currently the leader of the group
	RaftLeader RaftReplicaRole = "Leader"
	// RaftCandidate is a RaftReplicaRole indicating the RaftReplica is currently a candidate
	RaftCandidate RaftReplicaRole = "Candidate"
	// RaftFollower is a RaftReplicaRole indicating the RaftReplica is currently a follower
	RaftFollower RaftReplicaRole = "Follower"
)

type ReplicaID int64

type MemberID int64

type RaftReplicaSpec struct {
	GroupID   GroupID                     `json:"groupID"`
	ReplicaID ReplicaID                   `json:"replicaID"`
	MemberID  MemberID                    `json:"memberID"`
	Pod       corev1.LocalObjectReference `json:"pod"`
	Type      RaftReplicaType             `json:"type"`
	Peers     int32                       `json:"peers"`
	Join      bool                        `json:"join"`
}

// RaftReplicaStatus defines the status of a RaftReplica
type RaftReplicaStatus struct {
	PodRef            *corev1.ObjectReference      `json:"podRef"`
	Version           *int32                       `json:"version"`
	State             RaftReplicaState             `json:"state,omitempty"`
	Role              *RaftReplicaRole             `json:"role,omitempty"`
	Leader            *corev1.LocalObjectReference `json:"leader,omitempty"`
	Term              *int64                       `json:"term,omitempty"`
	LastUpdated       *metav1.Time                 `json:"lastUpdated,omitempty"`
	LastSnapshotIndex *int64                       `json:"lastSnapshotIndex,omitempty"`
	LastSnapshotTime  *metav1.Time                 `json:"lastSnapshotTime,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RaftReplica is the Schema for the RaftReplica API
// +k8s:openapi-gen=true
type RaftReplica struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              RaftReplicaSpec   `json:"spec,omitempty"`
	Status            RaftReplicaStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RaftReplicaList contains a list of RaftReplica
type RaftReplicaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	// Items is the RaftReplica of items in the list
	Items []RaftReplica `json:"items"`
}
