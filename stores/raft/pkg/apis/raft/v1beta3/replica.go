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
	// RaftReplicaNotReady indicates a RaftReplica is not ready
	RaftReplicaNotReady RaftReplicaState = "NotReady"
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

type ReplicaID uint64

type MemberID uint64

type RaftReplicaSpec struct {
	GroupID   GroupID                     `json:"groupID"`
	ReplicaID ReplicaID                   `json:"replicaID"`
	MemberID  MemberID                    `json:"memberID"`
	Pod       corev1.LocalObjectReference `json:"pod"`
	Type      RaftReplicaType             `json:"type"`
	Peers     uint32                      `json:"peers"`
	Join      bool                        `json:"join"`
}

// RaftReplicaStatus defines the status of a RaftReplica
type RaftReplicaStatus struct {
	PodRef            *corev1.ObjectReference      `json:"podRef"`
	Version           *int32                       `json:"version"`
	State             RaftReplicaState             `json:"state,omitempty"`
	Role              *RaftReplicaRole             `json:"role,omitempty"`
	Leader            *corev1.LocalObjectReference `json:"leader,omitempty"`
	Term              *uint64                      `json:"term,omitempty"`
	LastUpdated       *metav1.Time                 `json:"lastUpdated,omitempty"`
	LastSnapshotIndex *uint64                      `json:"lastSnapshotIndex,omitempty"`
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
