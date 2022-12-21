// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta2

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RaftMemberState is a state constant for RaftMember
type RaftMemberState string

const (
	// RaftMemberNotReady indicates a RaftMember is not ready
	RaftMemberNotReady RaftMemberState = "NotReady"
	// RaftMemberReady indicates a RaftMember is ready
	RaftMemberReady RaftMemberState = "Ready"
)

type RaftMemberType string

const (
	RaftVoter    RaftMemberType = "Member"
	RaftWitness  RaftMemberType = "Witness"
	RaftObserver RaftMemberType = "Observer"
)

// RaftMemberRole is a constant for RaftMember representing the current role of the member
type RaftMemberRole string

const (
	// RaftLeader is a RaftMemberRole indicating the RaftMember is currently the leader of the group
	RaftLeader RaftMemberRole = "Leader"
	// RaftCandidate is a RaftMemberRole indicating the RaftMember is currently a candidate
	RaftCandidate RaftMemberRole = "Candidate"
	// RaftFollower is a RaftMemberRole indicating the RaftMember is currently a follower
	RaftFollower RaftMemberRole = "Follower"
)

type MemberID uint64

type ReplicaID uint64

type RaftMemberSpec struct {
	Cluster   corev1.ObjectReference      `json:"cluster"`
	ShardID   ShardID                     `json:"shardID"`
	MemberID  MemberID                    `json:"memberID"`
	ReplicaID ReplicaID                   `json:"replicaID"`
	Pod       corev1.LocalObjectReference `json:"pod"`
	Type      RaftMemberType              `json:"type"`
	Peers     uint32                      `json:"peers"`
	Join      bool                        `json:"join"`
	Config    RaftConfig                  `json:"config"`
}

// RaftMemberStatus defines the status of a RaftMember
type RaftMemberStatus struct {
	PodRef            *corev1.ObjectReference `json:"podRef"`
	Version           *int32                  `json:"version"`
	State             RaftMemberState         `json:"state,omitempty"`
	Role              *RaftMemberRole         `json:"role,omitempty"`
	Leader            *MemberID               `json:"leader,omitempty"`
	Term              *uint64                 `json:"term,omitempty"`
	LastUpdated       *metav1.Time            `json:"lastUpdated,omitempty"`
	LastSnapshotIndex *uint64                 `json:"lastSnapshotIndex,omitempty"`
	LastSnapshotTime  *metav1.Time            `json:"lastSnapshotTime,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RaftMember is the Schema for the RaftMember API
// +k8s:openapi-gen=true
type RaftMember struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              RaftMemberSpec   `json:"spec,omitempty"`
	Status            RaftMemberStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RaftMemberList contains a list of RaftMember
type RaftMemberList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	// Items is the RaftMember of items in the list
	Items []RaftMember `json:"items"`
}
