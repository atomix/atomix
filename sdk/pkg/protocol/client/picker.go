// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

func init() {
	balancer.Register(base.NewBalancerBuilder(resolverName, &PickerBuilder{}, base.Config{}))
}

type PickerBuilder struct{}

func (p *PickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	var leader balancer.SubConn
	var followers []balancer.SubConn
	for sc, scInfo := range info.ReadySCs {
		isLeader := scInfo.Address.Attributes.Value("is_leader").(bool)
		if isLeader {
			leader = sc
			continue
		}
		followers = append(followers, sc)
	}
	log.Debugf("Built new picker. Leader: %s, Followers: %s", leader, followers)
	return &Picker{
		leader:    leader,
		followers: followers,
	}
}

var _ base.PickerBuilder = (*PickerBuilder)(nil)

type Picker struct {
	leader    balancer.SubConn
	followers []balancer.SubConn
	current   uint64
}

func (p *Picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	var result balancer.PickResult
	if p.leader != nil {
		result.SubConn = p.leader
	} else if len(p.followers) > 0 {
		result.SubConn = p.nextFollower()
	}
	if result.SubConn == nil {
		return result, balancer.ErrNoSubConnAvailable
	}
	return result, nil
}

func (p *Picker) nextFollower() balancer.SubConn {
	cur := atomic.AddUint64(&p.current, uint64(1))
	len := uint64(len(p.followers))
	idx := int(cur % len)
	return p.followers[idx]
}

var _ balancer.Picker = (*Picker)(nil)
