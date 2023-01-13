// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"fmt"
	protocol "github.com/atomix/atomix/protocols/rsm/api/v1"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

const resolverName = "atomix"

func newResolver(config *protocol.PartitionConfig) *partitionResolver {
	return &partitionResolver{
		config: config,
	}
}

type partitionResolver struct {
	clientConn    resolver.ClientConn
	serviceConfig *serviceconfig.ParseResult
	config        *protocol.PartitionConfig
}

func (r *partitionResolver) Scheme() string {
	return resolverName
}

func (r *partitionResolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.clientConn = cc
	r.serviceConfig = cc.ParseServiceConfig(
		fmt.Sprintf(`{"loadBalancingConfig":[{"%s":{}}]}`, resolverName),
	)
	if err := r.update(r.config); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *partitionResolver) update(config *protocol.PartitionConfig) error {
	r.config = config

	if config.Leader == "" && config.Followers == nil {
		return nil
	}

	var addrs []resolver.Address
	if config.Leader != "" {
		addrs = append(addrs, resolver.Address{
			Addr: config.Leader,
			Attributes: attributes.New(
				"is_leader",
				true,
			),
		})
	}

	for _, addr := range config.Followers {
		addrs = append(addrs, resolver.Address{
			Addr: addr,
			Attributes: attributes.New(
				"is_leader",
				false,
			),
		})
	}

	return r.clientConn.UpdateState(resolver.State{
		Addresses:     addrs,
		ServiceConfig: r.serviceConfig,
	})
}

func (r *partitionResolver) ResolveNow(resolver.ResolveNowOptions) {}

func (r *partitionResolver) Close() {}

var _ resolver.Builder = (*partitionResolver)(nil)
var _ resolver.Resolver = (*partitionResolver)(nil)
