// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"fmt"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/grpc/retry"
	"github.com/atomix/runtime/sdk/pkg/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
	"time"
)

func newPartition(id protocol.PartitionID, client *ProtocolClient, sessionTimeout time.Duration) *PartitionClient {
	return &PartitionClient{
		client:         client,
		id:             id,
		sessionTimeout: sessionTimeout,
	}
}

type PartitionClient struct {
	client         *ProtocolClient
	id             protocol.PartitionID
	sessionTimeout time.Duration
	state          *PartitionState
	watchers       map[int]chan<- PartitionState
	watcherID      int
	conn           *grpc.ClientConn
	resolver       *partitionResolver
	session        *SessionClient
	mu             sync.RWMutex
}

func (p *PartitionClient) ID() protocol.PartitionID {
	return p.id
}

func (p *PartitionClient) GetSession(ctx context.Context) (*SessionClient, error) {
	p.mu.RLock()
	session := p.session
	p.mu.RUnlock()
	if session != nil {
		return session, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.session != nil {
		return p.session, nil
	}
	if p.conn == nil {
		return nil, errors.NewUnavailable("not connected")
	}

	request := &protocol.OpenSessionRequest{
		Headers: &protocol.PartitionRequestHeaders{
			PartitionID: p.id,
		},
		OpenSessionInput: &protocol.OpenSessionInput{
			Timeout: p.sessionTimeout,
		},
	}

	client := protocol.NewPartitionClient(p.conn)
	response, err := client.OpenSession(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}

	session = newSessionClient(response.SessionID, p, p.conn, p.sessionTimeout)
	p.session = session
	return session, nil
}

func (p *PartitionClient) connect(ctx context.Context, config *protocol.PartitionConfig) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	address := fmt.Sprintf("%s:///%d", resolverName, p.id)
	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingPolicy":"%s"}`, resolverName)),
		grpc.WithResolvers(p.resolver),
		grpc.WithContextDialer(p.client.network.Connect),
		grpc.WithUnaryInterceptor(retry.RetryingUnaryClientInterceptor(retry.WithRetryOn(codes.Unavailable))),
		grpc.WithStreamInterceptor(retry.RetryingStreamClientInterceptor(retry.WithRetryOn(codes.Unavailable))),
	}
	dialOptions = append(dialOptions, p.client.GRPCDialOptions...)

	p.resolver = newResolver(config)
	conn, err := grpc.DialContext(ctx, address, dialOptions...)
	if err != nil {
		return errors.FromProto(err)
	}
	p.conn = conn
	return nil
}

func (p *PartitionClient) configure(config *protocol.PartitionConfig) error {
	return p.resolver.update(config)
}

func (p *PartitionClient) close(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.session != nil {
		return p.session.close(ctx)
	}
	return nil
}

type PartitionState struct {
	Leader    string
	Followers []string
}
