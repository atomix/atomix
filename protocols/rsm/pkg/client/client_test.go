// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"encoding/binary"
	"encoding/json"
	protocol "github.com/atomix/atomix/protocols/rsm/pkg/api/v1"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/network"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"io"
	"testing"
	"time"
)

func TestPrimitiveCreateClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	partitionServer := protocol.NewMockPartitionServer(ctrl)
	sessionServer := protocol.NewMockSessionServer(ctrl)
	testServer := protocol.NewMockTestServer(ctrl)

	network := network.NewLocalDriver()
	lis, err := network.Listen("localhost:5678")

	server := grpc.NewServer()
	protocol.RegisterPartitionServer(server, partitionServer)
	protocol.RegisterSessionServer(server, sessionServer)
	protocol.RegisterTestServer(server, testServer)
	go func() {
		assert.NoError(t, server.Serve(lis))
	}()

	client := NewClient(network)
	err = client.Connect(context.TODO(), protocol.ProtocolConfig{
		Partitions: []protocol.PartitionConfig{
			{
				PartitionID: 1,
				Leader:      "localhost:5678",
			},
		},
	})
	assert.NoError(t, err)

	partition := client.Partitions()[0]
	partitionServer.EXPECT().OpenSession(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.OpenSessionRequest) (*protocol.OpenSessionResponse, error) {
			return &protocol.OpenSessionResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 1,
				},
				OpenSessionOutput: &protocol.OpenSessionOutput{
					SessionID: 1,
				},
			}, nil
		})
	session, err := partition.GetSession(context.TODO())
	assert.NoError(t, err)

	sessionServer.EXPECT().CreatePrimitive(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.CreatePrimitiveRequest) (*protocol.CreatePrimitiveResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.Headers.SessionID)
			assert.Equal(t, protocol.SequenceNum(1), request.Headers.SequenceNum)
			return &protocol.CreatePrimitiveResponse{
				Headers: &protocol.ProposalResponseHeaders{
					CallResponseHeaders: protocol.CallResponseHeaders{
						PrimitiveResponseHeaders: protocol.PrimitiveResponseHeaders{
							SessionResponseHeaders: protocol.SessionResponseHeaders{
								PartitionResponseHeaders: protocol.PartitionResponseHeaders{
									Index: 2,
								},
							},
						},
						Status: protocol.CallResponseHeaders_OK,
					},
					OutputSequenceNum: 1,
				},
				CreatePrimitiveOutput: &protocol.CreatePrimitiveOutput{
					PrimitiveID: 2,
				},
			}, nil
		})
	err = session.CreatePrimitive(context.TODO(), protocol.PrimitiveSpec{Name: "name", Service: "service"})
	assert.NoError(t, err)

	primitive, err := session.GetPrimitive("name")
	assert.NoError(t, err)

	command := Proposal[*protocol.TestProposalResponse](primitive)
	testServer.EXPECT().TestPropose(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.TestProposalRequest) (*protocol.TestProposalResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.Headers.SessionID)
			assert.Equal(t, protocol.SequenceNum(2), request.Headers.SequenceNum)
			assert.Equal(t, protocol.PrimitiveID(2), request.Headers.PrimitiveID)
			return &protocol.TestProposalResponse{
				Headers: &protocol.ProposalResponseHeaders{
					CallResponseHeaders: protocol.CallResponseHeaders{
						PrimitiveResponseHeaders: protocol.PrimitiveResponseHeaders{
							SessionResponseHeaders: protocol.SessionResponseHeaders{
								PartitionResponseHeaders: protocol.PartitionResponseHeaders{
									Index: 3,
								},
							},
						},
						Status: protocol.CallResponseHeaders_OK,
					},
					OutputSequenceNum: 1,
				},
			}, nil
		})
	commandResponse, _, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*protocol.TestProposalResponse, error) {
		return protocol.NewTestClient(conn).TestPropose(context.TODO(), &protocol.TestProposalRequest{
			Headers: headers,
		})
	})
	assert.NoError(t, err)
	assert.NotNil(t, commandResponse)

	query := Query[*protocol.TestQueryResponse](primitive)
	testServer.EXPECT().TestQuery(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.TestQueryRequest) (*protocol.TestQueryResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.Headers.SessionID)
			assert.Equal(t, protocol.SequenceNum(3), request.Headers.SequenceNum)
			assert.Equal(t, protocol.PrimitiveID(2), request.Headers.PrimitiveID)
			return &protocol.TestQueryResponse{
				Headers: &protocol.QueryResponseHeaders{
					CallResponseHeaders: protocol.CallResponseHeaders{
						PrimitiveResponseHeaders: protocol.PrimitiveResponseHeaders{
							SessionResponseHeaders: protocol.SessionResponseHeaders{
								PartitionResponseHeaders: protocol.PartitionResponseHeaders{
									Index: 3,
								},
							},
						},
						Status: protocol.CallResponseHeaders_OK,
					},
				},
			}, nil
		})
	queryResponse, _, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*protocol.TestQueryResponse, error) {
		return protocol.NewTestClient(conn).TestQuery(context.TODO(), &protocol.TestQueryRequest{
			Headers: headers,
		})
	})
	assert.NoError(t, err)
	assert.NotNil(t, queryResponse)

	sessionServer.EXPECT().ClosePrimitive(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.ClosePrimitiveRequest) (*protocol.ClosePrimitiveResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.Headers.SessionID)
			assert.Equal(t, protocol.PrimitiveID(2), request.PrimitiveID)
			assert.Equal(t, protocol.SequenceNum(4), request.Headers.SequenceNum)
			return &protocol.ClosePrimitiveResponse{
				Headers: &protocol.ProposalResponseHeaders{
					CallResponseHeaders: protocol.CallResponseHeaders{
						PrimitiveResponseHeaders: protocol.PrimitiveResponseHeaders{
							SessionResponseHeaders: protocol.SessionResponseHeaders{
								PartitionResponseHeaders: protocol.PartitionResponseHeaders{
									Index: 4,
								},
							},
						},
						Status: protocol.CallResponseHeaders_OK,
					},
					OutputSequenceNum: 1,
				},
			}, nil
		})
	err = session.ClosePrimitive(context.TODO(), "name")
	assert.NoError(t, err)

	partitionServer.EXPECT().CloseSession(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.CloseSessionRequest) (*protocol.CloseSessionResponse, error) {
			return &protocol.CloseSessionResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 5,
				},
			}, nil
		})
	err = client.Close(context.TODO())
	assert.NoError(t, err)
}

func TestUnaryProposal(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	partitionServer := protocol.NewMockPartitionServer(ctrl)
	sessionServer := protocol.NewMockSessionServer(ctrl)
	testServer := protocol.NewMockTestServer(ctrl)

	network := network.NewLocalDriver()
	lis, err := network.Listen("localhost:5678")

	server := grpc.NewServer()
	protocol.RegisterPartitionServer(server, partitionServer)
	protocol.RegisterSessionServer(server, sessionServer)
	protocol.RegisterTestServer(server, testServer)
	go func() {
		assert.NoError(t, server.Serve(lis))
	}()

	client := NewClient(network)
	timeout := 10 * time.Second
	err = client.Connect(context.TODO(), protocol.ProtocolConfig{
		Partitions: []protocol.PartitionConfig{
			{
				PartitionID: 1,
				Leader:      "localhost:5678",
			},
		},
		SessionTimeout: &timeout,
	})
	assert.NoError(t, err)

	partition := client.Partitions()[0]
	partitionServer.EXPECT().OpenSession(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.OpenSessionRequest) (*protocol.OpenSessionResponse, error) {
			return &protocol.OpenSessionResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 1,
				},
				OpenSessionOutput: &protocol.OpenSessionOutput{
					SessionID: 1,
				},
			}, nil
		})
	session, err := partition.GetSession(context.TODO())
	assert.NoError(t, err)

	sessionServer.EXPECT().CreatePrimitive(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.CreatePrimitiveRequest) (*protocol.CreatePrimitiveResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.Headers.SessionID)
			assert.Equal(t, protocol.SequenceNum(1), request.Headers.SequenceNum)
			return &protocol.CreatePrimitiveResponse{
				Headers: &protocol.ProposalResponseHeaders{
					CallResponseHeaders: protocol.CallResponseHeaders{
						PrimitiveResponseHeaders: protocol.PrimitiveResponseHeaders{
							SessionResponseHeaders: protocol.SessionResponseHeaders{
								PartitionResponseHeaders: protocol.PartitionResponseHeaders{
									Index: 2,
								},
							},
						},
						Status: protocol.CallResponseHeaders_OK,
					},
					OutputSequenceNum: 1,
				},
				CreatePrimitiveOutput: &protocol.CreatePrimitiveOutput{
					PrimitiveID: 2,
				},
			}, nil
		})
	err = session.CreatePrimitive(context.TODO(), protocol.PrimitiveSpec{Name: "name", Service: "service"})
	assert.NoError(t, err)

	primitive, err := session.GetPrimitive("name")
	assert.NoError(t, err)

	command := Proposal[*protocol.TestProposalResponse](primitive)
	testServer.EXPECT().TestPropose(gomock.Any(), gomock.Any()).
		Return(nil, errors.ToProto(errors.NewUnavailable("unavailable")))
	testServer.EXPECT().TestPropose(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.TestProposalRequest) (*protocol.TestProposalResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.Headers.SessionID)
			assert.Equal(t, protocol.SequenceNum(2), request.Headers.SequenceNum)
			assert.Equal(t, protocol.PrimitiveID(2), request.Headers.PrimitiveID)
			return &protocol.TestProposalResponse{
				Headers: &protocol.ProposalResponseHeaders{
					CallResponseHeaders: protocol.CallResponseHeaders{
						PrimitiveResponseHeaders: protocol.PrimitiveResponseHeaders{
							SessionResponseHeaders: protocol.SessionResponseHeaders{
								PartitionResponseHeaders: protocol.PartitionResponseHeaders{
									Index: 3,
								},
							},
						},
						Status: protocol.CallResponseHeaders_OK,
					},
					OutputSequenceNum: 1,
				},
			}, nil
		})
	commandResponse, _, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*protocol.TestProposalResponse, error) {
		return protocol.NewTestClient(conn).TestPropose(context.TODO(), &protocol.TestProposalRequest{
			Headers: headers,
		})
	})
	assert.NoError(t, err)
	assert.NotNil(t, commandResponse)

	ch := make(chan struct{})
	partitionServer.EXPECT().KeepAlive(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.KeepAliveRequest) (*protocol.KeepAliveResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.SessionID)
			assert.Equal(t, protocol.SequenceNum(2), request.LastInputSequenceNum)
			inputFilter := &bloom.BloomFilter{}
			assert.NoError(t, json.Unmarshal(request.InputFilter, inputFilter))
			sequenceNumBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 1)
			assert.False(t, inputFilter.Test(sequenceNumBytes))
			sequenceNumBytes = make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 2)
			assert.False(t, inputFilter.Test(sequenceNumBytes))
			close(ch)
			return &protocol.KeepAliveResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 4,
				},
			}, nil
		})
	select {
	case <-ch:
	case <-time.After(time.Minute):
		t.FailNow()
	}

	partitionServer.EXPECT().CloseSession(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.CloseSessionRequest) (*protocol.CloseSessionResponse, error) {
			return &protocol.CloseSessionResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 5,
				},
			}, nil
		})
	err = client.Close(context.TODO())
	assert.NoError(t, err)
}

func TestStreamPropose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	partitionServer := protocol.NewMockPartitionServer(ctrl)
	sessionServer := protocol.NewMockSessionServer(ctrl)
	testServer := protocol.NewMockTestServer(ctrl)

	network := network.NewLocalDriver()
	lis, err := network.Listen("localhost:5678")

	server := grpc.NewServer()
	protocol.RegisterPartitionServer(server, partitionServer)
	protocol.RegisterSessionServer(server, sessionServer)
	protocol.RegisterTestServer(server, testServer)
	go func() {
		assert.NoError(t, server.Serve(lis))
	}()

	client := NewClient(network)
	timeout := 10 * time.Second
	err = client.Connect(context.TODO(), protocol.ProtocolConfig{
		Partitions: []protocol.PartitionConfig{
			{
				PartitionID: 1,
				Leader:      "localhost:5678",
			},
		},
		SessionTimeout: &timeout,
	})
	assert.NoError(t, err)

	partition := client.Partitions()[0]
	partitionServer.EXPECT().OpenSession(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.OpenSessionRequest) (*protocol.OpenSessionResponse, error) {
			return &protocol.OpenSessionResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 1,
				},
				OpenSessionOutput: &protocol.OpenSessionOutput{
					SessionID: 1,
				},
			}, nil
		})
	session, err := partition.GetSession(context.TODO())
	assert.NoError(t, err)

	sessionServer.EXPECT().CreatePrimitive(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.CreatePrimitiveRequest) (*protocol.CreatePrimitiveResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.Headers.SessionID)
			assert.Equal(t, protocol.SequenceNum(1), request.Headers.SequenceNum)
			return &protocol.CreatePrimitiveResponse{
				Headers: &protocol.ProposalResponseHeaders{
					CallResponseHeaders: protocol.CallResponseHeaders{
						PrimitiveResponseHeaders: protocol.PrimitiveResponseHeaders{
							SessionResponseHeaders: protocol.SessionResponseHeaders{
								PartitionResponseHeaders: protocol.PartitionResponseHeaders{
									Index: 2,
								},
							},
						},
						Status: protocol.CallResponseHeaders_OK,
					},
					OutputSequenceNum: 1,
				},
				CreatePrimitiveOutput: &protocol.CreatePrimitiveOutput{
					PrimitiveID: 2,
				},
			}, nil
		})
	err = session.CreatePrimitive(context.TODO(), protocol.PrimitiveSpec{Name: "name", Service: "service"})
	assert.NoError(t, err)

	primitive, err := session.GetPrimitive("name")
	assert.NoError(t, err)

	command := StreamProposal[*protocol.TestProposalResponse](primitive)
	sendResponseCh := make(chan struct{})
	testServer.EXPECT().TestStreamPropose(gomock.Any(), gomock.Any()).
		Return(errors.ToProto(errors.NewUnavailable("unavailable")))
	testServer.EXPECT().TestStreamPropose(gomock.Any(), gomock.Any()).
		DoAndReturn(func(request *protocol.TestProposalRequest, stream protocol.Test_TestStreamProposeServer) error {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.Headers.SessionID)
			assert.Equal(t, protocol.SequenceNum(2), request.Headers.SequenceNum)
			assert.Equal(t, protocol.PrimitiveID(2), request.Headers.PrimitiveID)
			var outputSequenceNum protocol.SequenceNum
			for range sendResponseCh {
				outputSequenceNum++
				assert.Nil(t, stream.Send(&protocol.TestProposalResponse{
					Headers: &protocol.ProposalResponseHeaders{
						CallResponseHeaders: protocol.CallResponseHeaders{
							PrimitiveResponseHeaders: protocol.PrimitiveResponseHeaders{
								SessionResponseHeaders: protocol.SessionResponseHeaders{
									PartitionResponseHeaders: protocol.PartitionResponseHeaders{
										Index: 3,
									},
								},
							},
							Status: protocol.CallResponseHeaders_OK,
						},
						OutputSequenceNum: outputSequenceNum,
					},
				}))
			}
			return nil
		})
	stream, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (ProposalStream[*protocol.TestProposalResponse], error) {
		return protocol.NewTestClient(conn).TestStreamPropose(context.TODO(), &protocol.TestProposalRequest{
			Headers: headers,
		})
	})
	assert.NoError(t, err)

	go func() {
		sendResponseCh <- struct{}{}
	}()
	response, _, err := stream.Recv()
	assert.NoError(t, err)
	assert.NotNil(t, response)
	go func() {
		sendResponseCh <- struct{}{}
	}()
	response, _, err = stream.Recv()
	assert.NoError(t, err)
	assert.NotNil(t, response)

	keepAliveDone := make(chan struct{})
	partitionServer.EXPECT().KeepAlive(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.KeepAliveRequest) (*protocol.KeepAliveResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.SessionID)
			assert.Equal(t, protocol.SequenceNum(2), request.LastInputSequenceNum)
			inputFilter := &bloom.BloomFilter{}
			assert.NoError(t, json.Unmarshal(request.InputFilter, inputFilter))
			sequenceNumBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 1)
			assert.False(t, inputFilter.Test(sequenceNumBytes))
			sequenceNumBytes = make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 2)
			assert.True(t, inputFilter.Test(sequenceNumBytes))
			assert.Equal(t, protocol.SequenceNum(2), request.LastOutputSequenceNums[2])
			close(keepAliveDone)
			return &protocol.KeepAliveResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 4,
				},
			}, nil
		})
	select {
	case <-keepAliveDone:
	case <-time.After(time.Minute):
		t.FailNow()
	}

	go func() {
		sendResponseCh <- struct{}{}
	}()
	response, _, err = stream.Recv()
	assert.NoError(t, err)
	assert.NotNil(t, response)

	close(sendResponseCh)

	keepAliveDone = make(chan struct{})
	partitionServer.EXPECT().KeepAlive(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.KeepAliveRequest) (*protocol.KeepAliveResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.SessionID)
			assert.Equal(t, protocol.SequenceNum(2), request.LastInputSequenceNum)
			inputFilter := &bloom.BloomFilter{}
			assert.NoError(t, json.Unmarshal(request.InputFilter, inputFilter))
			sequenceNumBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 1)
			assert.False(t, inputFilter.Test(sequenceNumBytes))
			sequenceNumBytes = make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 2)
			assert.True(t, inputFilter.Test(sequenceNumBytes))
			assert.Equal(t, protocol.SequenceNum(3), request.LastOutputSequenceNums[2])
			close(keepAliveDone)
			return &protocol.KeepAliveResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 5,
				},
			}, nil
		})
	select {
	case <-keepAliveDone:
	case <-time.After(time.Minute):
		t.FailNow()
	}

	response, _, err = stream.Recv()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, response)

	keepAliveDone = make(chan struct{})
	partitionServer.EXPECT().KeepAlive(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.KeepAliveRequest) (*protocol.KeepAliveResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.SessionID)
			assert.Equal(t, protocol.SequenceNum(2), request.LastInputSequenceNum)
			inputFilter := &bloom.BloomFilter{}
			assert.NoError(t, json.Unmarshal(request.InputFilter, inputFilter))
			sequenceNumBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 1)
			assert.False(t, inputFilter.Test(sequenceNumBytes))
			sequenceNumBytes = make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 2)
			assert.False(t, inputFilter.Test(sequenceNumBytes))
			_, ok := request.LastOutputSequenceNums[2]
			assert.False(t, ok)
			close(keepAliveDone)
			return &protocol.KeepAliveResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 6,
				},
			}, nil
		})
	select {
	case <-keepAliveDone:
	case <-time.After(time.Minute):
		t.FailNow()
	}

	partitionServer.EXPECT().CloseSession(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.CloseSessionRequest) (*protocol.CloseSessionResponse, error) {
			return &protocol.CloseSessionResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 7,
				},
			}, nil
		})
	err = client.Close(context.TODO())
	assert.NoError(t, err)
}

func TestStreamProposeCancel(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	partitionServer := protocol.NewMockPartitionServer(ctrl)
	sessionServer := protocol.NewMockSessionServer(ctrl)
	testServer := protocol.NewMockTestServer(ctrl)

	network := network.NewLocalDriver()
	lis, err := network.Listen("localhost:5678")

	server := grpc.NewServer()
	protocol.RegisterPartitionServer(server, partitionServer)
	protocol.RegisterSessionServer(server, sessionServer)
	protocol.RegisterTestServer(server, testServer)
	go func() {
		assert.NoError(t, server.Serve(lis))
	}()

	client := NewClient(network)
	timeout := 10 * time.Second
	err = client.Connect(context.TODO(), protocol.ProtocolConfig{
		Partitions: []protocol.PartitionConfig{
			{
				PartitionID: 1,
				Leader:      "localhost:5678",
			},
		},
		SessionTimeout: &timeout,
	})
	assert.NoError(t, err)

	partition := client.Partitions()[0]
	partitionServer.EXPECT().OpenSession(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.OpenSessionRequest) (*protocol.OpenSessionResponse, error) {
			return &protocol.OpenSessionResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 1,
				},
				OpenSessionOutput: &protocol.OpenSessionOutput{
					SessionID: 1,
				},
			}, nil
		})
	session, err := partition.GetSession(context.TODO())
	assert.NoError(t, err)

	sessionServer.EXPECT().CreatePrimitive(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.CreatePrimitiveRequest) (*protocol.CreatePrimitiveResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.Headers.SessionID)
			assert.Equal(t, protocol.SequenceNum(1), request.Headers.SequenceNum)
			return &protocol.CreatePrimitiveResponse{
				Headers: &protocol.ProposalResponseHeaders{
					CallResponseHeaders: protocol.CallResponseHeaders{
						PrimitiveResponseHeaders: protocol.PrimitiveResponseHeaders{
							SessionResponseHeaders: protocol.SessionResponseHeaders{
								PartitionResponseHeaders: protocol.PartitionResponseHeaders{
									Index: 2,
								},
							},
						},
						Status: protocol.CallResponseHeaders_OK,
					},
					OutputSequenceNum: 1,
				},
				CreatePrimitiveOutput: &protocol.CreatePrimitiveOutput{
					PrimitiveID: 2,
				},
			}, nil
		})
	err = session.CreatePrimitive(context.TODO(), protocol.PrimitiveSpec{Name: "name", Service: "service"})
	assert.NoError(t, err)

	primitive, err := session.GetPrimitive("name")
	assert.NoError(t, err)

	command := StreamProposal[*protocol.TestProposalResponse](primitive)
	testServer.EXPECT().TestStreamPropose(gomock.Any(), gomock.Any()).
		Return(errors.ToProto(errors.NewUnavailable("unavailable")))
	testServer.EXPECT().TestStreamPropose(gomock.Any(), gomock.Any()).
		DoAndReturn(func(request *protocol.TestProposalRequest, stream protocol.Test_TestStreamProposeServer) error {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.Headers.SessionID)
			assert.Equal(t, protocol.SequenceNum(2), request.Headers.SequenceNum)
			assert.Equal(t, protocol.PrimitiveID(2), request.Headers.PrimitiveID)
			assert.Nil(t, stream.Send(&protocol.TestProposalResponse{
				Headers: &protocol.ProposalResponseHeaders{
					CallResponseHeaders: protocol.CallResponseHeaders{
						PrimitiveResponseHeaders: protocol.PrimitiveResponseHeaders{
							SessionResponseHeaders: protocol.SessionResponseHeaders{
								PartitionResponseHeaders: protocol.PartitionResponseHeaders{
									Index: 3,
								},
							},
						},
						Status: protocol.CallResponseHeaders_OK,
					},
					OutputSequenceNum: 1,
				},
			}))
			assert.Nil(t, stream.Send(&protocol.TestProposalResponse{
				Headers: &protocol.ProposalResponseHeaders{
					CallResponseHeaders: protocol.CallResponseHeaders{
						PrimitiveResponseHeaders: protocol.PrimitiveResponseHeaders{
							SessionResponseHeaders: protocol.SessionResponseHeaders{
								PartitionResponseHeaders: protocol.PartitionResponseHeaders{
									Index: 3,
								},
							},
						},
						Status: protocol.CallResponseHeaders_OK,
					},
					OutputSequenceNum: 2,
				},
			}))
			<-stream.Context().Done()
			return stream.Context().Err()
		})

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (ProposalStream[*protocol.TestProposalResponse], error) {
		return protocol.NewTestClient(conn).TestStreamPropose(ctx, &protocol.TestProposalRequest{
			Headers: headers,
		})
	})
	assert.NoError(t, err)

	response, _, err := stream.Recv()
	assert.NoError(t, err)
	assert.NotNil(t, response)
	response, _, err = stream.Recv()
	assert.NoError(t, err)
	assert.NotNil(t, response)

	keepAliveDone := make(chan struct{})
	partitionServer.EXPECT().KeepAlive(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.KeepAliveRequest) (*protocol.KeepAliveResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.SessionID)
			assert.Equal(t, protocol.SequenceNum(2), request.LastInputSequenceNum)
			inputFilter := &bloom.BloomFilter{}
			assert.NoError(t, json.Unmarshal(request.InputFilter, inputFilter))
			sequenceNumBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 1)
			assert.False(t, inputFilter.Test(sequenceNumBytes))
			sequenceNumBytes = make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 2)
			assert.True(t, inputFilter.Test(sequenceNumBytes))
			assert.Equal(t, protocol.SequenceNum(2), request.LastOutputSequenceNums[2])
			close(keepAliveDone)
			return &protocol.KeepAliveResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 4,
				},
			}, nil
		})
	select {
	case <-keepAliveDone:
	case <-time.After(time.Minute):
		t.FailNow()
	}

	cancel()

	keepAliveDone = make(chan struct{})
	partitionServer.EXPECT().KeepAlive(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.KeepAliveRequest) (*protocol.KeepAliveResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.SessionID)
			assert.Equal(t, protocol.SequenceNum(2), request.LastInputSequenceNum)
			inputFilter := &bloom.BloomFilter{}
			assert.NoError(t, json.Unmarshal(request.InputFilter, inputFilter))
			sequenceNumBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 1)
			assert.False(t, inputFilter.Test(sequenceNumBytes))
			sequenceNumBytes = make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 2)
			assert.True(t, inputFilter.Test(sequenceNumBytes))
			close(keepAliveDone)
			return &protocol.KeepAliveResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 5,
				},
			}, nil
		})
	select {
	case <-keepAliveDone:
	case <-time.After(time.Minute):
		t.FailNow()
	}

	_, _, err = stream.Recv()
	assert.Error(t, err)

	keepAliveDone = make(chan struct{})
	partitionServer.EXPECT().KeepAlive(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.KeepAliveRequest) (*protocol.KeepAliveResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.SessionID)
			assert.Equal(t, protocol.SequenceNum(2), request.LastInputSequenceNum)
			inputFilter := &bloom.BloomFilter{}
			assert.NoError(t, json.Unmarshal(request.InputFilter, inputFilter))
			sequenceNumBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 1)
			assert.False(t, inputFilter.Test(sequenceNumBytes))
			sequenceNumBytes = make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 2)
			assert.False(t, inputFilter.Test(sequenceNumBytes))
			_, ok := request.LastOutputSequenceNums[2]
			assert.False(t, ok)
			close(keepAliveDone)
			return &protocol.KeepAliveResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 5,
				},
			}, nil
		})
	select {
	case <-keepAliveDone:
	case <-time.After(time.Minute):
		t.FailNow()
	}

	partitionServer.EXPECT().CloseSession(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.CloseSessionRequest) (*protocol.CloseSessionResponse, error) {
			return &protocol.CloseSessionResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 6,
				},
			}, nil
		})
	err = client.Close(context.TODO())
	assert.NoError(t, err)
}

func TestUnaryQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	partitionServer := protocol.NewMockPartitionServer(ctrl)
	sessionServer := protocol.NewMockSessionServer(ctrl)
	testServer := protocol.NewMockTestServer(ctrl)

	network := network.NewLocalDriver()
	lis, err := network.Listen("localhost:5678")

	server := grpc.NewServer()
	protocol.RegisterPartitionServer(server, partitionServer)
	protocol.RegisterSessionServer(server, sessionServer)
	protocol.RegisterTestServer(server, testServer)
	go func() {
		assert.NoError(t, server.Serve(lis))
	}()

	client := NewClient(network)
	timeout := 10 * time.Second
	err = client.Connect(context.TODO(), protocol.ProtocolConfig{
		Partitions: []protocol.PartitionConfig{
			{
				PartitionID: 1,
				Leader:      "localhost:5678",
			},
		},
		SessionTimeout: &timeout,
	})
	assert.NoError(t, err)

	partition := client.Partitions()[0]
	partitionServer.EXPECT().OpenSession(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.OpenSessionRequest) (*protocol.OpenSessionResponse, error) {
			return &protocol.OpenSessionResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 1,
				},
				OpenSessionOutput: &protocol.OpenSessionOutput{
					SessionID: 1,
				},
			}, nil
		})
	session, err := partition.GetSession(context.TODO())
	assert.NoError(t, err)

	sessionServer.EXPECT().CreatePrimitive(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.CreatePrimitiveRequest) (*protocol.CreatePrimitiveResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.Headers.SessionID)
			assert.Equal(t, protocol.SequenceNum(1), request.Headers.SequenceNum)
			return &protocol.CreatePrimitiveResponse{
				Headers: &protocol.ProposalResponseHeaders{
					CallResponseHeaders: protocol.CallResponseHeaders{
						PrimitiveResponseHeaders: protocol.PrimitiveResponseHeaders{
							SessionResponseHeaders: protocol.SessionResponseHeaders{
								PartitionResponseHeaders: protocol.PartitionResponseHeaders{
									Index: 2,
								},
							},
						},
						Status: protocol.CallResponseHeaders_OK,
					},
					OutputSequenceNum: 1,
				},
				CreatePrimitiveOutput: &protocol.CreatePrimitiveOutput{
					PrimitiveID: 2,
				},
			}, nil
		})
	err = session.CreatePrimitive(context.TODO(), protocol.PrimitiveSpec{Name: "name", Service: "service"})
	assert.NoError(t, err)

	primitive, err := session.GetPrimitive("name")
	assert.NoError(t, err)

	query := Query[*protocol.TestQueryResponse](primitive)
	testServer.EXPECT().TestQuery(gomock.Any(), gomock.Any()).
		Return(nil, errors.ToProto(errors.NewUnavailable("unavailable")))
	testServer.EXPECT().TestQuery(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.TestQueryRequest) (*protocol.TestQueryResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.Headers.SessionID)
			assert.Equal(t, protocol.SequenceNum(2), request.Headers.SequenceNum)
			assert.Equal(t, protocol.PrimitiveID(2), request.Headers.PrimitiveID)
			return &protocol.TestQueryResponse{
				Headers: &protocol.QueryResponseHeaders{
					CallResponseHeaders: protocol.CallResponseHeaders{
						PrimitiveResponseHeaders: protocol.PrimitiveResponseHeaders{
							SessionResponseHeaders: protocol.SessionResponseHeaders{
								PartitionResponseHeaders: protocol.PartitionResponseHeaders{
									Index: 3,
								},
							},
						},
						Status: protocol.CallResponseHeaders_OK,
					},
				},
			}, nil
		})
	response, _, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*protocol.TestQueryResponse, error) {
		return protocol.NewTestClient(conn).TestQuery(context.TODO(), &protocol.TestQueryRequest{
			Headers: headers,
		})
	})
	assert.NoError(t, err)
	assert.NotNil(t, response)

	ch := make(chan struct{})
	partitionServer.EXPECT().KeepAlive(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.KeepAliveRequest) (*protocol.KeepAliveResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.SessionID)
			assert.Equal(t, protocol.SequenceNum(2), request.LastInputSequenceNum)
			inputFilter := &bloom.BloomFilter{}
			assert.NoError(t, json.Unmarshal(request.InputFilter, inputFilter))
			sequenceNumBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 1)
			assert.False(t, inputFilter.Test(sequenceNumBytes))
			sequenceNumBytes = make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 2)
			assert.False(t, inputFilter.Test(sequenceNumBytes))
			close(ch)
			return &protocol.KeepAliveResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 4,
				},
			}, nil
		})
	select {
	case <-ch:
	case <-time.After(time.Minute):
		t.FailNow()
	}

	partitionServer.EXPECT().CloseSession(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.CloseSessionRequest) (*protocol.CloseSessionResponse, error) {
			return &protocol.CloseSessionResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 5,
				},
			}, nil
		})
	err = client.Close(context.TODO())
	assert.NoError(t, err)
}

func TestStreamQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	partitionServer := protocol.NewMockPartitionServer(ctrl)
	sessionServer := protocol.NewMockSessionServer(ctrl)
	testServer := protocol.NewMockTestServer(ctrl)

	network := network.NewLocalDriver()
	lis, err := network.Listen("localhost:5678")

	server := grpc.NewServer()
	protocol.RegisterPartitionServer(server, partitionServer)
	protocol.RegisterSessionServer(server, sessionServer)
	protocol.RegisterTestServer(server, testServer)
	go func() {
		assert.NoError(t, server.Serve(lis))
	}()

	client := NewClient(network)
	timeout := 10 * time.Second
	err = client.Connect(context.TODO(), protocol.ProtocolConfig{
		Partitions: []protocol.PartitionConfig{
			{
				PartitionID: 1,
				Leader:      "localhost:5678",
			},
		},
		SessionTimeout: &timeout,
	})
	assert.NoError(t, err)

	partition := client.Partitions()[0]
	partitionServer.EXPECT().OpenSession(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.OpenSessionRequest) (*protocol.OpenSessionResponse, error) {
			return &protocol.OpenSessionResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 1,
				},
				OpenSessionOutput: &protocol.OpenSessionOutput{
					SessionID: 1,
				},
			}, nil
		})
	session, err := partition.GetSession(context.TODO())
	assert.NoError(t, err)

	sessionServer.EXPECT().CreatePrimitive(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.CreatePrimitiveRequest) (*protocol.CreatePrimitiveResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.Headers.SessionID)
			assert.Equal(t, protocol.SequenceNum(1), request.Headers.SequenceNum)
			return &protocol.CreatePrimitiveResponse{
				Headers: &protocol.ProposalResponseHeaders{
					CallResponseHeaders: protocol.CallResponseHeaders{
						PrimitiveResponseHeaders: protocol.PrimitiveResponseHeaders{
							SessionResponseHeaders: protocol.SessionResponseHeaders{
								PartitionResponseHeaders: protocol.PartitionResponseHeaders{
									Index: 2,
								},
							},
						},
						Status: protocol.CallResponseHeaders_OK,
					},
					OutputSequenceNum: 1,
				},
				CreatePrimitiveOutput: &protocol.CreatePrimitiveOutput{
					PrimitiveID: 2,
				},
			}, nil
		})
	err = session.CreatePrimitive(context.TODO(), protocol.PrimitiveSpec{Name: "name", Service: "service"})
	assert.NoError(t, err)

	primitive, err := session.GetPrimitive("name")
	assert.NoError(t, err)

	query := StreamQuery[*protocol.TestQueryResponse](primitive)
	sendResponseCh := make(chan struct{})
	testServer.EXPECT().TestStreamQuery(gomock.Any(), gomock.Any()).
		Return(errors.ToProto(errors.NewUnavailable("unavailable")))
	testServer.EXPECT().TestStreamQuery(gomock.Any(), gomock.Any()).
		DoAndReturn(func(request *protocol.TestQueryRequest, stream protocol.Test_TestStreamQueryServer) error {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.Headers.SessionID)
			assert.Equal(t, protocol.SequenceNum(2), request.Headers.SequenceNum)
			assert.Equal(t, protocol.PrimitiveID(2), request.Headers.PrimitiveID)
			for range sendResponseCh {
				assert.Nil(t, stream.Send(&protocol.TestQueryResponse{
					Headers: &protocol.QueryResponseHeaders{
						CallResponseHeaders: protocol.CallResponseHeaders{
							PrimitiveResponseHeaders: protocol.PrimitiveResponseHeaders{
								SessionResponseHeaders: protocol.SessionResponseHeaders{
									PartitionResponseHeaders: protocol.PartitionResponseHeaders{
										Index: 3,
									},
								},
							},
							Status: protocol.CallResponseHeaders_OK,
						},
					},
				}))
			}
			return nil
		})
	stream, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (QueryStream[*protocol.TestQueryResponse], error) {
		return protocol.NewTestClient(conn).TestStreamQuery(context.TODO(), &protocol.TestQueryRequest{
			Headers: headers,
		})
	})
	assert.NoError(t, err)

	go func() {
		sendResponseCh <- struct{}{}
	}()
	response, _, err := stream.Recv()
	assert.NoError(t, err)
	assert.NotNil(t, response)
	go func() {
		sendResponseCh <- struct{}{}
	}()
	response, _, err = stream.Recv()
	assert.NoError(t, err)
	assert.NotNil(t, response)

	keepAliveDone := make(chan struct{})
	partitionServer.EXPECT().KeepAlive(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.KeepAliveRequest) (*protocol.KeepAliveResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.SessionID)
			assert.Equal(t, protocol.SequenceNum(2), request.LastInputSequenceNum)
			inputFilter := &bloom.BloomFilter{}
			assert.NoError(t, json.Unmarshal(request.InputFilter, inputFilter))
			sequenceNumBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 1)
			assert.False(t, inputFilter.Test(sequenceNumBytes))
			sequenceNumBytes = make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 2)
			assert.True(t, inputFilter.Test(sequenceNumBytes))
			close(keepAliveDone)
			return &protocol.KeepAliveResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 4,
				},
			}, nil
		})
	select {
	case <-keepAliveDone:
	case <-time.After(time.Minute):
		t.FailNow()
	}

	go func() {
		sendResponseCh <- struct{}{}
	}()
	response, _, err = stream.Recv()
	assert.NoError(t, err)
	assert.NotNil(t, response)

	close(sendResponseCh)

	keepAliveDone = make(chan struct{})
	partitionServer.EXPECT().KeepAlive(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.KeepAliveRequest) (*protocol.KeepAliveResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.SessionID)
			assert.Equal(t, protocol.SequenceNum(2), request.LastInputSequenceNum)
			inputFilter := &bloom.BloomFilter{}
			assert.NoError(t, json.Unmarshal(request.InputFilter, inputFilter))
			sequenceNumBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 1)
			assert.False(t, inputFilter.Test(sequenceNumBytes))
			sequenceNumBytes = make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 2)
			assert.True(t, inputFilter.Test(sequenceNumBytes))
			close(keepAliveDone)
			return &protocol.KeepAliveResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 5,
				},
			}, nil
		})
	select {
	case <-keepAliveDone:
	case <-time.After(time.Minute):
		t.FailNow()
	}

	response, _, err = stream.Recv()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, response)

	keepAliveDone = make(chan struct{})
	partitionServer.EXPECT().KeepAlive(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.KeepAliveRequest) (*protocol.KeepAliveResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.SessionID)
			assert.Equal(t, protocol.SequenceNum(2), request.LastInputSequenceNum)
			inputFilter := &bloom.BloomFilter{}
			assert.NoError(t, json.Unmarshal(request.InputFilter, inputFilter))
			sequenceNumBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 1)
			assert.False(t, inputFilter.Test(sequenceNumBytes))
			sequenceNumBytes = make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 2)
			assert.False(t, inputFilter.Test(sequenceNumBytes))
			_, ok := request.LastOutputSequenceNums[2]
			assert.False(t, ok)
			close(keepAliveDone)
			return &protocol.KeepAliveResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 6,
				},
			}, nil
		})
	select {
	case <-keepAliveDone:
	case <-time.After(time.Minute):
		t.FailNow()
	}

	partitionServer.EXPECT().CloseSession(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.CloseSessionRequest) (*protocol.CloseSessionResponse, error) {
			return &protocol.CloseSessionResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 7,
				},
			}, nil
		})
	err = client.Close(context.TODO())
	assert.NoError(t, err)
}

func TestStreamQueryCancel(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	partitionServer := protocol.NewMockPartitionServer(ctrl)
	sessionServer := protocol.NewMockSessionServer(ctrl)
	testServer := protocol.NewMockTestServer(ctrl)

	network := network.NewLocalDriver()
	lis, err := network.Listen("localhost:5678")

	server := grpc.NewServer()
	protocol.RegisterPartitionServer(server, partitionServer)
	protocol.RegisterSessionServer(server, sessionServer)
	protocol.RegisterTestServer(server, testServer)
	go func() {
		assert.NoError(t, server.Serve(lis))
	}()

	client := NewClient(network)
	timeout := 10 * time.Second
	err = client.Connect(context.TODO(), protocol.ProtocolConfig{
		Partitions: []protocol.PartitionConfig{
			{
				PartitionID: 1,
				Leader:      "localhost:5678",
			},
		},
		SessionTimeout: &timeout,
	})
	assert.NoError(t, err)

	partition := client.Partitions()[0]
	partitionServer.EXPECT().OpenSession(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.OpenSessionRequest) (*protocol.OpenSessionResponse, error) {
			return &protocol.OpenSessionResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 1,
				},
				OpenSessionOutput: &protocol.OpenSessionOutput{
					SessionID: 1,
				},
			}, nil
		})
	session, err := partition.GetSession(context.TODO())
	assert.NoError(t, err)

	sessionServer.EXPECT().CreatePrimitive(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.CreatePrimitiveRequest) (*protocol.CreatePrimitiveResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.Headers.SessionID)
			assert.Equal(t, protocol.SequenceNum(1), request.Headers.SequenceNum)
			return &protocol.CreatePrimitiveResponse{
				Headers: &protocol.ProposalResponseHeaders{
					CallResponseHeaders: protocol.CallResponseHeaders{
						PrimitiveResponseHeaders: protocol.PrimitiveResponseHeaders{
							SessionResponseHeaders: protocol.SessionResponseHeaders{
								PartitionResponseHeaders: protocol.PartitionResponseHeaders{
									Index: 2,
								},
							},
						},
						Status: protocol.CallResponseHeaders_OK,
					},
					OutputSequenceNum: 1,
				},
				CreatePrimitiveOutput: &protocol.CreatePrimitiveOutput{
					PrimitiveID: 2,
				},
			}, nil
		})
	err = session.CreatePrimitive(context.TODO(), protocol.PrimitiveSpec{Name: "name", Service: "service"})
	assert.NoError(t, err)

	primitive, err := session.GetPrimitive("name")
	assert.NoError(t, err)

	query := StreamQuery[*protocol.TestQueryResponse](primitive)
	testServer.EXPECT().TestStreamQuery(gomock.Any(), gomock.Any()).
		Return(errors.ToProto(errors.NewUnavailable("unavailable")))
	testServer.EXPECT().TestStreamQuery(gomock.Any(), gomock.Any()).
		DoAndReturn(func(request *protocol.TestQueryRequest, stream protocol.Test_TestStreamQueryServer) error {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.Headers.SessionID)
			assert.Equal(t, protocol.SequenceNum(2), request.Headers.SequenceNum)
			assert.Equal(t, protocol.PrimitiveID(2), request.Headers.PrimitiveID)
			assert.Nil(t, stream.Send(&protocol.TestQueryResponse{
				Headers: &protocol.QueryResponseHeaders{
					CallResponseHeaders: protocol.CallResponseHeaders{
						PrimitiveResponseHeaders: protocol.PrimitiveResponseHeaders{
							SessionResponseHeaders: protocol.SessionResponseHeaders{
								PartitionResponseHeaders: protocol.PartitionResponseHeaders{
									Index: 3,
								},
							},
						},
						Status: protocol.CallResponseHeaders_OK,
					},
				},
			}))
			assert.Nil(t, stream.Send(&protocol.TestQueryResponse{
				Headers: &protocol.QueryResponseHeaders{
					CallResponseHeaders: protocol.CallResponseHeaders{
						PrimitiveResponseHeaders: protocol.PrimitiveResponseHeaders{
							SessionResponseHeaders: protocol.SessionResponseHeaders{
								PartitionResponseHeaders: protocol.PartitionResponseHeaders{
									Index: 3,
								},
							},
						},
						Status: protocol.CallResponseHeaders_OK,
					},
				},
			}))
			<-stream.Context().Done()
			return stream.Context().Err()
		})

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (QueryStream[*protocol.TestQueryResponse], error) {
		return protocol.NewTestClient(conn).TestStreamQuery(ctx, &protocol.TestQueryRequest{
			Headers: headers,
		})
	})
	assert.NoError(t, err)

	response, _, err := stream.Recv()
	assert.NoError(t, err)
	assert.NotNil(t, response)
	response, _, err = stream.Recv()
	assert.NoError(t, err)
	assert.NotNil(t, response)

	keepAliveDone := make(chan struct{})
	partitionServer.EXPECT().KeepAlive(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.KeepAliveRequest) (*protocol.KeepAliveResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.SessionID)
			assert.Equal(t, protocol.SequenceNum(2), request.LastInputSequenceNum)
			inputFilter := &bloom.BloomFilter{}
			assert.NoError(t, json.Unmarshal(request.InputFilter, inputFilter))
			sequenceNumBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 1)
			assert.False(t, inputFilter.Test(sequenceNumBytes))
			sequenceNumBytes = make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 2)
			assert.True(t, inputFilter.Test(sequenceNumBytes))
			close(keepAliveDone)
			return &protocol.KeepAliveResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 4,
				},
			}, nil
		})
	select {
	case <-keepAliveDone:
	case <-time.After(time.Minute):
		t.FailNow()
	}

	cancel()

	keepAliveDone = make(chan struct{})
	partitionServer.EXPECT().KeepAlive(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.KeepAliveRequest) (*protocol.KeepAliveResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.SessionID)
			assert.Equal(t, protocol.SequenceNum(2), request.LastInputSequenceNum)
			inputFilter := &bloom.BloomFilter{}
			assert.NoError(t, json.Unmarshal(request.InputFilter, inputFilter))
			sequenceNumBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 1)
			assert.False(t, inputFilter.Test(sequenceNumBytes))
			sequenceNumBytes = make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 2)
			assert.True(t, inputFilter.Test(sequenceNumBytes))
			close(keepAliveDone)
			return &protocol.KeepAliveResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 5,
				},
			}, nil
		})
	select {
	case <-keepAliveDone:
	case <-time.After(time.Minute):
		t.FailNow()
	}

	_, _, err = stream.Recv()
	assert.Error(t, err)

	keepAliveDone = make(chan struct{})
	partitionServer.EXPECT().KeepAlive(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.KeepAliveRequest) (*protocol.KeepAliveResponse, error) {
			assert.Equal(t, protocol.PartitionID(1), request.Headers.PartitionID)
			assert.Equal(t, protocol.SessionID(1), request.SessionID)
			assert.Equal(t, protocol.SequenceNum(2), request.LastInputSequenceNum)
			inputFilter := &bloom.BloomFilter{}
			assert.NoError(t, json.Unmarshal(request.InputFilter, inputFilter))
			sequenceNumBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 1)
			assert.False(t, inputFilter.Test(sequenceNumBytes))
			sequenceNumBytes = make([]byte, 8)
			binary.BigEndian.PutUint64(sequenceNumBytes, 2)
			assert.False(t, inputFilter.Test(sequenceNumBytes))
			close(keepAliveDone)
			return &protocol.KeepAliveResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 5,
				},
			}, nil
		})
	select {
	case <-keepAliveDone:
	case <-time.After(time.Minute):
		t.FailNow()
	}

	partitionServer.EXPECT().CloseSession(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *protocol.CloseSessionRequest) (*protocol.CloseSessionResponse, error) {
			return &protocol.CloseSessionResponse{
				Headers: &protocol.PartitionResponseHeaders{
					Index: 6,
				},
			}, nil
		})
	err = client.Close(context.TODO())
	assert.NoError(t, err)
}
