// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	electionv1 "github.com/atomix/atomix/api/runtime/election/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	electionprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/election/v1"
	protocol "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/client"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"google.golang.org/grpc"
	"io"
)

var log = logging.GetLogger()

func NewLeaderElection(protocol *client.Protocol) electionv1.LeaderElectionServer {
	return &electionClient{
		Protocol: protocol,
	}
}

type electionClient struct {
	*client.Protocol
}

func (s *electionClient) Create(ctx context.Context, request *electionv1.CreateRequest) (*electionv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Stringer("CreateRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Create",
			logging.Stringer("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	if err := session.CreatePrimitive(ctx, runtimev1.PrimitiveMeta{
		Type:        electionv1.PrimitiveType,
		PrimitiveID: request.ID,
		Tags:        request.Tags,
	}); err != nil {
		log.Warnw("Create",
			logging.Stringer("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionv1.CreateResponse{}
	log.Debugw("Create",
		logging.Stringer("CreateRequest", request),
		logging.Stringer("CreateResponse", response))
	return response, nil
}

func (s *electionClient) Close(ctx context.Context, request *electionv1.CloseRequest) (*electionv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Stringer("CloseRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Close",
			logging.Stringer("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	if err := session.ClosePrimitive(ctx, request.ID.Name); err != nil {
		log.Warnw("Close",
			logging.Stringer("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionv1.CloseResponse{}
	log.Debugw("Close",
		logging.Stringer("CloseRequest", request),
		logging.Stringer("CloseResponse", response))
	return response, nil
}

func (s *electionClient) Enter(ctx context.Context, request *electionv1.EnterRequest) (*electionv1.EnterResponse, error) {
	log.Debugw("Enter",
		logging.Stringer("EnterRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Enter",
			logging.Stringer("EnterRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Enter",
			logging.Stringer("EnterRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	command := client.Proposal[*electionprotocolv1.EnterResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*electionprotocolv1.EnterResponse, error) {
		return electionprotocolv1.NewLeaderElectionClient(conn).Enter(ctx, &electionprotocolv1.EnterRequest{
			Headers: headers,
			EnterInput: &electionprotocolv1.EnterInput{
				Candidate: request.Candidate,
			},
		})
	})
	if !ok {
		log.Warnw("Put",
			logging.Stringer("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Put",
			logging.Stringer("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionv1.EnterResponse{
		Term: electionv1.Term{
			Term:       uint64(output.Term.Index),
			Leader:     output.Term.Leader,
			Candidates: output.Term.Candidates,
		},
	}
	log.Debugw("Enter",
		logging.Stringer("EnterRequest", request),
		logging.Stringer("EnterResponse", response))
	return response, nil
}

func (s *electionClient) Withdraw(ctx context.Context, request *electionv1.WithdrawRequest) (*electionv1.WithdrawResponse, error) {
	log.Debugw("Withdraw",
		logging.Stringer("WithdrawRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Withdraw",
			logging.Stringer("WithdrawRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Withdraw",
			logging.Stringer("WithdrawRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	command := client.Proposal[*electionprotocolv1.WithdrawResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*electionprotocolv1.WithdrawResponse, error) {
		return electionprotocolv1.NewLeaderElectionClient(conn).Withdraw(ctx, &electionprotocolv1.WithdrawRequest{
			Headers: headers,
			WithdrawInput: &electionprotocolv1.WithdrawInput{
				Candidate: request.Candidate,
			},
		})
	})
	if !ok {
		log.Warnw("Put",
			logging.Stringer("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Put",
			logging.Stringer("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionv1.WithdrawResponse{
		Term: electionv1.Term{
			Term:       uint64(output.Term.Index),
			Leader:     output.Term.Leader,
			Candidates: output.Term.Candidates,
		},
	}
	log.Debugw("Withdraw",
		logging.Stringer("WithdrawRequest", request),
		logging.Stringer("WithdrawResponse", response))
	return response, nil
}

func (s *electionClient) Anoint(ctx context.Context, request *electionv1.AnointRequest) (*electionv1.AnointResponse, error) {
	log.Debugw("Anoint",
		logging.Stringer("AnointRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Anoint",
			logging.Stringer("AnointRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Anoint",
			logging.Stringer("AnointRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	command := client.Proposal[*electionprotocolv1.AnointResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*electionprotocolv1.AnointResponse, error) {
		return electionprotocolv1.NewLeaderElectionClient(conn).Anoint(ctx, &electionprotocolv1.AnointRequest{
			Headers: headers,
			AnointInput: &electionprotocolv1.AnointInput{
				Candidate: request.Candidate,
			},
		})
	})
	if !ok {
		log.Warnw("Put",
			logging.Stringer("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Put",
			logging.Stringer("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionv1.AnointResponse{
		Term: electionv1.Term{
			Term:       uint64(output.Term.Index),
			Leader:     output.Term.Leader,
			Candidates: output.Term.Candidates,
		},
	}
	log.Debugw("Anoint",
		logging.Stringer("AnointRequest", request),
		logging.Stringer("AnointResponse", response))
	return response, nil
}

func (s *electionClient) Promote(ctx context.Context, request *electionv1.PromoteRequest) (*electionv1.PromoteResponse, error) {
	log.Debugw("Promote",
		logging.Stringer("PromoteRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Promote",
			logging.Stringer("PromoteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Promote",
			logging.Stringer("PromoteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	query := client.Proposal[*electionprotocolv1.PromoteResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*electionprotocolv1.PromoteResponse, error) {
		return electionprotocolv1.NewLeaderElectionClient(conn).Promote(ctx, &electionprotocolv1.PromoteRequest{
			Headers: headers,
			PromoteInput: &electionprotocolv1.PromoteInput{
				Candidate: request.Candidate,
			},
		})
	})
	if !ok {
		log.Warnw("Put",
			logging.Stringer("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Put",
			logging.Stringer("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionv1.PromoteResponse{
		Term: electionv1.Term{
			Term:       uint64(output.Term.Index),
			Leader:     output.Term.Leader,
			Candidates: output.Term.Candidates,
		},
	}
	log.Debugw("Promote",
		logging.Stringer("PromoteRequest", request),
		logging.Stringer("PromoteResponse", response))
	return response, nil
}

func (s *electionClient) Demote(ctx context.Context, request *electionv1.DemoteRequest) (*electionv1.DemoteResponse, error) {
	log.Debugw("Demote",
		logging.Stringer("DemoteRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Demote",
			logging.Stringer("DemoteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Demote",
			logging.Stringer("DemoteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	query := client.Proposal[*electionprotocolv1.DemoteResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*electionprotocolv1.DemoteResponse, error) {
		return electionprotocolv1.NewLeaderElectionClient(conn).Demote(ctx, &electionprotocolv1.DemoteRequest{
			Headers: headers,
			DemoteInput: &electionprotocolv1.DemoteInput{
				Candidate: request.Candidate,
			},
		})
	})
	if !ok {
		log.Warnw("Put",
			logging.Stringer("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Put",
			logging.Stringer("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionv1.DemoteResponse{
		Term: electionv1.Term{
			Term:       uint64(output.Term.Index),
			Leader:     output.Term.Leader,
			Candidates: output.Term.Candidates,
		},
	}
	log.Debugw("Demote",
		logging.Stringer("DemoteRequest", request),
		logging.Stringer("DemoteResponse", response))
	return response, nil
}

func (s *electionClient) Evict(ctx context.Context, request *electionv1.EvictRequest) (*electionv1.EvictResponse, error) {
	log.Debugw("Evict",
		logging.Stringer("EvictRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Evict",
			logging.Stringer("EvictRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Evict",
			logging.Stringer("EvictRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	command := client.Proposal[*electionprotocolv1.EvictResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*electionprotocolv1.EvictResponse, error) {
		return electionprotocolv1.NewLeaderElectionClient(conn).Evict(ctx, &electionprotocolv1.EvictRequest{
			Headers: headers,
			EvictInput: &electionprotocolv1.EvictInput{
				Candidate: request.Candidate,
			},
		})
	})
	if !ok {
		log.Warnw("Put",
			logging.Stringer("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Put",
			logging.Stringer("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionv1.EvictResponse{
		Term: electionv1.Term{
			Term:       uint64(output.Term.Index),
			Leader:     output.Term.Leader,
			Candidates: output.Term.Candidates,
		},
	}
	log.Debugw("Evict",
		logging.Stringer("EvictRequest", request),
		logging.Stringer("EvictResponse", response))
	return response, nil
}

func (s *electionClient) GetTerm(ctx context.Context, request *electionv1.GetTermRequest) (*electionv1.GetTermResponse, error) {
	log.Debugw("GetTerm",
		logging.Stringer("GetTermRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("GetTerm",
			logging.Stringer("GetTermRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("GetTerm",
			logging.Stringer("GetTermRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	query := client.Query[*electionprotocolv1.GetTermResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*electionprotocolv1.GetTermResponse, error) {
		return electionprotocolv1.NewLeaderElectionClient(conn).GetTerm(ctx, &electionprotocolv1.GetTermRequest{
			Headers:      headers,
			GetTermInput: &electionprotocolv1.GetTermInput{},
		})
	})
	if !ok {
		log.Warnw("Put",
			logging.Stringer("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Put",
			logging.Stringer("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionv1.GetTermResponse{
		Term: electionv1.Term{
			Term:       uint64(output.Term.Index),
			Leader:     output.Term.Leader,
			Candidates: output.Term.Candidates,
		},
	}
	log.Debugw("GetTerm",
		logging.Stringer("GetTermRequest", request),
		logging.Stringer("GetTermResponse", response))
	return response, nil
}

func (s *electionClient) Watch(request *electionv1.WatchRequest, server electionv1.LeaderElection_WatchServer) error {
	log.Debugw("Watch",
		logging.Stringer("WatchRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(server.Context())
	if err != nil {
		log.Warnw("Watch",
			logging.Stringer("WatchRequest", request),
			logging.Error("Error", err))
		return err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Watch",
			logging.Stringer("WatchRequest", request),
			logging.Error("Error", err))
		return err
	}
	query := client.StreamQuery[*electionprotocolv1.WatchResponse](primitive)
	stream, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (client.QueryStream[*electionprotocolv1.WatchResponse], error) {
		return electionprotocolv1.NewLeaderElectionClient(conn).Watch(server.Context(), &electionprotocolv1.WatchRequest{
			Headers:    headers,
			WatchInput: &electionprotocolv1.WatchInput{},
		})
	})
	if err != nil {
		log.Warnw("Watch",
			logging.Stringer("WatchRequest", request),
			logging.Error("Error", err))
		return err
	}
	for {
		output, ok, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if !ok {
			log.Warnw("Put",
				logging.Stringer("PutRequest", request),
				logging.Error("Error", err))
			return err
		} else if err != nil {
			log.Debugw("Put",
				logging.Stringer("PutRequest", request),
				logging.Error("Error", err))
			return err
		}
		response := &electionv1.WatchResponse{
			Term: electionv1.Term{
				Term:       uint64(output.Term.Index),
				Leader:     output.Term.Leader,
				Candidates: output.Term.Candidates,
			},
		}
		log.Debugw("Watch",
			logging.Stringer("WatchRequest", request),
			logging.Stringer("WatchResponse", response))
		if err := server.Send(response); err != nil {
			log.Warnw("Watch",
				logging.Stringer("WatchRequest", request),
				logging.Stringer("WatchResponse", response),
				logging.Error("Error", err))
			return err
		}
	}
}

var _ electionv1.LeaderElectionServer = (*electionClient)(nil)
