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
	runtimeelectionv1 "github.com/atomix/atomix/runtime/pkg/runtime/election/v1"
	"google.golang.org/grpc"
	"io"
)

var log = logging.GetLogger()

func NewLeaderElection(protocol *client.Protocol, id runtimev1.PrimitiveID) runtimeelectionv1.LeaderElectionProxy {
	return &leaderElectionProxy{
		Protocol: protocol,
		id:       id,
	}
}

type leaderElectionProxy struct {
	*client.Protocol
	id runtimev1.PrimitiveID
}

func (s *leaderElectionProxy) Open(ctx context.Context) error {
	log.Debugw("Create",
		logging.String("Name", s.id.Name))
	partition := s.PartitionBy([]byte(s.id.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Create",
			logging.String("Name", s.id.Name),
			logging.Error("Error", err))
		return err
	}
	meta := runtimev1.PrimitiveMeta{
		Type:        electionv1.PrimitiveType,
		PrimitiveID: s.id,
	}
	if err := session.CreatePrimitive(ctx, meta); err != nil {
		log.Warnw("Create",
			logging.String("Name", s.id.Name),
			logging.Error("Error", err))
		return err
	}
	return nil
}

func (s *leaderElectionProxy) Close(ctx context.Context) error {
	log.Debugw("Close",
		logging.String("Name", s.id.Name))
	partition := s.PartitionBy([]byte(s.id.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Create",
			logging.String("Name", s.id.Name),
			logging.Error("Error", err))
		return err
	}
	if err := session.ClosePrimitive(ctx, s.id.Name); err != nil {
		log.Warnw("Close",
			logging.String("Name", s.id.Name),
			logging.Error("Error", err))
		return err
	}
	return nil
}

func (s *leaderElectionProxy) Enter(ctx context.Context, request *electionv1.EnterRequest) (*electionv1.EnterResponse, error) {
	log.Debugw("Enter",
		logging.Trunc128("EnterRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Enter",
			logging.Trunc128("EnterRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Enter",
			logging.Trunc128("EnterRequest", request),
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
			logging.Trunc128("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Put",
			logging.Trunc128("PutRequest", request),
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
		logging.Trunc128("EnterRequest", request),
		logging.Trunc128("EnterResponse", response))
	return response, nil
}

func (s *leaderElectionProxy) Withdraw(ctx context.Context, request *electionv1.WithdrawRequest) (*electionv1.WithdrawResponse, error) {
	log.Debugw("Withdraw",
		logging.Trunc128("WithdrawRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Withdraw",
			logging.Trunc128("WithdrawRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Withdraw",
			logging.Trunc128("WithdrawRequest", request),
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
			logging.Trunc128("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Put",
			logging.Trunc128("PutRequest", request),
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
		logging.Trunc128("WithdrawRequest", request),
		logging.Trunc128("WithdrawResponse", response))
	return response, nil
}

func (s *leaderElectionProxy) Anoint(ctx context.Context, request *electionv1.AnointRequest) (*electionv1.AnointResponse, error) {
	log.Debugw("Anoint",
		logging.Trunc128("AnointRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Anoint",
			logging.Trunc128("AnointRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Anoint",
			logging.Trunc128("AnointRequest", request),
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
			logging.Trunc128("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Put",
			logging.Trunc128("PutRequest", request),
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
		logging.Trunc128("AnointRequest", request),
		logging.Trunc128("AnointResponse", response))
	return response, nil
}

func (s *leaderElectionProxy) Promote(ctx context.Context, request *electionv1.PromoteRequest) (*electionv1.PromoteResponse, error) {
	log.Debugw("Promote",
		logging.Trunc128("PromoteRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Promote",
			logging.Trunc128("PromoteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Promote",
			logging.Trunc128("PromoteRequest", request),
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
			logging.Trunc128("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Put",
			logging.Trunc128("PutRequest", request),
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
		logging.Trunc128("PromoteRequest", request),
		logging.Trunc128("PromoteResponse", response))
	return response, nil
}

func (s *leaderElectionProxy) Demote(ctx context.Context, request *electionv1.DemoteRequest) (*electionv1.DemoteResponse, error) {
	log.Debugw("Demote",
		logging.Trunc128("DemoteRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Demote",
			logging.Trunc128("DemoteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Demote",
			logging.Trunc128("DemoteRequest", request),
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
			logging.Trunc128("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Put",
			logging.Trunc128("PutRequest", request),
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
		logging.Trunc128("DemoteRequest", request),
		logging.Trunc128("DemoteResponse", response))
	return response, nil
}

func (s *leaderElectionProxy) Evict(ctx context.Context, request *electionv1.EvictRequest) (*electionv1.EvictResponse, error) {
	log.Debugw("Evict",
		logging.Trunc128("EvictRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Evict",
			logging.Trunc128("EvictRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Evict",
			logging.Trunc128("EvictRequest", request),
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
			logging.Trunc128("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Put",
			logging.Trunc128("PutRequest", request),
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
		logging.Trunc128("EvictRequest", request),
		logging.Trunc128("EvictResponse", response))
	return response, nil
}

func (s *leaderElectionProxy) GetTerm(ctx context.Context, request *electionv1.GetTermRequest) (*electionv1.GetTermResponse, error) {
	log.Debugw("GetTerm",
		logging.Trunc128("GetTermRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("GetTerm",
			logging.Trunc128("GetTermRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("GetTerm",
			logging.Trunc128("GetTermRequest", request),
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
			logging.Trunc128("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Put",
			logging.Trunc128("PutRequest", request),
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
		logging.Trunc128("GetTermRequest", request),
		logging.Trunc128("GetTermResponse", response))
	return response, nil
}

func (s *leaderElectionProxy) Watch(request *electionv1.WatchRequest, server electionv1.LeaderElection_WatchServer) error {
	log.Debugw("Watch",
		logging.Trunc128("WatchRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(server.Context())
	if err != nil {
		log.Warnw("Watch",
			logging.Trunc128("WatchRequest", request),
			logging.Error("Error", err))
		return err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Watch",
			logging.Trunc128("WatchRequest", request),
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
			logging.Trunc128("WatchRequest", request),
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
				logging.Trunc128("PutRequest", request),
				logging.Error("Error", err))
			return err
		} else if err != nil {
			log.Debugw("Put",
				logging.Trunc128("PutRequest", request),
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
			logging.Trunc128("WatchRequest", request),
			logging.Trunc128("WatchResponse", response))
		if err := server.Send(response); err != nil {
			log.Warnw("Watch",
				logging.Trunc128("WatchRequest", request),
				logging.Trunc128("WatchResponse", response),
				logging.Error("Error", err))
			return err
		}
	}
}

var _ electionv1.LeaderElectionServer = (*leaderElectionProxy)(nil)
