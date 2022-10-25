// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	electionv1 "github.com/atomix/runtime/api/atomix/runtime/election/v1"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"github.com/atomix/runtime/sdk/pkg/protocol"
	"github.com/atomix/runtime/sdk/pkg/protocol/client"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"github.com/atomix/runtime/sdk/pkg/stringer"
	"google.golang.org/grpc"
	"io"
)

func NewLeaderElectionProxy(protocol *client.Protocol, spec runtime.PrimitiveSpec) (electionv1.LeaderElectionServer, error) {
	return &electionProxy{
		Protocol:      protocol,
		PrimitiveSpec: spec,
	}, nil
}

type electionProxy struct {
	*client.Protocol
	runtime.PrimitiveSpec
}

func (s *electionProxy) Create(ctx context.Context, request *electionv1.CreateRequest) (*electionv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Create",
			logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	if err := session.CreatePrimitive(ctx, s.PrimitiveSpec); err != nil {
		log.Warnw("Create",
			logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &electionv1.CreateResponse{}
	log.Debugw("Create",
		logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("CreateResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *electionProxy) Close(ctx context.Context, request *electionv1.CloseRequest) (*electionv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Close",
			logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	if err := session.ClosePrimitive(ctx, request.ID.Name); err != nil {
		log.Warnw("Close",
			logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &electionv1.CloseResponse{}
	log.Debugw("Close",
		logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("CloseResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *electionProxy) Enter(ctx context.Context, request *electionv1.EnterRequest) (*electionv1.EnterResponse, error) {
	log.Debugw("Enter",
		logging.Stringer("EnterRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Enter",
			logging.Stringer("EnterRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Enter",
			logging.Stringer("EnterRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	command := client.Proposal[*EnterResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*EnterResponse, error) {
		return NewLeaderElectionClient(conn).Enter(ctx, &EnterRequest{
			Headers: headers,
			EnterInput: &EnterInput{
				Candidate: request.Candidate,
			},
		})
	})
	if !ok {
		log.Warnw("Put",
			logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Put",
			logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &electionv1.EnterResponse{
		Term: electionv1.Term{
			Term:       uint64(output.Term.Index),
			Leader:     output.Term.Leader,
			Candidates: output.Term.Candidates,
		},
	}
	log.Debugw("Enter",
		logging.Stringer("EnterRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("EnterResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *electionProxy) Withdraw(ctx context.Context, request *electionv1.WithdrawRequest) (*electionv1.WithdrawResponse, error) {
	log.Debugw("Withdraw",
		logging.Stringer("WithdrawRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Withdraw",
			logging.Stringer("WithdrawRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Withdraw",
			logging.Stringer("WithdrawRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	command := client.Proposal[*WithdrawResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*WithdrawResponse, error) {
		return NewLeaderElectionClient(conn).Withdraw(ctx, &WithdrawRequest{
			Headers: headers,
			WithdrawInput: &WithdrawInput{
				Candidate: request.Candidate,
			},
		})
	})
	if !ok {
		log.Warnw("Put",
			logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Put",
			logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &electionv1.WithdrawResponse{
		Term: electionv1.Term{
			Term:       uint64(output.Term.Index),
			Leader:     output.Term.Leader,
			Candidates: output.Term.Candidates,
		},
	}
	log.Debugw("Withdraw",
		logging.Stringer("WithdrawRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("WithdrawResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *electionProxy) Anoint(ctx context.Context, request *electionv1.AnointRequest) (*electionv1.AnointResponse, error) {
	log.Debugw("Anoint",
		logging.Stringer("AnointRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Anoint",
			logging.Stringer("AnointRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Anoint",
			logging.Stringer("AnointRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	command := client.Proposal[*AnointResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*AnointResponse, error) {
		return NewLeaderElectionClient(conn).Anoint(ctx, &AnointRequest{
			Headers: headers,
			AnointInput: &AnointInput{
				Candidate: request.Candidate,
			},
		})
	})
	if !ok {
		log.Warnw("Put",
			logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Put",
			logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &electionv1.AnointResponse{
		Term: electionv1.Term{
			Term:       uint64(output.Term.Index),
			Leader:     output.Term.Leader,
			Candidates: output.Term.Candidates,
		},
	}
	log.Debugw("Anoint",
		logging.Stringer("AnointRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("AnointResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *electionProxy) Promote(ctx context.Context, request *electionv1.PromoteRequest) (*electionv1.PromoteResponse, error) {
	log.Debugw("Promote",
		logging.Stringer("PromoteRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Promote",
			logging.Stringer("PromoteRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Promote",
			logging.Stringer("PromoteRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	query := client.Proposal[*PromoteResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*PromoteResponse, error) {
		return NewLeaderElectionClient(conn).Promote(ctx, &PromoteRequest{
			Headers: headers,
			PromoteInput: &PromoteInput{
				Candidate: request.Candidate,
			},
		})
	})
	if !ok {
		log.Warnw("Put",
			logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Put",
			logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &electionv1.PromoteResponse{
		Term: electionv1.Term{
			Term:       uint64(output.Term.Index),
			Leader:     output.Term.Leader,
			Candidates: output.Term.Candidates,
		},
	}
	log.Debugw("Promote",
		logging.Stringer("PromoteRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("PromoteResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *electionProxy) Demote(ctx context.Context, request *electionv1.DemoteRequest) (*electionv1.DemoteResponse, error) {
	log.Debugw("Demote",
		logging.Stringer("DemoteRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Demote",
			logging.Stringer("DemoteRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Demote",
			logging.Stringer("DemoteRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	query := client.Proposal[*DemoteResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*DemoteResponse, error) {
		return NewLeaderElectionClient(conn).Demote(ctx, &DemoteRequest{
			Headers: headers,
			DemoteInput: &DemoteInput{
				Candidate: request.Candidate,
			},
		})
	})
	if !ok {
		log.Warnw("Put",
			logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Put",
			logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &electionv1.DemoteResponse{
		Term: electionv1.Term{
			Term:       uint64(output.Term.Index),
			Leader:     output.Term.Leader,
			Candidates: output.Term.Candidates,
		},
	}
	log.Debugw("Demote",
		logging.Stringer("DemoteRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("DemoteResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *electionProxy) Evict(ctx context.Context, request *electionv1.EvictRequest) (*electionv1.EvictResponse, error) {
	log.Debugw("Evict",
		logging.Stringer("EvictRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Evict",
			logging.Stringer("EvictRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Evict",
			logging.Stringer("EvictRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	command := client.Proposal[*EvictResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*EvictResponse, error) {
		return NewLeaderElectionClient(conn).Evict(ctx, &EvictRequest{
			Headers: headers,
			EvictInput: &EvictInput{
				Candidate: request.Candidate,
			},
		})
	})
	if !ok {
		log.Warnw("Put",
			logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Put",
			logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &electionv1.EvictResponse{
		Term: electionv1.Term{
			Term:       uint64(output.Term.Index),
			Leader:     output.Term.Leader,
			Candidates: output.Term.Candidates,
		},
	}
	log.Debugw("Evict",
		logging.Stringer("EvictRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("EvictResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *electionProxy) GetTerm(ctx context.Context, request *electionv1.GetTermRequest) (*electionv1.GetTermResponse, error) {
	log.Debugw("GetTerm",
		logging.Stringer("GetTermRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("GetTerm",
			logging.Stringer("GetTermRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("GetTerm",
			logging.Stringer("GetTermRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	query := client.Query[*GetTermResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*GetTermResponse, error) {
		return NewLeaderElectionClient(conn).GetTerm(ctx, &GetTermRequest{
			Headers:      headers,
			GetTermInput: &GetTermInput{},
		})
	})
	if !ok {
		log.Warnw("Put",
			logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Put",
			logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &electionv1.GetTermResponse{
		Term: electionv1.Term{
			Term:       uint64(output.Term.Index),
			Leader:     output.Term.Leader,
			Candidates: output.Term.Candidates,
		},
	}
	log.Debugw("GetTerm",
		logging.Stringer("GetTermRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("GetTermResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *electionProxy) Watch(request *electionv1.WatchRequest, server electionv1.LeaderElection_WatchServer) error {
	log.Debugw("Watch",
		logging.Stringer("WatchRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(server.Context())
	if err != nil {
		log.Warnw("Watch",
			logging.Stringer("WatchRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Watch",
			logging.Stringer("WatchRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return errors.ToProto(err)
	}
	query := client.StreamQuery[*WatchResponse](primitive)
	stream, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (client.QueryStream[*WatchResponse], error) {
		return NewLeaderElectionClient(conn).Watch(server.Context(), &WatchRequest{
			Headers:    headers,
			WatchInput: &WatchInput{},
		})
	})
	if err != nil {
		log.Warnw("Watch",
			logging.Stringer("WatchRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return errors.ToProto(err)
	}
	for {
		output, ok, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if !ok {
			log.Warnw("Put",
				logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return errors.ToProto(err)
		} else if err != nil {
			log.Debugw("Put",
				logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return errors.ToProto(err)
		}
		response := &electionv1.WatchResponse{
			Term: electionv1.Term{
				Term:       uint64(output.Term.Index),
				Leader:     output.Term.Leader,
				Candidates: output.Term.Candidates,
			},
		}
		log.Debugw("Watch",
			logging.Stringer("WatchRequest", stringer.Truncate(request, truncLen)),
			logging.Stringer("WatchResponse", stringer.Truncate(response, truncLen)))
		if err := server.Send(response); err != nil {
			log.Warnw("Watch",
				logging.Stringer("WatchRequest", stringer.Truncate(request, truncLen)),
				logging.Stringer("WatchResponse", stringer.Truncate(response, truncLen)),
				logging.Error("Error", err))
			return err
		}
	}
}

var _ electionv1.LeaderElectionServer = (*electionProxy)(nil)
