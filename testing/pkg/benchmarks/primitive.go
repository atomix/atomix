// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package benchmarks

import (
	"context"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/utils/grpc/interceptors"
	petname "github.com/dustinkirkland/golang-petname"
	"github.com/onosproject/helmit/pkg/benchmark"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
)

type PrimitiveBenchmarkSuite struct {
	benchmark.Suite
	conn *grpc.ClientConn
	ID   runtimev1.PrimitiveID
}

func (s *PrimitiveBenchmarkSuite) SetupWorker(ctx context.Context) error {
	conn, err := grpc.DialContext(ctx, "127.0.0.1:5678",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(
			interceptors.ErrorHandlingUnaryClientInterceptor(),
			interceptors.RetryingUnaryClientInterceptor(interceptors.WithRetryOn(codes.Unavailable))),
		grpc.WithChainStreamInterceptor(
			interceptors.ErrorHandlingStreamClientInterceptor(),
			interceptors.RetryingStreamClientInterceptor(interceptors.WithRetryOn(codes.Unavailable))))
	if err != nil {
		return err
	}
	s.conn = conn
	return nil
}

func (s *PrimitiveBenchmarkSuite) TearDownWorker(ctx context.Context) error {
	return s.conn.Close()
}

func (s *PrimitiveBenchmarkSuite) SetupTest(ctx context.Context) error {
	s.ID = runtimev1.PrimitiveID{
		Name: petname.Generate(2, "-"),
	}
	return nil
}
