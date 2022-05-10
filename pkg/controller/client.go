// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	runtimev1 "github.com/atomix/api/pkg/atomix/runtime/v1"
	"github.com/atomix/sdk/pkg/errors"
	"github.com/atomix/sdk/pkg/grpc/retry"
	"github.com/atomix/sdk/pkg/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
)

var log = logging.GetLogger()

func NewClient(opts ...Option) (*Client, error) {
	var options Options
	options.apply(opts...)
	controller := &Client{
		Options: options,
	}
	if err := controller.connect(); err != nil {
		return nil, err
	}
	return controller, nil
}

type Client struct {
	Options
	conn *grpc.ClientConn
}

func (c *Client) connect() error {
	conn, err := grpc.Dial(
		fmt.Sprintf("%s:%d", c.Host, c.Port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(retry.RetryingUnaryClientInterceptor()),
		grpc.WithStreamInterceptor(retry.RetryingStreamClientInterceptor()))
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *Client) Connect(ctx context.Context, cluster string, ch chan<- *runtimev1.ConnectionInfo) error {
	client := runtimev1.NewControllerClient(c.conn)
	request := &runtimev1.ConnectRequest{
		Cluster: cluster,
	}
	stream, err := client.Connect(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}
	go func() {
		defer close(ch)
		for {
			response, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					log.Error(err)
				}
				return
			}
			ch <- response.Connection
		}
	}()
	return nil
}

func (c *Client) GetAtom(ctx context.Context, name string, version string, writer io.Writer) error {
	client := runtimev1.NewControllerClient(c.conn)
	request := &runtimev1.GetAtomRequest{
		AtomInfo: runtimev1.AtomInfo{
			Name:    name,
			Version: version,
		},
	}
	stream, err := client.GetAtom(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}

	sha := sha256.New()
	for {
		response, err := stream.Recv()
		if err != nil {
			return err
		}

		switch r := response.Plugin.(type) {
		case *runtimev1.GetAtomResponse_Chunk:
			_, err := writer.Write(r.Chunk.Data)
			if err != nil {
				return err
			}

			_, err = sha.Write(r.Chunk.Data)
			if err != nil {
				return err
			}
		case *runtimev1.GetAtomResponse_Trailer:
			hash := hex.EncodeToString(sha.Sum(nil))
			if hash != r.Trailer.Checksum {
				return errors.NewFault("checksum for Atom %s/%s did not match", name, version)
			}
			return nil
		}
	}
}

func (c *Client) GetAtoms(ctx context.Context) ([]runtimev1.AtomInfo, error) {
	client := runtimev1.NewControllerClient(c.conn)
	request := &runtimev1.ListAtomsRequest{}
	response, err := client.ListAtoms(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return response.Atoms, nil
}

func (c *Client) GetDriver(ctx context.Context, name string, version string, writer io.Writer) error {
	client := runtimev1.NewControllerClient(c.conn)
	request := &runtimev1.GetDriverRequest{
		DriverInfo: runtimev1.DriverInfo{
			Name:    name,
			Version: version,
		},
	}
	stream, err := client.GetDriver(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}

	sha := sha256.New()
	for {
		response, err := stream.Recv()
		if err != nil {
			return err
		}

		switch r := response.Plugin.(type) {
		case *runtimev1.GetDriverResponse_Chunk:
			_, err := writer.Write(r.Chunk.Data)
			if err != nil {
				return err
			}

			_, err = sha.Write(r.Chunk.Data)
			if err != nil {
				return err
			}
		case *runtimev1.GetDriverResponse_Trailer:
			hash := hex.EncodeToString(sha.Sum(nil))
			if hash != r.Trailer.Checksum {
				return errors.NewFault("checksum for Driver %s/%s did not match", name, version)
			}
			return nil
		}
	}
}

func (c *Client) GetDrivers(ctx context.Context) ([]runtimev1.DriverInfo, error) {
	client := runtimev1.NewControllerClient(c.conn)
	request := &runtimev1.ListDriversRequest{}
	response, err := client.ListDrivers(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return response.Drivers, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}
