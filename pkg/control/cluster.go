// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/runtime"
)

func newClusterServiceServer(clusters runtime.Store[*runtimev1.Cluster]) runtimev1.ClusterServiceServer {
	return &clusterServiceServer{
		clusters: clusters,
	}
}

type clusterServiceServer struct {
	clusters runtime.Store[*runtimev1.Cluster]
}

func (s *clusterServiceServer) GetCluster(ctx context.Context, request *runtimev1.GetClusterRequest) (*runtimev1.GetClusterResponse, error) {
	log.Debugw("GetCluster",
		logging.Stringer("GetClusterRequest", request))
	cluster, err := s.clusters.Get(request.ClusterID)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	response := &runtimev1.GetClusterResponse{
		Cluster: cluster,
	}
	log.Debugw("GetCluster",
		logging.Stringer("GetClusterRequest", response))
	return response, nil
}

func (s *clusterServiceServer) ListClusters(ctx context.Context, request *runtimev1.ListClustersRequest) (*runtimev1.ListClustersResponse, error) {
	log.Debugw("ListClusters",
		logging.Stringer("ListClustersRequest", request))
	clusters, err := s.clusters.List()
	if err != nil {
		return nil, errors.ToProto(err)
	}
	response := &runtimev1.ListClustersResponse{
		Clusters: clusters,
	}
	log.Debugw("ListClusters",
		logging.Stringer("ListClustersRequest", response))
	return response, nil
}

func (s *clusterServiceServer) CreateCluster(ctx context.Context, request *runtimev1.CreateClusterRequest) (*runtimev1.CreateClusterResponse, error) {
	log.Debugw("CreateCluster",
		logging.Stringer("CreateClusterRequest", request))
	cluster := request.Cluster
	err := s.clusters.Create(cluster)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	response := &runtimev1.CreateClusterResponse{
		Cluster: cluster,
	}
	log.Debugw("CreateCluster",
		logging.Stringer("CreateClusterRequest", response))
	return response, nil
}

func (s *clusterServiceServer) UpdateCluster(ctx context.Context, request *runtimev1.UpdateClusterRequest) (*runtimev1.UpdateClusterResponse, error) {
	log.Debugw("UpdateCluster",
		logging.Stringer("UpdateClusterRequest", request))
	cluster := request.Cluster
	err := s.clusters.Update(cluster)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	response := &runtimev1.UpdateClusterResponse{
		Cluster: cluster,
	}
	log.Debugw("UpdateCluster",
		logging.Stringer("UpdateClusterRequest", response))
	return response, nil
}

func (s *clusterServiceServer) DeleteCluster(ctx context.Context, request *runtimev1.DeleteClusterRequest) (*runtimev1.DeleteClusterResponse, error) {
	log.Debugw("DeleteCluster",
		logging.Stringer("DeleteClusterRequest", request))
	cluster := request.Cluster
	err := s.clusters.Delete(cluster)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	response := &runtimev1.DeleteClusterResponse{}
	log.Debugw("DeleteCluster",
		logging.Stringer("DeleteClusterRequest", response))
	return response, nil
}

var _ runtimev1.ClusterServiceServer = (*clusterServiceServer)(nil)
