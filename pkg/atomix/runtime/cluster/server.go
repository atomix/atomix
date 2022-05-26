// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/logging"
)

func NewServer(clusters Manager) runtimev1.ClusterServiceServer {
	return &clusterServiceServer{
		clusters: clusters,
	}
}

type clusterServiceServer struct {
	clusters Manager
}

func (s *clusterServiceServer) GetCluster(ctx context.Context, request *runtimev1.GetClusterRequest) (*runtimev1.GetClusterResponse, error) {
	log.Debugw("GetCluster",
		logging.Stringer("GetClusterRequest", request))

	cluster, ok := s.clusters.Store().Get(&request.ClusterID)
	if !ok {
		err := errors.NewNotFound("cluster '%s' not found", request.ClusterID)
		log.Warnw("GetCluster",
			logging.Stringer("GetClusterRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	response := &runtimev1.GetClusterResponse{
		Cluster: cluster,
	}
	log.Debugw("GetCluster",
		logging.Stringer("GetClusterResponse", response))
	return response, nil
}

func (s *clusterServiceServer) ListClusters(ctx context.Context, request *runtimev1.ListClustersRequest) (*runtimev1.ListClustersResponse, error) {
	log.Debugw("ListClusters",
		logging.Stringer("ListClustersRequest", request))

	clusters := s.clusters.Store().List()
	response := &runtimev1.ListClustersResponse{
		Clusters: clusters,
	}
	log.Debugw("ListClusters",
		logging.Stringer("ListClustersResponse", response))
	return response, nil
}

func (s *clusterServiceServer) CreateCluster(ctx context.Context, request *runtimev1.CreateClusterRequest) (*runtimev1.CreateClusterResponse, error) {
	log.Debugw("CreateCluster",
		logging.Stringer("CreateClusterRequest", request))

	cluster := request.Cluster
	err := s.clusters.Store().Create(cluster)
	if err != nil {
		log.Warnw("CreateCluster",
			logging.Stringer("CreateClusterRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	response := &runtimev1.CreateClusterResponse{
		Cluster: cluster,
	}
	log.Debugw("CreateCluster",
		logging.Stringer("CreateClusterResponse", response))
	return response, nil
}

func (s *clusterServiceServer) UpdateCluster(ctx context.Context, request *runtimev1.UpdateClusterRequest) (*runtimev1.UpdateClusterResponse, error) {
	log.Debugw("UpdateCluster",
		logging.Stringer("UpdateClusterRequest", request))

	cluster := request.Cluster
	err := s.clusters.Store().Update(cluster)
	if err != nil {
		log.Warnw("UpdateCluster",
			logging.Stringer("UpdateClusterRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	response := &runtimev1.UpdateClusterResponse{
		Cluster: cluster,
	}
	log.Debugw("UpdateCluster",
		logging.Stringer("UpdateClusterResponse", response))
	return response, nil
}

func (s *clusterServiceServer) DeleteCluster(ctx context.Context, request *runtimev1.DeleteClusterRequest) (*runtimev1.DeleteClusterResponse, error) {
	log.Debugw("DeleteCluster",
		logging.Stringer("DeleteClusterRequest", request))

	cluster := request.Cluster
	err := s.clusters.Store().Delete(cluster)
	if err != nil {
		log.Warnw("DeleteCluster",
			logging.Stringer("DeleteClusterRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	response := &runtimev1.DeleteClusterResponse{}
	log.Debugw("DeleteCluster",
		logging.Stringer("DeleteClusterResponse", response))
	return response, nil
}

var _ runtimev1.ClusterServiceServer = (*clusterServiceServer)(nil)
