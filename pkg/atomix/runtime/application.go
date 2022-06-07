// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/runtime/store"
)

func newApplicationServiceServer(applications *store.Store[*runtimev1.ApplicationId, *runtimev1.Application]) runtimev1.ApplicationServiceServer {
	return &applicationServiceServer{
		applications: applications,
	}
}

type applicationServiceServer struct {
	applications *store.Store[*runtimev1.ApplicationId, *runtimev1.Application]
}

func (s *applicationServiceServer) GetApplication(ctx context.Context, request *runtimev1.GetApplicationRequest) (*runtimev1.GetApplicationResponse, error) {
	log.Debugw("GetApplication",
		logging.Stringer("GetApplicationRequest", request))

	application, ok := s.applications.Get(&request.ApplicationID)
	if !ok {
		err := errors.NewNotFound("application '%s' not found", request.ApplicationID)
		log.Warnw("GetApplication",
			logging.Stringer("GetApplicationRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	response := &runtimev1.GetApplicationResponse{
		Application: application,
	}
	log.Debugw("GetApplication",
		logging.Stringer("GetApplicationResponse", response))
	return response, nil
}

func (s *applicationServiceServer) ListApplications(ctx context.Context, request *runtimev1.ListApplicationsRequest) (*runtimev1.ListApplicationsResponse, error) {
	log.Debugw("ListApplications",
		logging.Stringer("ListApplicationsRequest", request))

	applications := s.applications.List()
	response := &runtimev1.ListApplicationsResponse{
		Applications: applications,
	}
	log.Debugw("ListApplications",
		logging.Stringer("ListApplicationsResponse", response))
	return response, nil
}

func (s *applicationServiceServer) CreateApplication(ctx context.Context, request *runtimev1.CreateApplicationRequest) (*runtimev1.CreateApplicationResponse, error) {
	log.Debugw("CreateApplication",
		logging.Stringer("CreateApplicationRequest", request))

	application := request.Application
	err := s.applications.Create(application)
	if err != nil {
		log.Warnw("CreateApplication",
			logging.Stringer("CreateApplicationRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	response := &runtimev1.CreateApplicationResponse{
		Application: application,
	}
	log.Debugw("CreateApplication",
		logging.Stringer("CreateApplicationResponse", response))
	return response, nil
}

func (s *applicationServiceServer) UpdateApplication(ctx context.Context, request *runtimev1.UpdateApplicationRequest) (*runtimev1.UpdateApplicationResponse, error) {
	log.Debugw("UpdateApplication",
		logging.Stringer("UpdateApplicationRequest", request))

	application := request.Application
	err := s.applications.Update(application)
	if err != nil {
		log.Warnw("UpdateApplication",
			logging.Stringer("UpdateApplicationRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	response := &runtimev1.UpdateApplicationResponse{
		Application: application,
	}
	log.Debugw("UpdateApplication",
		logging.Stringer("UpdateApplicationResponse", response))
	return response, nil
}

func (s *applicationServiceServer) DeleteApplication(ctx context.Context, request *runtimev1.DeleteApplicationRequest) (*runtimev1.DeleteApplicationResponse, error) {
	log.Debugw("DeleteApplication",
		logging.Stringer("DeleteApplicationRequest", request))

	application := request.Application
	err := s.applications.Delete(application)
	if err != nil {
		log.Warnw("DeleteApplication",
			logging.Stringer("DeleteApplicationRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	response := &runtimev1.DeleteApplicationResponse{}
	log.Debugw("DeleteApplication",
		logging.Stringer("DeleteApplicationResponse", response))
	return response, nil
}

var _ runtimev1.ApplicationServiceServer = (*applicationServiceServer)(nil)
