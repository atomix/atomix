// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	"context"
	"fmt"
	"github.com/atomix/runtime/pkg/atomix/env"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"google.golang.org/grpc/metadata"
)

const (
	ApplicationIDHeader = "Application-ID"
	PrimitiveIDHeader   = "Primitive-ID"
	SessionIDHeader     = "Session-ID"
)

type ID struct {
	Application string
	Primitive   string
	Session     string
}

func (i ID) String() string {
	return fmt.Sprintf("%s.%s.%s", i.Application, i.Primitive, i.Session)
}

func getIDFromContext(ctx context.Context) (ID, error) {
	var id ID

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return id, errors.NewInvalid("missing metadata in context")
	}

	primitiveIDs := md.Get(PrimitiveIDHeader)
	if len(primitiveIDs) == 0 {
		return id, errors.NewInvalid("missing %s header in metadata", PrimitiveIDHeader)
	}

	id.Primitive = primitiveIDs[0]

	appIDs := md.Get(ApplicationIDHeader)
	if len(appIDs) == 0 {
		id.Application = env.GetApplicationID()
	} else {
		id.Application = appIDs[0]
	}

	sessionIDs := md.Get(SessionIDHeader)
	if len(sessionIDs) == 0 {
		id.Session = env.GetNodeID()
	} else {
		id.Session = sessionIDs[0]
	}
	return id, nil
}
