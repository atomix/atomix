// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	"context"
	"fmt"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"google.golang.org/grpc/metadata"
	"os"
)

const (
	ApplicationIDHeader = "Application-ID"
	PrimitiveIDHeader   = "Primitive-ID"
	SessionIDHeader     = "Session-ID"
)

const (
	applicationIDEnv = "APPLICATION_ID"
	nodeIDEnv        = "NODE_ID"
)

func newID(application, primitive, session string) ID {
	return ID{
		Application: application,
		Primitive:   primitive,
		Session:     session,
	}
}

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
		id.Application = getDefaultApplicationID()
	} else {
		id.Application = appIDs[0]
	}

	sessionIDs := md.Get(SessionIDHeader)
	if len(sessionIDs) == 0 {
		id.Session = getDefaultSessionID()
	} else {
		id.Session = sessionIDs[0]
	}
	return id, nil
}

func getDefaultApplicationID() string {
	return os.Getenv(applicationIDEnv)
}

func getDefaultSessionID() string {
	return os.Getenv(nodeIDEnv)
}
