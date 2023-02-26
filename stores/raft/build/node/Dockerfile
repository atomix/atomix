# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

FROM atomix/cli AS build

RUN mkdir /build
WORKDIR /build

COPY go.mod go.sum ./

RUN go mod download

COPY ./api ./api
COPY ./cmd ./cmd
COPY ./pkg ./pkg

RUN atomix build ./cmd/atomix-raft-node -o ./bin/atomix-raft-node

FROM alpine:3.15

RUN apk add libc6-compat bash

RUN addgroup -S atomix && adduser -S -G atomix atomix

USER atomix

COPY --from=build /build/dist/bin/atomix-raft-node /usr/local/bin/atomix-raft-node

ENTRYPOINT ["atomix-raft-node"]
