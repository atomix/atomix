# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

FROM atomix/build AS build

RUN mkdir /build
WORKDIR /build

COPY go.mod go.sum ./

RUN go mod download

COPY ./cmd/atomix-raft-controller-test ./cmd/atomix-raft-controller-test
COPY ./pkg ./pkg

RUN atomix build ./cmd/atomix-raft-controller-test -o ./bin/atomix-raft-controller-test

FROM alpine:3.15

RUN apk add libc6-compat

RUN addgroup -S atomix && adduser -S -G atomix atomix

USER atomix

COPY --from=build /build/bin/atomix-raft-controller-test /usr/local/bin/atomix-raft-controller-test

ENTRYPOINT ["atomix-raft-controller-test"]
