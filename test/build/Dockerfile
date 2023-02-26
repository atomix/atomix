# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

FROM atomix/cli AS build

RUN mkdir /build
WORKDIR /build

COPY go.mod go.sum ./

RUN go mod download

COPY ./cmd ./cmd
COPY ./internal ./internal

RUN atomix build ./cmd/atomix-test -o ./bin/atomix-test

FROM alpine:3.15

RUN apk add libc6-compat bash

RUN addgroup -S atomix && adduser -S -G atomix atomix

USER atomix

COPY --from=build /build/bin/atomix-test /usr/local/bin/atomix-test

ENTRYPOINT ["atomix-test"]
