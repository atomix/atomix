# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

FROM goreleaser/goreleaser-cross:v1.19 AS build

RUN mkdir /build
WORKDIR /build

COPY go.mod go.sum ./

RUN go mod download -x

COPY ./cmd ./cmd
COPY ./internal ./internal

RUN go build -mod=readonly -trimpath -o /build/dist/bin/atomix ./cmd/atomix

FROM goreleaser/goreleaser-cross:v1.19

COPY --from=build /build/dist/bin/atomix /usr/local/bin/atomix

ENV CC=gcc
ENV CXX=g++
ENV ATOMIX_CC_ENABLED=true

ENTRYPOINT ["atomix"]
