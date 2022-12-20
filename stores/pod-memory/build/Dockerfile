# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

FROM goreleaser/goreleaser-cross:v1.19 AS build

RUN mkdir /build
WORKDIR /build

COPY ./go.mod /build
COPY ./go.sum /build

RUN go mod download -x

COPY ./cmd /build/cmd
COPY ./pkg /build/pkg

RUN go build -mod=readonly -trimpath -o /build/dist/bin/atomix-pod-memory-controller ./cmd/atomix-pod-memory-controller

FROM alpine:3.15

RUN apk add libc6-compat

RUN addgroup -S atomix && adduser -S -G atomix atomix

USER atomix

COPY --from=build /build/dist/bin/atomix-pod-memory-controller /usr/local/bin/atomix-pod-memory-controller

ENTRYPOINT ["atomix-pod-memory-controller"]
