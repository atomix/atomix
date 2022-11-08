# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

FROM goreleaser/goreleaser-cross:v1.19 AS builder

RUN mkdir /build
WORKDIR /build

COPY ./go.mod /build
COPY ./go.sum /build
COPY ./cmd /build/cmd
COPY ./pkg /build/pkg

RUN go mod download

# Build the proxy binary
FROM builder AS build-proxy
RUN go run ./cmd/atomix-build proxy /build/bin/atomix-runtime-proxy

# Build the consensus storage driver
FROM builder AS build-consensus-driver
RUN go run ./cmd/atomix-build driver \
    github.com/atomix/consensus-storage/driver@v0.13.1 \
    /build/bin/Consensus@v1beta1.so

# Build the shared memory storage driver
FROM builder AS build-shared-memory-driver
RUN go run ./cmd/atomix-build driver \
    github.com/atomix/shared-memory-storage/driver@v0.1.1 \
    /build/bin/SharedMemory@v1beta1.so

# Build the pod memory storage driver
FROM builder AS build-pod-memory-driver
RUN go run ./cmd/atomix-build driver \
    github.com/atomix/pod-memory-storage/driver@v0.1.1 \
    /build/bin/PodMemory@v1beta1.so

# Pull binaries and plugins into the Alpine image
FROM alpine:3.15

RUN apk add libc6-compat

RUN addgroup -S atomix && adduser -S -G atomix atomix

USER atomix

COPY --from=build-proxy /build/bin/atomix-runtime-proxy /usr/local/bin/atomix-runtime-proxy
COPY --from=build-consensus-driver /build/bin/Consensus@v1beta1.so /var/atomix/plugins/
COPY --from=build-shared-memory-driver /build/bin/SharedMemory@v1beta1.so /var/atomix/plugins/
COPY --from=build-pod-memory-driver /build/bin/PodMemory@v1beta1.so /var/atomix/plugins/

ENTRYPOINT ["atomix-runtime-proxy"]
