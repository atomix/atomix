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

# Build the runtime binary
FROM builder AS build-runtime
RUN go run ./cmd/atomix-build runtime ./cmd/atomix-sidecar /build/bin/atomix-runtime

# Build the Etcd/v3 storage driver
FROM builder AS build-etcd-v3-driver
RUN go run ./cmd/atomix-build plugin \
    github.com/atomix/atomix/drivers/etcd@v0.4.0 \
    github.com/atomix/atomix/drivers/etcd/v3 \
    /build/bin/driver.so

# Build the PodMemory/v1 storage driver
FROM builder AS build-pod-memory-v1-driver
RUN go run ./cmd/atomix-build plugin \
    github.com/atomix/atomix/drivers/pod-memory@v0.4.0 \
    github.com/atomix/atomix/drivers/pod-memory/v1 \
    /build/bin/driver.so

# Build the Raft/v1 storage driver
FROM builder AS build-raft-v1-driver
RUN go run ./cmd/atomix-build plugin \
    github.com/atomix/atomix/drivers/raft@v0.4.0 \
    github.com/atomix/atomix/drivers/raft/v1 \
    /build/bin/driver.so

# Build the Redis/v8 storage driver
FROM builder AS build-redis-v8-driver
RUN go run ./cmd/atomix-build plugin \
    github.com/atomix/atomix/drivers/redis@v0.4.0 \
    github.com/atomix/atomix/drivers/redis/v8 \
    /build/bin/driver.so

# Build the Redis/v8 storage driver
FROM builder AS build-redis-v9-driver
RUN go run ./cmd/atomix-build plugin \
    github.com/atomix/atomix/drivers/redis@v0.4.0 \
    github.com/atomix/atomix/drivers/redis/v9 \
    /build/bin/driver.so

# Build the SharedMemory/v1 storage driver
FROM builder AS build-shared-memory-v1-driver
RUN go run ./cmd/atomix-build plugin \
    github.com/atomix/atomix/drivers/shared-memory@v0.4.0 \
    github.com/atomix/atomix/drivers/shared-memory/v1 \
    /build/bin/driver.so

# Pull binaries and plugins into the Alpine image
FROM alpine:3.15

RUN apk add libc6-compat

RUN addgroup -S atomix && adduser -S -G atomix atomix

USER atomix

COPY --from=build-runtime /build/bin/atomix-runtime /usr/local/bin/atomix-runtime
COPY --from=build-etcd-v3-driver /build/bin/driver.so /var/atomix/plugins/atomix.io/etcd@v3.so
COPY --from=build-pod-memory-v1-driver /build/bin/driver.so /var/atomix/plugins/atomix.io/pod-memory@v1.so
COPY --from=build-raft-v1-driver /build/bin/driver.so /var/atomix/plugins/atomix.io/raft@v1.so
COPY --from=build-redis-v8-driver /build/bin/driver.so /var/atomix/plugins/atomix.io/redis@v8.so
COPY --from=build-redis-v9-driver /build/bin/driver.so /var/atomix/plugins/atomix.io/redis@v9.so
COPY --from=build-shared-memory-v1-driver /build/bin/driver.so /var/atomix/plugins/atomix.io/shared-memory@v1.so

ENTRYPOINT ["atomix-runtime"]
