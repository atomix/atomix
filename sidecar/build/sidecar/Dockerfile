# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

FROM atomix/cli AS builder

RUN mkdir /build
WORKDIR /build

COPY ./go.mod /build
COPY ./go.sum /build

RUN go mod download

# Build the runtime binary
FROM builder AS build-sidecar

COPY ./cmd /build/cmd
COPY ./pkg /build/pkg

RUN atomix build \
    ./cmd/atomix-sidecar \
    -o /build/bin/atomix-sidecar

# Build the atomix.io/etcd@v3 storage driver
FROM builder AS build-etcd-v3-driver
RUN atomix build plugin \
    -o /build/bin/driver.so \
    github.com/atomix/atomix/drivers/etcd/v3@v0.6.1

# Build the atomix.io/pod-memory@v1 storage driver
FROM builder AS build-pod-memory-v1-driver
RUN atomix build plugin \
    -o /build/bin/driver.so \
    github.com/atomix/atomix/drivers/pod-memory/v1@v0.6.1

# Build the atomix.io/raft@v1 storage driver
FROM builder AS build-raft-v1-driver
RUN atomix build plugin \
    -o /build/bin/driver.so \
    github.com/atomix/atomix/drivers/raft/v1@v0.6.1

# Build the atomix.io/redis@v8 storage driver
FROM builder AS build-redis-v8-driver
RUN atomix build plugin \
    -o /build/bin/driver.so \
    github.com/atomix/atomix/drivers/redis/v8@v0.6.1

# Build the atomix.io/redis@v9 storage driver
FROM builder AS build-redis-v9-driver
RUN atomix build plugin \
    -o /build/bin/driver.so \
    github.com/atomix/atomix/drivers/redis/v9@v0.6.1

# Build the atomix.io/shared-memory@v1 storage driver
FROM builder AS build-shared-memory-v1-driver
RUN atomix build plugin \
    -o /build/bin/driver.so \
    github.com/atomix/atomix/drivers/shared-memory/v1@v0.6.1

# Pull binaries and plugins into the Alpine image
FROM alpine:3.15

RUN apk add libc6-compat

RUN addgroup -S atomix && adduser -S -G atomix atomix

USER atomix

COPY --from=build-sidecar /build/bin/atomix-sidecar /usr/local/bin/atomix-sidecar
COPY --from=build-etcd-v3-driver /build/bin/driver.so /var/atomix/plugins/atomix.io/etcd@v3.so
COPY --from=build-pod-memory-v1-driver /build/bin/driver.so /var/atomix/plugins/atomix.io/pod-memory@v1.so
COPY --from=build-raft-v1-driver /build/bin/driver.so /var/atomix/plugins/atomix.io/raft@v1.so
COPY --from=build-redis-v8-driver /build/bin/driver.so /var/atomix/plugins/atomix.io/redis@v8.so
COPY --from=build-redis-v9-driver /build/bin/driver.so /var/atomix/plugins/atomix.io/redis@v9.so
COPY --from=build-shared-memory-v1-driver /build/bin/driver.so /var/atomix/plugins/atomix.io/shared-memory@v1.so

ENTRYPOINT ["atomix-sidecar"]
