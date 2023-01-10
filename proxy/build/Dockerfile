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
RUN go run ./cmd/atomix-build proxy /build/bin/atomix-proxy

# Build the Etcd/v3 storage driver
FROM builder AS build-etcd-v3-driver
RUN go run ./cmd/atomix-build driver \
    github.com/atomix/atomix/drivers/etcd/v3@852c2733e \
    /build/bin/Etcd@v3.so

# Build the PodMemory/v1 storage driver
FROM builder AS build-pod-memory-driver
RUN go run ./cmd/atomix-build driver \
    github.com/atomix/atomix/drivers/pod-memory@852c2733e \
    /build/bin/PodMemory@v1.so

# Build the Raft/v1 storage driver
FROM builder AS build-raft-driver
RUN go run ./cmd/atomix-build driver \
    github.com/atomix/atomix/drivers/raft@852c2733e \
    /build/bin/Raft@v1.so

# Build the Redis/v8 storage driver
#FROM builder AS build-redis-v8-driver
#RUN go run ./cmd/atomix-build driver \
#    github.com/atomix/atomix/drivers/redis/v8@852c2733e \
#    /build/bin/Redis@v8.so

# Build the Redis/v8 storage driver
#FROM builder AS build-redis-v9-driver
#RUN go run ./cmd/atomix-build driver \
#    github.com/atomix/atomix/drivers/redis/v9@852c2733e \
#    /build/bin/Redis@v9.so

# Build the SharedMemory/v1 storage driver
FROM builder AS build-shared-memory-driver
RUN go run ./cmd/atomix-build driver \
    github.com/atomix/atomix/drivers/shared-memory@852c2733e \
    /build/bin/SharedMemory@v1.so

# Pull binaries and plugins into the Alpine image
FROM alpine:3.15

RUN apk add libc6-compat

RUN addgroup -S atomix && adduser -S -G atomix atomix

USER atomix

COPY --from=build-proxy /build/bin/atomix-proxy /usr/local/bin/atomix-proxy
COPY --from=build-etcd-v3-driver /build/bin/Etcd@v3.so /var/atomix/plugins/
COPY --from=build-pod-memory-driver /build/bin/PodMemory@v1.so /var/atomix/plugins/
COPY --from=build-raft-driver /build/bin/Raft@v1.so /var/atomix/plugins/
#COPY --from=build-redis-v8-driver /build/bin/Redis@v8.so /var/atomix/plugins/
#COPY --from=build-redis-v9-driver /build/bin/Redis@v9.so /var/atomix/plugins/
COPY --from=build-shared-memory-driver /build/bin/SharedMemory@v1.so /var/atomix/plugins/

ENTRYPOINT ["atomix-proxy"]
