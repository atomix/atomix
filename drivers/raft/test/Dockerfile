# SPDX-FileCopyrightText: 2023-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

FROM atomix/cli AS build

RUN mkdir -p /build/sidecar
WORKDIR /build/sidecar

COPY ./sidecar/go.mod ./sidecar/go.sum ./

RUN go mod download

COPY ./sidecar/cmd ./cmd
COPY ./sidecar/pkg ./pkg

RUN atomix build \
    ./cmd/atomix-sidecar \
    -o /build/bin/atomix-sidecar

RUN mkdir -p /build/driver
WORKDIR /build/driver

COPY ./drivers/raft/go.mod ./drivers/raft/go.sum ./

RUN go mod download

COPY ./drivers/raft/v1 ./v1

RUN atomix build plugin \
    -t /build/sidecar \
    -o /build/bin/driver.so \
    ./v1

# Pull binaries and plugins into the Alpine image
FROM alpine:3.15

RUN apk add libc6-compat

RUN addgroup -S atomix && adduser -S -G atomix atomix

USER atomix

COPY --from=build /build/bin/atomix-sidecar /usr/local/bin/atomix-sidecar
COPY --from=build /build/bin/driver.so /var/atomix/plugins/atomix.io/raft@v1.so

ENTRYPOINT ["atomix-sidecar"]
