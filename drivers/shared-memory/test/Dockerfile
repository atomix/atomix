# SPDX-FileCopyrightText: 2023-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

FROM atomix/cli AS builder

RUN mkdir -p /build/sidecar

COPY ./sidecar/go.mod /build/sidecar
COPY ./sidecar/go.sum /build/sidecar

WORKDIR /build/sidecar
RUN go mod download

COPY ./sidecar/cmd /build/sidecar/cmd
COPY ./sidecar/pkg /build/sidecar/pkg

RUN atomix build \
    ./cmd/atomix-sidecar \
    -o /build/bin/atomix-sidecar

RUN mkdir -p /build/driver

COPY ./drivers/shared-memory/go.mod /build/driver
COPY ./drivers/shared-memory/go.sum /build/driver

WORKDIR /build/driver
RUN go mod download

COPY ./drivers/shared-memory/v1 /build/driver/v1

RUN atomix build plugin \
    -t /build/sidecar \
    -o /build/bin/driver.so \
    ./v1

# Pull binaries and plugins into the Alpine image
FROM alpine:3.15

RUN apk add libc6-compat

RUN addgroup -S atomix && adduser -S -G atomix atomix

USER atomix

COPY --from=builder /build/bin/atomix-sidecar /usr/local/bin/atomix-sidecar
COPY --from=builder /build/bin/driver.so /var/atomix/plugins/atomix.io/shared-memory@v1.so

ENTRYPOINT ["atomix-sidecar"]
