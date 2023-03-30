# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

FROM atomix/build AS sidecar-builder

RUN mkdir /build
WORKDIR /build

COPY ./sidecar/go.mod ./sidecar/go.sum ./

RUN go mod download

COPY ./sidecar/cmd ./cmd
COPY ./sidecar/pkg ./pkg

RUN atomix build ./cmd/atomix-sidecar -o ./bin/atomix-sidecar

FROM atomix/build AS driver-builder-v8

RUN mkdir /build
WORKDIR /build

COPY ./drivers/redis/go.mod ./drivers/redis/go.sum ./

RUN go mod download

COPY ./drivers/redis/v8 ./v8

RUN atomix build plugin -o ./bin/driver.so ./v8

FROM atomix/build AS driver-builder-v9

RUN mkdir /build
WORKDIR /build

COPY ./drivers/redis/go.mod ./drivers/redis/go.sum ./

RUN go mod download

COPY ./drivers/redis/v9 ./v9

RUN atomix build plugin -o ./bin/driver.so ./v9

# Pull binaries and plugins into the Alpine image
FROM alpine:3.15

RUN apk add libc6-compat

RUN addgroup -S atomix && adduser -S -G atomix atomix

USER atomix

COPY --from=sidecar-builder /build/bin/atomix-sidecar /usr/local/bin/atomix-sidecar
COPY --from=driver-builder-v8 /build/bin/driver.so /var/atomix/plugins/atomix.io/redis@v8.so
COPY --from=driver-builder-v9 /build/bin/driver.so /var/atomix/plugins/atomix.io/redis@v9.so

ENTRYPOINT ["atomix-sidecar"]
