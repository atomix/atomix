# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

FROM atomix/build AS build

ARG driver
ARG version

RUN mkdir -p /build/test
WORKDIR /build/test

COPY ./drivers/test/go.mod ./drivers/test/go.sum ./

RUN go mod download

COPY ./drivers/test/cmd ./cmd
COPY ./drivers/test/pkg ./pkg

RUN atomix build \
    ./cmd/atomix-driver-test \
    -o /build/bin/atomix-driver-test

RUN mkdir -p /build/driver
WORKDIR /build/driver

COPY ./drivers/$driver/go.mod ./drivers/$driver/go.sum ./

RUN go mod download

COPY ./drivers/$driver/$version ./$version

RUN atomix build plugin \
    -t /build/test \
    -o /build/bin/driver.so \
    ./$version

# Pull binaries and plugins into the Alpine image
FROM alpine:3.15

RUN apk add libc6-compat

RUN addgroup -S atomix && adduser -S -G atomix atomix

USER atomix

COPY --from=build /build/bin/atomix-driver-test /usr/local/bin/atomix-driver-test
COPY --from=build /build/bin/driver.so /var/atomix/driver.so

ENTRYPOINT ["atomix-driver-test"]

