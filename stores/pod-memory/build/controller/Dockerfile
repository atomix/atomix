# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

FROM atomix/build AS build

RUN mkdir /build
WORKDIR /build

COPY go.mod go.sum ./

RUN go mod download

COPY ./cmd/atomix-pod-memory-controller ./cmd/atomix-pod-memory-controller
COPY ./pkg ./pkg

RUN atomix build ./cmd/atomix-pod-memory-controller -o ./bin/atomix-pod-memory-controller

FROM alpine:3.15

RUN apk add libc6-compat

RUN addgroup -S atomix && adduser -S -G atomix atomix

USER atomix

COPY --from=build /build/bin/atomix-pod-memory-controller /usr/local/bin/atomix-pod-memory-controller

ENTRYPOINT ["atomix-pod-memory-controller"]
