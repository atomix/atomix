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
#COPY ./pkg /build/pkg

RUN go build -mod=readonly -trimpath -o /build/dist/bin/atomix-bench ./cmd/atomix-bench

FROM alpine:3.15

RUN apk add libc6-compat bash

RUN addgroup -S atomix && adduser -S -G atomix atomix

USER atomix

COPY --from=build /build/dist/bin/atomix-bench /usr/local/bin/atomix-bench

ENTRYPOINT ["atomix-bench"]