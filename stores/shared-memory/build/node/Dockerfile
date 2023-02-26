# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

FROM atomix/build AS build

RUN mkdir /build
WORKDIR /build

COPY go.mod go.sum ./

RUN go mod download

COPY ./cmd ./cmd
COPY ./pkg ./pkg

RUN atomix build ./cmd/atomix-shared-memory-node -o ./bin/atomix-shared-memory-node

FROM alpine:3.15

RUN apk add libc6-compat bash

RUN addgroup -S atomix && adduser -S -G atomix atomix

USER atomix

COPY --from=build /build/dist/atomix-shared-memory-node /usr/local/bin/atomix-shared-memory-node

ENTRYPOINT ["atomix-shared-memory-node"]
