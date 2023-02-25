# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

FROM alpine:3.15

RUN apk add libc6-compat bash

RUN addgroup -S atomix && adduser -S -G atomix atomix

USER atomix

ADD atomix-shared-memory-node /usr/local/bin/atomix-shared-memory-node

ENTRYPOINT ["atomix-shared-memory-node"]
