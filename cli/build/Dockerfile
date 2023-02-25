# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

FROM goreleaser/goreleaser-cross:v1.19

ADD atomix /usr/local/bin/atomix

ENV CC=gcc
ENV CXX=g++
ENV ATOMIX_CC_ENABLED=true

ENTRYPOINT ["atomix"]
