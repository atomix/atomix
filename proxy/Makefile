# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

GOLANG_CROSS_VERSION := v1.18.1

RUNTIME_VERSION := $(shell go run github.com/atomix/runtime/cmd/atomix-runtime-version@master)

.PHONY: build
build: build-bin build-docker

build-bin:
	docker run \
		--rm \
		--privileged \
		-e CGO_ENABLED=1 \
		-e RUNTIME_VERSION=$(RUNTIME_VERSION) \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v `pwd`:/build \
		-w /build \
		goreleaser/goreleaser-cross:${GOLANG_CROSS_VERSION} \
		release -f ./build/bin.yaml --snapshot --rm-dist

build-docker:
	docker run \
		--rm \
		--privileged \
		-e CGO_ENABLED=1 \
		-e RUNTIME_VERSION=$(RUNTIME_VERSION) \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v `pwd`:/build \
		-w /build \
		goreleaser/goreleaser-cross:${GOLANG_CROSS_VERSION} \
		release -f ./build/docker.yaml --snapshot --rm-dist

.PHONY: release
release: release-bin release-docker

release-bin:
	docker run \
		--rm \
		--privileged \
		-e CGO_ENABLED=1 \
		-e RUNTIME_VERSION=$(RUNTIME_VERSION) \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v `pwd`:/build \
		-w /build \
		goreleaser/goreleaser-cross:${GOLANG_CROSS_VERSION} \
		release -f ./build/bin.yaml --rm-dist

release-docker:
	docker run \
		--rm \
		--privileged \
		-e CGO_ENABLED=1 \
		-e RUNTIME_VERSION=$(RUNTIME_VERSION) \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v `pwd`:/build \
		-w /build \
		goreleaser/goreleaser-cross:${GOLANG_CROSS_VERSION} \
		release -f ./build/docker.yaml --rm-dist

reuse-tool: # @HELP install reuse if not present
	command -v reuse || python3 -m pip install reuse

license: reuse-tool # @HELP run license checks
	reuse lint
