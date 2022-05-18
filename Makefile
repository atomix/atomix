# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

GOLANG_CROSS_VERSION := v1.18.2-v1.8.3

build: build-bin build-docker

build-bin:
	docker run \
		--rm \
		--privileged \
		-e CGO_ENABLED=1 \
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
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v `pwd`:/build \
		-w /build \
		goreleaser/goreleaser-cross:${GOLANG_CROSS_VERSION} \
		release -f ./build/docker.yaml --snapshot --rm-dist

release: release-bin release-docker

release-bin:
	docker run \
		--rm \
		--privileged \
		-e CGO_ENABLED=1 \
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
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v `pwd`:/build \
		-w /build \
		goreleaser/goreleaser-cross:${GOLANG_CROSS_VERSION} \
		release -f ./build/docker.yaml --rm-dist

api: go docs

go:
	@find ./api -name '*.pb.go' -delete
	docker run -it \
		-v `pwd`:/build \
		atomix/codegen:go-latest \
	    --proto-path ./api --go-path ./api --import-path github.com/atomix/runtime/api
	docker run -it \
		-v `pwd`:/build \
		atomix/codegen:latest \
		protoc -I=./api:/go/src/github.com/gogo/protobuf \
			--go_out=Mgoogle/protobuf/descriptor.proto=github.com/golang/protobuf/protoc-gen-go/descriptor,import_path=github.com/atomix/runtime/api/atomix/runtime/v1:api \
			api/atomix/runtime/v1/descriptor.proto

docs:
	@find ./api -name '*.md' -delete
	docker run -it \
		-v `pwd`:/build \
		atomix/codegen:docs-latest \
		--proto-path ./api --docs-path ./api --docs-format markdown

reuse-tool: # @HELP install reuse if not present
	command -v reuse || python3 -m pip install reuse

license: reuse-tool # @HELP run license checks
	reuse lint
