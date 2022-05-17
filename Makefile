# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

build:
	go build ./...

api: go docs

go:
	@find ./pkg -name '*.pb.go' -delete
	rm -r **/*.pb.go || true
	docker run -it \
		-v `pwd`:/build \
		atomix/codegen:go-latest \
	    --proto-path ./api --go-path ./api --import-path github.com/atomix/runtime/api
	docker run -it \
		-v `pwd`:/build \
		atomix/codegen:latest \
		protoc -I=./proto:/go/src/github.com/gogo/protobuf \
			--go_out=Mgoogle/protobuf/descriptor.proto=github.com/golang/protobuf/protoc-gen-go/descriptor,import_path=github.com/atomix/runtime/api/atomix/runtime/v1:pkg \
			api/atomix/runtime/v1/descriptor.proto

docs:
	@find ./docs -name '*.md' -delete
	docker run -it \
		-v `pwd`:/build \
		atomix/codegen:docs-latest \
		--proto-path ./api --docs-path ./api --docs-format markdown

reuse-tool: # @HELP install reuse if not present
	command -v reuse || python3 -m pip install reuse

license: reuse-tool # @HELP run license checks
	reuse lint
