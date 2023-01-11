# SPDX-FileCopyrightText: 2022-present Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build
build:
	go build ./driver/...

.PHONY: test
test:
	go test github.com/atomix/atomix/drivers/redis/v9/driver/... -p 1
