<!--
SPDX-FileCopyrightText: 2023-present Intel Corporation
SPDX-License-Identifier: Apache-2.0
-->

# Go logging framework

[![Build](https://img.shields.io/github/actions/workflow/status/atomix/atomix/logging-verify.yml)](https://github.com/atomix/atomix/actions/workflows/logging-verify.yml)
![Go Version](https://img.shields.io/github/go-mod/go-version/atomix/atomix?label=go%20version&filename=logging%2Fgo.mod)

This module provides a Go library for structured logging that supports granular log levels and inheritance.

The typical usage of the framework is to create a `Logger` for each Go package:

```go
var log = logging.GetLogger()
```

By default, the logger will be assigned the package path as its name.

## Log levels and inheritance

You can set the `logging.Level` for a logger at startup time via configuration files or at runtime
via the `Logger` API to control the granularity of a logger's output:

```go
log.SetLevel(logging.DebugLevel)
```

If the level for a logger is not explicitly set, it will inherit its level from its nearest ancestor in
the logger hierarchy. For example, setting the `github.com/atomix/atomix/runtime` logger to the `debug`
level will change the loggers for `github.com/atomix/atomix/runtime/pkg`, `github.com/atomix/atomix/runtime/pkg/utils`,
and all other packages underneath the `github.com/atomix/atomix/runtime` package to the `debug` level.

## Logging configuration

Loggers can be configured via a YAML configuration file. The configuration files may be in one of many
locations on the file system:

* `logging.yaml`
* `.atomix/logging.yaml`
* `~/.atomix/logging.yaml`
* `/etc/atomix/logging.yaml`

The configuration file contains a set of `loggers` which specifies the level and outputs of each logger,
and `sinks` which specify where and how to write log messages.

The `root` logger is a special logger for global configuration. The `root` logger is the ancestor of all loggers.

```yaml
loggers:
  root:
    level: info
    output:
      stdout:
        sink: stdout
      file:
        sink: file
  github.com/atomix/atomix:
    level: warn
  github.com/atomix/atomix/runtime:
    level: info
  github.com/atomix/atomix/drivers:
    level: debug
    output:
      jsonout:
        sink: jsonout
        level: info
sinks:
  stdout:
    encoding: console
    stdout: {}
  jsonout:
    encoding: json
    stdout: {}
  file:
    encoding: json
    file:
      path: test.log
```
