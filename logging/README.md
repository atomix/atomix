<!--
SPDX-FileCopyrightText: 2023-present Intel Corporation
SPDX-License-Identifier: Apache-2.0
-->

# flogr

## A facade for Go logging frameworks

[![Build](https://img.shields.io/github/actions/workflow/status/atomix/atomix/logging-verify.yml)](https://github.com/atomix/atomix/actions/workflows/logging-verify.yml)
![Go Version](https://img.shields.io/github/go-mod/go-version/atomix/atomix?label=go%20version&filename=logging%2Fgo.mod)

flogr is a lightweight wrapper around [zap](https://github.com/uber-go/zap) loggers that adds a logger hierarchy
with log level inheritence to enable fine-grained control and configuration of log levels, encoding, and formats.

The typical usage of the framework is to create a `Logger` for each Go package:

```go
var log = flogr.GetLogger()
```

By default, the logger will be assigned the package path as its name.

```go
const name := "Jordan Halterman"
...
log.Infof("My name is %s", name)
```

```
2023-03-31T01:13:30.607Z	INFO	main.go:12	My name is Jordan Halterman
```

## Log levels and inheritance

You can set the `flogr.Level` for a logger at startup time via configuration files or at runtime
via the `Logger` API to control the granularity of a logger's output:

```go
log.SetLevel(flogr.DebugLevel)
```

If the level for a logger is not explicitly set, it will inherit its level from its nearest ancestor in
the logger hierarchy. For example, setting the `github.com/atomix/atomix/runtime` logger to the `debug`
level will change the loggers for `github.com/atomix/atomix/runtime/pkg`, `github.com/atomix/atomix/runtime/pkg/utils`,
and all other packages underneath the `github.com/atomix/atomix/runtime` package to the `debug` level.

## Logging configuration

Loggers can be configured via a YAML configuration file. The configuration files may be in one of many
locations on the file system:

* `flogr.yaml`
* `.atomix/flogr.yaml`
* `~/.atomix/flogr.yaml`
* `/etc/atomix/flogr.yaml`

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
      jsonerr:
        sink: jsonerr
        level: error
sinks:
  stdout:
    path: stdout
    encoding: console
  jsonerr:
    path: stderr
    encoding: json
  file:
    path: file://app.log
    encoding: json
```
