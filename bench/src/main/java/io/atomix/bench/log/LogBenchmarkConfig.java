/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.bench.log;

import io.atomix.bench.BenchmarkConfig;
import io.atomix.bench.BenchmarkType;

/**
 * Log benchmark configuration.
 */
public class LogBenchmarkConfig extends BenchmarkConfig {
  private static final int DEFAULT_CONCURRENCY = 1;
  private static final int DEFAULT_ENTRY_SIZE = 1024;

  private int concurrency = DEFAULT_CONCURRENCY;
  private int entrySize = DEFAULT_ENTRY_SIZE;
  private LogBenchmarkMode mode = LogBenchmarkMode.PRODUCER;

  public LogBenchmarkConfig() {
  }

  public LogBenchmarkConfig(LogBenchmarkConfig config) {
    super(config);
    this.concurrency = config.concurrency;
    this.entrySize = config.entrySize;
    this.mode = config.mode;
  }

  @Override
  public BenchmarkConfig copy() {
    return new LogBenchmarkConfig(this);
  }

  @Override
  public BenchmarkType getType() {
    return LogBenchmarkType.INSTANCE;
  }

  @Override
  public LogBenchmarkConfig setBenchId(String benchId) {
    super.setBenchId(benchId);
    return this;
  }

  @Override
  public LogBenchmarkConfig setOperations(int operations) {
    super.setOperations(operations);
    return this;
  }

  public int getConcurrency() {
    return concurrency;
  }

  public LogBenchmarkConfig setConcurrency(int concurrency) {
    this.concurrency = concurrency;
    return this;
  }

  public int getEntrySize() {
    return entrySize;
  }

  public void setEntrySize(int entrySize) {
    this.entrySize = entrySize;
  }

  public LogBenchmarkMode getMode() {
    return mode;
  }

  public void setMode(LogBenchmarkMode mode) {
    this.mode = mode;
  }
}
