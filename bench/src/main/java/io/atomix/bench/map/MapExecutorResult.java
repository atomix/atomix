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
package io.atomix.bench.map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.atomix.bench.BenchmarkStatus;
import io.atomix.bench.ExecutorProgress;
import io.atomix.bench.ExecutorResult;

import java.math.BigDecimal;

/**
 * Benchmark runner progress report.
 */
public class MapExecutorResult extends ExecutorResult {
  private final int operations;
  private final int reads;
  private final int writes;
  private final int events;
  private final BigDecimal time;

  @JsonCreator
  public MapExecutorResult(
      @JsonProperty("operations") int operations,
      @JsonProperty("reads") int reads,
      @JsonProperty("writes") int writes,
      @JsonProperty("events") int events,
      @JsonProperty("time") BigDecimal time) {
    this.operations = operations;
    this.reads = reads;
    this.writes = writes;
    this.events = events;
    this.time = time;
  }

  @Override
  public ExecutorProgress asProgress() {
    return new MapExecutorProgress(BenchmarkStatus.COMPLETE, operations, reads, writes, events, time);
  }

  public int getOperations() {
    return operations;
  }

  public int getReads() {
    return reads;
  }

  public int getWrites() {
    return writes;
  }

  public int getEvents() {
    return events;
  }

  public BigDecimal getTime() {
    return time;
  }
}
