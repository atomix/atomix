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
package io.atomix.bench;

/**
 * Benchmark runner progress report.
 */
public class RunnerProgress {
  private final BenchmarkState state;
  private final int operations;
  private final int reads;
  private final int writes;
  private final int events;
  private final long time;

  public RunnerProgress(BenchmarkState state, int operations, int reads, int writes, int events, long time) {
    this.state = state;
    this.operations = operations;
    this.reads = reads;
    this.writes = writes;
    this.events = events;
    this.time = time;
  }

  public BenchmarkState getState() {
    return state;
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

  public long getTime() {
    return time;
  }
}
