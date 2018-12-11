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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Benchmark progress report.
 */
public class BenchmarkProgress {
  private final BenchmarkState state;
  private final Map<String, RunnerProgress> processes;

  @JsonCreator
  public BenchmarkProgress(
      @JsonProperty("state") BenchmarkState state,
      @JsonProperty("processes") Map<String, RunnerProgress> processes) {
    this.state = state;
    this.processes = processes;
  }

  public BenchmarkState getState() {
    return state;
  }

  public Map<String, RunnerProgress> getProcesses() {
    return processes;
  }
}
