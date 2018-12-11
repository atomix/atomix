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
package io.atomix.bench.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.atomix.bench.BenchmarkStatus;
import io.atomix.bench.ExecutorProgress;
import io.atomix.bench.ExecutorResult;

/**
 * Messaging executor progress.
 */
public class MessagingExecutorProgress extends ExecutorProgress {
  private final int requests;
  private final int responses;
  private final int failures;
  private final long time;
  private final long[] latency;

  @JsonCreator
  public MessagingExecutorProgress(
      @JsonProperty("state") BenchmarkStatus state,
      @JsonProperty("requests") int requests,
      @JsonProperty("responses") int responses,
      @JsonProperty("failures") int failures,
      @JsonProperty("time") long time,
      @JsonProperty("latency") long[] latency) {
    super(state);
    this.requests = requests;
    this.responses = responses;
    this.failures = failures;
    this.time = time;
    this.latency = latency;
  }

  @Override
  public ExecutorResult asResult() {
    return new MessagingExecutorResult(requests, responses, failures, time, latency);
  }

  public int getRequests() {
    return requests;
  }

  public int getResponses() {
    return responses;
  }

  public int getFailures() {
    return failures;
  }

  public long getTime() {
    return time;
  }

  public long[] getLatency() {
    return latency;
  }
}
