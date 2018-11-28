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
 * Benchmark constant strings.
 */
public final class BenchmarkConstants {
  public static final String BENCH_NODE_TYPE = "bench";

  public static final String START_SUBJECT = "atomix-bench-start";
  public static final String RUN_SUBJECT = "atomix-bench-run";
  public static final String KILL_SUBJECT = "atomix-bench-kill";
  public static final String PROGRESS_SUBJECT = "atomix-bench-progress";
  public static final String RESULT_SUBJECT = "atomix-bench-result";
  public static final String STOP_SUBJECT = "atomix-bench-stop";

  private BenchmarkConstants() {
  }
}
