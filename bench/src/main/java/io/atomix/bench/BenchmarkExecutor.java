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

import io.atomix.core.Atomix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Benchmark runner.
 */
public abstract class BenchmarkExecutor<C extends BenchmarkConfig> {
  private static final int REPORT_PERIOD = 1_000;

  private final Logger log = LoggerFactory.getLogger(getClass());
  private final ScheduledExecutorService reporterExecutor = Executors.newSingleThreadScheduledExecutor(
      namedThreads("atomix-bench-reporter", log));
  private final Atomix atomix;
  private C config;

  protected BenchmarkExecutor(Atomix atomix) {
    this.atomix = atomix;
  }

  /**
   * Returns the benchmark identifier.
   *
   * @return the benchmark identifier
   */
  public String getBenchId() {
    return config != null ? config.getBenchId() : null;
  }

  /**
   * Returns the current executor progress report.
   *
   * @return the current progress report for the executor
   */
  public abstract ExecutorProgress getProgress();

  /**
   * Starts the benchmark process.
   *
   * @param config the benchmark configuration
   */
  public void start(C config) {
    this.config = config;
    log.info("Running benchmark {}", config.getBenchId());
    reporterExecutor.scheduleAtFixedRate(this::report, REPORT_PERIOD, REPORT_PERIOD, TimeUnit.MILLISECONDS);
  }

  /**
   * Sends a progress report to the benchmark controller.
   */
  private void report() {
    atomix.getCommunicationService().broadcastIncludeSelf(getBenchId(), getProgress(), BenchmarkSerializer.INSTANCE::encode, false);
  }

  /**
   * Stops the benchmark process.
   */
  public void stop() {
    reporterExecutor.shutdownNow();
    report();
  }
}
