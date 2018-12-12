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

import com.google.common.collect.Maps;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.core.Atomix;
import io.atomix.utils.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Benchmark controller.
 */
public abstract class BenchmarkController<C extends BenchmarkConfig> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkController.class);

  private final Atomix atomix;
  private final Map<String, ExecutorProgress> reports = new ConcurrentHashMap<>();
  private C config;
  private volatile BenchmarkResult<?> result;

  public BenchmarkController(Atomix atomix) {
    this.atomix = atomix;
  }

  /**
   * Returns the benchmark identifier.
   *
   * @return the benchmark identifier
   */
  public String getBenchId() {
    return config.getBenchId();
  }

  /**
   * Returns the current state of the benchmark.
   *
   * @return the current state of the benchmark
   */
  public BenchmarkStatus getStatus() {
    return result != null ? BenchmarkStatus.COMPLETE : BenchmarkStatus.RUNNING;
  }

  /**
   * Returns the current benchmark progress report.
   *
   * @return the current benchmark progress report
   */
  public BenchmarkProgress getProgress() {
    if (result != null) {
      return new BenchmarkProgress<>(BenchmarkStatus.COMPLETE, result.getProcesses().entrySet().stream()
          .map(entry -> Maps.immutableEntry(entry.getKey(), entry.getValue().asProgress()))
          .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())));
    }
    return new BenchmarkProgress<>(BenchmarkStatus.RUNNING, reports);
  }

  /**
   * Returns the benchmark result.
   *
   * @return the benchmark result
   */
  public BenchmarkResult getResult() {
    return result;
  }

  /**
   * Returns a default progress report for the controller.
   *
   * @return a default progress report for the controller
   */
  protected abstract ExecutorProgress getDefaultProgress();

  /**
   * Starts the benchmark.
   *
   * @param config the benchmark configuration
   * @return a future to be completed once the benchmark has been started
   */
  public CompletableFuture<Void> start(C config) {
    this.config = config;
    LOGGER.info("Starting benchmark {}", getBenchId());

    atomix.getCommunicationService().subscribe(
        config.getBenchId(),
        BenchmarkSerializer.INSTANCE::decode,
        this::report,
        atomix.getExecutorService());

    List<MemberId> benchMembers = atomix.getMembershipService().getMembers().stream()
        .filter(member -> member.properties().getProperty(BenchmarkConstants.BENCH_NODE_TYPE, Boolean.FALSE.toString()).equals(Boolean.TRUE.toString()))
        .map(Member::id)
        .collect(Collectors.toList());

    benchMembers.forEach(member -> reports.put(member.id(), getDefaultProgress()));

    int operationsPerMember = config.getOperations() / benchMembers.size();
    List<CompletableFuture<Void>> runFutures = benchMembers.stream()
        .map(member -> atomix.getCommunicationService().<BenchmarkConfig, Void>send(
            BenchmarkConstants.RUN_SUBJECT,
            config.copy().setOperations(operationsPerMember),
            BenchmarkSerializer.INSTANCE::encode,
            BenchmarkSerializer.INSTANCE::decode,
            member))
        .collect(Collectors.toList());
    return Futures.allOf(runFutures).thenApply(v -> null);
  }

  /**
   * Stops the benchmark.
   *
   * @return a future to be completed once the benchmark has been stopped
   */
  public CompletableFuture<Void> stop() {
    LOGGER.info("Stopping benchmark {}", config.getBenchId());

    atomix.getCommunicationService().unsubscribe(config.getBenchId());
    if (reports.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    List<CompletableFuture<Void>> runFutures = reports.keySet().stream()
        .map(member -> atomix.getCommunicationService().<String, Void>send(
            BenchmarkConstants.KILL_SUBJECT,
            config.getBenchId(),
            BenchmarkSerializer.INSTANCE::encode,
            BenchmarkSerializer.INSTANCE::decode,
            MemberId.from(member)))
        .collect(Collectors.toList());
    return Futures.allOf(runFutures).thenApply(v -> null);
  }

  /**
   * Reports the benchmark progress for a runner.
   *
   * @param memberId the reporting member
   * @param progress the progress for a runner
   */
  private void report(MemberId memberId, ExecutorProgress progress) {
    reports.put(memberId.id(), progress);

    if (progress.getStatus() == BenchmarkStatus.COMPLETE) {
      boolean complete = reports.values().stream().allMatch(p -> p.getStatus() == BenchmarkStatus.COMPLETE);
      if (complete) {
        result = new BenchmarkResult<>(reports.entrySet().stream()
            .map(entry -> Maps.immutableEntry(entry.getKey(), entry.getValue().asResult()))
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())));
      }
    }
  }
}
