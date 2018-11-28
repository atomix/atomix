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

import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.core.Atomix;
import io.atomix.utils.concurrent.Futures;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Benchmark controller.
 */
public class BenchmarkController {
  private final Atomix atomix;
  private final BenchmarkConfig config;
  private final Map<MemberId, BenchmarkProgress> reports = new ConcurrentHashMap<>();
  private volatile BenchmarkResult result;

  public BenchmarkController(Atomix atomix, BenchmarkConfig config) {
    this.atomix = atomix;
    this.config = config;
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
  public BenchmarkState getState() {
    return result != null ? BenchmarkState.COMPLETE : BenchmarkState.RUNNING;
  }

  /**
   * Returns the current benchmark progress report.
   *
   * @return the current benchmark progress report
   */
  public BenchmarkProgress getProgress() {
    if (result != null) {
      return new BenchmarkProgress(BenchmarkState.COMPLETE, result.getOperations(), result.getTime());
    }

    int totalOperations = 0;
    int totalTime = 0;
    for (BenchmarkProgress progress : reports.values()) {
      totalOperations += progress.getOperations();
      totalTime += progress.getTime();
    }
    return new BenchmarkProgress(BenchmarkState.RUNNING, totalOperations, totalTime);
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
   * Starts the benchmark.
   *
   * @return a future to be completed once the benchmark has been started
   */
  public CompletableFuture<Void> start() {
    atomix.getCommunicationService().subscribe(
        config.getBenchId(),
        BenchmarkSerializer.INSTANCE::decode,
        this::report,
        atomix.getExecutorService());

    List<MemberId> benchMembers = atomix.getMembershipService().getMembers().stream()
        .filter(member -> member.properties().getProperty(BenchmarkConstants.BENCH_NODE_TYPE, Boolean.FALSE.toString()).equals(Boolean.TRUE.toString()))
        .map(Member::id)
        .collect(Collectors.toList());

    benchMembers.forEach(member -> reports.put(member, new BenchmarkProgress(BenchmarkState.RUNNING, 0, 0)));

    int operationsPerMember = config.getOperations() / benchMembers.size();
    List<CompletableFuture<Void>> runFutures = benchMembers.stream()
        .map(member -> atomix.getCommunicationService().<BenchmarkConfig, Void>send(
            BenchmarkConstants.RUN_SUBJECT,
            new BenchmarkConfig().setOperations(operationsPerMember),
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
    atomix.getCommunicationService().unsubscribe(config.getBenchId());
    if (reports.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    List<CompletableFuture<Void>> runFutures = reports.keySet().stream()
        .map(member -> atomix.getCommunicationService().<Void, Void>send(
            BenchmarkConstants.KILL_SUBJECT,
            null,
            BenchmarkSerializer.INSTANCE::encode,
            BenchmarkSerializer.INSTANCE::decode,
            member))
        .collect(Collectors.toList());
    return Futures.allOf(runFutures).thenApply(v -> null);
  }

  /**
   * Reports the benchmark progress for a runner.
   *
   * @param memberId the reporting member
   * @param progress the progress for a runner
   */
  private void report(MemberId memberId, BenchmarkProgress progress) {
    reports.put(memberId, progress);
  }
}
