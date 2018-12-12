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

import io.atomix.bench.BenchmarkController;
import io.atomix.bench.BenchmarkStatus;
import io.atomix.bench.ExecutorProgress;
import io.atomix.core.Atomix;
import io.atomix.primitive.Recovery;
import io.atomix.protocols.log.DistributedLogProtocol;

import java.math.BigDecimal;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static io.atomix.bench.util.Strings.randomString;

/**
 * Log benchmark controller.
 */
public class LogBenchmarkController extends BenchmarkController<LogBenchmarkConfig> {
  private final Atomix atomix;

  public LogBenchmarkController(Atomix atomix) {
    super(atomix);
    this.atomix = atomix;
  }

  @Override
  protected ExecutorProgress getDefaultProgress() {
    return new LogExecutorProgress(BenchmarkStatus.RUNNING, 0, BigDecimal.ZERO);
  }

  @Override
  public CompletableFuture<Void> start(LogBenchmarkConfig config) {
    return super.start(config).whenComplete((r, e) -> {
      if (config.getMode() == LogBenchmarkMode.CONSUMER) {
        atomix.<String>logBuilder()
            .withProtocol(DistributedLogProtocol.builder()
                .withRecovery(Recovery.RECOVER)
                .build())
            .buildAsync()
            .thenAccept(log -> {
              String entry = randomString(config.getEntrySize(), new Random());
              IntStream.range(0, config.getOperations()).forEach(i -> log.async().produce(entry));
            });
      }
    });
  }
}
