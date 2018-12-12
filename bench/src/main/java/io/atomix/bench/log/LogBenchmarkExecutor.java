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

import io.atomix.bench.BenchmarkExecutor;
import io.atomix.bench.BenchmarkStatus;
import io.atomix.bench.ExecutorProgress;
import io.atomix.core.Atomix;
import io.atomix.core.log.AsyncDistributedLog;
import io.atomix.primitive.Recovery;
import io.atomix.protocols.log.DistributedLogProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static io.atomix.bench.util.Strings.randomString;
import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Log benchmark executor.
 */
public class LogBenchmarkExecutor extends BenchmarkExecutor<LogBenchmarkConfig> {
  private static final Logger LOGGER = LoggerFactory.getLogger(LogBenchmarkExecutor.class);

  private final Atomix atomix;

  private final ExecutorService runnerExecutor = Executors.newSingleThreadExecutor(
      namedThreads("atomix-bench-runner", LOGGER));
  private long startTime;
  private long endTime;
  private volatile boolean running;
  private final AtomicInteger opCounter = new AtomicInteger();

  public LogBenchmarkExecutor(Atomix atomix) {
    super(atomix);
    this.atomix = atomix;
  }

  @Override
  public ExecutorProgress getProgress() {
    return new LogExecutorProgress(
        running ? BenchmarkStatus.RUNNING : BenchmarkStatus.COMPLETE,
        opCounter.get(),
        new BigDecimal((endTime > 0 ? endTime : System.currentTimeMillis()) - startTime)
            .setScale(3, RoundingMode.HALF_UP)
            .divide(new BigDecimal(1000.0)));
  }

  @Override
  public void start(LogBenchmarkConfig config) {
    super.start(config);
    LOGGER.debug("operations: {}", config.getOperations());
    LOGGER.debug("concurrency: {}", config.getConcurrency());
    LOGGER.debug("entrySize: {}", config.getEntrySize());
    LOGGER.debug("mode: {}", config.getMode());

    running = true;

    runnerExecutor.execute(() -> run(config));
  }

  /**
   * Runs the benchmark asynchronously.
   */
  private void run(LogBenchmarkConfig config) {
    AsyncDistributedLog<String> log = atomix.<String>logBuilder()
        .withProtocol(DistributedLogProtocol.builder()
            .withRecovery(Recovery.RECOVER)
            .build())
        .build()
        .async();

    startTime = System.currentTimeMillis();

    switch (config.getMode()) {
      case PRODUCER:
        Random random = new Random();
        for (int i = 0; i < config.getConcurrency(); i++) {
          runProducer(log, randomString(config.getEntrySize(), random), config.getOperations());
        }
        break;
      case CONSUMER:
        runConsumer(log, config.getOperations());
        break;
    }
  }

  /**
   * Runs a producer test.
   *
   * @param log the distributed log to which to produce messages
   * @param entry the entry to produce
   * @param operations the number of entries to produce
   */
  private void runProducer(AsyncDistributedLog<String> log, String entry, int operations) {
    if (opCounter.incrementAndGet() <= operations) {
      log.produce(entry).whenCompleteAsync((result, error) -> {
        runProducer(log, entry, operations);
      }, runnerExecutor);
    } else {
      opCounter.set(operations);
      stop();
    }
  }

  /**
   * Runs a consumer test.
   *
   * @param log the distributed log from which to consume messages
   * @param operations the number of operations to consume
   */
  private void runConsumer(AsyncDistributedLog<String> log, int operations) {
    log.consume(record -> {
      if (opCounter.incrementAndGet() == operations) {
        stop();
      }
    });
  }

  @Override
  public void stop() {
    running = false;
    endTime = System.currentTimeMillis();
    runnerExecutor.shutdownNow();
    super.stop();
  }
}
