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

import io.atomix.bench.BenchmarkExecutor;
import io.atomix.bench.BenchmarkStatus;
import io.atomix.bench.ExecutorProgress;
import io.atomix.core.Atomix;
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static io.atomix.bench.util.Strings.randomStrings;
import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Map benchmark executor.
 */
public class MapBenchmarkExecutor extends BenchmarkExecutor<MapBenchmarkConfig> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MapBenchmarkExecutor.class);

  private final Atomix atomix;

  private final ExecutorService runnerExecutor = Executors.newSingleThreadExecutor(
      namedThreads("atomix-bench-runner", LOGGER));
  private long startTime;
  private long endTime;
  private volatile boolean running;
  private final AtomicInteger opCounter = new AtomicInteger();
  private final AtomicInteger readCounter = new AtomicInteger();
  private final AtomicInteger writeCounter = new AtomicInteger();
  private final AtomicInteger eventCounter = new AtomicInteger();

  public MapBenchmarkExecutor(Atomix atomix) {
    super(atomix);
    this.atomix = atomix;
  }

  @Override
  public ExecutorProgress getProgress() {
    return new MapExecutorProgress(
        running ? BenchmarkStatus.RUNNING : BenchmarkStatus.COMPLETE,
        opCounter.get(),
        readCounter.get(),
        writeCounter.get(),
        eventCounter.get(),
        new BigDecimal((endTime > 0 ? endTime : System.currentTimeMillis()) - startTime)
            .setScale(3, RoundingMode.HALF_UP)
            .divide(new BigDecimal(1000.0)));
  }

  @Override
  public void start(MapBenchmarkConfig config) {
    super.start(config);
    LOGGER.debug("operations: {}", config.getOperations());
    LOGGER.debug("concurrency: {}", config.getConcurrency());
    LOGGER.debug("writePercentage: {}", config.getWritePercentage());
    LOGGER.debug("numKeys: {}", config.getNumKeys());
    LOGGER.debug("keyLength: {}", config.getKeyLength());
    LOGGER.debug("numValues: {}", config.getNumValues());
    LOGGER.debug("valueLength: {}", config.getValueLength());
    LOGGER.debug("includeEvents: {}", config.isIncludeEvents());
    LOGGER.debug("deterministic: {}", config.isDeterministic());

    running = true;
    if (config.isDeterministic()) {
      runnerExecutor.submit(new DeterministicSubmitter(config));
    } else {
      runnerExecutor.submit(new NonDeterministicSubmitter(config));
    }
  }

  @Override
  public void stop() {
    running = false;
    endTime = System.currentTimeMillis();
    runnerExecutor.shutdownNow();
    super.stop();
  }

  /**
   * Base submitter for primitive operations.
   */
  abstract class Submitter implements Runnable {
    final MapBenchmarkConfig config;
    final String[] keys;
    final String[] values;
    final Random random = new Random();
    AsyncAtomicMap<String, String> map;

    Submitter(MapBenchmarkConfig config) {
      this.config = config;
      this.keys = randomStrings(config.getKeyLength(), config.getNumKeys());
      this.values = randomStrings(config.getValueLength(), config.getNumValues());
    }

    @Override
    public void run() {
      setup();
      startTime = System.currentTimeMillis();
      try {
        Futures.allOf(IntStream.range(0, config.getConcurrency()).mapToObj(i -> execute())).join();
      } catch (Exception e) {
        LOGGER.warn("Exception in benchmark runner", e);
      }
      teardown();
    }

    CompletableFuture<Void> execute() {
      return execute(new CompletableFuture<>());
    }

    private CompletableFuture<Void> execute(CompletableFuture<Void> future) {
      if (running && opCounter.incrementAndGet() <= config.getOperations()) {
        submit().whenComplete((r, e) -> execute(future));
      } else {
        opCounter.set(config.getOperations());
        future.complete(null);
      }
      return future;
    }

    CompletableFuture<Void> read(String key) {
      return map.get(key).whenComplete((r, e) -> readCounter.incrementAndGet()).thenApply(v -> null);
    }

    CompletableFuture<Void> write(String key, String value) {
      return map.put(key, value).whenComplete((r, e) -> writeCounter.incrementAndGet()).thenApply(v -> null);
    }

    @SuppressWarnings("unchecked")
    void setup() {
      ProxyProtocol protocol;
      if (config.getProtocol() == null) {
        protocol = atomix.getPartitionService().getPartitionGroups().iterator().next().newProtocol();
      } else {
        protocol = (ProxyProtocol) config.getProtocol().getType().newProtocol(config.getProtocol());
      }
      map = atomix.<String, String>atomicMapBuilder(config.getBenchId())
          .withSerializer(Serializer.using(Namespaces.BASIC))
          .withProtocol(protocol)
          .build()
          .async();
      if (config.isIncludeEvents()) {
        map.addListener(event -> {
          eventCounter.incrementAndGet();
        });
      }
    }

    abstract CompletableFuture<Void> submit();

    void teardown() {
      map.sync().close();
      stop();
    }
  }

  private class NonDeterministicSubmitter extends Submitter {
    NonDeterministicSubmitter(MapBenchmarkConfig config) {
      super(config);
    }

    @Override
    CompletableFuture<Void> submit() {
      String key = keys[random.nextInt(keys.length)];
      if (random.nextInt(100) < config.getWritePercentage()) {
        return write(key, values[random.nextInt(values.length)]);
      } else {
        return read(key);
      }
    }
  }

  private class DeterministicSubmitter extends Submitter {
    private int index;

    DeterministicSubmitter(MapBenchmarkConfig config) {
      super(config);
    }

    @Override
    CompletableFuture<Void> submit() {
      if (random.nextInt(100) < config.getWritePercentage()) {
        return write(keys[index++ % keys.length], values[random.nextInt(values.length)]);
      } else {
        return read(keys[random.nextInt(keys.length)]);
      }
    }
  }
}
