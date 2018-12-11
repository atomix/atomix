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
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Map benchmark executor.
 */
public class MapBenchmarkExecutor implements BenchmarkExecutor<MapBenchmarkConfig> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkExecutor.class);

  private static final int REPORT_PERIOD = 1_000;

  private static final char[] CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();

  private final Atomix atomix;
  private MapBenchmarkConfig config;

  private final ExecutorService runnerExecutor = Executors.newSingleThreadExecutor(
      namedThreads("atomix-bench-runner", LOGGER));
  private final ScheduledExecutorService reporterExecutor = Executors.newSingleThreadScheduledExecutor(
      namedThreads("atomix-bench-reporter", LOGGER));
  private long startTime;
  private volatile boolean running;
  private final AtomicInteger opCounter = new AtomicInteger();
  private final AtomicInteger readCounter = new AtomicInteger();
  private final AtomicInteger writeCounter = new AtomicInteger();
  private final AtomicInteger eventCounter = new AtomicInteger();

  public MapBenchmarkExecutor(Atomix atomix) {
    this.atomix = atomix;
  }

  @Override
  public void start(MapBenchmarkConfig config) {
    this.config = config;
    LOGGER.info("Running benchmark {}", config.getBenchId());

    LOGGER.debug("operations: {}", config.getOperations());
    LOGGER.debug("concurrency: {}", config.getConcurrency());
    LOGGER.debug("writePercentage: {}", config.getWritePercentage());
    LOGGER.debug("numKeys: {}", config.getNumKeys());
    LOGGER.debug("keyLength: {}", config.getKeyLength());
    LOGGER.debug("numValues: {}", config.getNumValues());
    LOGGER.debug("valueLength: {}", config.getValueLength());
    LOGGER.debug("includeEvents: {}", config.isIncludeEvents());
    LOGGER.debug("deterministic: {}", config.isDeterministic());

    reporterExecutor.scheduleAtFixedRate(() -> report(config), REPORT_PERIOD, REPORT_PERIOD, TimeUnit.MILLISECONDS);
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
    runnerExecutor.shutdownNow();
    reporterExecutor.shutdownNow();
    report(config);
  }

  /**
   * Sends a progress report to the controller node.
   */
  private void report(BenchmarkConfig config) {
    RunnerProgress report = new RunnerProgress(
        running ? BenchmarkState.RUNNING : BenchmarkState.COMPLETE,
        opCounter.get(),
        readCounter.get(),
        writeCounter.get(),
        eventCounter.get(),
        System.currentTimeMillis() - startTime);
    atomix.getCommunicationService().broadcastIncludeSelf(config.getBenchId(), report, BenchmarkSerializer.INSTANCE::encode, false);
  }

  /**
   * Creates a deterministic array of strings to write to the cluster.
   *
   * @param length the string lengths
   * @param count  the string count
   * @return a deterministic array of strings
   */
  private String[] createStrings(int length, int count) {
    Random random = new Random(length);
    List<String> stringsList = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      stringsList.add(randomString(length, random));
    }
    return stringsList.toArray(new String[stringsList.size()]);
  }

  /**
   * Creates a deterministic string based on the given seed.
   *
   * @param length the seed from which to create the string
   * @param random the random object from which to create the string characters
   * @return the string
   */
  private String randomString(int length, Random random) {
    char[] buffer = new char[length];
    for (int i = 0; i < length; i++) {
      buffer[i] = CHARS[random.nextInt(CHARS.length)];
    }
    return new String(buffer);
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
      this.keys = createStrings(config.getKeyLength(), config.getNumKeys());
      this.values = createStrings(config.getValueLength(), config.getNumValues());
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
      ProxyProtocol protocol = (ProxyProtocol) config.getProtocol().getType().newProtocol(config.getProtocol());
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
