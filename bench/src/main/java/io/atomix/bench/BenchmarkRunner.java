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
import io.atomix.core.map.AtomicMap;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Benchmark runner.
 */
public class BenchmarkRunner {
  private final Logger log = LoggerFactory.getLogger(getClass());

  private static final int REPORT_PERIOD = 1_000;

  private static final char[] CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();

  private final Atomix atomix;
  private final BenchmarkConfig config;

  private final ExecutorService runnerExecutor = Executors.newSingleThreadExecutor(namedThreads("atomix-bench-runner", log));
  private final ScheduledExecutorService reporterExecutor = Executors.newSingleThreadScheduledExecutor(namedThreads("atomix-bench-reporter", log));
  private long startTime;
  private volatile boolean running;
  private final AtomicInteger totalCounter = new AtomicInteger();
  private final AtomicInteger readCounter = new AtomicInteger();
  private final AtomicInteger writeCounter = new AtomicInteger();

  public BenchmarkRunner(Atomix atomix, BenchmarkConfig config) {
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
   * Starts the benchmark runner.
   */
  public void start() {
    reporterExecutor.scheduleAtFixedRate(this::report, REPORT_PERIOD, REPORT_PERIOD, TimeUnit.MILLISECONDS);
    running = true;
    if (config.isDeterministic()) {
      runnerExecutor.submit(new DeterministicSubmitter());
    } else {
      runnerExecutor.submit(new NonDeterministicSubmitter());
    }
  }

  /**
   * Stops the benchmark runner.
   */
  public void stop() {
    running = false;
    runnerExecutor.shutdownNow();
    reporterExecutor.shutdownNow();
  }

  /**
   * Sends a progress report to the controller node.
   */
  private void report() {
    BenchmarkProgress report = new BenchmarkProgress(
        running ? BenchmarkState.RUNNING : BenchmarkState.COMPLETE,
        totalCounter.get(),
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
    final String[] keys;
    final String[] values;
    final Random random = new Random();
    AtomicMap<String, String> map;

    Submitter() {
      this.keys = createStrings(config.getKeyLength(), config.getNumKeys());
      this.values = createStrings(config.getValueLength(), config.getNumValues());
    }

    @Override
    public void run() {
      setup();
      startTime = System.currentTimeMillis();
      while (running) {
        try {
          submit();
        } catch (Exception e) {
          log.warn("Exception during cycle", e);
        } finally {
          totalCounter.incrementAndGet();
        }
      }
      teardown();
    }

    void read(String key) {
      map.get(key);
      readCounter.incrementAndGet();
    }

    void write(String key, String value) {
      map.put(key, value);
      writeCounter.incrementAndGet();
    }

    @SuppressWarnings("unchecked")
    void setup() {
      ProxyProtocol protocol = (ProxyProtocol) atomix.getRegistry().getType(PrimitiveProtocol.Type.class, "foo").newProtocol(config.getProtocol());
      map = atomix.<String, String>atomicMapBuilder("bench")
          .withSerializer(Serializer.using(Namespaces.BASIC))
          .withProtocol(protocol)
          .build();
      if (config.isIncludeEvents()) {
        map.addListener(event -> {
        });
      }
    }

    abstract void submit();

    void teardown() {
      //map.destroy();
    }
  }

  private class NonDeterministicSubmitter extends Submitter {
    @Override
    void submit() {
      String key = keys[random.nextInt(keys.length)];
      if (random.nextInt(100) < config.getWritePercentage()) {
        write(key, values[random.nextInt(values.length)]);
      } else {
        read(key);
      }
    }
  }

  private class DeterministicSubmitter extends Submitter {
    private int index;

    @Override
    void submit() {
      if (random.nextInt(100) < config.getWritePercentage()) {
        write(keys[index++ % keys.length], values[random.nextInt(values.length)]);
      } else {
        read(keys[random.nextInt(keys.length)]);
      }
    }
  }
}
