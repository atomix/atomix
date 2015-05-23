/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.raft.storage.compact;

import net.kuujo.copycat.raft.storage.RaftEntryFilter;
import net.kuujo.copycat.raft.storage.SegmentManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Performs log compaction tasks.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Compactor implements Runnable, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(Compactor.class);
  private final SegmentManager manager;
  private RaftEntryFilter filter = entry -> true;
  private CompactionStrategy compactionStrategy;
  private ScheduledExecutorService executor;
  private ScheduledFuture<?> scheduledFuture;
  private final AtomicBoolean running = new AtomicBoolean();

  public Compactor(SegmentManager manager) {
    this.manager = manager;
  }

  /**
   * Sets the entry filter.
   *
   * @param filter The entry filter.
   * @return The log compactor.
   */
  public Compactor withEntryFilter(RaftEntryFilter filter) {
    this.filter = filter != null ? filter : entry -> true;
    return this;
  }

  /**
   * Sets the compaction strategy.
   *
   * @param compactionStrategy The compaction strategy.
   */
  public Compactor withCompactionStrategy(CompactionStrategy compactionStrategy) {
    this.compactionStrategy = compactionStrategy;
    return this;
  }

  /**
   * Schedules compaction for the given interval.
   *
   * @param interval The interval for which to schedule compaction.
   */
  public void schedule(long interval) {
    schedule(interval, TimeUnit.MILLISECONDS);
  }

  /**
   * Schedules compaction for the given interval.
   *
   * @param interval The interval for which to schedule compaction.
   * @param unit The interval time unit.
   */
  public void schedule(long interval, TimeUnit unit) {
    if (scheduledFuture != null)
      scheduledFuture.cancel(true);
    LOGGER.debug("Scheduling compaction: {} {}", interval, unit);
    if (executor == null)
      executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("log-compactor-%d"));
    scheduledFuture = executor.scheduleAtFixedRate(this, interval, interval, unit);
  }

  /**
   * Executes the compactor.
   */
  public void execute() {
    if (executor == null)
      executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("log-compactor-%d"));
    executor.execute(this);
  }

  @Override
  public void run() {
    if (running.compareAndSet(false, true)) {
      compact();
      running.set(false);
    }
  }

  /**
   * Compacts committed segments in the log.
   */
  private void compact() {
    LOGGER.info("Starting log compaction...");
    if (compactionStrategy != null)
      compactionStrategy.compact(filter, manager);
  }

  @Override
  public void close() {
    executor.shutdown();
    executor = null;
  }

  /**
   * Named thread factory.
   */
  private static class NamedThreadFactory implements ThreadFactory {
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String nameFormat;

    /**
     * Creates a thread factory that names threads according to the {@code nameFormat} by supplying a
     * single argument to the format representing the thread number.
     */
    public NamedThreadFactory(String nameFormat) {
      this.nameFormat = nameFormat;
    }

    @Override
    public Thread newThread(Runnable r) {
      return new Thread(r, String.format(nameFormat, threadNumber.getAndIncrement()));
    }
  }
}
