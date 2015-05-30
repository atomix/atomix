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
package net.kuujo.copycat.raft.log;

import net.kuujo.copycat.raft.log.entry.EntryFilter;
import net.kuujo.copycat.util.ExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Log compactor.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LogCompactor implements Runnable, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(LogCompactor.class);
  private static final int DEFAULT_COMPACTION_FACTOR = 10;
  private static final long DEFAULT_COMPACTION_INTERVAL = TimeUnit.HOURS.toMillis(1);
  private static final long COMPACT_INTERVAL = 60 * 1000;

  private final Log log;
  private final EntryFilter filter;
  private final ExecutionContext context;
  private int compactionFactor = DEFAULT_COMPACTION_FACTOR;
  private long compactionInterval = DEFAULT_COMPACTION_INTERVAL;
  private long commit;
  private long compact;
  private Compaction compaction;
  private long previousCompaction;
  private ScheduledFuture<?> future;

  public LogCompactor(Log log, EntryFilter filter, ExecutionContext context) {
    this.log = log;
    this.filter = filter;
    this.context = context;
  }

  /**
   * Sets the compaction factor.
   *
   * @param compactionFactor The compaction factor.
   */
  public void setCompactionFactor(int compactionFactor) {
    if (compactionFactor < 1)
      throw new IllegalArgumentException("compaction factor must be positive");
    this.compactionFactor = compactionFactor;
  }

  /**
   * Returns the compaction factor.
   *
   * @return The compaction factor.
   */
  public int getCompactionFactor() {
    return compactionFactor;
  }

  /**
   * Sets the compaction factor, returning the compaction strategy for method chaining.
   *
   * @param compactionFactor The compaction factor.
   * @return The log compactor.
   */
  public LogCompactor withCompactionFactor(int compactionFactor) {
    setCompactionFactor(compactionFactor);
    return this;
  }

  /**
   * Sets the interval at which major compaction is run.
   *
   * @param compactionInterval The interval at which major compaction is run.
   */
  public void setCompactionInterval(long compactionInterval) {
    if (compactionInterval < 1)
      throw new IllegalArgumentException("compaction interval must be positive");
    this.compactionInterval = compactionInterval;
  }

  /**
   * Returns the compaction interval.
   *
   * @return The interval at which major compaction is run.
   */
  public long getCompactionInterval() {
    return compactionInterval;
  }

  /**
   * Sets the interval at which major compaction is run.
   *
   * @param compactionInterval The interval at which major compaction is run.
   * @return The log compactor.
   */
  public LogCompactor withCompactionInterval(long compactionInterval) {
    setCompactionInterval(compactionInterval);
    return this;
  }

  /**
   * Opens the log compactor.
   */
  public void open() {
    future = context.scheduleAtFixedRate(this::run, COMPACT_INTERVAL, COMPACT_INTERVAL, TimeUnit.MILLISECONDS);
  }

  /**
   * Sets the compactor commit index.
   *
   * @param index The compactor commit index.
   */
  public void commit(long index) {
    this.commit = index;
  }

  /**
   * Sets the compactor compact index.
   *
   * @param index The compact index.
   */
  public void compact(long index) {
    this.compact = index;
  }

  /**
   * Compacts the log.
   */
  public void compact() {
    context.execute(this::run);
  }

  @Override
  public void run() {
    if (compaction == null || compaction.isComplete()) {
      if (System.currentTimeMillis() - previousCompaction > compactionInterval) {
        compaction = new MajorCompaction(compact, filter, context);
        previousCompaction = System.currentTimeMillis();
      } else {
        compaction = new MinorCompaction(commit, filter, context);
      }
      compaction.run(log.segments);
    }
  }

  @Override
  public void close() {
    if (future != null) {
      future.cancel(false);
      future = null;
    }
  }

}
