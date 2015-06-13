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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Log compactor.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Compactor implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(Compactor.class);
  private static final long DEFAULT_MINOR_COMPACTION_INTERVAL = TimeUnit.MINUTES.toMillis(1);
  private static final long DEFAULT_MAJOR_COMPACTION_INTERVAL = TimeUnit.HOURS.toMillis(1);

  private final Log log;
  private EntryFilter filter;
  private ExecutionContext context;
  private long minorCompactionInterval = DEFAULT_MINOR_COMPACTION_INTERVAL;
  private long majorCompactionInterval = DEFAULT_MAJOR_COMPACTION_INTERVAL;
  private long commit;
  private long compact;
  private Compaction compaction;
  private long lastCompaction;
  private CompletableFuture<Void> compactFuture;
  private ScheduledFuture<?> scheduledFuture;

  public Compactor(Log log) {
    this.log = log;
  }

  /**
   * Sets the compactor entry filter.
   *
   * @param filter The compactor entry filter.
   * @return The log compactor.
   */
  public Compactor filter(EntryFilter filter) {
    if (filter == null)
      filter = (entry, compaction) -> CompletableFuture.completedFuture(true);
    this.filter = filter;
    return this;
  }

  /**
   * Sets the interval at which major compaction is run.
   *
   * @param compactionInterval The interval at which major compaction is run.
   */
  public void setMinorCompactionInterval(long compactionInterval) {
    if (compactionInterval < 1)
      throw new IllegalArgumentException("compaction interval must be positive");
    this.minorCompactionInterval = compactionInterval;
  }

  /**
   * Returns the compaction interval.
   *
   * @return The interval at which major compaction is run.
   */
  public long getMinorCompactionInterval() {
    return minorCompactionInterval;
  }

  /**
   * Sets the interval at which major compaction is run.
   *
   * @param compactionInterval The interval at which major compaction is run.
   * @return The log compactor.
   */
  public Compactor withMinorCompactionInterval(long compactionInterval) {
    setMinorCompactionInterval(compactionInterval);
    return this;
  }

  /**
   * Sets the interval at which major compaction is run.
   *
   * @param compactionInterval The interval at which major compaction is run.
   */
  public void setMajorCompactionInterval(long compactionInterval) {
    if (compactionInterval < 1)
      throw new IllegalArgumentException("compaction interval must be positive");
    this.majorCompactionInterval = compactionInterval;
  }

  /**
   * Returns the compaction interval.
   *
   * @return The interval at which major compaction is run.
   */
  public long getMajorCompactionInterval() {
    return majorCompactionInterval;
  }

  /**
   * Sets the interval at which major compaction is run.
   *
   * @param compactionInterval The interval at which major compaction is run.
   * @return The log compactor.
   */
  public Compactor withMajorCompactionInterval(long compactionInterval) {
    setMajorCompactionInterval(compactionInterval);
    return this;
  }

  /**
   * Opens the log compactor.
   */
  public void open(ExecutionContext context) {
    scheduledFuture = context.scheduleAtFixedRate(() -> compact(context), minorCompactionInterval, minorCompactionInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * Sets the compactor commit index.
   *
   * @param index The compactor commit index.
   */
  public void setCommitIndex(long index) {
    this.commit = index;
  }

  /**
   * Sets the compactor compact index.
   *
   * @param index The compact index.
   */
  public void setCompactIndex(long index) {
    this.compact = index;
  }

  /**
   * Compacts the log.
   */
  synchronized CompletableFuture<Void> compact(ExecutionContext context) {
    if (compactFuture != null) {
      return compactFuture;
    }

    compactFuture = CompletableFuture.supplyAsync(() -> {
      if (compaction == null) {
        if (System.currentTimeMillis() - lastCompaction > majorCompactionInterval) {
          compaction = new MajorCompaction(compact, filter, context);
          lastCompaction = System.currentTimeMillis();
        } else {
          compaction = new MinorCompaction(commit, filter, context);
        }
        return compaction;
      }
      return null;
    }, context).thenCompose(c -> {
      if (compaction != null) {
        return compaction.run(log.segments).thenRun(() -> {
          synchronized (this) {
            compactFuture = null;
          }
          compaction = null;
        });
      }
      return CompletableFuture.completedFuture(null);
    });
    return compactFuture;
  }

  @Override
  public void close() {
    if (compactFuture != null) {
      compactFuture.cancel(false);
      compactFuture = null;
    }
    if (scheduledFuture != null) {
      scheduledFuture.cancel(false);
      scheduledFuture = null;
    }
  }

}
