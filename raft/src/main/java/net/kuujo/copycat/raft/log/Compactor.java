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
  private static final long DEFAULT_COMPACTION_INTERVAL = TimeUnit.HOURS.toMillis(1);
  private static final long COMPACT_INTERVAL = TimeUnit.MINUTES.toMillis(1);

  private final Log log;
  private final EntryFilter filter;
  private final ExecutionContext context;
  private long compactionInterval = DEFAULT_COMPACTION_INTERVAL;
  private long commit;
  private long compact;
  private Compaction compaction;
  private long lastCompaction;
  private CompletableFuture<Void> compactFuture;
  private ScheduledFuture<?> scheduledFuture;

  public Compactor(Log log, EntryFilter filter, ExecutionContext context) {
    this.log = log;
    this.filter = filter;
    this.context = context;
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
  public Compactor withCompactionInterval(long compactionInterval) {
    setCompactionInterval(compactionInterval);
    return this;
  }

  /**
   * Opens the log compactor.
   */
  public void open() {
    scheduledFuture = context.scheduleAtFixedRate(this::compact, COMPACT_INTERVAL, COMPACT_INTERVAL, TimeUnit.MILLISECONDS);
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
  synchronized CompletableFuture<Void> compact() {
    if (compactFuture != null) {
      return compactFuture;
    }

    compactFuture = CompletableFuture.supplyAsync(() -> {
      if (compaction == null) {
        if (System.currentTimeMillis() - lastCompaction > compactionInterval) {
          compaction = new MajorCompaction(compact, filter, context);
          lastCompaction = System.currentTimeMillis();
        } else {
          compaction = new MinorCompaction(commit, filter, context);
        }
        return compaction;
      }
      return null;
    }, context).thenComposeAsync(c -> {
      if (compaction != null) {
        return compaction.run(log.segments).thenRun(() -> {
          synchronized (this) {
            compactFuture = null;
          }
          compaction = null;
        });
      }
      return CompletableFuture.completedFuture(null);
    }, context);
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
