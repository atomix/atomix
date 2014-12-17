/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.log;

import com.typesafe.config.Config;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.internal.util.Configs;
import net.kuujo.copycat.internal.util.concurrent.NamedThreadFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Abstract log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractLog extends AbstractLogger implements Log {
  private static final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("log-compactor-%d"));
  protected final LogConfig config;
  protected final File base;
  protected final TreeMap<Long, LogSegment> segments = new TreeMap<>();
  protected LogSegment currentSegment;
  private long lastFlush;

  protected AbstractLog(String resource) {
    this(Configs.load(resource, "copycat.log").toConfig());
  }

  protected AbstractLog(Map<String, Object> config) {
    this(Configs.load(config, "copycat.log").toConfig());
  }

  protected AbstractLog(Config config) {
    this(config.getString("name"), new LogConfig(config));
  }

  protected AbstractLog(String name, LogConfig config) {
    this.config = config.copy();
    this.base = new File(config.getDirectory(), name);
  }

  /**
   * Loads all log segments.
   *
   * @return A collection of all existing log segments.
   */
  protected abstract Collection<LogSegment> loadSegments();

  /**
   * Creates a new log segment.
   *
   * @param segmentNumber The log segment number.
   * @return A new log segment.
   */
  protected abstract LogSegment createSegment(long segmentNumber);

  /**
   * Deletes a log segment.
   *
   * @param segmentNumber The log segment number.
   */
  protected void deleteSegment(long segmentNumber) {
    segments.remove(segmentNumber);
  }

  @Override
  public LogConfig config() {
    return config;
  }

  @Override
  public File base() {
    return base;
  }

  @Override
  public File directory() {
    return base.getParentFile();
  }

  /**
   * Returns a collection of log segments.
   */
  Collection<LogSegment> segments() {
    return segments.values();
  }

  /**
   * Returns the current log segment.
   */
  LogSegment segment() {
    return currentSegment;
  }

  /**
   * Returns a log segment by index.
   */
  LogSegment segment(long index) {
    assertIsOpen();
    Map.Entry<Long, LogSegment> segment = segments.floorEntry(index);
    return segment != null ? segment.getValue() : null;
  }

  /**
   * Returns the first log segment.
   */
  LogSegment firstSegment() {
    assertIsOpen();
    Map.Entry<Long, LogSegment> segment = segments.firstEntry();
    return segment != null ? segment.getValue() : null;
  }

  /**
   * Returns the last log segment.
   */
  LogSegment lastSegment() {
    assertIsOpen();
    Map.Entry<Long, LogSegment> segment = segments.lastEntry();
    return segment != null ? segment.getValue() : null;
  }

  @Override
  public synchronized void open() {
    assertIsNotOpen();
    if (!config.getDirectory().exists()) {
      config.getDirectory().mkdirs();
    }
    for (LogSegment segment : loadSegments()) {
      segment.open();
      segments.put(segment.segment(), segment);
    }
    if (!segments.isEmpty()) {
      currentSegment = segments.lastEntry().getValue();
    } else {
      currentSegment = createSegment(1);
      currentSegment.open();
      segments.put(1L, currentSegment);
    }
  }

  @Override
  public boolean isOpen() {
    return currentSegment != null;
  }

  @Override
  public long size() {
    assertIsOpen();
    return segments.values().stream().mapToLong(LogSegment::size).sum();
  }

  @Override
  public boolean isEmpty() {
    return firstIndex() == null;
  }

  @Override
  public long appendEntry(ByteBuffer entry) {
    assertIsOpen();
    checkRollOver();
    checkFlush();
    return currentSegment.appendEntry(entry);
  }

  @Override
  public List<Long> appendEntries(List<ByteBuffer> entries) {
    assertIsOpen();
    checkRollOver();
    checkFlush();
    return currentSegment.appendEntries(entries);
  }

  @Override
  public Long firstIndex() {
    assertIsOpen();
    return firstSegment().firstIndex();
  }

  @Override
  public Long lastIndex() {
    assertIsOpen();
    return lastSegment().lastIndex();
  }

  @Override
  public boolean containsIndex(long index) {
    Long firstIndex = firstIndex();
    return firstIndex != null && firstIndex <= index && index <= lastIndex();
  }

  @Override
  public ByteBuffer getEntry(long index) {
    assertIsOpen();
    return currentSegment.getEntry(index);
  }

  @Override
  public List<ByteBuffer> getEntries(long from, long to) {
    assertIsOpen();
    return currentSegment.getEntries(from, to);
  }

  @Override
  public void removeAfter(long index) {
    assertIsOpen();
    Long floorKey = segments.floorKey(index < 1 ? 1 : index);
    // Prevent concurrent modification exceptions when deleting removed segments.
    Map<Long, LogSegment> partitionedSegments = segments.tailMap(floorKey);
    for (LogSegment segment : Arrays.asList(partitionedSegments.values().toArray(new LogSegment[partitionedSegments.size()]))) {
      segment.removeAfter(index);
      if (segment.isEmpty()) {
        segment.delete();
      }
    }
  }

  @Override
  public void compact(long index) {
    assertIsOpen();
    LogSegment segment = segment(index);
    Assert.index(index, segment != null, "Invalid log index %d", index);
    segment.compact(index);
  }

  @Override
  public void compact(long index, ByteBuffer entry) {
    assertIsOpen();
    LogSegment segment = segment(index);
    Assert.index(index, segment != null, "Invalid log index %d", index);
    segment.compact(index, entry);
  }

  @Override
  public void flush() {
    assertIsOpen();
    currentSegment.flush();
  }

  @Override
  public void flush(boolean force) {
    assertIsOpen();
    currentSegment.flush(force);
  }

  @Override
  public synchronized void close() {
    for (LogSegment segment : segments.values())
      segment.close();
    segments.clear();
    currentSegment = null;
  }

  @Override
  public boolean isClosed() {
    return currentSegment == null;
  }

  @Override
  public void delete() {
    for (LogSegment segment : segments.values())
      segment.delete();
    segments.clear();
  }

  /**
   * Checks whether the current segment needs to be rolled over to a new segment.
   */
  private void checkRollOver() {
    boolean segmentSizeExceeded = currentSegment.size() >= config.getSegmentSize();
    boolean segmentExpired = config.getSegmentInterval() < Long.MAX_VALUE && System.currentTimeMillis() > currentSegment.timestamp() + config.getSegmentInterval();

    if (segmentSizeExceeded || segmentExpired) {
      long nextIndex = currentSegment.lastIndex() + 1;
      currentSegment.flush();
      currentSegment = createSegment(nextIndex);
      currentSegment.open();
      segments.put(nextIndex, currentSegment);
      lastFlush = System.currentTimeMillis();
      checkRetention();
    }
  }

  /**
   * Checks whether any existing segments need to be deleted. Does not allow the last log segment to be checked.
   */
  private void checkRetention() {
    for (Iterator<Map.Entry<Long, LogSegment>> i = segments.entrySet().iterator(); i.hasNext();) {
      Map.Entry<Long, LogSegment> entry = i.next();
      if (!segments.isEmpty() && segments.lastKey() != entry.getKey() && !config.getRetentionPolicy().retain(entry.getValue())) {
        i.remove();
      }
    }
  }

  /**
   * Checks whether the current segment needs to be flushed to disk.
   */
  private void checkFlush() {
    if (System.currentTimeMillis() - lastFlush > config.getFlushInterval()) {
      flush(true);
    }
  }

}
