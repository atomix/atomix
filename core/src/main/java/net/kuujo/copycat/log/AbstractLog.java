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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.internal.util.Configs;

import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

/**
 * Abstract log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractLog extends AbstractLoggable implements Log {
  private final org.slf4j.Logger log = LoggerFactory.getLogger(getClass());
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
  @Override
  public Collection<LogSegment> segments() {
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
   * 
   * @throws IndexOutOfBoundsException if no segment exists for the {@code index}
   */
  LogSegment segment(long index) {
    assertIsOpen();
    Map.Entry<Long, LogSegment> segment = segments.floorEntry(index);
    Assert.index(index, segment != null, "Invalid log index %d", index);
    return segment.getValue();
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
  public synchronized void open() throws IOException {
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
  public long entries() {
    return segments.values().stream().mapToLong(LogSegment::entries).sum();
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
    LogSegment firstSegment = firstSegment();
    return firstSegment == null ? null : firstSegment.firstIndex();
  }

  @Override
  public Long lastIndex() {
    assertIsOpen();
    LogSegment lastSegment = lastSegment();
    return lastSegment == null ? null : lastSegment().lastIndex();
  }

  @Override
  public boolean containsIndex(long index) {
    Long firstIndex = firstIndex();
    return firstIndex != null && firstIndex <= index && index <= lastIndex();
  }

  /**
   * Returns the entry for the {@code index} by checking the current segment first, then looking up
   * the correct segment.
   */
  @Override
  @SuppressWarnings("resource")
  public ByteBuffer getEntry(long index) {
    assertIsOpen();
    LogSegment segment = currentSegment.containsIndex(index) ? currentSegment : segment(index);
    return segment.getEntry(index);
  }

  /**
   * Returns the entries for the given {@code from}-{@code to} range by starting at the current
   * segment first and looking up segments through segments as needed.
   */
  @Override
  public List<ByteBuffer> getEntries(long from, long to) {
    assertIsOpen();

    List<ByteBuffer> entries = new ArrayList<>();
    LogSegment segment = currentSegment;
    for (long i = from; i <= to; i++) {
      if (!segment.containsIndex(i))
        segment = segment(i);
      entries.add(segment.getEntry(i));
    }

    return entries;
  }

  @Override
  public void removeAfter(long index) {
    assertIsOpen();
    Long floorKey = segments.floorKey(index < 1 ? 1 : index);
    // Prevent concurrent modification exceptions when deleting removed segments.
    Map<Long, LogSegment> partitionedSegments = segments.tailMap(floorKey);
    for (LogSegment segment : Arrays.asList(partitionedSegments.values().toArray(
      new LogSegment[partitionedSegments.size()]))) {
      segment.removeAfter(index);
      if (segment.isEmpty()) {
        segment.delete();
      }
    }
  }

  @Override
  @SuppressWarnings("resource")
  public void compact(long index) {
    assertIsOpen();
    LogSegment segment = currentSegment.containsIndex(index) ? currentSegment : segment(index);
    segment.compact(index);
  }

  @Override
  @SuppressWarnings("resource")
  public void compact(long index, ByteBuffer entry) {
    assertIsOpen();
    LogSegment segment = currentSegment.containsIndex(index) ? currentSegment : segment(index);
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
  public synchronized void close() throws IOException {
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
    Long lastIndex = currentSegment.lastIndex();
    if (lastIndex == null)
      return;

    boolean segmentSizeExceeded = currentSegment.size() >= config.getSegmentSize();
    boolean segmentExpired = config.getSegmentInterval() < Long.MAX_VALUE
      && System.currentTimeMillis() > currentSegment.timestamp() + config.getSegmentInterval();

    if (segmentSizeExceeded || segmentExpired) {
      long nextIndex = lastIndex + 1;
      currentSegment.flush();
      currentSegment = createSegment(nextIndex);
      log.debug("Rolling over to new segment at new index {}", nextIndex);

      try {
        currentSegment.open();
      } catch (IOException e) {
        throw new LogException(e, "Failed to open new segment");
      }

      segments.put(nextIndex, currentSegment);
      lastFlush = System.currentTimeMillis();
      checkRetention();
    }
  }

  /**
   * Checks whether any existing segments need to be deleted. Does not allow the last log segment to
   * be checked.
   */
  private void checkRetention() {
    for (Iterator<Map.Entry<Long, LogSegment>> i = segments.entrySet().iterator(); i.hasNext();) {
      Map.Entry<Long, LogSegment> entry = i.next();
      if (!segments.isEmpty() && segments.lastKey() != entry.getKey()
        && !config.getRetentionPolicy().retain(entry.getValue())) {
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
