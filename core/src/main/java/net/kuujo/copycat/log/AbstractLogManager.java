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

import net.kuujo.copycat.internal.util.Assert;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Abstract log. Not threadsafe.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractLogManager extends AbstractLoggable implements LogManager {
  private final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(getClass());
  private Log config;
  protected final TreeMap<Long, LogSegment> segments = new TreeMap<>();
  protected LogSegment currentSegment;
  private long nextSegmentId;
  private long lastFlush;

  protected AbstractLogManager(Log config) {
    this.config = (Log) config.copy();
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
   * @param segmentId The log segment id.
   * @param firstIndex The index at which the segment starts.
   * @return A new log segment.
   */
  protected abstract LogSegment createSegment(long segmentId, long firstIndex);

  /**
   * Deletes a log segment.
   *
   * @param firstIndex The first index of the segment to delete
   */
  protected void deleteSegment(long firstIndex) {
    segments.remove(firstIndex);
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
    for (LogSegment segment : loadSegments()) {
      segment.open();
      segments.put(segment.firstIndex(), segment);
    }
    if (!segments.isEmpty()) {
      currentSegment = segments.lastEntry().getValue();
    } else {
      createInitialSegment();
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
  public long entryCount() {
    return segments.values().stream().mapToLong(LogSegment::entryCount).sum();
  }

  @Override
  public boolean isEmpty() {
    LogSegment firstSegment = firstSegment();
    return firstSegment == null || firstSegment.size() == 0;
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
    List<Long> indices = new ArrayList<>(entries.size());
    for (ByteBuffer entry : entries) {
      checkRollOver();
      indices.add(currentSegment.appendEntry(entry));
    }

    checkFlush();
    return indices;
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
    Long lastIndex = lastIndex();
    return firstIndex != null && lastIndex != null && firstIndex <= index && index <= lastIndex;
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
   * Returns the entries for the given {@code from}-{@code to} range, inclusive, by starting at the
   * current segment first and looking up segments through segments as needed.
   */
  @Override
  public List<ByteBuffer> getEntries(long from, long to) {
    assertIsOpen();
    assertContainsIndex(from);
    assertContainsIndex(to);

    List<ByteBuffer> entries = new ArrayList<>((int) (to - from + 1));

    // If the current segment contains the first index then just get the entries.
    if (currentSegment.containsIndex(from)) {
      return currentSegment.getEntries(from, to);
    }

    // Iterate through segments and build the entries list.
    for (LogSegment segment : segments.values()) {
      if (segment.containsIndex(from) && segment.containsIndex(to)) {
        return segment.getEntries(from, to);
      } else if (segment.containsIndex(from)) {
        entries.addAll(segment.getEntries(from, segment.lastIndex()));
      } else if (segment.containsIndex(to)) {
        entries.addAll(segment.getEntries(segment.firstIndex(), to));
      } else if (segment.firstIndex() != null && segment.firstIndex() > from && segment.lastIndex() != null && segment.lastIndex() < to) {
        entries.addAll(segment.getEntries(segment.firstIndex(), segment.lastIndex()));
      }
    }

    return entries;
  }

  @Override
  public void removeAfter(long index) {
    assertIsOpen();
    assertContainsIndex(index + 1);
    Long segmentIndex = segments.floorKey(index < 1 ? 1 : index);
    // Segments to inspect for removal
    Collection<LogSegment> removalSegments = segments.tailMap(segmentIndex).values();
    for (Iterator<LogSegment> i = removalSegments.iterator(); i.hasNext();) {
      LogSegment segment = i.next();
      if (index < segment.firstIndex()) {
        segment.delete();
        i.remove();
        nextSegmentId--;
      } else {
        segment.removeAfter(index);
      }
    }

    Map.Entry<Long, LogSegment> lastSegment = segments.lastEntry();
    if (lastSegment != null) {
      currentSegment = lastSegment.getValue();
    } else {
      try {
        createInitialSegment();
      } catch (IOException e) {
        throw new LogException(e, "Failed to open new segment");
      }
    }
  }

  @Override
  public void compact(long index, ByteBuffer entry) throws IOException {
    assertIsOpen();
    assertContainsIndex(index);
    LogSegment segment = null;
    for (Iterator<LogSegment> i = segments.values().iterator(); i.hasNext();) {
      segment = i.next();
      i.remove();
      if (segment.lastIndex() < index) {
        segment.delete();
      } else {
        segment.compact(index, entry);
        segments.put(segment.firstIndex(), segment);
        break;
      }
    }

    currentSegment = segments.lastEntry().getValue();
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

  @Override
  public String toString() {
    return segments.toString();
  }
  
  private void createInitialSegment() throws IOException {
    currentSegment = createSegment(++nextSegmentId, 1);
    currentSegment.open();
    segments.put(Long.valueOf(1), currentSegment);
  }

  /**
   * Checks whether the current segment needs to be rolled over to a new segment.
   * 
   * @throws LogException if a new segment cannot be opened
   */
  private void checkRollOver() {
    Long lastIndex = currentSegment.lastIndex();
    if (lastIndex == null)
      return;

    boolean segmentSizeExceeded = currentSegment.size() >= config.getSegmentSize();
    boolean segmentExpired = config.getSegmentInterval() < Long.MAX_VALUE
      && System.currentTimeMillis() > currentSegment.timestamp() + config.getSegmentInterval();

    if (segmentSizeExceeded || segmentExpired) {
      currentSegment.flush();
      long nextIndex = lastIndex + 1;
      currentSegment = createSegment(++nextSegmentId, nextIndex);
      LOGGER.debug("Rolling over to new segment at new index {}", nextIndex);

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
      if (!segments.isEmpty() && !segments.lastKey().equals(entry.getKey()) && !config.getRetentionPolicy().retain(entry.getValue())) {
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
