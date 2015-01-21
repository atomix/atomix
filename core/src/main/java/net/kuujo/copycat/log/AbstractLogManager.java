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
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

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
    this.config = config.copy();
  }

  @Override
  public LogConfig config() {
    return config;
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
  @Override
  public TreeMap<Long, LogSegment> segments() {
    return segments;
  }

  /**
   * Returns the current log segment.
   */
  @Override
  public LogSegment segment() {
    return currentSegment;
  }

  /**
   * Returns a log segment by index.
   * 
   * @throws IndexOutOfBoundsException if no segment exists for the {@code index}
   */
  @Override
  public LogSegment segment(long index) {
    assertIsOpen();
    Map.Entry<Long, LogSegment> segment = segments.floorEntry(index);
    Assert.index(index, segment != null, "Invalid log index %d", index);
    return segment.getValue();
  }

  /**
   * Returns the first log segment.
   */
  @Override
  public LogSegment firstSegment() {
    assertIsOpen();
    Map.Entry<Long, LogSegment> segment = segments.firstEntry();
    return segment != null ? segment.getValue() : null;
  }

  /**
   * Returns the last log segment.
   */
  @Override
  public LogSegment lastSegment() {
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
  public long appendEntry(ByteBuffer entry) throws IOException {
    assertIsOpen();
    checkRollOver();
    return currentSegment.appendEntry(entry);
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
  public LogSegment rollOver() throws IOException {
    // If the current segment is empty then don't roll over to a new segment, just keep the existing segment.
    Long lastIndex = currentSegment.lastIndex();
    if (lastIndex == null)
      return currentSegment;

    // Flush the segment and create a new segment.
    currentSegment.flush();
    long nextIndex = lastIndex + 1;
    currentSegment = createSegment(++nextSegmentId, nextIndex);
    LOGGER.debug("Rolling over to new segment at new index {}", nextIndex);

    // Open the new segment.
    currentSegment.open();

    segments.put(nextIndex, currentSegment);

    // Reset the segment flush time and check whether old segments need to be deleted.
    lastFlush = System.currentTimeMillis();
    return currentSegment;
  }

  @Override
  public void flush() {
    assertIsOpen();
    // Only flush the current segment is flush-on-write is enabled or the flush timeout has passed since the last flush.
    // Flushes will be attempted each time the algorithm is done writing entries to the log.
    if (config.isFlushOnWrite()) {
      currentSegment.flush();
    } else if (System.currentTimeMillis() - lastFlush > config.getFlushInterval()) {
      currentSegment.flush();
      lastFlush = System.currentTimeMillis();
    }
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

  /**
   * Creates an initial segment for the log.
   *
   * @throws IOException If the segment could not be opened.
   */
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
  private void checkRollOver() throws IOException {
    Long lastIndex = currentSegment.lastIndex();
    if (lastIndex == null)
      return;

    boolean segmentSizeExceeded = currentSegment.size() >= config.getSegmentSize();
    boolean segmentExpired = config.getSegmentInterval() < Long.MAX_VALUE
      && System.currentTimeMillis() > currentSegment.timestamp() + config.getSegmentInterval();

    if (segmentSizeExceeded || segmentExpired) {
      rollOver();
    }
  }

}
