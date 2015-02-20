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

import net.kuujo.copycat.util.internal.Assert;
import org.slf4j.Logger;
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
  private final Logger LOGGER = LoggerFactory.getLogger(getClass());
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

    // Load existing log segments from disk.
    for (LogSegment segment : loadSegments()) {
      segment.open();
      segments.put(segment.index(), segment);
    }

    // If a segment doesn't already exist, create an initial segment starting at index 1.
    if (!segments.isEmpty()) {
      currentSegment = segments.lastEntry().getValue();
    } else {
      createInitialSegment();
    }

    clean();
  }

  /**
   * Cleans the log at startup.
   *
   * In the event that a failure occurred during compaction, it's possible that the log could contain a significant
   * gap in indexes between segments. When the log is opened, check segments to ensure that first and last indexes of
   * each segment agree with other segments in the log. If not, remove all segments that appear prior to any index gap.
   */
  private void clean() throws IOException {
    Long lastIndex = null;
    Long compactIndex = null;
    for (LogSegment segment : segments.values()) {
      if (segment.index() != segments.lastKey() && (lastIndex == null || segment.index() > lastIndex + 1)) {
        compactIndex = segment.index();
      }
      lastIndex = segment.lastIndex();
    }

    if (compactIndex != null) {
      compact(compactIndex);
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
    assertIsOpen();
    return segments.values().stream().mapToLong(LogSegment::entryCount).sum();
  }

  @Override
  public boolean isEmpty() {
    assertIsOpen();
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
  public long index() {
    assertIsOpen();
    LogSegment firstSegment = firstSegment();
    return firstSegment == null ? 1 : firstSegment.index();
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
    return lastSegment == null ? null : lastSegment.lastIndex();
  }

  @Override
  public boolean containsIndex(long index) {
    assertIsOpen();
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
      if (index < segment.index()) {
        segment.delete();
        i.remove();
        nextSegmentId--;
      } else {
        segment.removeAfter(index);
      }
    }

    LogSegment lastSegment = lastSegment();
    if (lastSegment != null) {
      currentSegment = lastSegment;
    } else {
      try {
        createInitialSegment();
      } catch (IOException e) {
        throw new LogException(e, "Failed to open new segment");
      }
    }
  }

  @Override
  public void roll(long index) throws IOException {
    // If the current segment is empty then just remove it.
    if (currentSegment.isEmpty()) {
      segments.remove(currentSegment.index());
      currentSegment.close();
      currentSegment.delete();
      currentSegment = null;
    } else {
      currentSegment.flush();
    }

    currentSegment = createSegment(++nextSegmentId, index);
    LOGGER.debug("Rolling over to new segment at new index {}", index);

    // Open the new segment.
    currentSegment.open();

    segments.put(index, currentSegment);

    // Reset the segment flush time and check whether old segments need to be deleted.
    lastFlush = System.currentTimeMillis();
  }

  @Override
  public void compact(long index) throws IOException {
    Assert.index(index, index >= index() && (lastIndex() == null || index <= lastIndex()), "%s is invalid for the log", index);
    Assert.arg(index, segments.containsKey(index), "%s must be the first index of a segment", index);
    
    // Iterate through all segments in the log. If a segment's first index matches the given index or its last index
    // is less than the given index then remove/close/delete the segment.
    LOGGER.debug("Compacting log at index {}", index);
    for (Iterator<Map.Entry<Long, LogSegment>> iterator = segments.entrySet().iterator(); iterator.hasNext();) {
      Map.Entry<Long, LogSegment> entry = iterator.next();
      LogSegment segment = entry.getValue();
      if (index > segment.index()) {
        iterator.remove();
        segment.close();
        segment.delete();
      }
    }
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
  public Iterator<ByteBuffer> iterator() {
    return new LoggableIterator(this);
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
      roll(lastIndex + 1);
    }
  }

}
