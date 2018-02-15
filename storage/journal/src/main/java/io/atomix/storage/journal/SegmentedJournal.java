/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.storage.journal;

import com.google.common.collect.Sets;
import io.atomix.serializer.Serializer;
import io.atomix.storage.StorageException;
import io.atomix.storage.StorageLevel;
import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.FileBuffer;
import io.atomix.storage.buffer.HeapBuffer;
import io.atomix.storage.buffer.MappedBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Segmented journal implementation.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class SegmentedJournal<E> implements Journal<E> {

  /**
   * Returns a new segmented journal builder.
   *
   * @return A new segmented journal builder.
   */
  public static <E> Builder<E> newBuilder() {
    return new Builder<>();
  }

  private static final int DEFAULT_BUFFER_SIZE = 1024 * 64;
  private static final int SEGMENT_BUFFER_FACTOR = 3;

  private final Logger log = LoggerFactory.getLogger(getClass());
  private final String name;
  private final StorageLevel storageLevel;
  private final File directory;
  private final Serializer serializer;
  private final int maxSegmentSize;
  private final int maxEntriesPerSegment;
  private final double indexDensity;
  private final int cacheSize;

  private final NavigableMap<Long, JournalSegment<E>> segments = new ConcurrentSkipListMap<>();
  private final Collection<SegmentedJournalReader<E>> readers = Sets.newConcurrentHashSet();
  private JournalSegment<E> currentSegment;

  private final SegmentedJournalWriter<E> writer;
  private volatile boolean open = true;

  public SegmentedJournal(
      String name,
      StorageLevel storageLevel,
      File directory,
      Serializer serializer,
      int maxSegmentSize,
      int maxEntriesPerSegment,
      double indexDensity,
      int cacheSize) {
    this.name = checkNotNull(name, "name cannot be null");
    this.storageLevel = checkNotNull(storageLevel, "storageLevel cannot be null");
    this.directory = checkNotNull(directory, "directory cannot be null");
    this.serializer = checkNotNull(serializer, "serializer cannot be null");
    this.maxSegmentSize = maxSegmentSize;
    this.maxEntriesPerSegment = maxEntriesPerSegment;
    this.indexDensity = indexDensity;
    this.cacheSize = cacheSize;
    open();
    this.writer = openWriter();
  }

  /**
   * Returns the segment file name prefix.
   *
   * @return The segment file name prefix.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the storage directory.
   * <p>
   * The storage directory is the directory to which all segments write files. Segment files
   * for multiple logs may be stored in the storage directory, and files for each log instance will be identified
   * by the {@code prefix} provided when the log is opened.
   *
   * @return The storage directory.
   */
  public File directory() {
    return directory;
  }

  /**
   * Returns the storage level.
   * <p>
   * The storage level dictates how entries within individual journal segments should be stored.
   *
   * @return The storage level.
   */
  public StorageLevel storageLevel() {
    return storageLevel;
  }

  /**
   * Returns the maximum journal segment size.
   * <p>
   * The maximum segment size dictates the maximum size any segment in a segment may consume
   * in bytes.
   *
   * @return The maximum segment size in bytes.
   */
  public int maxSegmentSize() {
    return maxSegmentSize;
  }

  /**
   * Returns the maximum number of entries per segment.
   * <p>
   * The maximum entries per segment dictates the maximum number of entries
   * that are allowed to be stored in any segment in a journal.
   *
   * @return The maximum number of entries per segment.
   */
  public int maxEntriesPerSegment() {
    return maxEntriesPerSegment;
  }

  /**
   * Opens a new journal writer.
   *
   * @return A new journal writer.
   */
  protected SegmentedJournalWriter<E> openWriter() {
    return new SegmentedJournalWriter<>(this);
  }

  /**
   * Opens the segments.
   */
  private void open() {
    // Load existing log segments from disk.
    for (JournalSegment<E> segment : loadSegments()) {
      segments.put(segment.descriptor().index(), segment);
    }

    // If a segment doesn't already exist, create an initial segment starting at index 1.
    if (!segments.isEmpty()) {
      currentSegment = segments.lastEntry().getValue();
    } else {
      JournalSegmentDescriptor descriptor = JournalSegmentDescriptor.newBuilder()
          .withId(1)
          .withIndex(1)
          .withMaxSegmentSize(maxSegmentSize)
          .withMaxEntries(maxEntriesPerSegment)
          .build();

      currentSegment = createSegment(descriptor);
      currentSegment.descriptor().update(System.currentTimeMillis());

      segments.put(1L, currentSegment);
    }
  }

  /**
   * Asserts that the manager is open.
   *
   * @throws IllegalStateException if the segment manager is not open
   */
  private void assertOpen() {
    checkState(currentSegment != null, "journal not open");
  }

  /**
   * Asserts that enough disk space is available to allocate a new segment.
   */
  private void assertDiskSpace() {
    if (directory().getUsableSpace() < maxSegmentSize() * SEGMENT_BUFFER_FACTOR) {
      throw new StorageException.OutOfDiskSpace("Not enough space to allocate a new journal segment");
    }
  }

  /**
   * Resets the current segment, creating a new segment if necessary.
   */
  private synchronized void resetCurrentSegment() {
    JournalSegment<E> lastSegment = getLastSegment();
    if (lastSegment != null) {
      currentSegment = lastSegment;
    } else {
      JournalSegmentDescriptor descriptor = JournalSegmentDescriptor.newBuilder()
          .withId(1)
          .withIndex(1)
          .withMaxSegmentSize(maxSegmentSize)
          .withMaxEntries(maxEntriesPerSegment)
          .build();

      currentSegment = createSegment(descriptor);

      segments.put(1L, currentSegment);
    }
  }

  /**
   * Resets and returns the first segment in the journal.
   *
   * @param index the starting index of the journal
   * @return the first segment
   */
  JournalSegment<E> resetSegments(long index) {
    assertOpen();

    // If the index already equals the first segment index, skip the reset.
    JournalSegment<E> firstSegment = getFirstSegment();
    if (index == firstSegment.index()) {
      return firstSegment;
    }

    for (JournalSegment<E> segment : segments.values()) {
      segment.close();
      segment.delete();
    }
    segments.clear();

    JournalSegmentDescriptor descriptor = JournalSegmentDescriptor.newBuilder()
        .withId(1)
        .withIndex(index)
        .withMaxSegmentSize(maxSegmentSize)
        .withMaxEntries(maxEntriesPerSegment)
        .build();
    currentSegment = createSegment(descriptor);
    segments.put(index, currentSegment);
    return currentSegment;
  }

  /**
   * Returns the first segment in the log.
   *
   * @throws IllegalStateException if the segment manager is not open
   */
  JournalSegment<E> getFirstSegment() {
    assertOpen();
    Map.Entry<Long, JournalSegment<E>> segment = segments.firstEntry();
    return segment != null ? segment.getValue() : null;
  }

  /**
   * Returns the last segment in the log.
   *
   * @throws IllegalStateException if the segment manager is not open
   */
  JournalSegment<E> getLastSegment() {
    assertOpen();
    Map.Entry<Long, JournalSegment<E>> segment = segments.lastEntry();
    return segment != null ? segment.getValue() : null;
  }

  /**
   * Creates and returns the next segment.
   *
   * @return The next segment.
   * @throws IllegalStateException if the segment manager is not open
   */
  synchronized JournalSegment<E> getNextSegment() {
    assertOpen();
    assertDiskSpace();

    JournalSegment lastSegment = getLastSegment();
    JournalSegmentDescriptor descriptor = JournalSegmentDescriptor.newBuilder()
        .withId(lastSegment != null ? lastSegment.descriptor().id() + 1 : 1)
        .withIndex(currentSegment.lastIndex() + 1)
        .withMaxSegmentSize(maxSegmentSize)
        .withMaxEntries(maxEntriesPerSegment)
        .build();

    currentSegment = createSegment(descriptor);

    segments.put(descriptor.index(), currentSegment);
    return currentSegment;
  }

  /**
   * Returns the segment following the segment with the given ID.
   *
   * @param index The segment index with which to look up the next segment.
   * @return The next segment for the given index.
   */
  JournalSegment<E> getNextSegment(long index) {
    Map.Entry<Long, JournalSegment<E>> nextSegment = segments.higherEntry(index);
    return nextSegment != null ? nextSegment.getValue() : null;
  }

  /**
   * Returns the segment for the given index.
   *
   * @param index The index for which to return the segment.
   * @throws IllegalStateException if the segment manager is not open
   */
  synchronized JournalSegment<E> getSegment(long index) {
    assertOpen();
    // Check if the current segment contains the given index first in order to prevent an unnecessary map lookup.
    if (currentSegment != null && index > currentSegment.index()) {
      return currentSegment;
    }

    // If the index is in another segment, get the entry with the next lowest first index.
    Map.Entry<Long, JournalSegment<E>> segment = segments.floorEntry(index);
    if (segment != null) {
      return segment.getValue();
    }
    return getFirstSegment();
  }

  /**
   * Removes a segment.
   *
   * @param segment The segment to remove.
   */
  synchronized void removeSegment(JournalSegment segment) {
    segments.remove(segment.index());
    segment.close();
    segment.delete();
    resetCurrentSegment();
  }

  /**
   * Creates a new segment.
   */
  JournalSegment<E> createSegment(JournalSegmentDescriptor descriptor) {
    switch (storageLevel) {
      case MEMORY:
        return createMemorySegment(descriptor);
      case MAPPED:
        return createMappedSegment(descriptor);
      case DISK:
        return createDiskSegment(descriptor);
      default:
        throw new AssertionError();
    }
  }

  /**
   * Creates a new segment instance.
   *
   * @param segmentFile The segment file.
   * @param descriptor The segment descriptor.
   * @return The segment instance.
   */
  protected JournalSegment<E> newSegment(JournalSegmentFile segmentFile, JournalSegmentDescriptor descriptor) {
    return new JournalSegment<>(segmentFile, descriptor, indexDensity, cacheSize, serializer);
  }

  /**
   * Creates a new segment.
   */
  private JournalSegment<E> createDiskSegment(JournalSegmentDescriptor descriptor) {
    File segmentFile = JournalSegmentFile.createSegmentFile(name, directory, descriptor.id());
    Buffer buffer = FileBuffer.allocate(segmentFile, Math.min(DEFAULT_BUFFER_SIZE, descriptor.maxSegmentSize()), Integer.MAX_VALUE);
    descriptor.copyTo(buffer);
    JournalSegment<E> segment = newSegment(new JournalSegmentFile(segmentFile), descriptor);
    log.debug("Created disk segment: {}", segment);
    return segment;
  }

  /**
   * Creates a new segment.
   */
  private JournalSegment<E> createMappedSegment(JournalSegmentDescriptor descriptor) {
    File segmentFile = JournalSegmentFile.createSegmentFile(name, directory, descriptor.id());
    Buffer buffer = MappedBuffer.allocate(segmentFile, Math.min(DEFAULT_BUFFER_SIZE, descriptor.maxSegmentSize()), Integer.MAX_VALUE);
    descriptor.copyTo(buffer);
    JournalSegment<E> segment = newSegment(new JournalSegmentFile(segmentFile), descriptor);
    log.debug("Created memory mapped segment: {}", segment);
    return segment;
  }

  /**
   * Creates a new segment.
   */
  private JournalSegment<E> createMemorySegment(JournalSegmentDescriptor descriptor) {
    File segmentFile = JournalSegmentFile.createSegmentFile(name, directory, descriptor.id());
    Buffer buffer = HeapBuffer.allocate(Math.min(DEFAULT_BUFFER_SIZE, descriptor.maxSegmentSize()), Integer.MAX_VALUE);
    descriptor.copyTo(buffer);
    JournalSegment<E> segment = newSegment(new JournalSegmentFile(segmentFile), descriptor);
    log.debug("Created memory segment: {}", segment);
    return segment;
  }

  /**
   * Loads a segment.
   */
  private JournalSegment<E> loadSegment(long segmentId) {
    switch (storageLevel) {
      case MEMORY:
        return loadMemorySegment(segmentId);
      case MAPPED:
        return loadMappedSegment(segmentId);
      case DISK:
        return loadDiskSegment(segmentId);
      default:
        throw new AssertionError();
    }
  }

  /**
   * Loads a segment.
   */
  private JournalSegment<E> loadDiskSegment(long segmentId) {
    File file = JournalSegmentFile.createSegmentFile(name, directory, segmentId);
    Buffer buffer = FileBuffer.allocate(file, Math.min(DEFAULT_BUFFER_SIZE, maxSegmentSize), Integer.MAX_VALUE);
    JournalSegmentDescriptor descriptor = new JournalSegmentDescriptor(buffer);
    JournalSegment<E> segment = newSegment(new JournalSegmentFile(file), descriptor);
    log.debug("Loaded disk segment: {} ({})", descriptor.id(), file.getName());
    return segment;
  }

  /**
   * Loads a segment.
   */
  private JournalSegment<E> loadMappedSegment(long segmentId) {
    File file = JournalSegmentFile.createSegmentFile(name, directory, segmentId);
    Buffer buffer = MappedBuffer.allocate(file, Math.min(DEFAULT_BUFFER_SIZE, maxSegmentSize), Integer.MAX_VALUE);
    JournalSegmentDescriptor descriptor = new JournalSegmentDescriptor(buffer);
    JournalSegment<E> segment = newSegment(new JournalSegmentFile(file), descriptor);
    log.debug("Loaded disk segment: {} ({})", descriptor.id(), file.getName());
    return segment;
  }

  /**
   * Loads a segment.
   */
  private JournalSegment<E> loadMemorySegment(long segmentId) {
    File file = JournalSegmentFile.createSegmentFile(name, directory, segmentId);
    Buffer buffer = HeapBuffer.allocate(Math.min(DEFAULT_BUFFER_SIZE, maxSegmentSize), Integer.MAX_VALUE);
    JournalSegmentDescriptor descriptor = new JournalSegmentDescriptor(buffer);
    JournalSegment<E> segment = newSegment(new JournalSegmentFile(file), descriptor);
    log.debug("Loaded memory segment: {}", descriptor.id());
    return segment;
  }

  /**
   * Loads all segments from disk.
   *
   * @return A collection of segments for the log.
   */
  protected Collection<JournalSegment<E>> loadSegments() {
    // Ensure log directories are created.
    directory.mkdirs();

    TreeMap<Long, JournalSegment<E>> segments = new TreeMap<>();

    // Iterate through all files in the log directory.
    for (File file : directory.listFiles(File::isFile)) {

      // If the file looks like a segment file, attempt to load the segment.
      if (JournalSegmentFile.isSegmentFile(name, file)) {
        JournalSegmentFile segmentFile = new JournalSegmentFile(file);
        JournalSegmentDescriptor descriptor = new JournalSegmentDescriptor(FileBuffer.allocate(file, JournalSegmentDescriptor.BYTES));

        // Load the segment.
        JournalSegment<E> segment = loadSegment(descriptor.id());

        // If a segment with an equal or lower index has already been loaded, ensure this segment is not superseded
        // by the earlier segment. This can occur due to segments being combined during log compaction.
        Map.Entry<Long, JournalSegment<E>> previousEntry = segments.floorEntry(segment.index());
        if (previousEntry != null) {

          // If an existing descriptor exists with a lower index than this segment's first index, check to determine
          // whether this segment's first index is contained in that existing index. If it is, determine which segment
          // should take precedence based on segment versions.
          JournalSegment previousSegment = previousEntry.getValue();

          // If the two segments start at the same index, the segment with the higher version number is used.
          if (previousSegment.index() == segment.index()) {
            if (segment.descriptor().version() > previousSegment.descriptor().version()) {
              log.debug("Replaced segment {} with newer version: {} ({})", previousSegment.descriptor().id(), segment.descriptor().version(), segmentFile.file().getName());
              segments.remove(previousEntry.getKey());
              previousSegment.close();
              previousSegment.delete();
            } else {
              segment.close();
              segment.delete();
              continue;
            }
          }
          // If the existing segment's entries overlap with the loaded segment's entries, the existing segment always
          // supersedes the loaded segment. Log compaction processes ensure this is always the case.
          else if (previousSegment.index() + previousSegment.length() > segment.index()) {
            segment.close();
            segment.delete();
            continue;
          }
        }

        // Add the segment to the segments list.
        log.debug("Found segment: {} ({})", segment.descriptor().id(), segmentFile.file().getName());
        segments.put(segment.index(), segment);

        // Ensure any segments later in the log with which this segment overlaps are removed.
        Map.Entry<Long, JournalSegment<E>> nextEntry = segments.higherEntry(segment.index());
        while (nextEntry != null) {
          if (nextEntry.getValue().index() < segment.index() + segment.length()) {
            segments.remove(nextEntry.getKey());
            nextEntry = segments.higherEntry(segment.index());
          } else {
            break;
          }
        }

        descriptor.close();
      }
    }

    for (Long segmentId : segments.keySet()) {
      JournalSegment<E> segment = segments.get(segmentId);
      Map.Entry<Long, JournalSegment<E>> previousEntry = segments.floorEntry(segmentId - 1);
      if (previousEntry != null) {
        JournalSegment<E> previousSegment = previousEntry.getValue();
        if (previousSegment.lastIndex() != segment.index() - 1) {
          log.warn("Found misaligned segment {}", segment);
          segments.remove(segmentId);
        }
      }
    }

    return segments.values();
  }

  /**
   * Resets journal readers to the given head.
   *
   * @param index The index at which to reset readers.
   */
  void resetHead(long index) {
    for (SegmentedJournalReader<E> reader : readers) {
      if (reader.getNextIndex() < index) {
        reader.reset(index);
      }
    }
  }

  /**
   * Resets journal readers to the given tail.
   *
   * @param index The index at which to reset readers.
   */
  void resetTail(long index) {
    for (SegmentedJournalReader<E> reader : readers) {
      if (reader.getNextIndex() >= index) {
        reader.reset(index);
      }
    }
  }

  @Override
  public SegmentedJournalWriter<E> writer() {
    return writer;
  }

  @Override
  public SegmentedJournalReader<E> openReader(long index) {
    SegmentedJournalReader<E> reader = new SegmentedJournalReader<>(this, index);
    readers.add(reader);
    return reader;
  }

  void closeReader(SegmentedJournalReader<E> reader) {
    readers.remove(reader);
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  /**
   * Returns a boolean indicating whether a segment can be removed from the journal prior to the given index.
   *
   * @param index the index from which to remove segments
   * @return indicates whether a segment can be removed from the journal
   */
  public boolean isCompactable(long index) {
    Map.Entry<Long, JournalSegment<E>> segmentEntry = segments.floorEntry(index);
    return segmentEntry != null && segments.headMap(segmentEntry.getValue().index()).size() > 0;
  }

  /**
   * Returns the index of the last segment in the log.
   *
   * @param index the compaction index
   * @return the starting index of the last segment in the log
   */
  public long getCompactableIndex(long index) {
    Map.Entry<Long, JournalSegment<E>> segmentEntry = segments.floorEntry(index);
    return segmentEntry != null ? segmentEntry.getValue().index() : 0;
  }

  /**
   * Compacts the journal up to the given index.
   * <p>
   * The semantics of compaction are not specified by this interface.
   *
   * @param index The index up to which to compact the journal.
   */
  public void compact(long index) {
    Map.Entry<Long, JournalSegment<E>> segmentEntry = segments.floorEntry(index);
    if (segmentEntry != null) {
      SortedMap<Long, JournalSegment<E>> compactSegments = segments.headMap(segmentEntry.getValue().index());
      if (!compactSegments.isEmpty()) {
        log.debug("{} - Compacting {} segment(s)", name, compactSegments.size());
        for (JournalSegment segment : compactSegments.values()) {
          log.trace("Deleting segment: {}", segment);
          segment.close();
          segment.delete();
        }
        compactSegments.clear();
        resetHead(segmentEntry.getValue().index());
      }
    }
  }

  @Override
  public void close() {
    segments.values().forEach(segment -> {
      log.debug("Closing segment: {}", segment);
      segment.close();
    });
    currentSegment = null;
    open = false;
  }

  /**
   * Segmented journal builder.
   */
  public static class Builder<E> implements io.atomix.utils.Builder<SegmentedJournal<E>> {
    private static final String DEFAULT_NAME = "atomix";
    private static final String DEFAULT_DIRECTORY = System.getProperty("user.dir");
    private static final int DEFAULT_MAX_SEGMENT_SIZE = 1024 * 1024 * 32;
    private static final int DEFAULT_MAX_ENTRIES_PER_SEGMENT = 1024 * 1024;
    private static final double DEFAULT_INDEX_DENSITY = .005;
    private static final int DEFAULT_CACHE_SIZE = 1024;

    protected String name = DEFAULT_NAME;
    protected StorageLevel storageLevel = StorageLevel.DISK;
    protected File directory = new File(DEFAULT_DIRECTORY);
    protected Serializer serializer;
    protected int maxSegmentSize = DEFAULT_MAX_SEGMENT_SIZE;
    protected int maxEntriesPerSegment = DEFAULT_MAX_ENTRIES_PER_SEGMENT;
    protected double indexDensity = DEFAULT_INDEX_DENSITY;
    protected int cacheSize = DEFAULT_CACHE_SIZE;

    protected Builder() {
    }

    /**
     * Sets the storage name.
     *
     * @param name The storage name.
     * @return The storage builder.
     */
    public Builder<E> withName(String name) {
      this.name = checkNotNull(name, "name cannot be null");
      return this;
    }

    /**
     * Sets the log storage level, returning the builder for method chaining.
     * <p>
     * The storage level indicates how individual entries should be persisted in the journal.
     *
     * @param storageLevel The log storage level.
     * @return The storage builder.
     */
    public Builder<E> withStorageLevel(StorageLevel storageLevel) {
      this.storageLevel = checkNotNull(storageLevel, "storageLevel cannot be null");
      return this;
    }

    /**
     * Sets the log directory, returning the builder for method chaining.
     * <p>
     * The log will write segment files into the provided directory.
     *
     * @param directory The log directory.
     * @return The storage builder.
     * @throws NullPointerException If the {@code directory} is {@code null}
     */
    public Builder<E> withDirectory(String directory) {
      return withDirectory(new File(checkNotNull(directory, "directory cannot be null")));
    }

    /**
     * Sets the log directory, returning the builder for method chaining.
     * <p>
     * The log will write segment files into the provided directory.
     *
     * @param directory The log directory.
     * @return The storage builder.
     * @throws NullPointerException If the {@code directory} is {@code null}
     */
    public Builder<E> withDirectory(File directory) {
      this.directory = checkNotNull(directory, "directory cannot be null");
      return this;
    }

    /**
     * Sets the journal serializer, returning the builder for method chaining.
     *
     * @param serializer The journal serializer.
     * @return The journal builder.
     */
    public Builder<E> withSerializer(Serializer serializer) {
      this.serializer = checkNotNull(serializer, "serializer cannot be null");
      return this;
    }

    /**
     * Sets the maximum segment size in bytes, returning the builder for method chaining.
     * <p>
     * The maximum segment size dictates when logs should roll over to new segments. As entries are written to
     * a segment of the log, once the size of the segment surpasses the configured maximum segment size, the
     * log will create a new segment and append new entries to that segment.
     * <p>
     * By default, the maximum segment size is {@code 1024 * 1024 * 32}.
     *
     * @param maxSegmentSize The maximum segment size in bytes.
     * @return The storage builder.
     * @throws IllegalArgumentException If the {@code maxSegmentSize} is not positive
     */
    public Builder<E> withMaxSegmentSize(int maxSegmentSize) {
      checkArgument(maxSegmentSize > JournalSegmentDescriptor.BYTES, "maxSegmentSize must be greater than " + JournalSegmentDescriptor.BYTES);
      this.maxSegmentSize = maxSegmentSize;
      return this;
    }

    /**
     * Sets the maximum number of allows entries per segment, returning the builder for method chaining.
     * <p>
     * The maximum entry count dictates when logs should roll over to new segments. As entries are written to
     * a segment of the log, if the entry count in that segment meets the configured maximum entry count, the
     * log will create a new segment and append new entries to that segment.
     * <p>
     * By default, the maximum entries per segment is {@code 1024 * 1024}.
     *
     * @param maxEntriesPerSegment The maximum number of entries allowed per segment.
     * @return The storage builder.
     * @throws IllegalArgumentException If the {@code maxEntriesPerSegment} not greater than the default max entries per
     *                                  segment
     */
    public Builder<E> withMaxEntriesPerSegment(int maxEntriesPerSegment) {
      checkArgument(maxEntriesPerSegment > 0, "max entries per segment must be positive");
      checkArgument(maxEntriesPerSegment <= DEFAULT_MAX_ENTRIES_PER_SEGMENT,
          "max entries per segment cannot be greater than " + DEFAULT_MAX_ENTRIES_PER_SEGMENT);
      this.maxEntriesPerSegment = maxEntriesPerSegment;
      return this;
    }

    /**
     * Sets the journal index density.
     * <p>
     * The index density is the frequency at which the position of entries written to the journal will be recorded in
     * an in-memory index for faster seeking.
     *
     * @param indexDensity the index density
     * @return the journal builder
     * @throws IllegalArgumentException if the density is not between 0 and 1
     */
    public Builder<E> withIndexDensity(double indexDensity) {
      checkArgument(indexDensity > 0 && indexDensity < 1, "index density must be between 0 and 1");
      this.indexDensity = indexDensity;
      return this;
    }

    /**
     * Sets the journal cache size.
     *
     * @param cacheSize the journal cache size
     * @return the journal builder
     * @throws IllegalArgumentException if the cache size is not positive
     */
    public Builder<E> withCacheSize(int cacheSize) {
      checkArgument(cacheSize >= 0, "cacheSize must be positive");
      this.cacheSize = cacheSize;
      return this;
    }

    /**
     * Builds the journal.
     *
     * @return The built storage configuration.
     */
    @Override
    public SegmentedJournal<E> build() {
      return new SegmentedJournal<>(name, storageLevel, directory, serializer, maxSegmentSize, maxEntriesPerSegment, indexDensity, cacheSize);
    }
  }
}