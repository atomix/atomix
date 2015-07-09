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

import net.kuujo.alleycat.io.Buffer;
import net.kuujo.alleycat.io.FileBuffer;
import net.kuujo.alleycat.io.HeapBuffer;
import net.kuujo.copycat.ConfigurationException;
import net.kuujo.copycat.util.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Log segment manager.
 * <p>
 * The segment manager keeps track of segments in a given {@link Log} and provides an interface to loading, retrieving,
 * and compacting those segments.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SegmentManager implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentManager.class);
  protected final LogConfig config;
  private NavigableMap<Long, Segment> segments = new ConcurrentSkipListMap<>();
  private Context context;
  private Segment currentSegment;

  public SegmentManager(LogConfig config) {
    if (config == null)
      throw new NullPointerException("config cannot be null");
    this.config = config;
  }

  /**
   * Returns the log configuration.
   *
   * @return The log configuration.
   */
  LogConfig config() {
    return config;
  }

  /**
   * Opens the segments.
   *
   * @param context The context in which to open the segments.
   */
  public void open(Context context) {
    this.context = context;

    // Load existing log segments from disk.
    for (Segment segment : loadSegments()) {
      segments.put(segment.descriptor().index(), segment);
    }

    // If a segment doesn't already exist, create an initial segment starting at index 1.
    if (!segments.isEmpty()) {
      currentSegment = segments.lastEntry().getValue();
    } else {
      try (SegmentDescriptor descriptor = SegmentDescriptor.builder()
        .withId(1)
        .withVersion(1)
        .withIndex(1)
        .withMaxEntrySize(config.getMaxEntrySize())
        .withMaxSegmentSize(config.getMaxSegmentSize())
        .withMaxEntries(config.getMaxEntriesPerSegment())
        .build()) {
        currentSegment = createSegment(descriptor);
      }
      segments.put(1l, currentSegment);
    }
  }

  /**
   * Checks whether the manager is open.
   */
  private void checkOpen() {
    if (currentSegment == null)
      throw new IllegalStateException("segment manager not open");
  }

  /**
   * Returns the current segment.
   *
   * @return The current segment.
   */
  public Segment currentSegment() {
    return currentSegment != null ? currentSegment : lastSegment();
  }

  /**
   * Resets the current segment, creating a new segment if necessary.
   */
  private void resetCurrentSegment() {
    Segment lastSegment = lastSegment();
    if (lastSegment != null) {
      currentSegment = lastSegment;
    } else {
      try (SegmentDescriptor descriptor = SegmentDescriptor.builder()
        .withId(1)
        .withVersion(1)
        .withIndex(1)
        .withMaxEntrySize(config.getMaxEntrySize())
        .withMaxSegmentSize(config.getMaxSegmentSize())
        .withMaxEntries(config.getMaxEntriesPerSegment())
        .build()) {
        currentSegment = createSegment(descriptor);
      }
      segments.put(1L, currentSegment);
    }
  }

  /**
   * Returns the first segment in the log.
   */
  public Segment firstSegment() {
    checkOpen();
    Map.Entry<Long, Segment> segment = segments.firstEntry();
    return segment != null ? segment.getValue() : null;
  }

  /**
   * Returns the last segment in the log.
   */
  public Segment lastSegment() {
    checkOpen();
    Map.Entry<Long, Segment> segment = segments.lastEntry();
    return segment != null ? segment.getValue() : null;
  }

  /**
   * Creates and returns the next segment.
   *
   * @return The next segment.
   */
  public Segment nextSegment() {
    checkOpen();
    Segment lastSegment = lastSegment();
    try (SegmentDescriptor descriptor = SegmentDescriptor.builder()
      .withId(lastSegment != null ? lastSegment.descriptor().id() + 1 : 1)
      .withVersion(1)
      .withIndex(currentSegment.lastIndex() + 1)
      .withMaxEntrySize(config.getMaxEntrySize())
      .withMaxSegmentSize(config.getMaxSegmentSize())
      .withMaxEntries(config.getMaxEntriesPerSegment())
      .build()) {
      currentSegment = createSegment(descriptor);
      segments.put(descriptor.index(), currentSegment);
    }
    return currentSegment;
  }

  /**
   * Returns the collection of segments.
   *
   * @return An ordered collection of segments.
   */
  public Collection<Segment> segments() {
    return segments.values();
  }

  /**
   * Returns the segment for the given index.
   *
   * @param index The index for which to return the segment.
   */
  public Segment segment(long index) {
    checkOpen();
    // Check if the current segment contains the given index first in order to prevent an unnecessary map lookup.
    if (currentSegment != null && currentSegment.containsIndex(index))
      return currentSegment;

    // If the index is in another segment, get the entry with the next lowest first index.
    Map.Entry<Long, Segment> segment = segments.floorEntry(index);
    return segment != null ? segment.getValue() : null;
  }

  /**
   * Removes a segment.
   *
   * @param segment The segment to remove.
   */
  public void remove(Segment segment) {
    currentSegment = null;

    Map<Long, Segment> removalSegments = segments.tailMap(segment.descriptor().index());
    removalSegments.clear();
    for (Iterator<Segment> i = removalSegments.values().iterator(); i.hasNext();) {
      i.next().delete();
    }
    resetCurrentSegment();
  }

  /**
   * Creates a new segment.
   */
  public Segment createSegment(SegmentDescriptor descriptor) {
    switch (config.getStorageLevel()) {
      case DISK:
        return createDiskSegment(descriptor);
      case MEMORY:
        return createMemorySegment(descriptor);
      default:
        throw new ConfigurationException("unknown storage level: " + config.getStorageLevel());
    }
  }

  /**
   * Create an on-disk segment.
   */
  private Segment createDiskSegment(SegmentDescriptor descriptor) {
    File segmentFile = SegmentFile.createSegmentFile(config.getDirectory(), descriptor.id(), descriptor.version());
    Buffer buffer = FileBuffer.allocate(segmentFile, 1024 * 1024, descriptor.maxSegmentSize() + SegmentDescriptor.BYTES);
    Segment segment = Segment.open(buffer.position(SegmentDescriptor.BYTES).slice(), descriptor, createIndex(descriptor), context);
    LOGGER.debug("Created persistent segment: {}", segment);
    return segment;
  }

  /**
   * Creates an in memory segment.
   */
  private Segment createMemorySegment(SegmentDescriptor descriptor) {
    Buffer buffer = HeapBuffer.allocate(Math.min(1024 * 1024, config.getMaxSegmentSize() + config.getMaxEntrySize() + SegmentDescriptor.BYTES), config.getMaxSegmentSize() + config.getMaxEntrySize() + SegmentDescriptor.BYTES);
    Segment segment = Segment.open(buffer.position(SegmentDescriptor.BYTES).slice(), descriptor, createIndex(descriptor), context);
    LOGGER.debug("Created ephemeral segment: {}", segment);
    return segment;
  }

  /**
   * Loads a segment.
   */
  public Segment loadSegment(long segmentId, long segmentVersion) {
    switch (config.getStorageLevel()) {
      case DISK:
        return loadDiskSegment(segmentId, segmentVersion);
      case MEMORY:
        return loadMemorySegment(segmentId, segmentVersion);
      default:
        throw new ConfigurationException("unknown storage level: " + config.getStorageLevel());
    }
  }

  /**
   * Loads a segment from disk.
   */
  private Segment loadDiskSegment(long segmentId, long segmentVersion) {
    File file = SegmentFile.createSegmentFile(config.getDirectory(), segmentId, segmentVersion);
    try (SegmentDescriptor descriptor = new SegmentDescriptor(FileBuffer.allocate(file, SegmentDescriptor.BYTES))) {
      Buffer buffer = FileBuffer.allocate(file, Math.min(1024 * 1024, config.getMaxSegmentSize() + config.getMaxEntrySize() + SegmentDescriptor.BYTES), config.getMaxSegmentSize() + config.getMaxEntrySize() + SegmentDescriptor.BYTES);
      buffer = buffer.position(SegmentDescriptor.BYTES).slice();
      Segment segment = Segment.open(buffer, descriptor, createIndex(descriptor), context);
      LOGGER.debug("Loaded segment: {} ({})", descriptor.id(), file.getName());
      return segment;
    }
  }

  /**
   * Loads a segment from memory.
   */
  private Segment loadMemorySegment(long segmentId, long segmentVersion) {
    throw new IllegalStateException("cannot load memory segment");
  }

  /**
   * Creates a segment index.
   */
  private OffsetIndex createIndex(SegmentDescriptor descriptor) {
    switch (config.getStorageLevel()) {
      case DISK:
        return createDiskIndex(descriptor);
      case MEMORY:
        return createMemoryIndex(descriptor);
      default:
        throw new ConfigurationException("unknown storage level: " + config.getStorageLevel());
    }
  }

  /**
   * Creates an on disk segment index.
   */
  private OffsetIndex createDiskIndex(SegmentDescriptor descriptor) {
    File file = SegmentFile.createIndexFile(config.getDirectory(), descriptor.id(), descriptor.version());
    return new SearchableOffsetIndex(FileBuffer.allocate(file, Math.min(1024 * 1024, descriptor.maxEntries()), SearchableOffsetIndex.size(descriptor.maxEntries())));
  }

  /**
   * Creates an in memory segment index.
   */
  private OffsetIndex createMemoryIndex(SegmentDescriptor descriptor) {
    return new SearchableOffsetIndex(HeapBuffer.allocate(Math.min(1024 * 1024, descriptor.maxEntries()), SearchableOffsetIndex.size(descriptor.maxEntries())));
  }

  /**
   * Loads all segments from disk.
   *
   * @return A collection of segments for the log.
   */
  protected Collection<Segment> loadSegments() {
    // Ensure log directories are created.
    config.getDirectory().mkdirs();

    // Create a map of descriptors for each existing segment in the log. This is done by iterating through the log
    // directory and finding segment files for this log name. For each segment file, check the consistency of the file
    // by comparing versions and locked state in order to prevent lost data from failures during log compaction.
    Map<Long, SegmentDescriptor> descriptors = new HashMap<>();
    for (File file : config.getDirectory().listFiles(File::isFile)) {
      if (SegmentFile.isSegmentFile(file)) {
        SegmentFile segmentFile = new SegmentFile(file);
        try {
          // Create a new segment descriptor.
          SegmentDescriptor descriptor = new SegmentDescriptor(FileBuffer.allocate(file, SegmentDescriptor.BYTES));

          // Check that the descriptor matches the segment file metadata.
          if (descriptor.id() != segmentFile.id()) {
            throw new DescriptorException(String.format("descriptor ID does not match filename ID: %s", segmentFile.file().getName()));
          }
          if (descriptor.version() != segmentFile.version()) {
            throw new DescriptorException(String.format("descriptor version does not match filename version: %s", segmentFile.file().getName()));
          }

          // If a descriptor already exists for the segment, compare the descriptor versions.
          SegmentDescriptor existingDescriptor = descriptors.get(segmentFile.id());

          // If this segment's version is greater than the existing segment's version and the segment is locked then
          // overwrite it. The segment will be locked if all entries have been committed, e.g. after compaction.
          if (existingDescriptor == null) {
            LOGGER.debug("Found segment: {} ({})", descriptor.id(), segmentFile.file().getName());
            descriptors.put(descriptor.id(), descriptor);
          } else if (descriptor.version() > existingDescriptor.version() && descriptor.locked()) {
            LOGGER.debug("Replaced segment {} with newer version: {} ({})", existingDescriptor.id(), descriptor.version(), segmentFile.file().getName());
            descriptors.put(descriptor.id(), descriptor);
            existingDescriptor.close();
            existingDescriptor.delete();
          } else {
            descriptor.close();
          }
        } catch (NumberFormatException e) {
          // It must not have been a valid segment file.
        }
      }
    }

    // Once we've constructed a map of the most recent descriptors, load the segments.
    List<Segment> segments = new ArrayList<>();
    for (SegmentDescriptor descriptor : descriptors.values()) {
      segments.add(loadSegment(descriptor.id(), descriptor.version()));
      descriptor.close();
    }
    return segments;
  }

  /**
   * Replaces the existing segment with the given ID with the given segment.
   */
  public void replace(Segment segment) {
    LOGGER.debug("Replacing segment: {}", segment.descriptor().id());
    Segment oldSegment = segments.put(segment.descriptor().index(), segment);
    if (oldSegment != null) {
      LOGGER.debug("Deleting segment: {}-{}", oldSegment.descriptor().id(), oldSegment.descriptor().version());
      oldSegment.close();
      oldSegment.delete();
    }
  }

  /**
   * Assigns a new segment list and deletes segments removed from the old segments map.
   */
  public void update(Collection<Segment> segments) {
    NavigableMap<Long, Segment> newSegments = new ConcurrentSkipListMap<>();
    segments.forEach(s -> newSegments.put(s.descriptor().index(), s));

    // Assign the new segments map and delete any segments that were removed from the map.
    NavigableMap<Long, Segment> oldSegments = this.segments;
    this.segments = newSegments;
    resetCurrentSegment();

    // Deletable segments are determined by whether the segment does not have a matching segment/version in the new segments.
    for (Segment oldSegment : oldSegments.values()) {
      Segment segment = this.segments.get(oldSegment.descriptor().index());
      if (segment == null || segment.descriptor().id() != oldSegment.descriptor().id() || segment.descriptor().version() > oldSegment.descriptor().version()) {
        LOGGER.debug("Deleting segment: {}-{}", oldSegment.descriptor().id(), oldSegment.descriptor().version());
        oldSegment.close();
        oldSegment.delete();
      }
    }
  }

  @Override
  public void close() {
    segments.values().forEach(s -> {
      LOGGER.debug("Closing segment: {}", s.descriptor().id());
      s.close();
    });
    segments.clear();
    currentSegment = null;
  }

  /**
   * Deletes all segments.
   */
  public void delete() {
    loadSegments().forEach(s -> {
      LOGGER.debug("Deleting segment: {}", s.descriptor().id());
      s.delete();
    });
  }

}
