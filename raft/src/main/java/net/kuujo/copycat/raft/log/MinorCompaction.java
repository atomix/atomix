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

import net.kuujo.copycat.raft.log.entry.Entry;
import net.kuujo.copycat.raft.log.entry.EntryFilter;
import net.kuujo.copycat.util.ExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Minor compaction.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MinorCompaction extends Compaction {
  private final Logger LOGGER = LoggerFactory.getLogger(MinorCompaction.class);
  private final EntryFilter filter;
  private final ExecutionContext context;

  public MinorCompaction(long index, EntryFilter filter, ExecutionContext context) {
    super(index);
    this.filter = filter;
    this.context = context;
  }

  @Override
  public Type type() {
    return Type.MINOR;
  }

  @Override
  CompletableFuture<Void> run(SegmentManager segments) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    context.execute(() -> {
      LOGGER.info("Compacting the log");
      setRunning(true);
      compactLevels(getCompactSegments(segments).iterator(), segments, future).whenComplete((result, error) -> {
        setRunning(false);
      });
    });
    return future;
  }

  /**
   * Returns a list of segment levels to compact.
   */
  private List<List<Segment>> getCompactSegments(SegmentManager manager) {
    List<List<Segment>> allSegments = new ArrayList<>();
    SortedMap<Long, List<Segment>> levels = createLevels(manager);

    // Given a sorted list of segment levels, iterate through segments to find a level that should be compacted.
    // Compaction eligibility is determined based on the level and compaction factor.
    for (Map.Entry<Long, List<Segment>> entry : levels.entrySet()) {
      long version = entry.getKey();
      List<Segment> level = entry.getValue();
      if (level.stream().mapToLong(Segment::size).sum() > Math.pow(manager.config.getMaxSegmentSize(), version - 1)) {
        allSegments.add(level);
      }
    }
    return allSegments;
  }

  /**
   * Creates a map of level numbers to segments.
   */
  private SortedMap<Long, List<Segment>> createLevels(SegmentManager segments) {
    // Iterate through segments from oldest to newest and create a map of levels based on segment versions. Because of
    // the nature of this compaction strategy, segments of the same level should always be next to one another.
    TreeMap<Long, List<Segment>> levels = new TreeMap<>();
    for (Segment segment : segments.segments()) {
      if (segment.lastIndex() <= index()) {
        List<Segment> level = levels.get(segment.descriptor().version());
        if (level == null) {
          level = new ArrayList<>();
          levels.put(segment.descriptor().version(), level);
        }
        level.add(segment);
      }
    }
    return levels;
  }

  /**
   * Compacts all levels.
   */
  private CompletableFuture<Void> compactLevels(Iterator<List<Segment>> iterator, SegmentManager manager, CompletableFuture<Void> future) {
    if (iterator.hasNext()) {
      compactLevel(iterator.next(), manager, new CompletableFuture<>()).whenComplete((result, error) -> {
        if (error == null) {
          compactLevels(iterator, manager, future);
        } else {
          future.completeExceptionally(error);
        }
      });
    } else {
      future.complete(null);
    }
    return future;
  }

  /**
   * Compacts a level.
   */
  private CompletableFuture<Void> compactLevel(List<Segment> segments, SegmentManager manager, CompletableFuture<Void> future) {
    LOGGER.debug("compacting {}", segments);

    // Copy the list of segments. We'll be removing segments as they're compacted, but we need to remember the full
    // list of segments for finalizing the compaction as well.
    List<Segment> levelSegments = new ArrayList<>(segments);

    // Remove the first segment from the level.
    Segment segment = levelSegments.remove(0);

    // Create an initial compact segment.
    Segment compactSegment;
    try (SegmentDescriptor descriptor = SegmentDescriptor.builder()
      .withId(segment.descriptor().id())
      .withVersion(segment.descriptor().version() + 1)
      .withIndex(segment.descriptor().index())
      .withMaxEntrySize(segment.descriptor().maxEntrySize())
      .withMaxSegmentSize(segment.descriptor().maxSegmentSize())
      .withMaxEntries(segment.descriptor().maxEntries())
      .build()) {
      compactSegment = manager.createSegment(descriptor);
    }

    // Create a list of compact segments. This list will track all segments used to compact the level.
    List<Segment> compactSegments = new ArrayList<>();
    compactSegments.add(compactSegment);

    compactSegments(segment, segment.firstIndex(), compactSegment, levelSegments, compactSegments, manager, new CompletableFuture<>()).whenComplete((result, error) -> {
      if (error == null) {
        updateSegments(segments, result, manager);
        future.complete(null);
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  /**
   * Compacts a set of segments in the level.
   */
  private CompletableFuture<List<Segment>> compactSegments(Segment segment, long index, Segment compactSegment, List<Segment> segments, List<Segment> compactSegments, SegmentManager manager, CompletableFuture<List<Segment>> future) {
    // Read the entry from the segment. If the entry is null or filtered out of the log, skip the entry, otherwise
    // append it to the compact segment.
    Entry entry = segment.getEntry(index);
    if (entry != null) {
      filter.accept(entry, this).whenCompleteAsync((accept, error) -> {
        if (error == null) {
          if (accept) {
            compactSegment.appendEntry(entry);
          } else {
            compactSegment.skip(1);
          }

          if (index == segment.lastIndex()) {
            if (segments.isEmpty()) {
              future.complete(compactSegments);
            } else {
              Segment nextSegment = segments.remove(0);
              compactSegments(nextSegment, nextSegment.firstIndex(), nextCompactSegment(nextSegment.firstIndex(), nextSegment, compactSegment, compactSegments, manager), segments, compactSegments, manager, future);
            }
          } else {
            compactSegments(segment, index + 1, nextCompactSegment(index + 1, segment, compactSegment, compactSegments, manager), segments, compactSegments, manager, future);
          }
        }
        entry.close();
      }, context);
    } else {
      compactSegment.skip(1);
    }
    return future;
  }

  /**
   * Returns the next compact segment for the given segment and compact segment.
   */
  private Segment nextCompactSegment(long index, Segment segment, Segment compactSegment, List<Segment> compactSegments, SegmentManager manager) {
    if (compactSegment.isFull()) {
      try (SegmentDescriptor descriptor = SegmentDescriptor.builder()
        .withId(segment.descriptor().id())
        .withVersion(segment.descriptor().version() + 1)
        .withIndex(index)
        .withMaxEntrySize(segment.descriptor().maxEntrySize())
        .withMaxSegmentSize(segment.descriptor().maxSegmentSize())
        .withMaxEntries(segment.descriptor().maxEntries())
        .build()) {
        Segment newSegment = manager.createSegment(descriptor);
        compactSegments.add(newSegment);
        return newSegment;
      }
    } else {
      return compactSegment;
    }
  }

  /**
   * Updates the log segments.
   */
  private void updateSegments(List<Segment> segments, List<Segment> compactSegments, SegmentManager manager) {
    Set<Long> segmentIds = segments.stream().map(s -> s.descriptor().id()).collect(Collectors.toSet());
    Map<Long, Segment> mappedSegments = compactSegments.stream().collect(Collectors.toMap(s -> s.descriptor().id(), s -> s));

    List<Segment> updatedSegments = new ArrayList<>();
    for (Segment segment : manager.segments()) {
      if (!segmentIds.contains(segment.descriptor().id())) {
        updatedSegments.add(segment);
      } else {
        Segment compactSegment = mappedSegments.get(segment.descriptor().id());
        if (compactSegment != null) {
          updatedSegments.add(compactSegment);
        }
      }
    }

    manager.update(updatedSegments);
  }

}
