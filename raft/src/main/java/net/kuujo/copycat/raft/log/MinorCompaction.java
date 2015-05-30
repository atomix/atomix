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

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Minor compaction.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MinorCompaction extends Compaction {
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
  void run(SegmentManager segments) {
    setRunning(true);
    compactLevels(getCompactSegments(segments).iterator(), segments, new CompletableFuture<>());
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
    if (segments.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    Segment segment = segments.remove(0);
    Segment compactSegment = manager.createSegment(segment.descriptor().id(), segment.descriptor().index(), segment.descriptor().version() + 1, segment.descriptor().range());
    compactSegments(segment, segment.firstIndex(), compactSegment, segments, manager, new CompletableFuture<>()).whenComplete((result, error) -> {
      if (error == null) {
        compactLevel(segments, manager, future);
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  /**
   * Compacts a set of segments in the level.
   */
  private CompletableFuture<Void> compactSegments(Segment segment, long index, Segment compactSegment, List<Segment> segments, SegmentManager manager, CompletableFuture<Void> future) {
    try (Entry entry = segment.getEntry(index)) {
      if (entry != null && filter.accept(entry, this)) {
        compactSegment.appendEntry(entry);
      }
    }

    if (index == segment.lastIndex()) {
      if (segments.isEmpty()) {
        future.complete(null);
      } else {
        Segment nextSegment = segments.get(0);
        if (compactSegment.size() + nextSegment.size() < manager.config.getMaxSegmentSize()) {
          context.execute(() -> compactSegments(segments.remove(0), nextSegment.firstIndex(), compactSegment, segments, manager, future));
        } else {
          future.complete(null);
        }
      }
    }
    return future;
  }

}
