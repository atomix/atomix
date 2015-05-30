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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Major compaction.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MajorCompaction extends Compaction {
  private final EntryFilter filter;
  private final ExecutionContext context;

  public MajorCompaction(long index, EntryFilter filter, ExecutionContext context) {
    super(index);
    this.filter = filter;
    this.context = context;
  }

  @Override
  public Type type() {
    return Type.MAJOR;
  }

  @Override
  void run(SegmentManager segments) {
    setRunning(true);
    compactSegments(getActiveSegments(segments).iterator(), segments, new CompletableFuture<>());
  }

  /**
   * Returns a list of active segments.
   */
  private List<Segment> getActiveSegments(SegmentManager manager) {
    List<Segment> segments = new ArrayList<>();
    for (Segment segment : manager.segments()) {
      if (!segment.isEmpty() && segment.lastIndex() <= index()) {
        segments.add(segment);
      }
    }
    return segments;
  }

  /**
   * Compacts a list of active segments.
   */
  private CompletableFuture<Void> compactSegments(Iterator<Segment> iterator, SegmentManager manager, CompletableFuture<Void> future) {
    if (iterator.hasNext()) {
      compactSegment(iterator.next(), manager).whenComplete((result, error) -> {
        if (error == null) {
          compactSegments(iterator, manager, future);
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
   * Compacts a single active segment.
   */
  private CompletableFuture<Void> compactSegment(Segment segment, SegmentManager manager) {
    return shouldCompactSegment(segment, new CompletableFuture<>()).thenCompose(compact -> {
      if (compact) {
        Segment compactSegment = manager.createSegment(segment.descriptor().id(), segment.descriptor().index(), segment.descriptor().version() + 1, segment.descriptor().range());
        return compactSegment(segment, segment.firstIndex(), compactSegment, new CompletableFuture<>()).thenAccept(manager::replace);
      } else {
        return CompletableFuture.completedFuture(null);
      }
    });
  }

  /**
   * Compacts a single active segment recursively.
   */
  private CompletableFuture<Segment> compactSegment(Segment segment, long index, Segment compactSegment, CompletableFuture<Segment> future) {
    try (Entry entry = segment.getEntry(index)) {
      if (entry != null && filter.accept(entry, this)) {
        compactSegment.appendEntry(entry);
      }
      if (index == segment.lastIndex()) {
        future.complete(compactSegment);
      } else {
        context.execute(() -> compactSegment(segment, index + 1, compactSegment, future));
      }
    }
    return future;
  }

  /**
   * Determines whether a segment should be compacted.
   */
  private CompletableFuture<Boolean> shouldCompactSegment(Segment segment, CompletableFuture<Boolean> future) {
    return shouldCompactSegment(segment, segment.firstIndex(), future);
  }

  /**
   * Determines whether a segment should be compacted.
   */
  private CompletableFuture<Boolean> shouldCompactSegment(Segment segment, long index, CompletableFuture<Boolean> future) {
    try (Entry entry = segment.getEntry(index)) {
      if (entry != null && !filter.accept(entry, this)) {
        future.complete(true);
      } else if (index == segment.lastIndex()) {
        future.complete(false);
      } else {
        context.execute(() -> shouldCompactSegment(segment, index + 1, future));
      }
    }
    return future;
  }

}
