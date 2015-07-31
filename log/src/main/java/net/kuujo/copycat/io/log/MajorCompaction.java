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
package net.kuujo.copycat.io.log;

import net.kuujo.copycat.util.concurrent.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final Logger LOGGER = LoggerFactory.getLogger(MajorCompaction.class);
  private final EntryFilter filter;
  private final Context context;

  public MajorCompaction(long index, EntryFilter filter, Context context) {
    super(index);
    this.filter = filter;
    this.context = context;
  }

  @Override
  public Type type() {
    return Type.MAJOR;
  }

  @Override
  CompletableFuture<Void> run(SegmentManager segments) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    context.execute(() -> {
      LOGGER.info("Compacting the log");
      setRunning(true);
      compactSegments(getActiveSegments(segments).iterator(), segments, future).whenComplete((result, error) -> {
        setRunning(false);
      });
    });
    return future;
  }

  /**
   * Returns a list of active segments.
   */
  private List<Segment> getActiveSegments(SegmentManager manager) {
    List<Segment> segments = new ArrayList<>();
    for (Segment segment : manager.segments()) {
      if (segment.isFull() && segment.lastIndex() <= index()) {
        segments.add(segment);
      }
    }
    LOGGER.debug("Found {} compactable segments", segments.size());
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
        LOGGER.debug("Compacting {}", segment);
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
    Entry entry = segment.getEntry(index);
    if (entry != null) {
      filter.accept(entry, this).whenCompleteAsync((accept, error) -> {
        if (error == null) {
          if (accept) {
            compactSegment.appendEntry(entry);
          } else {
            LOGGER.debug("Filtered {} from segment {}", entry, segment.descriptor().id());
            compactSegment.skip(1);
          }

          if (index == segment.lastIndex()) {
            future.complete(compactSegment);
          } else {
            compactSegment(segment, index + 1, compactSegment, future);
          }
        } else {
          LOGGER.warn("An error occurred during compaction: {}", error);
          future.completeExceptionally(error);
        }
        entry.close();
      }, context);
    } else {
      compactSegment.skip(1);

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
    LOGGER.debug("Evaluating {} for major compaction", segment);
    return shouldCompactSegment(segment, segment.firstIndex(), future);
  }

  /**
   * Determines whether a segment should be compacted.
   */
  private CompletableFuture<Boolean> shouldCompactSegment(Segment segment, long index, CompletableFuture<Boolean> future) {
    Entry entry = segment.getEntry(index);
    if (entry != null) {
      filter.accept(entry, this).whenCompleteAsync((accept, error) -> {
        entry.close();
        if (error == null) {
          if (!accept) {
            future.complete(true);
          } else if (index == segment.lastIndex()) {
            future.complete(false);
          } else {
            shouldCompactSegment(segment, index + 1, future);
          }
        } else {
          future.complete(true);
        }
      }, context);
    } else if (index == segment.lastIndex()) {
      future.complete(false);
    } else {
      context.execute(() -> shouldCompactSegment(segment, index + 1, future));
    }
    return future;
  }

}
