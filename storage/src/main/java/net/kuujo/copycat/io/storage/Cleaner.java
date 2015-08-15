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
package net.kuujo.copycat.io.storage;

import net.kuujo.copycat.util.Listener;
import net.kuujo.copycat.util.Listeners;
import net.kuujo.copycat.util.concurrent.Context;
import net.kuujo.copycat.util.concurrent.ThreadPoolContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Log cleaner.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Cleaner implements AutoCloseable {
  private static final double CLEAN_THRESHOLD = 0.5;
  private final SegmentManager manager;
  private final Listeners<EntryCleaner> startListeners = new Listeners<>();
  private final Listeners<EntryCleaner> completeListeners = new Listeners<>();
  private final ScheduledExecutorService executor;
  private CompletableFuture<Void> cleanFuture;

  public Cleaner(SegmentManager manager, ScheduledExecutorService executor) {
    if (manager == null)
      throw new NullPointerException("manager cannot be null");
    if (executor == null)
      throw new NullPointerException("executor cannot be null");
    this.manager = manager;
    this.executor = executor;
  }

  /**
   * Registers a compaction start listener.
   *
   * @param listener The listener to invoke when a cleaner process starts.
   * @return The listener context.
   */
  public Listener<EntryCleaner> onStart(Consumer<EntryCleaner> listener) {
    return startListeners.add(listener);
  }

  /**
   * Registers a cleaner complete listener.
   *
   * @param listener The listener to invoke when a cleaner process completes.
   * @return The listener context.
   */
  public Listener<EntryCleaner> onComplete(Consumer<EntryCleaner> listener) {
    return completeListeners.add(listener);
  }

  /**
   * Cleans the log.
   *
   * @return A completable future to be completed once the log has been cleaned.
   */
  public CompletableFuture<Void> clean() {
    if (cleanFuture != null)
      return cleanFuture;

    cleanFuture = new CompletableFuture<>();
    cleanSegments(Context.currentContext());
    return cleanFuture.whenComplete((result, error) -> cleanFuture = null);
  }

  /**
   * Cleans all cleanable segments.
   */
  private void cleanSegments(Context context) {
    AtomicInteger counter = new AtomicInteger();
    List<List<Segment>> cleanSegments = getCleanSegments();
    if (!cleanSegments.isEmpty()) {
      for (List<Segment> segments : cleanSegments) {
        EntryCleaner cleaner = new EntryCleaner(manager, new ThreadPoolContext(executor, manager.serializer()));
        executor.execute(() -> {
          cleaner.clean(segments).whenComplete((result, error) -> {
            if (counter.incrementAndGet() == cleanSegments.size()) {
              if (context != null) {
                context.execute(() -> cleanFuture.complete(null));
              } else {
                cleanFuture.complete(null);
              }
            }
          });
        });
      }
    } else {
      cleanFuture.complete(null);
    }
  }

  /**
   * Returns a list of segment sets to clean.
   *
   * @return A list of segment sets to clean in the order in which they should be cleaned.
   */
  private List<List<Segment>> getCleanSegments() {
    List<List<Segment>> clean = new ArrayList<>();
    List<Segment> segments = null;
    Segment previousSegment = null;
    for (Segment segment : getCleanableSegments()) {
      if (segments == null) {
        segments = new ArrayList<>();
        segments.add(segment);
      }
      // If the previous segment is not an instance of the same version as this segment then reset the segments list.
      // Similarly, if the previous segment doesn't directly end with the index prior to the first index in this segment then
      // reset the segments list. We can only combine segments that are direct neighbors of one another.
      else if (previousSegment != null && (previousSegment.descriptor().version() != segment.descriptor().version() || previousSegment.lastIndex() != segment.firstIndex() - 1)) {
        clean.add(segments);
        segments = new ArrayList<>();
        segments.add(segment);
      }
      // If the total count of entries in all segments is less then the total slots in any individual segment, combine the segments.
      else if (segments.stream().mapToLong(Segment::count).sum() + segment.count() < segments.stream().mapToLong(Segment::length).max().getAsLong()) {
        segments.add(segment);
      }
      // If there's not enough room to combine segments, reset the segments list.
      else {
        clean.add(segments);
        segments = new ArrayList<>();
        segments.add(segment);
      }
      previousSegment = segment;
    }

    // Ensure all cleanable segments have been added to the clean segments list.
    if (segments != null) {
      clean.add(segments);
    }
    return clean;
  }

  /**
   * Returns a list of compactable segments.
   *
   * @return A list of compactable segments.
   */
  private Iterable<Segment> getCleanableSegments() {
    List<Segment> segments = new ArrayList<>();
    for (Segment segment : manager.segments()) {
      // Only allow compaction of segments that are full.
      if (segment.isFull()) {

        // Calculate the percentage of entries that have been marked for cleaning in the segment.
        double cleanPercentage = (segment.length() - segment.count()) / (double) segment.length();

        // If the percentage of entries marked for cleaning times the segment version meets the cleaning threshold,
        // add the segment to the segments list for cleaning.
        if (cleanPercentage * segment.descriptor().version() > CLEAN_THRESHOLD) {
          segments.add(segment);
        }
      }
    }
    return segments;
  }

  /**
   * Closes the log cleaner.
   */
  @Override
  public void close() {
    executor.shutdown();
  }

}
