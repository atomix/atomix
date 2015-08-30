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

import net.kuujo.copycat.util.Assert;
import net.kuujo.copycat.util.concurrent.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Log entry cleaner.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class EntryCleaner implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(EntryCleaner.class);
  private final SegmentManager manager;
  private final Context context;
  private CompletableFuture<Void> cleanFuture;

  /**
   * @throws NullPointerException if {@code manager} or {@code context} are null
   */
  public EntryCleaner(SegmentManager manager, Context context) {
    this.manager = Assert.notNull(manager, "manager");
    this.context = Assert.notNull(context, "context");
  }

  /**
   * Cleans a list of segments.
   *
   * @param segments The segments to clean.
   * @return A completable future to be completed once the segments have been cleaned.
   * @throws NullPointerException if {@code segments} is null
   */
  CompletableFuture<Void> clean(List<Segment> segments) {
    Assert.notNull(segments, "segments");
    if (cleanFuture != null)
      return cleanFuture;

    if (segments.isEmpty())
      return CompletableFuture.completedFuture(null);

    cleanFuture = context.execute(() -> {
      cleanSegments(segments);
    }).whenComplete((result, error) -> cleanFuture = null);
    return cleanFuture;
  }

  /**
   * Cleans the given segments.
   *
   * @param segments The segments to clean.
   */
  private void cleanSegments(List<Segment> segments) {
    Segment firstSegment = segments.iterator().next();

    Segment cleanSegment = manager.createSegment(SegmentDescriptor.builder()
      .withId(firstSegment.descriptor().id())
      .withVersion(firstSegment.descriptor().version() + 1)
      .withIndex(firstSegment.descriptor().index())
      .withMaxEntrySize(segments.stream().mapToInt(s -> s.descriptor().maxEntrySize()).max().getAsInt())
      .withMaxSegmentSize(segments.stream().mapToLong(s -> s.descriptor().maxSegmentSize()).max().getAsLong())
      .withMaxEntries(segments.stream().mapToInt(s -> s.descriptor().maxEntries()).max().getAsInt())
      .build());

    cleanEntry(firstSegment.firstIndex(), firstSegment, cleanSegment);

    manager.insertSegment(cleanSegment);

    for (Segment segment : segments) {
      cleanSegment(segment, cleanSegment);
    }

    cleanSegment.descriptor().update(System.currentTimeMillis());
    cleanSegment.descriptor().lock();

    for (Segment segment : segments) {
      segment.delete();
    }
  }

  /**
   * Cleans the given segment.
   *
   * @param segment The segment to clean.
   * @param cleanSegment The segment to which to write the cleaned segment.
   */
  private void cleanSegment(Segment segment, Segment cleanSegment) {
    while (!segment.isEmpty()) {
      long index = segment.firstIndex();
      cleanEntry(index, segment, cleanSegment);
      manager.moveSegment(index, segment);
    }
  }

  /**
   * Cleans the entry at the given index.
   *
   * @param index The index at which to clean the entry.
   * @param segment The segment to clean.
   * @param cleanSegment The segment to which to write the cleaned segment.
   */
  private void cleanEntry(long index, Segment segment, Segment cleanSegment) {
    try (Entry entry = segment.get(index)) {
      if (entry != null) {
        cleanSegment.append(entry);
      } else {
        cleanSegment.skip(1);
        LOGGER.debug("Cleaned entry {} from segment {}", index, segment.descriptor().id());
      }
    }

    segment.compact(index + 1);
  }

  /**
   * Closes the entry cleaner.
   */
  @Override
  public void close() {

  }

}
