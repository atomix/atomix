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
package net.kuujo.copycat.raft.storage.compact;

import net.kuujo.copycat.raft.storage.Segment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Leveled compaction strategy.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LeveledCompactionStrategy extends AbstractCompactionStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(LeveledCompactionStrategy.class);
  private static final int DEFAULT_COMPACTION_FACTOR = 10;
  private int compactionFactor = DEFAULT_COMPACTION_FACTOR;

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  /**
   * Sets the compaction factor.
   *
   * @param compactionFactor The compaction factor.
   */
  public void setCompactionFactor(int compactionFactor) {
    if (compactionFactor < 1)
      throw new IllegalArgumentException("compaction factor must be positive");
    this.compactionFactor = compactionFactor;
  }

  /**
   * Returns the compaction factor.
   *
   * @return The compaction factor.
   */
  public int getCompactionFactor() {
    return compactionFactor;
  }

  /**
   * Sets the compaction factor, returning the compaction strategy for method chaining.
   *
   * @param compactionFactor The compaction factor.
   * @return The compaction strategy.
   */
  public LeveledCompactionStrategy withCompactionFactor(int compactionFactor) {
    setCompactionFactor(compactionFactor);
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected List<List<Segment>> selectSegments(List<Segment> segments) {
    List<List<Segment>> allSegments = new ArrayList<>();
    if (!segments.isEmpty()) {

      // Sort all segments in reverse order. This ensures that we favor compacting higher indexes over lower indexes.
      Collections.sort(segments, (s1, s2) -> (int) (s2.descriptor().index() - s1.descriptor().index()));

      // Create a sorted map of levels. Levels are identified by segment versions.
      SortedMap<Long, List<Segment>> levels = new TreeMap<>();
      for (Segment segment : segments) {
        // Only compact segments where all entries have been committed (segments are locked).
        if (segment.isLocked()) {
          List<Segment> level = levels.get(segment.descriptor().version());
          if (level == null) {
            level = new ArrayList<>(segments.size());
            levels.put(segment.descriptor().version(), level);
          }
          level.add(segment);
        }
      }

      // Given a sorted list of segment levels, iterate through segments to find a level that should be compacted.
      // Compaction eligibility is determined based on the level and compaction factor.
      for (Map.Entry<Long, List<Segment>> entry : levels.entrySet()) {
        long version = entry.getKey();
        List<Segment> level = entry.getValue();
        if (level.stream().mapToLong(Segment::size).sum() > 1024 * 1024 * (long) Math.pow(compactionFactor, version - 1)) {
          allSegments.add(level);
        }
      }
    }
    return allSegments;
  }

}
