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
package net.kuujo.copycat.protocol.raft.storage.compact;

import net.kuujo.copycat.protocol.raft.storage.KeySet;
import net.kuujo.copycat.protocol.raft.storage.Segment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Cardinality based compaction strategy.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class KeyCountingCompactionStrategy extends LeveledCompactionStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCountingCompactionStrategy.class);

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  protected List<List<Segment>> selectSegments(List<Segment> segments) {
    return super.selectSegments(segments).stream()
      .map(l -> calculateCompactSegments(l, config.getEntriesPerSegment()))
      .collect(Collectors.toList());
  }

  /**
   * Calculates the set of segments to compact for the given level segments.
   */
  private List<Segment> calculateCompactSegments(List<Segment> segments, int entriesPerSegment) {
    // Determine the set of segments that can be compacted from the level by estimating the cardinality of the oldest
    // set of segments up to the maximum number of entries allowed in a segment.
    List<Segment> compactSegments = new ArrayList<>();
    KeySet keys = null;
    for (Segment segment : segments) {
      if (keys == null) {
        keys = segment.keys();
        compactSegments.add(segment);
      } else {
        keys = keys.merge(segment.keys());

        if (keys.cardinality() > entriesPerSegment) {
          return compactSegments;
        } else {
          compactSegments.add(segment);
        }
      }
    }
    return compactSegments;
  }

}
