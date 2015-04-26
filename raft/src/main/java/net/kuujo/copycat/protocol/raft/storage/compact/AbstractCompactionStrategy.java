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

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.NativeBuffer;
import net.kuujo.copycat.protocol.raft.storage.RaftEntry;
import net.kuujo.copycat.protocol.raft.storage.Segment;
import net.kuujo.copycat.protocol.raft.storage.SegmentManager;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Abstract compaction strategy implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractCompactionStrategy implements CompactionStrategy {

  /**
   * Returns the compaction strategy logger.
   */
  protected abstract Logger logger();

  /**
   * Selects a list of segments to compact.
   */
  protected abstract List<List<Segment>> selectSegments(List<Segment> segments);

  /**
   * Sorts segments in ascending order.
   */
  private List<Segment> sortSegments(List<Segment> segments) {
    Collections.sort(segments, (s1, s2) -> (int) (s1.descriptor().index() - s2.descriptor().index()));
    return segments;
  }

  @Override
  public void compact(SegmentManager manager) {
    // Select a list of segments to compact.
    List<List<Segment>> allSegments = selectSegments(manager.segments().stream().filter(Segment::isLocked).collect(Collectors.toList()));

    // We allow the compaction of even a single segment.
    if (!allSegments.isEmpty()) {
      // Iterate through each of the segments and compact them.
      logger().debug("Compacting {} segment(s)", allSegments.stream().mapToInt(List::size).sum());
      for (List<Segment> segments : allSegments) {
        sortSegments(segments);
        compactSegments(segments, manager);
      }
    } else {
      logger().debug("No segments to compact");
    }
  }

  /**
   * Compacts a set of segments.
   */
  private void compactSegments(List<Segment> segments, SegmentManager manager) {
    // In order to determine the segments to compact, we iterate through each segment and build a key table of segment keys.
    // If the total number of keys in two adjacent segments are less than the total number of entries allowed in a segment
    // then we can compact the segments together, otherwise the segment will be rewritten by itself.
    List<Segment> tempSegments = new ArrayList<>(segments.size());
    List<KeyTable> keyTables = new ArrayList<>(segments.size());
    for (Segment segment : segments) {

      // Because segments are not thread safe, we need to create a temporary segment with a new file descriptor in
      // order to read entries from the segment. There's no risk of a race condition here since we're only compacting
      // segments in which all entries have been committed.
      Segment temp = manager.loadSegment(segment.descriptor().id(), segment.descriptor().version());

      KeyTable keyTable = new KeyTable(temp.descriptor().entries());
      try (Buffer key = NativeBuffer.allocate(1024, temp.descriptor().maxKeySize())) {
        for (long i = temp.firstIndex(); i <= temp.lastIndex(); i++) {
          try (RaftEntry entry = temp.getEntry(i)) {
            if (entry != null) {
              RaftEntry.Mode mode = entry.readMode();
              if (mode == RaftEntry.Mode.PERSISTENT || (mode == RaftEntry.Mode.DURABLE && segment.recycleIndex() < i)) {
                entry.readKey(key);
                keyTable.update(key.flip(), (int) (i = temp.firstIndex()));
              }
            }
          }
        }
      }

      tempSegments.add(temp);
      keyTables.add(keyTable);

      if (keyTables.stream().mapToLong(KeyTable::size).sum() > manager.config().getEntriesPerSegment()) {
        compactSegments(tempSegments.subList(0, tempSegments.size() - 1), keyTables.subList(0, keyTables.size() - 1), manager);
        tempSegments = tempSegments.subList(tempSegments.size() - 1, tempSegments.size());
        keyTables = keyTables.subList(keyTables.size() - 1, keyTables.size());
      }
    }

    if (!tempSegments.isEmpty()) {
      compactSegments(tempSegments, keyTables, manager);
    } else {
      tempSegments.stream().forEach(Segment::close);
      keyTables.stream().forEach(KeyTable::close);
    }
  }

  /**
   * Compacts a set of segments together.
   */
  private void compactSegments(List<Segment> segments, List<KeyTable> keyTables, SegmentManager manager) {
    int cleanCount = 0;
    int transferCount = 0;
    Segment compactSegment = createCompactSegment(manager, segments, keyTables.stream().mapToInt(KeyTable::size).sum());
    for (int i = 0; i < segments.size(); i++) {
      Segment segment = segments.get(i);
      KeyTable keyTable = keyTables.get(i);
      try (Buffer key = NativeBuffer.allocate(1024, segment.descriptor().maxKeySize())) {
        for (long index = segment.firstIndex(); index <= segment.lastIndex(); index++) {
          try (RaftEntry entry = segment.getEntry(index)) {
            if (entry != null) {
              entry.readKey(key);
              int offset = (int) (index - segment.firstIndex());
              if (keyTable.lookup(key.flip()) == offset) {
                compactSegment.transferEntry(entry);
                transferCount++;
              } else {
                cleanCount++;
              }
            } else {
              cleanCount++;
            }
          }
        }
      }

      logger().debug("Transferred {} entries from: {}", transferCount, segment);
      logger().debug("Cleaned {} entries from: {}", cleanCount, segment);
    }

    logger().debug("Commit entries to: {}", compactSegment);
    compactSegment.commit(compactSegment.lastIndex());

    updateSegments(compactSegment, manager);
  }

  /**
   * Updates the current segments list with the new set of segments.
   */
  private void updateSegments(Segment compactSegment, SegmentManager manager) {
    // Once the segments have been compacted together we create a new segments list. This is preferred to modifying
    // the existing segments in place in order to prevent race conditions.
    List<Segment> newSegments = new ArrayList<>();
    newSegments.add(compactSegment);
    for (Segment segment : manager.segments()) {
      if (segment.descriptor().index() < compactSegment.descriptor().index()
        || segment.descriptor().index() >= compactSegment.descriptor().index() + compactSegment.descriptor().range()) {
        newSegments.add(segment);
      }
    }

    // Assign the new segments.
    manager.update(newSegments);
  }

  /**
   * Creates a single compact segment for the given ordered list of segments.
   */
  private Segment createCompactSegment(SegmentManager manager, List<Segment> segments, int entries) {
    // Sort compact segments in ascending order.
    sortSegments(segments);

    // Create a segment and calculate the updated version, number of entries, and resulting segment size.
    Segment firstSegment = segments.get(0);

    // Create a new compact segment. This segment will contain all the combined entries from all segments.
    long id = firstSegment.descriptor().id();
    long index = firstSegment.descriptor().index();
    long version = firstSegment.descriptor().version() + 1;
    int range = segments.stream().mapToInt(s -> s.descriptor().range()).sum();
    return manager.createSegment(id, index, version, entries, range);
  }

}
